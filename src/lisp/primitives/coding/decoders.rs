//! coding.c's decoders, each a call over ONE_MORE_BYTE's units that ends
//! when its charbuf is full or its source runs out, and decode_coding's
//! loop of those calls.

use super::units::{CHARBUF_SIZE, CallEnd, CallRuns, MAX_ANNOTATION_LENGTH, Units};
use super::*;

/// decode_coding_utf_8 over a whole unibyte text without a BOM or a dos
/// eol, what it leaves unfinished flushed as binary characters.
pub(crate) fn decode_utf_8_whole(bytes: &[u8]) -> DecodedText {
    let mut out = DecodedText::new();
    let (CallEnd::Full(at) | CallEnd::Ended(at)) = utf_8_call(
        bytes,
        0,
        false,
        false,
        &mut DecoderSpec::default(),
        &mut out,
    );
    bytes.flush(at, &mut out);
    out
}

/// UTF_8_EXTRA_OCTET_P.
#[inline]
fn extra_octet(c: i32) -> bool {
    c & 0xC0 == 0x80
}

/// BYTE8_TO_CHAR of the unit C, as an invalid_code stores it: a value that
/// comes out negative is stored as an annotation, which produce_chars
/// skips (false).
#[inline]
fn store_byte8(out: &mut DecodedText, c: i32) -> bool {
    let c = c + RAW_BYTE8_BASE as i32;
    if c < 0 {
        return false;
    }
    out.push_code(c as u32);
    true
}

/// decode_coding_utf_8, one call: with BOM (CODING_UTF_8_BOM not
/// utf_without_bom) a BOM is looked for first; then the standard
/// sequences, and Emacs's four-octet ones up to #x1FFFFF and five-octet
/// ones from #x200000 to MAX_CHAR.  An overlong sequence, a surrogate or
/// any other invalid octet is that octet's eight-bit character, decoding
/// resuming after it; a multibyte source's character comes through as
/// itself.  Under a dos eol a CR reads the unit after it as byte_after_cr,
/// the next character's first unit: one that leads an invalid sequence is
/// lost, invalid_code storing the unit after it (as ASCII or
/// BYTE8_TO_CHAR), and a character there is lost too.
pub(super) fn utf_8_call<S: Units + ?Sized>(
    src: &S,
    start: usize,
    bom: bool,
    eol_dos: bool,
    spec: &mut DecoderSpec,
    out: &mut DecodedText,
) -> CallEnd {
    let mut p = start;
    let mut src_base;
    // Without a dos eol neither a charbuf's end nor a CR changes what
    // the text decodes to.
    let mut room = if eol_dos { CHARBUF_SIZE } else { usize::MAX };
    let mut byte_after_cr: i32 = -1;
    'call: {
        if bom && !spec.bom_done {
            src_base = p;
            let c1 = one_more!(src, p, break 'call CallEnd::Ended(src_base));
            if c1 & 0xF0 != 0xE0 {
                p = src_base;
            } else {
                let c2 = one_more!(src, p, break 'call CallEnd::Ended(src_base));
                if !extra_octet(c2) {
                    p = src_base;
                } else {
                    let c3 = one_more!(src, p, break 'call CallEnd::Ended(src_base));
                    if !extra_octet(c3) || (c1, c2, c3) != (0xEF, 0xBB, 0xBF) {
                        p = src_base;
                    }
                }
            }
            spec.bom_done = true;
        }
        'decode: loop {
            src_base = p;
            if room == 0 {
                if byte_after_cr >= 0 {
                    src_base -= 1;
                }
                break 'call CallEnd::Full(src_base);
            }
            // The standard sequences of a unibyte source, a valid run at a
            // time: a CR ending the run waits for the slow path, where the
            // unit after it is read.
            if byte_after_cr < 0
                && let Some(bytes) = src.bytes()
            {
                let rest = &bytes[p..];
                let mut valid = match std::str::from_utf8(rest) {
                    Ok(_) => rest.len(),
                    Err(error) => error.valid_up_to(),
                };
                if eol_dos {
                    if valid > 0 && rest[valid - 1] == b'\r' {
                        valid -= 1;
                    }
                    let mut chars = 0;
                    for (index, &byte) in rest[..valid].iter().enumerate() {
                        if byte & 0xC0 != 0x80 {
                            if chars == room {
                                valid = index;
                                break;
                            }
                            chars += 1;
                        }
                    }
                    room -= chars;
                }
                if valid > 0 {
                    // SAFETY: from_utf8 validated these bytes.
                    out.push_str(unsafe { std::str::from_utf8_unchecked(&rest[..valid]) });
                    p += valid;
                    continue 'decode;
                }
            }
            let c1 = if byte_after_cr >= 0 {
                std::mem::replace(&mut byte_after_cr, -1)
            } else {
                one_more!(src, p, break 'call CallEnd::Ended(src_base))
            };
            'invalid: {
                if c1 < 0 {
                    out.push_code((-c1) as u32);
                    room -= 1;
                    continue 'decode;
                }
                if c1 < 0x80 {
                    if eol_dos && c1 == i32::from(b'\r') {
                        byte_after_cr = one_more!(src, p, break 'call CallEnd::Ended(src_base));
                    }
                    out.push_byte(c1 as u8);
                    room -= 1;
                    continue 'decode;
                }
                let c2 = one_more!(src, p, break 'call CallEnd::Ended(src_base));
                if c2 < 0 || !extra_octet(c2) {
                    break 'invalid;
                }
                let c = if c1 & 0xE0 == 0xC0 {
                    let c = ((c1 & 0x1F) << 6) | (c2 & 0x3F);
                    if c < 0x80 {
                        break 'invalid;
                    }
                    c
                } else {
                    let c3 = one_more!(src, p, break 'call CallEnd::Ended(src_base));
                    if c3 < 0 || !extra_octet(c3) {
                        break 'invalid;
                    }
                    if c1 & 0xF0 == 0xE0 {
                        let c = ((c1 & 0xF) << 12) | ((c2 & 0x3F) << 6) | (c3 & 0x3F);
                        if c < 0x800 || (0xD800..0xE000).contains(&c) {
                            break 'invalid;
                        }
                        c
                    } else {
                        let c4 = one_more!(src, p, break 'call CallEnd::Ended(src_base));
                        if c4 < 0 || !extra_octet(c4) {
                            break 'invalid;
                        }
                        if c1 & 0xF8 == 0xF0 {
                            let c = ((c1 & 0x7) << 18)
                                | ((c2 & 0x3F) << 12)
                                | ((c3 & 0x3F) << 6)
                                | (c4 & 0x3F);
                            if c < 0x1_0000 {
                                break 'invalid;
                            }
                            c
                        } else {
                            let c5 = one_more!(src, p, break 'call CallEnd::Ended(src_base));
                            if c5 < 0 || !extra_octet(c5) || c1 & 0xFC != 0xF8 {
                                break 'invalid;
                            }
                            let c = ((c1 & 0x3) << 24)
                                | ((c2 & 0x3F) << 18)
                                | ((c3 & 0x3F) << 12)
                                | ((c4 & 0x3F) << 6)
                                | (c5 & 0x3F);
                            if c > MAX_CHAR as i32 || c < 0x20_0000 {
                                break 'invalid;
                            }
                            c
                        }
                    }
                };
                out.push_code(c as u32);
                room -= 1;
                continue 'decode;
            }
            // invalid_code.
            p = src_base;
            let c = one_more!(src, p, break 'call CallEnd::Ended(src_base));
            if (0..0x80).contains(&c) {
                out.push_byte(c as u8);
            } else {
                store_byte8(out, c);
            }
            room -= 1;
        }
    }
}

/// decode_coding_charset, one call: each first unit selects, through the
/// `charset_valids' table, the charsets (sorted by dimension) whose
/// following units complete the code; a unit outside the table or a code
/// no charset decodes is invalid_code's, which stores the first unit (a
/// character as itself, an ASCII byte as itself, any other as its
/// eight-bit character).  A non-ASCII charset opens a `charset' run that
/// ASCII characters do not close.  Under a dos eol a CR reads the unit
/// after it, which becomes the next character's first unit.
pub(super) fn charset_call<S: Units + ?Sized>(
    interp: &Interpreter,
    src: &S,
    start: usize,
    valids: &[Option<Vec<std::rc::Rc<CharsetInfo>>>],
    eol_dos: bool,
    out: &mut DecodedText,
) -> CallEnd {
    let limit = CHARBUF_SIZE - MAX_ANNOTATION_LENGTH * 2;
    let mut used = 0;
    let mut runs = CallRuns::new();
    let mut p = start;
    let mut src_base;
    let mut byte_after_cr: i32 = -1;
    let end = 'call: loop {
        src_base = p;
        if used >= limit {
            if byte_after_cr >= 0 {
                src_base -= 1;
            }
            break 'call CallEnd::Full(src_base);
        }
        let c = if byte_after_cr >= 0 {
            std::mem::replace(&mut byte_after_cr, -1)
        } else {
            let c = one_more!(src, p, break 'call CallEnd::Ended(src_base));
            if eol_dos && c == i32::from(b'\r') {
                byte_after_cr = one_more!(src, p, break 'call CallEnd::Ended(src_base));
            }
            c
        };
        'invalid: {
            if c < 0 {
                break 'invalid;
            }
            let Some(charsets) = valids[c as usize].as_ref() else {
                break 'invalid;
            };
            let mut code = c as u32;
            let mut len = 1;
            let mut decoded = None;
            for charset in charsets {
                while len < charset.dimension as usize {
                    let c = one_more!(src, p, break 'call CallEnd::Ended(src_base));
                    code = (code << 8) | c as u32;
                    len += 1;
                }
                if let Some(ch) = charset.decode(interp, code) {
                    decoded = Some((charset, ch));
                    break;
                }
            }
            let Some((charset, ch)) = decoded else {
                break 'invalid;
            };
            if runs.opens(charset) {
                used += runs.open_info(charset, out);
            }
            out.push_code(ch);
            used += 1;
            continue 'call;
        }
        // invalid_code.
        p = src_base;
        let c = one_more!(src, p, break 'call CallEnd::Ended(src_base));
        if c < 0 {
            out.push_code((-c) as u32);
        } else {
            out.push_byte(c as u8);
        }
        used += 1;
    };
    runs.finish(out);
    end
}

/// The (roman kana kanji [kanji2]) charsets of a shift-jis coding.
pub(super) struct SjisCharsets {
    roman: std::rc::Rc<CharsetInfo>,
    kana: std::rc::Rc<CharsetInfo>,
    kanji: std::rc::Rc<CharsetInfo>,
    kanji2: Option<std::rc::Rc<CharsetInfo>>,
}

impl SjisCharsets {
    pub(super) fn new(interp: &Interpreter, coding: &str) -> Option<Self> {
        let names = coding_system_charset_names(interp, coding);
        let info = |index: usize, default: &str| {
            charset_info(interp, names.get(index).map_or(default, String::as_str))
        };
        Some(SjisCharsets {
            roman: info(0, "ascii")?,
            kana: info(1, "katakana-jisx0201")?,
            kanji: info(2, "japanese-jisx0208")?,
            kanji2: names.get(3).and_then(|name| charset_info(interp, name)),
        })
    }
}

/// decode_coding_sjis, one call: 0xA1..0xDF is halfwidth katakana,
/// 0x81..0x9F and 0xE0..0xEF lead a two-unit code (trail 0x40..0xFC except
/// 0x7F) decoded through SJIS_TO_JIS, 0xF0..0xFC one through SJIS_TO_JIS2
/// when a fourth charset exists.  A code its charset does not decode is
/// stored as -1, which produce_chars drops (the `charset' run counts it).
/// invalid_code stores the first unit as a character or as
/// BYTE8_TO_CHAR (an ASCII byte too).  Under a dos eol a CR reads the
/// unit after it, which becomes the next character's first unit.
pub(super) fn sjis_call<S: Units + ?Sized>(
    interp: &Interpreter,
    src: &S,
    start: usize,
    charsets: &SjisCharsets,
    eol_dos: bool,
    out: &mut DecodedText,
) -> CallEnd {
    let limit = CHARBUF_SIZE - MAX_ANNOTATION_LENGTH * 2;
    let mut used = 0;
    let mut runs = CallRuns::new();
    let mut p = start;
    let mut src_base;
    let mut byte_after_cr: i32 = -1;
    let end = 'call: loop {
        src_base = p;
        if used >= limit {
            if byte_after_cr >= 0 {
                src_base -= 1;
            }
            break 'call CallEnd::Full(src_base);
        }
        let c = if byte_after_cr >= 0 {
            std::mem::replace(&mut byte_after_cr, -1)
        } else {
            one_more!(src, p, break 'call CallEnd::Ended(src_base))
        };
        'invalid: {
            if c < 0 {
                break 'invalid;
            }
            let (charset, code) = if c < 0x80 {
                if eol_dos && c == i32::from(b'\r') {
                    byte_after_cr = one_more!(src, p, break 'call CallEnd::Ended(src_base));
                }
                (&charsets.roman, c as u32)
            } else if c == 0x80 || c == 0xA0 {
                break 'invalid;
            } else if (0xA1..=0xDF).contains(&c) {
                (&charsets.kana, (c & 0x7F) as u32)
            } else if c <= 0xEF || (c <= 0xFC && charsets.kanji2.is_some()) {
                let c1 = one_more!(src, p, break 'call CallEnd::Ended(src_base));
                if !(0x40..=0xFC).contains(&c1) || c1 == 0x7F {
                    break 'invalid;
                }
                let code = ((c << 8) | c1) as u32;
                match &charsets.kanji2 {
                    Some(kanji2) if c > 0xEF => (kanji2, sjis_to_jis2(code)),
                    _ => (&charsets.kanji, sjis_to_jis(code)),
                }
            } else {
                break 'invalid;
            };
            if runs.opens(charset) {
                used += runs.open_info(charset, out);
            }
            match charset.decode(interp, code) {
                Some(ch) => out.push_code(ch),
                None => runs.drop_char(),
            }
            used += 1;
            continue 'call;
        }
        // invalid_code.
        p = src_base;
        let c = one_more!(src, p, break 'call CallEnd::Ended(src_base));
        if c < 0 {
            out.push_code((-c) as u32);
        } else {
            store_byte8(out, c);
        }
        used += 1;
    };
    runs.finish(out);
    end
}

/// decode_coding_big5, one call: a lead 0xA1..0xFE with a trail
/// 0x40..0x7E or 0xA1..0xFE decodes through the second charset, ASCII
/// through the first; anything else is invalid_code's, as in
/// decode_coding_sjis.
pub(super) fn big5_call<S: Units + ?Sized>(
    interp: &Interpreter,
    src: &S,
    start: usize,
    roman: &CharsetInfo,
    big5: &CharsetInfo,
    eol_dos: bool,
    out: &mut DecodedText,
) -> CallEnd {
    let limit = CHARBUF_SIZE - MAX_ANNOTATION_LENGTH * 2;
    let mut used = 0;
    let mut runs = CallRuns::new();
    let mut p = start;
    let mut src_base;
    let mut byte_after_cr: i32 = -1;
    let end = 'call: loop {
        src_base = p;
        if used >= limit {
            if byte_after_cr >= 0 {
                src_base -= 1;
            }
            break 'call CallEnd::Full(src_base);
        }
        let c = if byte_after_cr >= 0 {
            std::mem::replace(&mut byte_after_cr, -1)
        } else {
            one_more!(src, p, break 'call CallEnd::Ended(src_base))
        };
        'invalid: {
            if c < 0 {
                break 'invalid;
            }
            let (charset, code) = if c < 0x80 {
                if eol_dos && c == i32::from(b'\r') {
                    byte_after_cr = one_more!(src, p, break 'call CallEnd::Ended(src_base));
                }
                (roman, c as u32)
            } else {
                if !(0xA1..=0xFE).contains(&c) {
                    break 'invalid;
                }
                let c1 = one_more!(src, p, break 'call CallEnd::Ended(src_base));
                if c1 < 0x40 || (c1 > 0x7E && c1 < 0xA1) || c1 > 0xFE {
                    break 'invalid;
                }
                (big5, ((c << 8) | c1) as u32)
            };
            if runs.opens(charset) {
                used += runs.open_info(charset, out);
            }
            match charset.decode(interp, code) {
                Some(ch) => out.push_code(ch),
                None => runs.drop_char(),
            }
            used += 1;
            continue 'call;
        }
        // invalid_code.
        p = src_base;
        let c = one_more!(src, p, break 'call CallEnd::Ended(src_base));
        if c < 0 {
            out.push_code((-c) as u32);
        } else {
            store_byte8(out, c);
        }
        used += 1;
    };
    runs.finish(out);
    end
}

/// decode_coding_utf_16, one call, once detect_coding has settled the
/// byte order: with CHECK_BOM (CODING_UTF_16_BOM is utf_with_bom) the
/// first call skips a BOM that matches its order; a surrogate pair is its
/// character; a lone low surrogate is its own character; a high surrogate
/// before anything but a low one comes out as its two bytes (as
/// characters 0..255), and, as CODING_UTF_16_SURROGATE is cleared only by
/// the next high or low surrogate, so does every unit after it until
/// then.  A multibyte source's character comes through as itself (after
/// the unit before it, when it is a pair's second).  Under a dos eol a CR
/// reads the two units after it, the next pair's (a character among them
/// is lost).
pub(super) fn utf_16_call<S: Units + ?Sized>(
    src: &S,
    start: usize,
    big_endian: bool,
    check_bom: bool,
    eol_dos: bool,
    spec: &mut DecoderSpec,
    out: &mut DecodedText,
) -> CallEnd {
    let limit = CHARBUF_SIZE - 2;
    let mut used = 0;
    let mut p = start;
    let mut src_base;
    let (mut byte_after_cr1, mut byte_after_cr2): (i32, i32) = (-1, -1);
    let is_high = |c: i32| (0xD800..0xDC00).contains(&c);
    let is_low = |c: i32| (0xDC00..0xE000).contains(&c);
    'call: {
        if !spec.bom_done {
            if check_bom {
                src_base = p;
                let c1 = one_more!(src, p, break 'call CallEnd::Ended(src_base));
                let c2 = one_more!(src, p, break 'call CallEnd::Ended(src_base));
                let c = (c1 << 8) | c2;
                if c != if big_endian { 0xFEFF } else { 0xFFFE } {
                    p = src_base;
                }
            }
            spec.bom_done = true;
        }
        'decode: loop {
            src_base = p;
            if used >= limit {
                if byte_after_cr1 >= 0 {
                    src_base -= 2;
                }
                break 'call CallEnd::Full(src_base);
            }
            let c1 = if byte_after_cr1 >= 0 {
                std::mem::replace(&mut byte_after_cr1, -1)
            } else {
                one_more!(src, p, break 'call CallEnd::Ended(src_base))
            };
            if c1 < 0 {
                out.push_code((-c1) as u32);
                used += 1;
                continue 'decode;
            }
            let c2 = if byte_after_cr2 >= 0 {
                std::mem::replace(&mut byte_after_cr2, -1)
            } else {
                one_more!(src, p, break 'call CallEnd::Ended(src_base))
            };
            if c2 < 0 {
                out.push_byte(c1 as u8);
                out.push_code((-c2) as u32);
                used += 2;
                continue 'decode;
            }
            let c = if big_endian {
                (c1 << 8) | c2
            } else {
                (c2 << 8) | c1
            };
            let surrogate = spec.surrogate as i32;
            if surrogate != 0 {
                if !is_low(c) {
                    let (first, second) = if big_endian {
                        (surrogate >> 8, surrogate & 0xFF)
                    } else {
                        (surrogate & 0xFF, surrogate >> 8)
                    };
                    out.push_code(first as u32);
                    out.push_code(second as u32);
                    used += 2;
                    if is_high(c) {
                        spec.surrogate = c as u32;
                    } else {
                        // CODING_UTF_16_SURROGATE stays as it was.
                        out.push_code(c as u32);
                        used += 1;
                    }
                } else {
                    out.push_code(
                        0x1_0000 + ((((surrogate - 0xD800) << 10) | (c - 0xDC00)) as u32),
                    );
                    spec.surrogate = 0;
                    used += 1;
                }
            } else if is_high(c) {
                spec.surrogate = c as u32;
            } else {
                if eol_dos && c == i32::from(b'\r') {
                    byte_after_cr1 = one_more!(src, p, break 'call CallEnd::Ended(src_base));
                    byte_after_cr2 = one_more!(src, p, break 'call CallEnd::Ended(src_base));
                }
                out.push_code(c as u32);
                used += 1;
            }
        }
    }
}

/// emacs_mule_char: the character whose units start at SRC_BASE (where
/// the decoder's loop began this character, past a unit it read after a
/// CR), with its charset and the index after it; Err (true) for one
/// emacs_mule_char finds invalid, Err (false) for one the source ends
/// inside (no_more_source).  A multibyte source's character is itself,
/// of emacs_mule_charset[0] (ascii).
fn emacs_mule_char<S: Units + ?Sized>(
    interp: &Interpreter,
    src: &S,
    src_base: usize,
    layout: &EmacsMuleLayout,
) -> Result<(u32, String, usize), bool> {
    let mut p = src_base;
    let lead = one_more!(src, p, return Err(false));
    if lead < 0 {
        return Ok(((-lead) as u32, "ascii".to_string(), p));
    }
    let charset_of = |c: i32| -> Option<&String> {
        usize::try_from(c)
            .ok()
            .and_then(|index| layout.charsets.get(index))
            .and_then(Option::as_ref)
    };
    // Composition sequences (0xA0 and above inside one) are not decoded.
    if lead >= 0xA0 {
        return Err(true);
    }
    let (charset, code) = match layout.lengths[lead as usize] {
        2 => {
            let charset = charset_of(lead).ok_or(true)?;
            let c = one_more!(src, p, return Err(false));
            if c < 0xA0 {
                return Err(true);
            }
            (charset.clone(), (c & 0x7F) as u32)
        }
        3 if lead == 0x9A || lead == 0x9B => {
            let c = one_more!(src, p, return Err(false));
            let charset = (c >= 0xA0).then(|| charset_of(c)).flatten().ok_or(true)?;
            let c = one_more!(src, p, return Err(false));
            if c < 0xA0 {
                return Err(true);
            }
            (charset.clone(), (c & 0x7F) as u32)
        }
        3 => {
            let charset = charset_of(lead).ok_or(true)?;
            let c = one_more!(src, p, return Err(false));
            if c < 0xA0 {
                return Err(true);
            }
            let high = ((c & 0x7F) << 8) as u32;
            let c = one_more!(src, p, return Err(false));
            if c < 0xA0 {
                return Err(true);
            }
            (charset.clone(), high | (c & 0x7F) as u32)
        }
        4 => {
            let c = one_more!(src, p, return Err(false));
            let charset = charset_of(c).ok_or(true)?;
            let c = one_more!(src, p, return Err(false));
            if c < 0xA0 {
                return Err(true);
            }
            let high = ((c & 0x7F) << 8) as u32;
            let c = one_more!(src, p, return Err(false));
            if c < 0xA0 {
                return Err(true);
            }
            (charset.clone(), high | (c & 0x7F) as u32)
        }
        _ => {
            let charset = if lead < 0x80 { "ascii" } else { "eight-bit" };
            (charset.to_string(), lead as u32)
        }
    };
    let ch = decode_charset_code(interp, &charset, code).ok_or(true)?;
    Ok((ch, charset, p))
}

/// decode_coding_emacs_mule, one call: an ASCII unit is itself, a
/// multibyte sequence emacs_mule_char's character, a multibyte source's
/// character itself; any other unit is invalid_code's (as ASCII or as its
/// eight-bit character).  Every character's charset is compared with the
/// open run's, so an ASCII one ends it.  The composition sequences 0x80
/// starts are taken as invalid codes.  Under a dos eol a CR reads the
/// unit after it, which becomes the next character's first unit.
pub(super) fn emacs_mule_call<S: Units + ?Sized>(
    interp: &Interpreter,
    src: &S,
    start: usize,
    layout: &EmacsMuleLayout,
    eol_dos: bool,
    out: &mut DecodedText,
) -> CallEnd {
    let limit = CHARBUF_SIZE - MAX_ANNOTATION_LENGTH * 3 - 1;
    let mut used = 0;
    let mut runs = CallRuns::new();
    let mut p = start;
    let mut src_base;
    let mut byte_after_cr: i32 = -1;
    let end = 'call: loop {
        src_base = p;
        if used >= limit {
            if byte_after_cr >= 0 {
                src_base -= 1;
            }
            break 'call CallEnd::Full(src_base);
        }
        let c = if byte_after_cr >= 0 {
            std::mem::replace(&mut byte_after_cr, -1)
        } else {
            one_more!(src, p, break 'call CallEnd::Ended(src_base))
        };
        if c < 0 {
            out.push_code((-c) as u32);
            used += 1;
            continue 'call;
        }
        'invalid: {
            if c == 0x80 {
                break 'invalid;
            }
            let (ch, charset) = if c < 0x80 {
                if eol_dos && c == i32::from(b'\r') {
                    byte_after_cr = one_more!(src, p, break 'call CallEnd::Ended(src_base));
                }
                (c as u32, None)
            } else {
                match emacs_mule_char(interp, src, src_base, layout) {
                    Ok((ch, charset, after)) => {
                        p = after;
                        (ch, Some(charset))
                    }
                    Err(true) => break 'invalid,
                    Err(false) => break 'call CallEnd::Ended(src_base),
                }
            };
            match charset.as_deref() {
                Some(charset) if charset != "ascii" => {
                    if !runs.is(charset) {
                        used += runs.open(charset, out);
                    }
                }
                _ => used += runs.close(out),
            }
            out.push_code(ch);
            used += 1;
            continue 'call;
        }
        // invalid_code.
        p = src_base;
        let c = one_more!(src, p, break 'call CallEnd::Ended(src_base));
        if (0..0x80).contains(&c) {
            out.push_byte(c as u8);
        } else {
            store_byte8(out, c);
        }
        used += 1;
    };
    runs.finish(out);
    end
}

/// decode_coding_raw_text: the source as it is (a unibyte source's bytes
/// as characters, a multibyte one's characters), all of it consumed but a
/// CR at the end under a dos eol.
pub(super) fn raw_text_call<S: Units + ?Sized>(
    src: &S,
    start: usize,
    eol_dos: bool,
    out: &mut DecodedText,
) -> CallEnd {
    let mut end = src.len();
    if eol_dos && end > start && src.at(end - 1) == i32::from(b'\r') {
        end -= 1;
    }
    match src.bytes() {
        Some(bytes) => {
            out.push_raw_bytes(&bytes[start..end]);
        }
        None => {
            for index in start..end {
                let unit = src.at(index);
                if unit < 0 {
                    out.push_code((-unit) as u32);
                } else {
                    out.push_byte(unit as u8);
                }
            }
        }
    }
    CallEnd::Ended(end)
}

/// A coding system's decoder, set up once for decode_coding's calls.
enum Decoder {
    Utf8 { bom: bool },
    Utf16 { big_endian: bool, check_bom: bool },
    Charset(Vec<Option<Vec<std::rc::Rc<CharsetInfo>>>>),
    Sjis(SjisCharsets),
    Big5(std::rc::Rc<CharsetInfo>, std::rc::Rc<CharsetInfo>),
    EmacsMule(Box<EmacsMuleLayout>),
    Iso2022,
    RawText,
}

impl Decoder {
    fn new(interp: &Interpreter, canonical: &str) -> Self {
        let kind = interp
            .coding_system_kind_name(canonical)
            .unwrap_or_else(|| canonical.to_string());
        // decode_coding_charset covers every charset-type coding system, the
        // single-byte ones (koi8-r, windows-125x) included: their `charset'
        // property is the coding's charset, as the oracle reports for
        // `(decode-coding-string "a\301" 'koi8-r)'.
        match kind.as_str() {
            "iso-2022" => Decoder::Iso2022,
            "shift-jis" => {
                SjisCharsets::new(interp, canonical).map_or(Decoder::RawText, Decoder::Sjis)
            }
            "big5" => {
                let names = coding_system_charset_names(interp, canonical);
                let roman = charset_info(interp, names.first().map_or("ascii", String::as_str));
                let big5 = charset_info(interp, names.get(1).map_or("big5", String::as_str));
                match (roman, big5) {
                    (Some(roman), Some(big5)) => Decoder::Big5(roman, big5),
                    _ => Decoder::RawText,
                }
            }
            "emacs-mule" => Decoder::EmacsMule(Box::new(emacs_mule_layout(interp))),
            "charset" => Decoder::Charset(charset_valids(interp, canonical)),
            "utf-8" | "prefer-utf-8" | "utf-8-auto" => Decoder::Utf8 {
                bom: coding_system_consumes_bom(interp, canonical),
            },
            "utf-8-with-signature" => Decoder::Utf8 { bom: true },
            "utf-16" => {
                // An auto-BOM coding reaching the decoder is one detect_coding
                // could not settle (utf_detect_bom): it decodes without a BOM in
                // its own byte order.
                let (big_endian, with_bom, detect_bom) =
                    coding_system_utf16_options(interp, canonical, &kind);
                Decoder::Utf16 {
                    big_endian,
                    check_bom: with_bom && !detect_bom,
                }
            }
            _ => Decoder::RawText,
        }
    }
}

/// decode_coding's loop over SRC with the coding system CANONICAL: the
/// decoder is called again from where its charbuf filled until its source
/// runs out; then the last block flushes what it left unconsumed as
/// binary characters, where another leaves it for the next block
/// (BLOCK's CONSUMED).
pub(super) fn decode_units<S: Units + ?Sized>(
    interp: &Interpreter,
    src: &S,
    canonical: &str,
    block: &mut DecodeBlock,
) -> DecodedText {
    let decoder = Decoder::new(interp, canonical);
    let mut out = DecodedText::new();
    let eol_dos = block.eol_dos;
    let mut consumed = 0;
    loop {
        let end = match &decoder {
            Decoder::Utf8 { bom } => utf_8_call(src, consumed, *bom, eol_dos, block.spec, &mut out),
            Decoder::Utf16 {
                big_endian,
                check_bom,
            } => utf_16_call(
                src,
                consumed,
                *big_endian,
                *check_bom,
                eol_dos,
                block.spec,
                &mut out,
            ),
            Decoder::Charset(valids) => {
                charset_call(interp, src, consumed, valids, eol_dos, &mut out)
            }
            Decoder::Sjis(charsets) => {
                sjis_call(interp, src, consumed, charsets, eol_dos, &mut out)
            }
            Decoder::Big5(roman, big5) => {
                big5_call(interp, src, consumed, roman, big5, eol_dos, &mut out)
            }
            Decoder::EmacsMule(layout) => {
                emacs_mule_call(interp, src, consumed, layout, eol_dos, &mut out)
            }
            Decoder::Iso2022 => iso2022::decode_call(
                interp, src, consumed, canonical, eol_dos, block.last, block.spec, &mut out,
            ),
            Decoder::RawText => raw_text_call(src, consumed, eol_dos, &mut out),
        };
        match end {
            CallEnd::Full(at) if at < src.len() => consumed = at,
            CallEnd::Full(at) | CallEnd::Ended(at) => {
                consumed = at;
                break;
            }
        }
    }
    if block.last {
        src.flush(consumed, &mut out);
        block.consumed = src.len();
    } else {
        block.consumed = consumed;
    }
    out
}
