//! coding.c's decoders, each a call over ONE_MORE_BYTE's units that ends
//! when its charbuf is full or its source runs out, and decode_coding's
//! loop of those calls.

use super::units::{CallEnd, CallRuns, MAX_ANNOTATION_LENGTH, Units, charbuf_size};
use super::*;

/// decode_coding_utf_8 over a whole unibyte text without a BOM or a dos
/// eol, what it leaves unfinished flushed as binary characters.
pub(crate) fn decode_utf_8_whole(bytes: &[u8]) -> DecodedText {
    let mut out = DecodedText::with_capacity(bytes.len());
    let mut spec = DecoderSpec::default();
    let mut consumed = 0;
    loop {
        match utf_8_call(bytes, consumed, false, false, &mut spec, &mut out) {
            CallEnd::Full(at) if at < bytes.len() => consumed = at,
            CallEnd::Again(at) => consumed = at,
            CallEnd::Full(at) | CallEnd::Ended(at) => {
                consumed = at;
                break;
            }
        }
    }
    bytes.flush(consumed, &mut out);
    out
}

/// decode_coding_gap's insert_from_gap of BYTES that detect_coding_utf_8
/// accepted to their end: each sequence is the character
/// string_char_and_length reads from it, an overlong sequence or a
/// surrogate as its code (C0 and C1 leading the internal form of an
/// eight-bit character) and a four-octet one up to #x1FFFFF.
pub(crate) fn decode_detected_utf_8(bytes: &[u8]) -> DecodedText {
    let mut out = DecodedText::with_capacity(bytes.len());
    let mut p = 0;
    while p < bytes.len() {
        let rest = &bytes[p..];
        let valid = match std::str::from_utf8(rest) {
            Ok(_) => rest.len(),
            Err(error) => error.valid_up_to(),
        };
        if valid > 0 {
            // SAFETY: from_utf8 validated these bytes.
            out.push_str(unsafe { std::str::from_utf8_unchecked(&rest[..valid]) });
            p += valid;
            continue;
        }
        let tail = |n: usize| u32::from(rest[n] & 0x3F);
        let c = u32::from(rest[0]);
        let (code, len) = if c & 0x20 == 0 {
            let d = ((c & 0x1F) << 6) | tail(1);
            (if c < 0xC2 { d + 0x3F_FF80 } else { d }, 2)
        } else if c & 0x10 == 0 {
            (((c & 0x0F) << 12) | (tail(1) << 6) | tail(2), 3)
        } else {
            (
                ((c & 0x07) << 18) | (tail(1) << 12) | (tail(2) << 6) | tail(3),
                4,
            )
        };
        out.push_code(code);
        p += len;
    }
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
    let mut room = charbuf_size(src);
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
                // ROOM bytes hold no more characters than the charbuf has
                // room for, and each byte is looked at once: a run cut
                // inside a character goes on from it.
                let rest = &bytes[p..];
                let window = &rest[..rest.len().min(room)];
                let mut valid = match std::str::from_utf8(window) {
                    Ok(_) => window.len(),
                    Err(error) => error.valid_up_to(),
                };
                // A CR the run ends with reads the unit after it as
                // byte_after_cr, so the CRs it ends with wait for the slow
                // path: of CRs that end the source, the last is read as the
                // one before's byte_after_cr and its own finds no more
                // source, so neither stores it nor leaves it to the flush.
                while eol_dos && valid > 0 && rest[valid - 1] == b'\r' {
                    valid -= 1;
                }
                room -= window[..valid]
                    .iter()
                    .filter(|&&byte| byte & 0xC0 != 0x80)
                    .count();
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
    charsets: &mut CharsetDecode,
    eol_dos: bool,
    out: &mut DecodedText,
) -> CallEnd {
    let CharsetDecode { valids, single } = charsets;
    let limit = charbuf_size(src) - MAX_ANNOTATION_LENGTH * 2;
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
            let byte = c as usize;
            let Some(list) = valids[byte].as_ref() else {
                break 'invalid;
            };
            let (charset, ch) = match single[byte] {
                Single::Decoded(index, ch) => (&list[index], ch),
                Single::Invalid => break 'invalid,
                Single::Unknown => {
                    let mut code = c as u32;
                    let mut len = 1;
                    let mut decoded = None;
                    for (index, charset) in list.iter().enumerate() {
                        while len < charset.dimension as usize {
                            let c = one_more!(src, p, break 'call CallEnd::Ended(src_base));
                            code = (code << 8) | c as u32;
                            len += 1;
                        }
                        if let Some(ch) = charset.decode(interp, code) {
                            decoded = Some((index, ch));
                            break;
                        }
                    }
                    // What the unit decodes to without another unit read
                    // stays so for the decode.
                    if len == 1 {
                        single[byte] = match decoded {
                            Some((index, ch)) => Single::Decoded(index, ch),
                            None => Single::Invalid,
                        };
                    }
                    let Some((index, ch)) = decoded else {
                        break 'invalid;
                    };
                    (&list[index], ch)
                }
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

/// A charset coding's charsets by first unit (CODING_CHARSET_VALIDS, each
/// list by dimension), and what each first unit has decoded to on its
/// own so far in the decode.
pub(super) struct CharsetDecode {
    valids: super::CharsetValids,
    single: [Single; 256],
}

impl CharsetDecode {
    fn new(interp: &Interpreter, canonical: &str) -> Self {
        CharsetDecode {
            valids: charset_valids(interp, canonical),
            single: [Single::Unknown; 256],
        }
    }
}

/// A first unit's decoding by its one-dimensional charsets: the index of
/// the charset that decoded it and the character, none of them, or not
/// yet known (or needing another unit).
#[derive(Clone, Copy)]
enum Single {
    Unknown,
    Decoded(usize, u32),
    Invalid,
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
        let info = |index: usize, default: &str| match names.get(index) {
            Some(name) => charset_info(interp, name),
            None => charset_info(interp, default),
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
    let limit = charbuf_size(src) - MAX_ANNOTATION_LENGTH * 2;
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
    let limit = charbuf_size(src) - MAX_ANNOTATION_LENGTH * 2;
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
    let limit = charbuf_size(src) - 2;
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

/// composite.h's composition methods and coding.h's composition_state.
const COMPOSITION_RELATIVE: i32 = 0;
const COMPOSITION_WITH_RULE: i32 = 1;
const COMPOSITION_WITH_RULE_ALTCHARS: i32 = 3;
const COMPOSING_NO: u8 = 0;
const COMPOSING_CHAR: u8 = 1;
const COMPOSING_RULE: u8 = 2;
const COMPOSING_COMPONENT_CHAR: u8 = 3;
const COMPOSING_COMPONENT_RULE: u8 = 4;
/// composite.h MAX_COMPOSITION_COMPONENTS.
const MAX_COMPOSITION_COMPONENTS: i32 = 16;
/// coding.h CODING_ANNOTATE_COMPOSITION_MASK.
const ANNOTATE_COMPOSITION_MASK: i32 = 1;

/// coding->spec.emacs_mule.cmp_status: the composition being decoded,
/// SEGMENT its charbuf ints from the annotation header on (cmp_status's
/// LENGTH is their count), which the decoder rewrites until the
/// composition ends and the carryover a block leaves the next.
#[derive(Default)]
pub(crate) struct EmacsMuleComposing {
    state: u8,
    method: i32,
    old_form: bool,
    nchars: i32,
    ncomps: i32,
    segment: Vec<i32>,
}

impl EmacsMuleComposing {
    /// ADD_COMPOSITION_DATA's header, the composition's state set.
    fn start(&mut self, old_form: bool, method: i32, state: u8, nchars: i32, nbytes: i32) {
        self.old_form = old_form;
        self.method = method;
        self.state = state;
        self.segment.clear();
        self.segment.extend([
            -(MAX_ANNOTATION_LENGTH as i32),
            ANNOTATE_COMPOSITION_MASK,
            nchars,
            nbytes,
            method,
        ]);
    }

    /// EMACS_MULE_COMPOSITION_END: the header's NCHARS (the old form) or
    /// length (a new form one with components) made final.
    fn end(&mut self, out: &mut DecodedText, runs: &mut CallRuns) {
        if self.old_form {
            self.segment[2] = self.nchars;
        } else if self.method > COMPOSITION_RELATIVE {
            self.segment[0] = self.segment[2] - self.segment.len() as i32;
        }
        self.state = COMPOSING_NO;
        self.produce(true, 0, out, runs);
    }

    /// EMACS_MULE_MAYBE_FINISH_COMPOSITION, emacs_mule_finish_composition:
    /// an old form one with characters ends there (an invalid last rule
    /// becoming its byte's character), another's header becomes the bytes
    /// that started it; the characters this adds to char_offset.
    fn maybe_finish(&mut self, out: &mut DecodedText, runs: &mut CallRuns) {
        if self.state == COMPOSING_NO {
            return;
        }
        let segment = &mut self.segment;
        let len = segment.len();
        let (annotated, new_chars) = if self.old_form && self.nchars > 0 {
            segment[2] = self.nchars;
            if self.method == COMPOSITION_WITH_RULE && self.state == COMPOSING_CHAR {
                // The last rule was invalid.
                segment[len - 2] = byte8_char(segment[len - 1] + 0xA0);
                segment[len - 1] = -1;
                (true, 1)
            } else {
                (true, 0)
            }
        } else {
            segment[0] = byte8_char(0x80);
            if self.method == COMPOSITION_WITH_RULE {
                segment[1] = byte8_char(0xFF);
                segment[2] = -3;
                segment[3] = 0;
                (false, 1)
            } else {
                let (nchars, nbytes) = (segment[2] + 0xA0, segment[3] + 0xA0);
                segment[1] = byte8_char(0xF2 + self.method);
                segment[2] = byte8_char(nbytes);
                segment[3] = byte8_char(nchars);
                segment[4] = -1;
                (false, 4)
            }
        };
        self.state = COMPOSING_NO;
        self.produce(annotated, new_chars, out, runs);
    }

    /// The segment once the composition ended: produce_chars' characters
    /// of it (an annotation skipping its length), char_offset counting
    /// NEW_CHARS of them, and when ANNOTATED produce_composition's record
    /// of the header, from the characters the segment starts.
    fn produce(
        &mut self,
        annotated: bool,
        new_chars: usize,
        out: &mut DecodedText,
        runs: &mut CallRuns,
    ) {
        let segment = std::mem::take(&mut self.segment);
        let pos = out.char_count();
        if annotated {
            let nchars = segment[2].max(0) as usize;
            let method = segment[4];
            let components = if method == COMPOSITION_RELATIVE {
                CompositionComponents::Relative
            } else {
                let len = if method == COMPOSITION_WITH_RULE {
                    segment[2] * 3 - 2
                } else {
                    -segment[0] - MAX_ANNOTATION_LENGTH as i32
                };
                // GNU reads on past the annotation when a new form
                // WITH_RULE header claims more than it holds.
                let entries = &segment[MAX_ANNOTATION_LENGTH..];
                let len = (len.max(0) as usize).min(entries.len());
                let (mut i, mut items, mut all_chars) = (0, Vec::new(), true);
                while i < len && entries[i] != -1 {
                    if entries[i] >= 0 {
                        items.push(entries[i] as u32);
                    } else {
                        i += 1;
                        all_chars = false;
                        items.push(entries.get(i).map_or(0, |&rule| rule % 0x100) as u32);
                    }
                    i += 1;
                }
                if all_chars {
                    CompositionComponents::Chars(items)
                } else {
                    CompositionComponents::Vector(items)
                }
            };
            out.compositions.push(DecodedComposition {
                seq: out.charsets.len(),
                start: pos,
                end: pos + nchars,
                components,
            });
        }
        let mut i = 0;
        while i < segment.len() {
            let c = segment[i];
            if c >= 0 {
                out.push_code(c as u32);
                i += 1;
            } else {
                i += (-c) as usize;
            }
        }
        runs.uncount(out.char_count() - pos - new_chars);
        self.segment = segment;
        self.segment.clear();
    }
}

/// BYTE8_TO_CHAR (B) as produce_chars stores it: a value past MAX_CHAR
/// (B above #xFF) has the CHAR_ALT bit, which char_string drops, so it is
/// the ASCII character (B & #xFF) in a multibyte destination and the byte
/// CHAR_TO_BYTE8 makes of it in a unibyte one alike.
fn byte8_char(b: i32) -> i32 {
    if b > 0xFF {
        b & 0xFF
    } else {
        b + RAW_BYTE8_BASE as i32
    }
}

/// emacs_mule_char: the character whose units start at SRC_BASE (where
/// the decoder's loop began this character, past a unit it read after a
/// CR), with its charset, the index after it and the units it took; Err
/// (true) for one emacs_mule_char finds invalid (-1), Err (false) for
/// one the source ends inside (no_more_source).  A multibyte source's
/// character is itself, of emacs_mule_charset[0] (ascii).  Inside an old
/// form composition a unit from #xA0 is a component: in COMPOSING_CHAR the
/// character of its leading code less #x20 (#xA0 an ASCII one, from the
/// unit after it less #x80), negated, and in COMPOSING_RULE the unit
/// itself negated, of no charset.
fn emacs_mule_char<S: Units + ?Sized>(
    interp: &Interpreter,
    src: &S,
    src_base: usize,
    layout: &EmacsMuleLayout,
    cmp: &EmacsMuleComposing,
) -> Result<(i32, Option<SymbolName>, usize), bool> {
    let mut p = src_base;
    let mut lead = one_more!(src, p, return Err(false));
    if lead < 0 {
        return Ok((-lead, Some(cached_symbol!("ascii").symbol()), p));
    }
    let charset_of = |c: i32| -> Option<&SymbolName> {
        usize::try_from(c)
            .ok()
            .and_then(|index| layout.charsets.get(index))
            .and_then(Option::as_ref)
    };
    let mut mseq_found = false;
    if lead >= 0xA0 {
        if cmp.state == COMPOSING_NO || !cmp.old_form {
            return Err(true);
        }
        if cmp.state != COMPOSING_CHAR {
            return Ok((-lead, None, p));
        }
        if lead == 0xA0 {
            lead = one_more!(src, p, return Err(false)) - 0x80;
            if lead < 0 {
                return Err(true);
            }
        } else {
            lead -= 0x20;
        }
        mseq_found = true;
    }
    let (charset, code) = match layout.lengths[lead as usize] {
        2 => {
            let charset = charset_of(lead).ok_or(true)?;
            let c = one_more!(src, p, return Err(false));
            if c < 0xA0 {
                return Err(true);
            }
            (*charset, (c & 0x7F) as u32)
        }
        3 if lead == 0x9A || lead == 0x9B => {
            let c = one_more!(src, p, return Err(false));
            let charset = (c >= 0xA0).then(|| charset_of(c)).flatten().ok_or(true)?;
            let c = one_more!(src, p, return Err(false));
            if c < 0xA0 {
                return Err(true);
            }
            (*charset, (c & 0x7F) as u32)
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
            (*charset, high | (c & 0x7F) as u32)
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
            (*charset, high | (c & 0x7F) as u32)
        }
        _ => {
            let charset = if lead < 0x80 {
                cached_symbol!("ascii")
            } else {
                cached_symbol!("eight-bit")
            };
            (charset.symbol(), lead as u32)
        }
    };
    let ch = decode_charset_code(interp, &charset, code).ok_or(true)? as i32;
    Ok((if mseq_found { -ch } else { ch }, Some(charset), p))
}

/// decode_coding_emacs_mule, one call: an ASCII unit is itself, a
/// multibyte sequence emacs_mule_char's character, a multibyte source's
/// character itself; any other unit is invalid_code's (as ASCII or as its
/// eight-bit character).  Every character's charset is compared with the
/// open run's, so an ASCII one ends it.  Under a dos eol a CR reads the
/// unit after it, which becomes the next character's first unit.  0x80
/// starts a composition: Emacs 21's 0x80 0xF2+METHOD BYTES CHARS header
/// and its components and characters, or Emacs 20's relative (0x80 then
/// components from 0xA0) and rule-based (0x80 0xFF) ones; its characters
/// are produced when it ends, uncounted by char_offset and with no charset
/// run, a header that ends early its original bytes.  A composition the
/// source or the charbuf leaves unfinished is finished in the last block
/// and carried to the next call in another (SPEC), unless that call's
/// charbuf has no room after it.
#[allow(clippy::too_many_arguments)]
pub(super) fn emacs_mule_call<S: Units + ?Sized>(
    interp: &Interpreter,
    src: &S,
    start: usize,
    layout: &EmacsMuleLayout,
    eol_dos: bool,
    last_block: bool,
    spec: &mut DecoderSpec,
    out: &mut DecodedText,
) -> CallEnd {
    let limit = charbuf_size(src) - MAX_ANNOTATION_LENGTH * 3 - 1;
    let mut cmp = spec
        .emacs_mule
        .take()
        .map_or_else(Default::default, |cmp| *cmp);
    let mut used = cmp.segment.len();
    let mut runs = CallRuns::new();
    if used >= limit {
        // A carried composition the charbuf has no room after: GNU's
        // emacs_abort, or a call that carries it again for ever.  It ends
        // as a last block's would.
        cmp.maybe_finish(out, &mut runs);
    }
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
        let mut c = if byte_after_cr >= 0 {
            std::mem::replace(&mut byte_after_cr, -1)
        } else {
            one_more!(src, p, break 'call CallEnd::Ended(src_base))
        };
        'invalid: {
            if c < 0 || c == 0x80 {
                cmp.maybe_finish(out, &mut runs);
                if c < 0 {
                    out.push_code((-c) as u32);
                    used += 1;
                    continue 'call;
                }
                // DECODE_EMACS_MULE_COMPOSITION_START.
                let current = p;
                c = one_more!(src, p, break 'call CallEnd::Ended(src_base));
                if c < 0 {
                    break 'invalid;
                }
                if (0xF2..=0xF2 + COMPOSITION_WITH_RULE_ALTCHARS).contains(&c) {
                    let method = c - 0xF2;
                    c = one_more!(src, p, break 'call CallEnd::Ended(src_base));
                    if c < 0 {
                        break 'invalid;
                    }
                    let nbytes = c - 0xA0;
                    if nbytes < 3 || (method == COMPOSITION_RELATIVE && nbytes != 4) {
                        break 'invalid;
                    }
                    c = one_more!(src, p, break 'call CallEnd::Ended(src_base));
                    let nchars = c - 0xA0;
                    if nchars <= 0 || nchars >= MAX_COMPOSITION_COMPONENTS {
                        break 'invalid;
                    }
                    let state = if method == COMPOSITION_RELATIVE {
                        COMPOSING_CHAR
                    } else {
                        COMPOSING_COMPONENT_CHAR
                    };
                    cmp.start(false, method, state, nchars, nbytes);
                    cmp.nchars = nchars;
                    cmp.ncomps = nbytes - 4;
                } else if c < 0xA0 {
                    break 'invalid;
                } else if c < 0xC0 {
                    cmp.start(true, COMPOSITION_RELATIVE, COMPOSING_CHAR, 0, 0);
                    cmp.nchars = 0;
                    cmp.ncomps = 0;
                    // Read C again as a component.
                    p = current;
                } else if c == 0xFF {
                    cmp.start(true, COMPOSITION_WITH_RULE, COMPOSING_CHAR, 0, 0);
                    cmp.nchars = 0;
                    cmp.ncomps = 0;
                } else {
                    break 'invalid;
                }
                used += MAX_ANNOTATION_LENGTH;
                continue 'call;
            }
            let charset = if c < 0x80 {
                if eol_dos && c == i32::from(b'\r') {
                    byte_after_cr = one_more!(src, p, break 'call CallEnd::Ended(src_base));
                }
                if cmp.state != COMPOSING_NO {
                    if cmp.old_form {
                        cmp.maybe_finish(out, &mut runs);
                    } else if cmp.state >= COMPOSING_COMPONENT_CHAR {
                        cmp.ncomps -= 1;
                    }
                }
                None
            } else {
                match emacs_mule_char(interp, src, src_base, layout, &cmp) {
                    // A component character of code 1 or 2 is taken for
                    // the -1 and -2 results.  After -2 decode_coding calls
                    // again from SRC_BASE, which the last block's finished
                    // composition decodes past; another block's carried
                    // one would read it again for ever, so it ends as the
                    // last block's does.
                    Ok((-1, _, _)) => break 'invalid,
                    Ok((-2, _, _)) => {
                        cmp.maybe_finish(out, &mut runs);
                        break 'call CallEnd::Full(src_base);
                    }
                    Ok((ch, charset, after)) => {
                        c = ch;
                        p = after;
                        if cmp.state >= COMPOSING_COMPONENT_CHAR {
                            cmp.ncomps -= (p - src_base) as i32;
                        }
                        charset
                    }
                    Err(true) => break 'invalid,
                    Err(false) => break 'call CallEnd::Ended(src_base),
                }
            };
            match cmp.state {
                COMPOSING_NO => {
                    match charset {
                        Some(charset) if !charset.is_named("ascii") => {
                            if !runs.is(charset) {
                                used += runs.open(charset, out);
                            }
                        }
                        _ => used += runs.close(out),
                    }
                    out.push_code(c as u32);
                    used += 1;
                }
                COMPOSING_CHAR if cmp.old_form => {
                    if c >= 0 {
                        cmp.maybe_finish(out, &mut runs);
                        out.push_code(c as u32);
                    } else {
                        cmp.segment.push(-c);
                        cmp.nchars += 1;
                        if cmp.nchars == MAX_COMPOSITION_COMPONENTS {
                            cmp.end(out, &mut runs);
                        } else if cmp.method == COMPOSITION_WITH_RULE {
                            cmp.state = COMPOSING_RULE;
                        }
                    }
                    used += 1;
                }
                COMPOSING_CHAR => {
                    cmp.segment.push(c);
                    used += 1;
                    cmp.nchars -= 1;
                    if cmp.nchars == 0 {
                        cmp.end(out, &mut runs);
                    }
                }
                COMPOSING_RULE => {
                    if c >= 0 {
                        cmp.end(out, &mut runs);
                        out.push_code(c as u32);
                        used += 1;
                    } else {
                        // DECODE_EMACS_MULE_COMPOSITION_RULE_20.
                        let c = -c - 0xA0;
                        if !(0..81).contains(&c) {
                            break 'invalid;
                        }
                        let (mut gref, mut nref) = (c / 9, c % 9);
                        if gref == 4 {
                            gref = 10;
                        }
                        if nref == 4 {
                            nref = 10;
                        }
                        cmp.segment.extend([-2, gref * 12 + nref]);
                        used += 2;
                        cmp.state = COMPOSING_CHAR;
                    }
                }
                COMPOSING_COMPONENT_CHAR => {
                    cmp.segment.push(c);
                    used += 1;
                    match cmp.ncomps.cmp(&0) {
                        std::cmp::Ordering::Equal => cmp.state = COMPOSING_CHAR,
                        std::cmp::Ordering::Greater => {
                            if cmp.method == COMPOSITION_WITH_RULE_ALTCHARS {
                                cmp.state = COMPOSING_COMPONENT_RULE;
                            }
                        }
                        std::cmp::Ordering::Less => cmp.maybe_finish(out, &mut runs),
                    }
                }
                _ => {
                    // COMPOSING_COMPONENT_RULE: DECODE_EMACS_MULE_COMPOSITION_RULE_21.
                    let gref = c - 0x20;
                    if !(0..81).contains(&gref) {
                        break 'invalid;
                    }
                    let c = one_more!(src, p, break 'call CallEnd::Ended(src_base));
                    let nref = c - 0x20;
                    if !(0..81).contains(&nref) {
                        break 'invalid;
                    }
                    cmp.segment.extend([-2, gref * 12 + nref]);
                    used += 2;
                    cmp.ncomps -= 1;
                    if cmp.ncomps > 0 {
                        cmp.state = COMPOSING_COMPONENT_CHAR;
                    } else {
                        cmp.maybe_finish(out, &mut runs);
                    }
                }
            }
            continue 'call;
        }
        // invalid_code.
        cmp.maybe_finish(out, &mut runs);
        p = src_base;
        let c = one_more!(src, p, break 'call CallEnd::Ended(src_base));
        if (0..0x80).contains(&c) {
            out.push_byte(c as u8);
        } else {
            store_byte8(out, c);
        }
        used += 1;
    };
    if cmp.state != COMPOSING_NO {
        if last_block {
            cmp.maybe_finish(out, &mut runs);
        } else {
            spec.emacs_mule = Some(Box::new(cmp));
        }
    }
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

/// coding->spec.ccl of a decode: the decoder program, set up once for
/// decode_coding's calls (setup_ccl_program), and CODING_ATTR_CHARSET_LIST.
pub(super) struct CclDecode {
    ccl: crate::lisp::primitives::ccl::CclProgram,
    charset_list: Option<Vec<SymbolName>>,
}

impl CclDecode {
    fn new(interp: &Interpreter, canonical: &str) -> Self {
        let mut ccl = crate::lisp::primitives::ccl::CclProgram::default();
        let program = coding_ccl_program(interp, canonical, 0);
        if !crate::lisp::primitives::ccl::setup_ccl_program(interp, &mut ccl, &program) {
            // No program to run (GNU runs what the struct held): the call
            // ends as an interrupted one, decoding nothing.
            ccl.status = crate::lisp::primitives::ccl::CCL_STAT_INVALID_CMD;
            ccl.ic = -1;
        }
        CclDecode {
            ccl,
            charset_list: coding_charset_list(interp, canonical),
        }
    }
}

/// CODING_ATTR_CHARSET_LIST of a coding: None for nil.
pub(super) fn coding_charset_list(
    interp: &Interpreter,
    canonical: &(impl crate::lisp::eval::coding::CodingRef + ?Sized),
) -> Option<Vec<SymbolName>> {
    let names = coding_system_charset_names(interp, canonical);
    (!names.is_empty()).then_some(names)
}

/// CODING_CCL_DECODER (INDEX 0) or CODING_CCL_ENCODER (INDEX 1).
pub(super) fn coding_ccl_program(
    interp: &Interpreter,
    canonical: &(impl crate::lisp::eval::coding::CodingRef + ?Sized),
    index: usize,
) -> Value {
    interp
        .coding_system(canonical)
        .and_then(|state| state.type_args.get(index).copied())
        .unwrap_or(Value::Nil)
}

/// decode_coding_ccl: the source a CCL_BLOCK of characters at a time
/// through the decoder program into a charbuf, until the source or the
/// charbuf runs out; then produce_chars of the charbuf, where a
/// negative int is an annotation as long as it says.
pub(super) fn ccl_call<S: Units + ?Sized>(
    interp: &Interpreter,
    src: &S,
    start: usize,
    decode: &mut CclDecode,
    last: bool,
    out: &mut DecodedText,
) -> Result<CallEnd, LispError> {
    use crate::lisp::primitives::ccl::{
        CCL_BLOCK, CCL_STAT_SUSPEND_BY_DST, CCL_STAT_SUSPEND_BY_SRC, CodingHost, ccl_driver,
    };
    if decode.ccl.ic < 0 {
        return Ok(CallEnd::Ended(start));
    }
    let multibyte = src.bytes().is_none();
    let mut charbuf = vec![0i32; charbuf_size(src)];
    let mut produced = 0;
    let mut pos = start;
    let mut source = Vec::with_capacity(CCL_BLOCK);
    loop {
        source.clear();
        let mut p = pos;
        while source.len() < CCL_BLOCK && p < src.len() {
            let unit = src.at(p);
            // A multibyte source's character, string_char_advance's: a
            // raw byte's two-byte form is its eight-bit character.
            source.push(if !multibyte {
                unit
            } else if unit < 0 {
                -unit
            } else if unit >= 0x80 {
                unit + 0x3F_FF00
            } else {
                unit
            });
            p += 1;
        }
        if p == src.len() && last {
            decode.ccl.last_block = true;
        }
        let _ = ccl_driver(
            &mut CodingHost { interp },
            &mut decode.ccl,
            Some(&source),
            Some(&mut charbuf[produced..]),
            decode.charset_list.as_deref(),
        );
        produced += decode.ccl.produced;
        pos += decode.ccl.consumed;
        if p == src.len() || decode.ccl.status != CCL_STAT_SUSPEND_BY_SRC {
            break;
        }
    }
    let mut index = 0;
    while index < produced {
        let c = charbuf[index];
        if c < 0 {
            // An annotation datum: (-C) is its length.
            index = index.saturating_add(c.unsigned_abs() as usize);
            continue;
        }
        // CHAR_STRING_ADVANCE_NO_UNIFY.
        out.push_code(crate::lisp::primitives::ccl::written_character(c)?);
        index += 1;
    }
    Ok(match decode.ccl.status {
        CCL_STAT_SUSPEND_BY_DST => CallEnd::Again(pos),
        crate::lisp::primitives::ccl::CCL_STAT_SUCCESS => CallEnd::Full(pos),
        _ => CallEnd::Ended(pos),
    })
}

/// LOOKUP_TRANSLATION_TABLE: the entry of TABLE (a char-table or a list
/// of them) for *C.  A character entry replaces *C (a list going on to
/// the next table with it) and leaves nil; another non-nil entry ends the
/// search and is returned.
fn lookup_translation_table(table: Value, c: &mut u32) -> Value {
    let character = |value: Value| {
        value
            .integer_value()
            .filter(|code| (0..=i64::from(MAX_CHAR)).contains(code))
            .map(|code| code as u32)
    };
    match table.kind() {
        Kind::CharTable(chartab) => {
            let trans = chartab.get(*c);
            match character(trans) {
                Some(code) => {
                    *c = code;
                    Value::Nil
                }
                None => trans,
            }
        }
        Kind::Cons(_) => {
            let mut trans = Value::Nil;
            let mut tail = table;
            while let Some((head, rest)) = tail.cons_values() {
                if let Kind::CharTable(chartab) = head.kind() {
                    trans = chartab.get(*c);
                    if let Some(code) = character(trans) {
                        *c = code;
                        trans = Value::Nil;
                    } else if !trans.is_nil() {
                        break;
                    }
                }
                tail = rest;
            }
            trans
        }
        _ => Value::Nil,
    }
}

/// What get_translation makes of a non-character entry.
enum Translation {
    /// The value, replacing this many characters.
    Value(Value, usize),
    /// Qt: the source ran out before an entry's sequence was matched.
    Incomplete,
    /// Qnil: no entry's sequence matched.
    None,
}

/// coding.c get_translation over the charbuf from the character at
/// CODES[AT]: a fixnum or vector replaces that character; an alist of
/// (FROM-VECTOR . TO) gives the TO of the first FROM the charbuf holds
/// there.  MARKERS are the charbuf positions an annotation datum
/// precedes, which no FROM matches; the charbuf's end before a FROM is
/// done is Qt.
fn get_translation(trans: Value, codes: &[u32], at: usize, markers: &[usize]) -> Translation {
    if trans.integer_value().is_some() || matches!(trans.kind(), Kind::Vector(_)) {
        return Translation::Value(trans, 1);
    }
    let mut tail = trans;
    while let Some((entry, rest)) = tail.cons_values() {
        tail = rest;
        let Some((from, to)) = entry.cons_values() else {
            continue;
        };
        let Kind::Vector(from) = from.kind() else {
            continue;
        };
        let len = from.len();
        let mut matched = 0;
        for (i, item) in from.slots().enumerate() {
            let q = at + i;
            if i > 0 && markers.binary_search(&q).is_ok() {
                break;
            }
            if q == codes.len() {
                return Translation::Incomplete;
            }
            if item.integer_value() != Some(i64::from(codes[q])) {
                break;
            }
            matched += 1;
        }
        if matched == len {
            return Translation::Value(to, len);
        }
    }
    Translation::None
}

/// produce_chars' translation of the characters from the FROM'th (text
/// from byte FROM_BYTE) on, which the decoder put in its charbuf after
/// any carried over from the call before: each character through
/// LOOKUP_TRANSLATION_TABLE and, for another entry, get_translation,
/// whose fixnum replaces it, whose vector's characters stand for it, and
/// whose Qt stops the scan unless LAST_BLOCK, the rest carried over to
/// the next call.  Returns where the carried characters start.
fn produce_translated(
    out: &mut DecodedText,
    table: Value,
    from: (usize, usize),
    last_block: bool,
) -> Option<(usize, usize)> {
    let (from_char, from_byte) = from;
    let codes = out.codes_from(from_char, from_byte);
    // The annotation data among these characters: a `charset' run's after
    // its last character, a composition's before its first.
    let mut markers: Vec<usize> = out
        .charsets
        .iter()
        .rev()
        .take_while(|(_, end, _)| *end > from_char)
        .map(|(_, end, _)| end - from_char)
        .chain(
            out.compositions
                .iter()
                .rev()
                .take_while(|composition| composition.start >= from_char)
                .map(|composition| composition.start - from_char),
        )
        .collect();
    markers.sort_unstable();
    markers.dedup();
    let mut produced = Vec::with_capacity(codes.len());
    let mut i = 0;
    let mut carried = None;
    while i < codes.len() {
        let mut c = codes[i];
        let mut from_nchars = 1;
        let trans = lookup_translation_table(table, &mut c);
        if trans.is_nil() {
            produced.push(c);
            i += 1;
            continue;
        }
        match get_translation(trans, &codes, i, &markers) {
            Translation::Value(value, nchars) => {
                from_nchars = nchars;
                if let Some(code) = value
                    .integer_value()
                    .filter(|code| (0..=i64::from(MAX_CHAR)).contains(code))
                {
                    produced.push(code as u32);
                } else if let Kind::Vector(items) = value.kind() {
                    produced.extend(
                        items
                            .slots()
                            .filter_map(|item| item.integer_value())
                            .filter(|code| (0..=i64::from(MAX_CHAR)).contains(code))
                            .map(|code| code as u32),
                    );
                } else {
                    produced.push(c);
                }
            }
            Translation::Incomplete if !last_block => {
                carried = Some(i);
                break;
            }
            Translation::Incomplete | Translation::None => produced.push(c),
        }
        i += from_nchars;
    }
    out.truncate_chars(from_char, from_byte);
    for &code in &produced {
        out.push_code(code);
    }
    let carried_at = (out.char_count(), out.text.len());
    let start = carried?;
    for &code in &codes[start..] {
        out.push_code(code);
    }
    Some(carried_at)
}

/// The first of this call's annotations, in produce_annotation's order
/// (those after the first ANNOTATED `charset' runs and compositions),
/// whose range ends past the PRODUCED characters, where a translation
/// made the call's text shorter than the decoder counted it; with the
/// `charset' runs and compositions put on the text before it.
type Overrun = ((usize, usize, Option<String>), (usize, usize));

fn annotation_overrun(
    out: &DecodedText,
    annotated: (usize, usize),
    produced: usize,
) -> Option<Overrun> {
    let (mut runs, mut compositions) = (0, 0);
    for annotation in annotations(&out.charsets, &out.compositions) {
        match annotation {
            Annotation::Charset(start, end, charset) => {
                if runs >= annotated.0 && end > produced {
                    return Some((
                        (start, end, Some(charset.to_string())),
                        (runs, compositions),
                    ));
                }
                runs += 1;
            }
            Annotation::Composition(composition) => {
                if compositions >= annotated.1 && composition.end > produced {
                    return Some((
                        (composition.start, composition.end, None),
                        (runs, compositions),
                    ));
                }
                compositions += 1;
            }
        }
    }
    None
}

/// A coding system's decoder, set up once for decode_coding's calls.
enum Decoder {
    Utf8 { bom: bool },
    Utf16 { big_endian: bool, check_bom: bool },
    Charset(Box<CharsetDecode>),
    Sjis(SjisCharsets),
    Big5(std::rc::Rc<CharsetInfo>, std::rc::Rc<CharsetInfo>),
    EmacsMule(Box<EmacsMuleLayout>),
    Iso2022,
    Ccl(Box<CclDecode>),
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
                let roman = match names.first() {
                    Some(name) => charset_info(interp, name),
                    None => charset_info(interp, "ascii"),
                };
                let big5 = match names.get(1) {
                    Some(name) => charset_info(interp, name),
                    None => charset_info(interp, "big5"),
                };
                match (roman, big5) {
                    (Some(roman), Some(big5)) => Decoder::Big5(roman, big5),
                    _ => Decoder::RawText,
                }
            }
            "emacs-mule" => Decoder::EmacsMule(Box::new(emacs_mule_layout(interp))),
            "ccl" => Decoder::Ccl(Box::new(CclDecode::new(interp, canonical))),
            "charset" => Decoder::Charset(Box::new(CharsetDecode::new(interp, canonical))),
            "utf-8" | "prefer-utf-8" | "utf-8-auto" => Decoder::Utf8 {
                bom: coding_system_consumes_bom(interp, canonical),
            },
            "utf-8-with-signature" => Decoder::Utf8 { bom: true },
            "utf-16" => {
                // An auto-BOM coding reaching the decoder is one detect_coding
                // could not settle (utf_detect_bom): it decodes without a BOM in
                // its own byte order.
                let (big_endian, with_bom, detect_bom) =
                    coding_system_utf16_options(interp, canonical);
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
) -> Result<DecodedText, LispError> {
    let mut decoder = Decoder::new(interp, canonical);
    // decode_coding's get_translation_table, which produce_chars applies
    // to every decoder's charbuf but decode_coding_raw_text's characters
    // at source.
    let table = if matches!(decoder, Decoder::RawText) {
        Value::Nil
    } else {
        decode_translation_table(interp, canonical)
    };
    // A unit decodes to a character of at least a byte.
    let mut out = DecodedText::with_capacity(src.len());
    let eol_dos = block.eol_dos;
    let mut consumed = 0;
    // The characters produce_chars carried over to the next call.
    let mut carried: Option<(usize, usize)> = None;
    loop {
        let before = carried.map_or(out.char_count(), |(chars, _)| chars);
        let from = carried.take().unwrap_or((out.char_count(), out.text.len()));
        let annotated = (out.charsets.len(), out.compositions.len());
        let end = match &mut decoder {
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
            Decoder::Charset(charsets) => {
                charset_call(interp, src, consumed, charsets, eol_dos, &mut out)
            }
            Decoder::Sjis(charsets) => {
                sjis_call(interp, src, consumed, charsets, eol_dos, &mut out)
            }
            Decoder::Big5(roman, big5) => {
                big5_call(interp, src, consumed, roman, big5, eol_dos, &mut out)
            }
            Decoder::EmacsMule(layout) => emacs_mule_call(
                interp, src, consumed, layout, eol_dos, block.last, block.spec, &mut out,
            ),
            Decoder::Iso2022 => iso2022::decode_call(
                interp, src, consumed, canonical, eol_dos, block.last, block.spec, &mut out,
            ),
            Decoder::Ccl(ccl) => ccl_call(interp, src, consumed, ccl, block.last, &mut out)?,
            Decoder::RawText => raw_text_call(src, consumed, eol_dos, &mut out),
        };
        if !table.is_nil() {
            carried = produce_translated(&mut out, table, from, false);
        }
        // produce_chars inserts what the call produced.
        let produced = carried.map_or(out.char_count(), |(chars, _)| chars);
        if produced > before {
            out.chunks.push(produced - before);
        }
        if !table.is_nil()
            && let Some((overrun, (runs, compositions))) =
                annotation_overrun(&out, annotated, produced)
        {
            // produce_annotation's Fput_text_property signals, the
            // annotations after it never put on the text, and what the
            // decoder carried over is never produced.
            if let Some((chars, bytes)) = carried.take() {
                out.truncate_chars(chars, bytes);
            }
            out.charsets.truncate(runs);
            out.compositions.truncate(compositions);
            out.overrun = Some(overrun);
            block.consumed = src.len();
            return Ok(out);
        }
        match end {
            CallEnd::Full(at) if at < src.len() => consumed = at,
            CallEnd::Again(at) => consumed = at,
            CallEnd::Full(at) | CallEnd::Ended(at) => {
                consumed = at;
                break;
            }
        }
    }
    // decode_coding's last produce_chars of what was carried over.
    if let Some(from) = carried {
        produce_translated(&mut out, table, from, true);
        if out.char_count() > from.0 {
            out.chunks.push(out.char_count() - from.0);
        }
    }
    if block.last {
        let before = out.char_count();
        src.flush(consumed, &mut out);
        if out.char_count() > before {
            out.chunks.push(out.char_count() - before);
        }
        block.consumed = src.len();
    } else {
        block.consumed = consumed;
    }
    Ok(out)
}
