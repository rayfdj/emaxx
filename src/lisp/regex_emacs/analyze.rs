//! regex-emacs.c's analyses of a compiled pattern: `forall_firstchar' and
//! what it drives (`analyze_first', the fastmap, `mutually_exclusive_p'),
//! `skip_one_char' and `execute_charset'.  Positions are indices into the
//! pattern buffer; GNU's `buffer - 1' and `buffer + used + 1' loop bounds
//! are -1 and len + 1.

use super::compile::{
    BIT_ALNUM, BIT_ALPHA, BIT_BLANK, BIT_GRAPH, BIT_LOWER, BIT_MULTIBYTE, BIT_PRINT, BIT_PUNCT,
    BIT_SPACE, BIT_UPPER, BIT_WORD, charset_bitmap_size, charset_range_table,
    charset_range_table_bits, charset_range_table_exists_p, extract_address, extract_character,
    extract_number, isalnum, isalpha, isblank, isgraph, isprint, ispunct, isspace, isword,
};
use super::*;

/// The lead byte every raw byte is stored under (U+E080..U+E0FF).
const RAW_BYTE_LEAD: u8 = 0xEE;

/// forall_firstchar_1: call F on every char-matching operation directly
/// reachable from P, LOOP_BEG..LOOP_END delimiting the current loop.
fn forall_firstchar_1(
    buf: &[u8],
    mut p: isize,
    pend: Option<isize>,
    mut loop_beg: isize,
    mut loop_end: isize,
    f: &mut dyn FnMut(usize) -> bool,
) -> bool {
    loop {
        let p_orig = p;
        if Some(p) == pend {
            return false;
        } else if p == loop_end {
            return true;
        } else if p > loop_end {
            // A broken assumption about the code's shape.
            return false;
        }
        let at = p as usize;
        if at >= buf.len() {
            // The end of a POSIX pattern, which regex_compile leaves
            // without a final `succeed' (GNU reads past the buffer here):
            // the empty string matches, as at PEND.
            return false;
        }
        let (mut newp1, mut newp2);
        match buf[at] {
            op::NO_OP => {
                p += 1;
                continue;
            }
            op::SUCCEED
            | op::EXACTN
            | op::CHARSET
            | op::CHARSET_NOT
            | op::ANYCHAR
            | op::SYNTAXSPEC
            | op::NOTSYNTAXSPEC
            | op::CATEGORYSPEC
            | op::NOTCATEGORYSPEC => return f(at),
            op::AT_DOT
            | op::BEGBUF
            | op::WORDBOUND
            | op::NOTWORDBOUND
            | op::BEGLINE
            | op::ENDLINE
            | op::ENDBUF
            | op::WORDBEG
            | op::WORDEND
            | op::SYMBEG
            | op::SYMEND => {
                if f(at) {
                    return true;
                }
                p += 1;
                continue;
            }
            op::JUMP | op::JUMP_N => {
                newp1 = extract_address(buf, at + 1) as isize;
                if newp1 > p {
                    // A forward jump, boring.
                    p = newp1;
                    continue;
                }
                match buf[newp1 as usize] {
                    op::ON_FAILURE_JUMP
                    | op::ON_FAILURE_KEEP_STRING_JUMP
                    | op::ON_FAILURE_JUMP_NASTYLOOP
                    | op::ON_FAILURE_JUMP_LOOP
                    | op::ON_FAILURE_JUMP_SMART
                    | op::SUCCEED_N => {
                        newp2 = extract_address(buf, newp1 as usize + 1) as isize;
                    }
                    _ => {
                        // The "safe" choice.
                        newp2 = loop_end;
                        match do_jump(p_orig, newp1, newp2, &mut loop_beg, &mut loop_end) {
                            Ok(next) => {
                                p = next;
                                continue;
                            }
                            Err(safe) => return safe,
                        }
                    }
                }
            }
            op::ON_FAILURE_JUMP
            | op::ON_FAILURE_KEEP_STRING_JUMP
            | op::ON_FAILURE_JUMP_NASTYLOOP
            | op::ON_FAILURE_JUMP_LOOP
            | op::ON_FAILURE_JUMP_SMART => {
                newp1 = extract_address(buf, at + 1) as isize;
                newp2 = p + 3;
                // For `+' loops, an on_failure_jump often skips forward
                // over the jump back to the loop's entry.
                if buf.get(newp2 as usize) == Some(&op::JUMP) {
                    let p3 = extract_address(buf, newp2 as usize + 1) as isize;
                    if p3 > p_orig || newp1 > p_orig {
                        newp2 = p3;
                    }
                }
            }
            op::SUCCEED_N => {
                newp1 = extract_address(buf, at + 1) as isize;
                // Skip the two bytes of the count.
                newp2 = p + 5;
            }
            op::SET_NUMBER_AT => {
                let offset = extract_number(buf, at + 1);
                // Setting the counter of an immediately following
                // succeed_n: its next execution only falls through.
                p += if offset == 5 && buf.get(at + 5) == Some(&op::SUCCEED_N) {
                    10
                } else {
                    5
                };
                continue;
            }
            op::START_MEMORY | op::STOP_MEMORY => {
                p += 2;
                continue;
            }
            op::DUPLICATE => {
                if !f(at) {
                    return false;
                }
                p += 2;
                continue;
            }
            _ => return false,
        }

        // do_twoway_jump: both destinations must be safe; NEWP1 is the
        // smaller.
        if newp1 > newp2 {
            std::mem::swap(&mut newp1, &mut newp2);
        }
        if newp2 <= p_orig {
            // Both destinations go backward.
            return false;
        }
        if !forall_firstchar_1(buf, newp2, pend, loop_beg, loop_end, f) {
            return false;
        }
        match do_jump(p_orig, newp1, newp2, &mut loop_beg, &mut loop_end) {
            Ok(next) => p = next,
            Err(safe) => return safe,
        }
    }
}

/// The `do_jump' tail of forall_firstchar_1: where to continue after a
/// jump to NEWP1, narrowing the loop when NEWP1 enters a nested one, or
/// the path's answer: true for a zero-length cycle through the current
/// loop, false for a jump before it (a broken assumption).
fn do_jump(
    p_orig: isize,
    newp1: isize,
    newp2: isize,
    loop_beg: &mut isize,
    loop_end: &mut isize,
) -> Result<isize, bool> {
    if newp1 <= p_orig {
        if newp1 < *loop_beg {
            return Err(false);
        } else if newp1 == *loop_beg {
            return Err(true);
        }
        // `p' gets smaller, but `loop_beg' bigger.
        *loop_beg = newp1;
        *loop_end = newp2;
    }
    Ok(newp1)
}

/// forall_firstchar.  WHOLE says the pattern is complete (GNU's non-null
/// BUFP, `bufp->used' nonzero), so the outermost loop is the whole buffer;
/// otherwise P..PEND must be a self-contained block.
fn forall_firstchar(
    buf: &[u8],
    whole: bool,
    p: usize,
    pend: Option<usize>,
    f: &mut dyn FnMut(usize) -> bool,
) -> bool {
    let (loop_beg, loop_end) = if whole {
        (-1, buf.len() as isize + 1)
    } else {
        (p as isize, pend.expect("a block has an end") as isize)
    };
    forall_firstchar_1(
        buf,
        p as isize,
        pend.map(|end| end as isize),
        loop_beg,
        loop_end,
        f,
    )
}

/// The lead byte of every stored character in FROM..=TO, set in FASTMAP:
/// CHAR_LEADING_CODE over a range-table entry, where characters beyond
/// Unicode share the placeholder's lead and raw bytes theirs.
fn set_range_leads(fastmap: &mut [bool; 256], from: u32, to: u32) {
    if from < 0x80 {
        for byte in from..=to.min(0x7F) {
            fastmap[byte as usize] = true;
        }
    }
    let unicode_from = from.max(0x80);
    let unicode_to = to.min(0x10_FFFF);
    if unicode_from <= unicode_to {
        let lc1 = leading_byte(unicode_from);
        let lc2 = leading_byte(unicode_to);
        for lead in lc1..=lc2 {
            fastmap[lead as usize] = true;
        }
    }
    if to > 0x10_FFFF && from < BYTE8_FIRST {
        fastmap[leading_byte(EXTENDED_PLACEHOLDER as u32) as usize] = true;
    }
    if to >= BYTE8_FIRST {
        fastmap[RAW_BYTE_LEAD as usize] = true;
    }
}

fn set_all_multibyte_leads(fastmap: &mut [bool; 256], done: &mut bool) {
    if !*done {
        for lead in MIN_LEADING_BYTE..=MAX_LEADING_BYTE {
            fastmap[lead as usize] = true;
        }
        *done = true;
    }
}

/// analyze_first_fastmap: add to FASTMAP the lead bytes the operation at P
/// can start a match with.  The fastmap is indexed by the first byte of a
/// character's stored form, so a raw byte, in a unibyte text or a
/// multibyte one, is its U+E080..U+E0FF lead.
fn analyze_first_fastmap<E: MatchEnv + ?Sized>(
    buf: &[u8],
    p: usize,
    fastmap: &mut [bool; 256],
    match_any_multibyte: &mut bool,
    env: &mut E,
) -> bool {
    match buf[p] {
        op::SUCCEED => false,
        // The group a back reference names matched already, so the
        // reference that comes first matched it empty: go on past it.
        op::DUPLICATE => true,
        op::EXACTN => {
            fastmap[usize::from(buf[p + 2])] = true;
            true
        }
        op::ANYCHAR => false,
        op::CHARSET | op::CHARSET_NOT => {
            let not = buf[p] == op::CHARSET_NOT;
            let nbits = charset_bitmap_size(buf, p) * 8;
            let map = p + 2;
            if not && nbits < 256 {
                // Characters beyond the bitmap are possible matches.
                if nbits < 0x80 {
                    fastmap[nbits..0x80].fill(true);
                }
                fastmap[usize::from(RAW_BYTE_LEAD)] = true;
            }
            for j in 0..nbits {
                if ((buf[map + j / 8] & (1 << (j % 8))) != 0) != not {
                    if j < 0x80 {
                        fastmap[j] = true;
                    } else {
                        fastmap[usize::from(RAW_BYTE_LEAD)] = true;
                    }
                }
            }
            let has_table = charset_range_table_exists_p(buf, p);
            if not || (has_table && charset_range_table_bits(buf, p) != 0) {
                // Any lead can start a character outside the set, or one
                // of a class.
                set_all_multibyte_leads(fastmap, match_any_multibyte);
            } else if has_table && !*match_any_multibyte {
                let mut at = charset_range_table(buf, p);
                let count = extract_number(buf, at);
                at += 2;
                for _ in 0..count {
                    let from = extract_character(buf, at);
                    let to = extract_character(buf, at + 3);
                    set_range_leads(fastmap, from, to);
                    at += 6;
                }
            }
            true
        }
        // These depend on text properties: no optimization.
        op::SYNTAXSPEC | op::NOTSYNTAXSPEC => false,
        op::CATEGORYSPEC | op::NOTCATEGORYSPEC => {
            let not = buf[p] == op::NOTCATEGORYSPEC;
            let category = buf[p + 1];
            for j in 0..0x80u32 {
                if env.has_category(j, category) != not {
                    fastmap[j as usize] = true;
                }
            }
            // GNU tests the Latin-1 characters too, whose leads the line
            // below sets anyway.
            set_all_multibyte_leads(fastmap, match_any_multibyte);
            true
        }
        // Not failure, "not succeeded yet".
        _ => false,
    }
}

/// analyze_first_null: whether the operation at P makes the path safe
/// (consumes a character, or cannot reach the end).
fn analyze_first_null(buf: &[u8], p: usize) -> bool {
    matches!(
        buf[p],
        op::SUCCEED
            | op::DUPLICATE
            | op::EXACTN
            | op::ANYCHAR
            | op::CHARSET_NOT
            | op::CHARSET
            | op::SYNTAXSPEC
            | op::NOTSYNTAXSPEC
            | op::CATEGORYSPEC
            | op::NOTCATEGORYSPEC
    )
}

/// analyze_first during compilation (no fastmap, `bufp->used' zero):
/// whether P..PEND might match the empty string.
pub(super) fn analyze_first(
    buf: &[u8],
    p: usize,
    pend: usize,
    _fastmap: Option<&mut [bool; 256]>,
    _multibyte: bool,
) -> bool {
    let mut f = |at: usize| analyze_first_null(buf, at);
    !forall_firstchar(buf, false, p, Some(pend), &mut f)
}

/// re_compile_fastmap: fill PATTERN's fastmap and `can_be_null'.
pub(super) fn re_compile_fastmap<E: MatchEnv + ?Sized>(pattern: &mut Pattern, env: &mut E) {
    let mut fastmap = [false; 256];
    let mut match_any_multibyte = false;
    let buf = &pattern.buffer;
    let whole = !buf.is_empty();
    let safe = {
        let mut f =
            |at: usize| analyze_first_fastmap(buf, at, &mut fastmap, &mut match_any_multibyte, env);
        forall_firstchar(buf, whole, 0, Some(buf.len()), &mut f)
    };
    pattern.fastmap = fastmap;
    pattern.fastmap_accurate = true;
    pattern.ascii_admits = None;
    pattern.can_be_null = !safe;
}

/// skip_one_char: the operation after a single-character match at P,
/// None when P is not one.
pub(super) fn skip_one_char(buf: &[u8], p: usize) -> Option<usize> {
    match buf[p] {
        op::ANYCHAR => Some(p + 1),
        op::EXACTN => Some(p + 2 + usize::from(buf[p + 1])),
        op::CHARSET | op::CHARSET_NOT => Some(charset_end(buf, p)),
        op::SYNTAXSPEC | op::NOTSYNTAXSPEC | op::CATEGORYSPEC | op::NOTCATEGORYSPEC => Some(p + 2),
        _ => None,
    }
}

/// The position after the charset at P.
#[inline]
pub(super) fn charset_end(buf: &[u8], p: usize) -> usize {
    if charset_range_table_exists_p(buf, p) {
        let at = charset_range_table(buf, p);
        let count = extract_number(buf, at) as usize;
        at + 2 + count * 6
    } else {
        p + 2 + charset_bitmap_size(buf, p)
    }
}

/// execute_charset: whether C (CORIG before translation) matches the
/// charset at P.  UNIBYTE says C is a byte (ASCII or raw), tested against
/// the bitmap; CANON whether the pattern folds case.
#[inline]
pub(super) fn execute_charset<E: MatchEnv + ?Sized>(
    buf: &[u8],
    p: usize,
    c: u32,
    corig: u32,
    unibyte: bool,
    canon: bool,
    env: &mut E,
) -> bool {
    let not = buf[p] == op::CHARSET_NOT;
    let size = charset_bitmap_size(buf, p);
    if unibyte && c < 256 {
        let index = c as usize;
        if index < size * 8 && buf[p + 2 + index / 8] & (1 << (index % 8)) != 0 {
            return !not;
        }
    } else if charset_range_table_exists_p(buf, p) {
        let class_bits = charset_range_table_bits(buf, p);
        if class_bits != 0
            && ((class_bits & BIT_MULTIBYTE) != 0
                || (class_bits & BIT_ALNUM != 0 && isalnum(env, c))
                || (class_bits & BIT_ALPHA != 0 && isalpha(env, c))
                || (class_bits & BIT_SPACE != 0 && isspace(env, c))
                || (class_bits & BIT_BLANK != 0 && isblank(env, c))
                || (class_bits & BIT_WORD != 0 && isword(env, c))
                || (class_bits & BIT_UPPER != 0
                    && (env.uppercasep(corig) || (canon && env.lowercasep(corig))))
                || (class_bits & BIT_LOWER != 0
                    && (env.lowercasep(corig) || (canon && env.uppercasep(corig))))
                || (class_bits & BIT_PUNCT != 0 && ispunct(env, c))
                || (class_bits & BIT_GRAPH != 0 && isgraph(env, c))
                || (class_bits & BIT_PRINT != 0 && isprint(env, c)))
        {
            return !not;
        }
        let mut at = charset_range_table(buf, p);
        let count = extract_number(buf, at) as usize;
        at += 2;
        for _ in 0..count {
            let from = extract_character(buf, at);
            let to = extract_character(buf, at + 3);
            if from <= c && c <= to {
                return !not;
            }
            at += 6;
        }
    }
    not
}

/// RE_STRING_CHAR at P in the pattern: the character an `exactn' stores,
/// as a byte for a unibyte pattern.
#[inline]
fn pattern_char(buf: &[u8], p: usize, multibyte: bool) -> u32 {
    let code = code_of_stored(decode_stored(&buf[p..]).0);
    if multibyte {
        code
    } else {
        char_to_unibyte(code).unwrap_or(code)
    }
}

/// mutually_exclusive_exactn: P2 is an `exactn' or `endline'.
fn mutually_exclusive_exactn<E: MatchEnv + ?Sized>(
    buf: &[u8],
    multibyte: bool,
    p1: usize,
    p2: usize,
    env: &mut E,
) -> bool {
    let c = if buf[p2] == op::ENDLINE {
        u32::from(b'\n')
    } else {
        pattern_char(buf, p2 + 2, multibyte)
    };
    match buf[p1] {
        op::EXACTN => c != pattern_char(buf, p1 + 2, multibyte),
        op::CHARSET | op::CHARSET_NOT => {
            !execute_charset(buf, p1, c, c, !multibyte || c < 0x80, false, env)
        }
        op::ANYCHAR => c == u32::from(b'\n'),
        _ => false,
    }
}

/// mutually_exclusive_charset: P2 is a `charset'.
fn mutually_exclusive_charset(buf: &[u8], multibyte: bool, p1: usize, p2: usize) -> bool {
    // A charset P2 that lists multibyte characters is hard to compare.
    if multibyte && charset_range_table_exists_p(buf, p2) {
        return false;
    }
    // P2 has no range table, so its length byte is its bitmap size.
    let p2_size = usize::from(buf[p2 + 1]);
    let p1_size = charset_bitmap_size(buf, p1);
    match buf[p1] {
        op::CHARSET => {
            // No overlap between the set inside the loop and the one after.
            let mut idx = 0;
            while idx < p2_size && idx < p1_size {
                if buf[p2 + 2 + idx] & buf[p1 + 2 + idx] != 0 {
                    break;
                }
                idx += 1;
            }
            idx == p2_size || idx == p1_size
        }
        op::CHARSET_NOT => {
            // The charset_not inside the loop lists every character the
            // charset after it does.
            let mut idx = 0;
            while idx < p2_size {
                if !(buf[p2 + 2 + idx] == 0
                    || (idx < p1_size && buf[p2 + 2 + idx] & !buf[p1 + 2 + idx] == 0))
                {
                    break;
                }
                idx += 1;
            }
            idx == p2_size
        }
        _ => false,
    }
}

/// mutually_exclusive_one: whether "P1 matches" implies the operation at P2
/// fails; UNCONSTRAINED tracks whether an empty-matching operation stands
/// between the loop's exit and a `succeed'.
fn mutually_exclusive_one<E: MatchEnv + ?Sized>(
    buf: &[u8],
    multibyte: bool,
    p1: usize,
    p2: usize,
    unconstrained: &mut bool,
    env: &mut E,
) -> bool {
    let mut constrain = |value: bool| {
        if !value {
            *unconstrained = false;
        }
        value
    };
    match buf[p2] {
        // The succeed after N+1 iterations always wins over the one after
        // N, when nothing that matches can come between.
        op::SUCCEED => *unconstrained,
        op::ENDLINE => {
            let value = mutually_exclusive_exactn(buf, multibyte, p1, p2, env);
            constrain(value)
        }
        op::EXACTN => mutually_exclusive_exactn(buf, multibyte, p1, p2, env),
        op::CHARSET => {
            if buf[p1] == op::EXACTN {
                mutually_exclusive_exactn(buf, multibyte, p2, p1, env)
            } else {
                mutually_exclusive_charset(buf, multibyte, p1, p2)
            }
        }
        op::CHARSET_NOT => match buf[p1] {
            op::EXACTN => mutually_exclusive_exactn(buf, multibyte, p2, p1, env),
            op::CHARSET => mutually_exclusive_charset(buf, multibyte, p2, p1),
            _ => false,
        },
        op::ANYCHAR => false,
        op::SYNTAXSPEC => buf[p1] == op::NOTSYNTAXSPEC && buf[p1 + 1] == buf[p2 + 1],
        op::NOTSYNTAXSPEC => buf[p1] == op::SYNTAXSPEC && buf[p1 + 1] == buf[p2 + 1],
        op::CATEGORYSPEC => buf[p1] == op::NOTCATEGORYSPEC && buf[p1 + 1] == buf[p2 + 1],
        op::NOTCATEGORYSPEC => buf[p1] == op::CATEGORYSPEC && buf[p1 + 1] == buf[p2 + 1],
        op::ENDBUF => true,
        op::WORDBEG => constrain(buf[p1] == op::NOTSYNTAXSPEC && buf[p1 + 1] == syntax_code::WORD),
        op::WORDEND => constrain(buf[p1] == op::SYNTAXSPEC && buf[p1 + 1] == syntax_code::WORD),
        op::SYMBEG => constrain(
            buf[p1] == op::NOTSYNTAXSPEC
                && (buf[p1 + 1] == syntax_code::SYMBOL || buf[p1 + 1] == syntax_code::WORD),
        ),
        op::SYMEND => constrain(
            buf[p1] == op::SYNTAXSPEC
                && (buf[p1 + 1] == syntax_code::SYMBOL || buf[p1 + 1] == syntax_code::WORD),
        ),
        op::AT_DOT | op::BEGBUF | op::WORDBOUND | op::NOTWORDBOUND | op::BEGLINE => {
            constrain(false)
        }
        // Nothing is known of what a back reference matches.
        _ => false,
    }
}

/// mutually_exclusive_p: whether "P1 matches something" implies "P2
/// fails", in the complete pattern BUF.
pub(super) fn mutually_exclusive_p<E: MatchEnv + ?Sized>(
    buf: &[u8],
    multibyte: bool,
    p1: usize,
    p2: usize,
    env: &mut E,
) -> bool {
    let mut unconstrained = true;
    let mut f = |at: usize| mutually_exclusive_one(buf, multibyte, p1, at, &mut unconstrained, env);
    forall_firstchar(buf, true, p2, None, &mut f)
}
