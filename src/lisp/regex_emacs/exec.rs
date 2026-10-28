//! regex-emacs.c's matcher: `re_search_2' and `re_match_2_internal', with
//! GNU's failure stack, run over a [`Text`] read in place.  Data positions
//! are byte offsets into the text where GNU keeps pointers; there is one
//! string (GNU's `string2'), the text, so the two-string bookkeeping
//! reduces to STOP.

use super::analyze::{charset_end, execute_charset, mutually_exclusive_p, re_compile_fastmap};
use super::compile::{extract_address, extract_number, store_number};
use super::*;

/// emacs_re_max_failures * TYPICAL_FAILURE_SIZE: the failure stack's
/// largest size, in items.
const FAIL_STACK_LIMIT: usize = 40_000 * 20;

/// REG_UNSET, and GNU's NULL string position.
const UNSET: isize = -1;

/// Reads the text a chunk at a time.  Chunks end on character boundaries,
/// so a character never straddles two.
struct Reader<'a> {
    /// The rope read a chunk at a time; None for a string, whose bytes
    /// are all of `chunk'.
    rope: Option<RopeRange<'a>>,
    chunk: &'a [u8],
    start: usize,
    /// Text::latin1_bytes.
    latin1_bytes: bool,
}

impl<'a> Reader<'a> {
    fn new(text: &Text<'a>) -> Self {
        let (rope, chunk) = match text.source() {
            super::TextSource::Str(text) => (None, text.as_bytes()),
            super::TextSource::Rope(rope) => (Some(rope), &[][..]),
        };
        Reader {
            rope,
            chunk,
            start: 0,
            latin1_bytes: text.latin1_bytes,
        }
    }

    #[inline(always)]
    fn ensure(&mut self, d: usize) {
        if d.wrapping_sub(self.start) >= self.chunk.len() {
            self.load(d);
        }
    }

    #[cold]
    #[inline(never)]
    fn load(&mut self, d: usize) {
        // A string's one run is already loaded.
        let Some(rope) = self.rope else {
            return;
        };
        let (chunk, start) = rope.chunk_at_byte(d);
        self.chunk = chunk;
        self.start = start;
    }

    #[inline(always)]
    fn byte(&mut self, d: usize) -> u8 {
        self.ensure(d);
        self.chunk[d - self.start]
    }

    /// The stored character at D and its length.
    #[inline(always)]
    fn stored(&mut self, d: usize) -> (char, usize) {
        self.ensure(d);
        decode_stored(&self.chunk[d - self.start..])
    }

    /// The start of the character that ends at D (D > 0).
    #[inline]
    fn prev_start(&mut self, d: usize) -> usize {
        self.ensure(d - 1);
        let mut i = d - 1 - self.start;
        while i > 0 && self.chunk[i] & 0xC0 == 0x80 {
            i -= 1;
        }
        self.start + i
    }

    /// The rest of the current chunk from D on.
    #[inline(always)]
    fn rest(&mut self, d: usize) -> &'a [u8] {
        self.ensure(d);
        &self.chunk[d - self.start..]
    }
}

/// The failure stack (`fail_stack_type') and the register arrays, kept
/// across the start positions of one search.
struct MatchState {
    stack: Vec<isize>,
    frame: usize,
    regstart: Vec<isize>,
    regend: Vec<isize>,
    best_regstart: Vec<isize>,
    best_regend: Vec<isize>,
}

thread_local! {
    /// The last finished search's arrays, reused as GNU reuses its failure
    /// stack's allocation instead of allocating for every search (a
    /// search inside a search makes its own).
    static SPARE_STATE: std::cell::Cell<Option<Box<MatchState>>> =
        const { std::cell::Cell::new(None) };
}

/// The failure stack length above which a finished search's stack is not
/// kept for the next one.
const SPARE_STACK_LIMIT: usize = 1 << 16;

/// A search's MatchState, given back to SPARE_STATE when it ends.
struct PooledState(std::mem::ManuallyDrop<Box<MatchState>>);

impl std::ops::Deref for PooledState {
    type Target = MatchState;
    fn deref(&self) -> &MatchState {
        &self.0
    }
}

impl std::ops::DerefMut for PooledState {
    fn deref_mut(&mut self) -> &mut MatchState {
        &mut self.0
    }
}

impl Drop for PooledState {
    fn drop(&mut self) {
        // SAFETY: the state is taken once, here, and not used after.
        let state = unsafe { std::mem::ManuallyDrop::take(&mut self.0) };
        if state.stack.capacity() <= SPARE_STACK_LIMIT {
            let _ = SPARE_STATE.try_with(|spare| spare.set(Some(state)));
        }
    }
}

impl PooledState {
    /// A MatchState for NUM_REGS registers, the last finished search's
    /// arrays when they are spare.  The best registers are written before
    /// they are read, as re_match_2_internal's are.
    fn new(num_regs: usize) -> Self {
        let mut st = SPARE_STATE
            .try_with(std::cell::Cell::take)
            .ok()
            .flatten()
            .unwrap_or_else(|| {
                Box::new(MatchState {
                    stack: Vec::new(),
                    frame: 0,
                    regstart: Vec::new(),
                    regend: Vec::new(),
                    best_regstart: Vec::new(),
                    best_regend: Vec::new(),
                })
            });
        st.stack.clear();
        st.frame = 0;
        for regs in [&mut st.regstart, &mut st.regend] {
            regs.clear();
            regs.resize(num_regs, UNSET);
        }
        for regs in [&mut st.best_regstart, &mut st.best_regend] {
            if regs.len() < num_regs {
                regs.resize(num_regs, UNSET);
            }
        }
        PooledState(std::mem::ManuallyDrop::new(st))
    }
}

impl MatchState {
    /// The start of re_match_2_internal: an empty failure stack and, when
    /// the pattern has groups, each group's registers unset.  Register 0
    /// is never read; a pattern without groups initializes nothing, as
    /// GNU does not "bother to initialize all the register variables".
    fn reset(&mut self) {
        self.stack.clear();
        self.frame = 0;
        if self.regstart.len() > 1 {
            self.regstart[1..].fill(UNSET);
            self.regend[1..].fill(UNSET);
        }
    }

    /// ENSURE_FAIL_STACK: room for SPACE more items, or GNU's -2.
    #[inline(always)]
    fn ensure(&self, space: usize) -> Result<(), MatchError> {
        if FAIL_STACK_LIMIT - self.stack.len() <= space {
            Err(MatchError::Overflow)
        } else {
            Ok(())
        }
    }

    /// PUSH_FAILURE_POINT.
    #[inline(always)]
    fn push_failure_point(&mut self, pattern: usize, string: isize) -> Result<(), MatchError> {
        self.ensure(3)?;
        self.stack.push(self.frame as isize);
        self.stack.push(string);
        self.stack.push(pattern as isize);
        self.frame = self.stack.len();
        Ok(())
    }

    /// PUSH_FAILURE_REG.
    #[inline(always)]
    fn push_failure_reg(&mut self, n: usize) -> Result<(), MatchError> {
        self.ensure(3)?;
        self.stack.push(self.regstart[n]);
        self.stack.push(self.regend[n]);
        self.stack.push(n as isize);
        Ok(())
    }

    /// PUSH_NUMBER: set the counter at AT to VALUE, restored on backtrack.
    fn push_number(&mut self, buf: &mut [u8], at: usize, value: i32) -> Result<(), MatchError> {
        self.ensure(3)?;
        self.stack.push(extract_number(buf, at) as isize);
        self.stack.push(at as isize);
        self.stack.push(-1);
        store_number(buf, at, value);
        Ok(())
    }

    /// POP_FAILURE_POINT: restore the registers and counters the top frame
    /// saved; its string and pattern positions.
    fn pop_failure_point(&mut self, buf: &mut [u8]) -> (isize, usize) {
        while self.frame < self.stack.len() {
            let reg = self.stack.pop().expect("in the frame");
            if reg == -1 {
                let at = self.stack.pop().expect("a counter's place") as usize;
                let value = self.stack.pop().expect("a counter's value") as i32;
                store_number(buf, at, value);
            } else {
                let reg = reg as usize;
                self.regend[reg] = self.stack.pop().expect("a register's end");
                self.regstart[reg] = self.stack.pop().expect("a register's start");
            }
        }
        let pattern = self.stack.pop().expect("a failure point") as usize;
        let string = self.stack.pop().expect("a failure point");
        self.frame = self.stack.pop().expect("a failure point") as usize;
        (string, pattern)
    }

    /// CHECK_INFINITE_LOOP: whether a failure point for PAT_CUR at the same
    /// string position is on the stack already.
    fn check_infinite_loop(&self, pat_cur: usize, string_place: isize) -> bool {
        let mut failure = self.frame;
        while failure > 0
            && (self.stack[failure - 2] == string_place || self.stack[failure - 2] == UNSET)
        {
            if self.stack[failure - 1] == pat_cur as isize {
                return true;
            }
            failure = self.stack[failure - 3] as usize;
        }
        false
    }
}

/// The Emacs character stored at D, with the side table consulted.
#[inline(always)]
fn code_at<E: MatchEnv + ?Sized>(
    rd: &mut Reader<'_>,
    d: usize,
    has_side: bool,
    env: &mut E,
) -> (u32, usize) {
    let (ch, len) = rd.stored(d);
    let mut code = code_of_stored(ch);
    if rd.latin1_bytes && (0x80..0x100).contains(&code) {
        // A unibyte buffer's byte: RE_CHAR_TO_MULTIBYTE makes it the
        // raw-byte character, as a unibyte string's marker reads.
        code = unibyte_to_char(code);
    }
    if has_side
        && stored_char_is_ambiguous(ch)
        && let Some(actual) = env.side_table_char(d)
    {
        code = actual;
    }
    (code, len)
}

/// category.h's WORD_BOUNDARY_P: two single-byte (ASCII or Latin-1)
/// word constituents never have a word boundary between them, and only
/// another pair asks category.c's word_boundary_p.
#[inline(always)]
fn word_boundary<E: MatchEnv + ?Sized>(env: &mut E, c1: u32, c2: u32) -> bool {
    !(c1 < 0x100 && c2 < 0x100) && env.word_boundary_p(c1, c2)
}

/// RE_STRING_CHAR for the text: the character at D, a unibyte text's as
/// its byte.
#[inline]
fn string_char<E: MatchEnv + ?Sized>(
    rd: &mut Reader<'_>,
    d: usize,
    target_multibyte: bool,
    has_side: bool,
    env: &mut E,
) -> (u32, usize) {
    let (code, len) = code_at(rd, d, has_side, env);
    if target_multibyte {
        (code, len)
    } else {
        (char_to_unibyte(code).unwrap_or(code), len)
    }
}

/// What an operation leaves the matcher to do.
enum Next {
    Continue,
    Fail,
    Accept,
}

/// re_match_2_internal: match PATTERN at POS, not past STOP.  The end of
/// the match on success, the registers left in ST.
fn re_match_2_internal<E: MatchEnv + ?Sized>(
    bufp: &mut Pattern,
    text: &Text<'_>,
    rd: &mut Reader<'_>,
    pos: usize,
    stop: usize,
    st: &mut MatchState,
    env: &mut E,
) -> Result<Option<usize>, MatchError> {
    let multibyte = bufp.multibyte;
    let translate = bufp.translate;
    let target_multibyte = text.multibyte;
    let has_side = text.has_side_table;
    let size = text.len();
    let num_regs = bufp.re_nsub + 1;
    let buf: &mut [u8] = &mut bufp.buffer;
    let pend = buf.len();

    st.reset();
    let mut best_regs_set = false;
    let mut match_end: isize = UNSET;
    let mut p = 0usize;
    let mut d = pos;

    macro_rules! tr {
        ($c:expr) => {{
            let c = $c;
            if translate { env.translate(c) } else { c }
        }};
    }

    // AT_STRINGS_BEG and AT_STRINGS_END: the text's own ends, not STOP.
    let at_strings_beg = |d: usize| d == 0 || size == 0;

    let accepted = 'matching: loop {
        let next = if p == pend {
            // The end of the pattern: maybe a match.
            if d != stop {
                // Try backtracking for the longest match (posix).
                let best_match_p = d as isize > match_end;
                if st.frame != 0 {
                    if !best_regs_set || best_match_p {
                        best_regs_set = true;
                        match_end = d as isize;
                        st.best_regstart[1..num_regs].copy_from_slice(&st.regstart[1..num_regs]);
                        st.best_regend[1..num_regs].copy_from_slice(&st.regend[1..num_regs]);
                    }
                    Next::Fail
                } else {
                    if best_regs_set && !best_match_p {
                        d = match_end as usize;
                        st.regstart[1..num_regs].copy_from_slice(&st.best_regstart[1..num_regs]);
                        st.regend[1..num_regs].copy_from_slice(&st.best_regend[1..num_regs]);
                    }
                    Next::Accept
                }
            } else {
                Next::Accept
            }
        } else {
            let opcode = buf[p];
            p += 1;
            match opcode {
                op::NO_OP => Next::Continue,

                op::SUCCEED => Next::Accept,

                op::EXACTN => 'exactn: {
                    let mut mcnt = usize::from(buf[p]);
                    p += 1;
                    let dfail = d;
                    if !translate && !has_side && !rd.latin1_bytes {
                        // Equal characters are equal stored bytes.
                        if stop - d < mcnt {
                            break 'exactn Next::Fail;
                        }
                        let mut want = &buf[p..p + mcnt];
                        let mut at = d;
                        while !want.is_empty() {
                            let rest = rd.rest(at);
                            let n = rest.len().min(want.len());
                            // GNU's byte loop: an attempt nearly always
                            // fails at its first byte, tested here before
                            // the rest is compared.
                            if rest[0] != want[0] || rest[1..n] != want[1..n] {
                                break 'exactn Next::Fail;
                            }
                            want = &want[n..];
                            at += n;
                        }
                        p += mcnt;
                        d += mcnt;
                        break 'exactn Next::Continue;
                    }
                    loop {
                        if d == stop {
                            d = dfail;
                            break 'exactn Next::Fail;
                        }
                        let (pat_stored, pat_len) = decode_stored(&buf[p..]);
                        let pat_mb = code_of_stored(pat_stored);
                        let (buf_ch, buf_len) = code_at(rd, d, has_side, env);
                        let same = if target_multibyte {
                            tr!(buf_ch) == pat_mb
                        } else {
                            let pat_ch = char_to_unibyte(pat_mb);
                            let raw = char_to_unibyte(buf_ch).unwrap_or(buf_ch);
                            let buf_byte = if char_byte8_p(buf_ch) {
                                raw
                            } else {
                                char_to_unibyte(tr!(buf_ch)).unwrap_or(raw)
                            };
                            pat_ch == Some(buf_byte)
                        };
                        if !same {
                            d = dfail;
                            break 'exactn Next::Fail;
                        }
                        p += pat_len;
                        d += buf_len;
                        mcnt -= pat_len;
                        if mcnt == 0 {
                            break 'exactn Next::Continue;
                        }
                    }
                }

                op::ANYCHAR => {
                    if d == stop {
                        Next::Fail
                    } else if !translate {
                        let lead = rd.byte(d);
                        if lead == b'\n' {
                            Next::Fail
                        } else {
                            d += bytes_by_char_head(lead);
                            Next::Continue
                        }
                    } else {
                        let (c, len) = string_char(rd, d, target_multibyte, has_side, env);
                        if tr!(c) == u32::from(b'\n') {
                            Next::Fail
                        } else {
                            d += len;
                            Next::Continue
                        }
                    }
                }

                op::CHARSET | op::CHARSET_NOT => 'charset: {
                    let at = p - 1;
                    if d == stop {
                        break 'charset Next::Fail;
                    }
                    let lead = rd.byte(d);
                    let matched;
                    let len;
                    if lead < 0x80 && !translate {
                        // An ASCII character tests the bitmap alone.
                        let index = usize::from(lead);
                        let not = opcode == op::CHARSET_NOT;
                        let in_map = index < charset_bitmap_size_of(buf, at) * 8
                            && buf[at + 2 + index / 8] & (1 << (index % 8)) != 0;
                        matched = in_map != not;
                        len = 1;
                    } else {
                        let (code, clen) = code_at(rd, d, has_side, env);
                        len = clen;
                        let mut unibyte_char = false;
                        let corig;
                        let mut c;
                        if target_multibyte {
                            corig = code;
                            c = tr!(code);
                            if let Some(byte) = char_to_unibyte(c) {
                                unibyte_char = true;
                                c = byte;
                            }
                        } else {
                            corig = char_to_unibyte(code).unwrap_or(code);
                            c = corig;
                            if !char_byte8_p(code) {
                                if let Some(byte) = char_to_unibyte(tr!(code)) {
                                    unibyte_char = true;
                                    c = byte;
                                }
                            } else {
                                unibyte_char = true;
                            }
                        }
                        matched = execute_charset(buf, at, c, corig, unibyte_char, translate, env);
                    }
                    if !matched {
                        break 'charset Next::Fail;
                    }
                    p = charset_end(buf, at);
                    d += len;
                    Next::Continue
                }

                op::START_MEMORY => {
                    let reg = usize::from(buf[p]);
                    st.push_failure_reg(reg)?;
                    st.regstart[reg] = d as isize;
                    p += 1;
                    Next::Continue
                }

                op::STOP_MEMORY => {
                    let reg = usize::from(buf[p]);
                    st.regend[reg] = d as isize;
                    p += 1;
                    Next::Continue
                }

                op::DUPLICATE => 'duplicate: {
                    let regno = usize::from(buf[p]);
                    p += 1;
                    if st.regend[regno] == UNSET {
                        break 'duplicate Next::Fail;
                    }
                    let mut d2 = st.regstart[regno] as usize;
                    let dend2 = st.regend[regno] as usize;
                    let dfail = d;
                    while d2 != dend2 {
                        if d == stop {
                            d = dfail;
                            break 'duplicate Next::Fail;
                        }
                        let dcnt = (stop - d).min(dend2 - d2);
                        let differ = if translate || has_side {
                            compare_translated(rd, d, d2, dcnt, translate, has_side, env)
                        } else {
                            compare_bytes(rd, d, d2, dcnt)
                        };
                        if differ {
                            d = dfail;
                            break 'duplicate Next::Fail;
                        }
                        d += dcnt;
                        d2 += dcnt;
                    }
                    Next::Continue
                }

                op::BEGLINE => {
                    if at_strings_beg(d) || rd.byte(d - 1) == b'\n' {
                        Next::Continue
                    } else {
                        Next::Fail
                    }
                }

                op::ENDLINE => {
                    if d == size || rd.byte(d) == b'\n' {
                        Next::Continue
                    } else {
                        Next::Fail
                    }
                }

                op::BEGBUF => {
                    if at_strings_beg(d) {
                        Next::Continue
                    } else {
                        Next::Fail
                    }
                }

                op::ENDBUF => {
                    if d == size {
                        Next::Continue
                    } else {
                        Next::Fail
                    }
                }

                op::ON_FAILURE_KEEP_STRING_JUMP => {
                    p += 2;
                    st.push_failure_point(p - 3, UNSET)?;
                    Next::Continue
                }

                op::ON_FAILURE_JUMP_NASTYLOOP => {
                    p += 2;
                    // A cycle: continue without the "try again" point.
                    if !st.check_infinite_loop(p - 4, d as isize) {
                        st.push_failure_point(p - 3, d as isize)?;
                    }
                    Next::Continue
                }

                op::ON_FAILURE_JUMP_LOOP => {
                    on_failure_jump_loop(buf, &mut p, d, st)?;
                    Next::Continue
                }

                op::ON_FAILURE_JUMP => {
                    p += 2;
                    st.push_failure_point(p - 3, d as isize)?;
                    Next::Continue
                }

                op::ON_FAILURE_JUMP_SMART => {
                    let mcnt = extract_number(buf, p);
                    p += 2;
                    // The next operation, the jump's destination, and this
                    // opcode, re-executed once rewritten.
                    let p1 = p;
                    let p2 = (p as isize + mcnt as isize) as usize;
                    let p3 = p - 3;
                    p = p3;
                    let jump = extract_number(buf, p2 - 2);
                    if mutually_exclusive_p(buf, multibyte, p1, p2, env) {
                        // A fast loop that keeps the string position.
                        buf[p3] = op::ON_FAILURE_KEEP_STRING_JUMP;
                        store_number(buf, p2 - 2, jump + 3);
                    } else {
                        buf[p3] = op::ON_FAILURE_JUMP;
                    }
                    Next::Continue
                }

                op::JUMP => {
                    env.maybe_quit()?;
                    let mcnt = extract_number(buf, p);
                    p = (p as isize + 2 + mcnt as isize) as usize;
                    Next::Continue
                }

                op::SUCCEED_N => {
                    let mcnt = extract_number(buf, p + 2);
                    if mcnt != 0 {
                        let counter = p + 2;
                        p += 4;
                        st.push_number(buf, counter, mcnt - 1)?;
                    } else {
                        // The zero count's two bytes are two no_ops.
                        on_failure_jump_loop(buf, &mut p, d, st)?;
                    }
                    Next::Continue
                }

                op::JUMP_N => {
                    let mcnt = extract_number(buf, p + 2);
                    if mcnt != 0 {
                        let counter = p + 2;
                        st.push_number(buf, counter, mcnt - 1)?;
                        env.maybe_quit()?;
                        let offset = extract_number(buf, p);
                        p = (p as isize + 2 + offset as isize) as usize;
                    } else {
                        p += 4;
                    }
                    Next::Continue
                }

                op::SET_NUMBER_AT => {
                    let offset = extract_number(buf, p);
                    p += 2;
                    let counter = (p as isize + offset as isize) as usize;
                    let value = extract_number(buf, p);
                    p += 2;
                    st.push_number(buf, counter, value)?;
                    Next::Continue
                }

                op::WORDBOUND | op::NOTWORDBOUND => {
                    let mut not = opcode == op::NOTWORDBOUND;
                    if at_strings_beg(d) || d == size {
                        not = !not;
                    } else {
                        let before = rd.prev_start(d);
                        let (c1, _) = code_at(rd, before, has_side, env);
                        let s1 = env.syntax(c1, before);
                        let (c2, _) = code_at(rd, d, has_side, env);
                        let s2 = env.syntax(c2, d);
                        if (s1 == syntax_code::WORD) != (s2 == syntax_code::WORD)
                            || (s1 == syntax_code::WORD && word_boundary(env, c1, c2))
                        {
                            not = !not;
                        }
                    }
                    if not { Next::Continue } else { Next::Fail }
                }

                op::WORDBEG => 'wordbeg: {
                    if d == size || d == stop {
                        break 'wordbeg Next::Fail;
                    }
                    let (c2, _) = code_at(rd, d, has_side, env);
                    let s2 = env.syntax(c2, d);
                    if s2 != syntax_code::WORD {
                        break 'wordbeg Next::Fail;
                    }
                    if !at_strings_beg(d) {
                        let before = rd.prev_start(d);
                        let (c1, _) = code_at(rd, before, has_side, env);
                        let s1 = env.syntax(c1, before);
                        if s1 == syntax_code::WORD && !word_boundary(env, c1, c2) {
                            break 'wordbeg Next::Fail;
                        }
                    }
                    Next::Continue
                }

                op::WORDEND => 'wordend: {
                    if at_strings_beg(d) {
                        break 'wordend Next::Fail;
                    }
                    let before = rd.prev_start(d);
                    let (c1, _) = code_at(rd, before, has_side, env);
                    let s1 = env.syntax(c1, before);
                    if s1 != syntax_code::WORD {
                        break 'wordend Next::Fail;
                    }
                    if d != size {
                        let (c2, _) = code_at(rd, d, has_side, env);
                        let s2 = env.syntax(c2, d);
                        if s2 == syntax_code::WORD && !word_boundary(env, c1, c2) {
                            break 'wordend Next::Fail;
                        }
                    }
                    Next::Continue
                }

                op::SYMBEG => 'symbeg: {
                    if d == size || d == stop {
                        break 'symbeg Next::Fail;
                    }
                    let (c2, _) = string_char(rd, d, target_multibyte, has_side, env);
                    let s2 = env.syntax(c2, d);
                    if s2 != syntax_code::WORD && s2 != syntax_code::SYMBOL {
                        break 'symbeg Next::Fail;
                    }
                    if !at_strings_beg(d) {
                        let before = rd.prev_start(d);
                        let (c1, _) = code_at(rd, before, has_side, env);
                        let s1 = env.syntax(c1, before);
                        if s1 == syntax_code::WORD || s1 == syntax_code::SYMBOL {
                            break 'symbeg Next::Fail;
                        }
                    }
                    Next::Continue
                }

                op::SYMEND => 'symend: {
                    if at_strings_beg(d) {
                        break 'symend Next::Fail;
                    }
                    let before = rd.prev_start(d);
                    let (c1, _) = code_at(rd, before, has_side, env);
                    let s1 = env.syntax(c1, before);
                    if s1 != syntax_code::WORD && s1 != syntax_code::SYMBOL {
                        break 'symend Next::Fail;
                    }
                    if d != size {
                        let (c2, _) = string_char(rd, d, target_multibyte, has_side, env);
                        let s2 = env.syntax(c2, d);
                        if s2 == syntax_code::WORD || s2 == syntax_code::SYMBOL {
                            break 'symend Next::Fail;
                        }
                    }
                    Next::Continue
                }

                op::SYNTAXSPEC | op::NOTSYNTAXSPEC => {
                    let not = opcode == op::NOTSYNTAXSPEC;
                    let class = buf[p];
                    p += 1;
                    if d == stop {
                        Next::Fail
                    } else {
                        let (c, len) = code_at(rd, d, has_side, env);
                        if (env.syntax(c, d) != class) != not {
                            Next::Fail
                        } else {
                            d += len;
                            Next::Continue
                        }
                    }
                }

                op::AT_DOT => {
                    if env.point() == Some(d) {
                        Next::Continue
                    } else {
                        Next::Fail
                    }
                }

                op::CATEGORYSPEC | op::NOTCATEGORYSPEC => {
                    let not = opcode == op::NOTCATEGORYSPEC;
                    let category = buf[p];
                    p += 1;
                    if d == stop {
                        Next::Fail
                    } else {
                        let (c, len) = code_at(rd, d, has_side, env);
                        if !env.has_category(c, category) != not {
                            Next::Fail
                        } else {
                            d += len;
                            Next::Continue
                        }
                    }
                }

                _ => unreachable!("regexp opcode {opcode}"),
            }
        };

        match next {
            Next::Continue => continue,
            Next::Accept => break 'matching true,
            Next::Fail => loop {
                // fail: back to the last failure point.
                env.maybe_quit()?;
                if st.frame == 0 {
                    break 'matching false;
                }
                let (string, pat) = st.pop_failure_point(buf);
                match buf[pat] {
                    op::ON_FAILURE_KEEP_STRING_JUMP => {
                        p = extract_address(buf, pat + 1);
                    }
                    op::ON_FAILURE_JUMP_NASTYLOOP => {
                        // The cycle-detection frame for the next iteration.
                        st.push_failure_point(pat - 1, string)?;
                        d = string as usize;
                        p = extract_address(buf, pat + 1);
                    }
                    op::ON_FAILURE_JUMP_LOOP | op::ON_FAILURE_JUMP | op::SUCCEED_N => {
                        d = string as usize;
                        p = extract_address(buf, pat + 1);
                    }
                    // A nastyloop's cycle-detection frame.
                    op::NO_OP => continue,
                    other => unreachable!("failure point at opcode {other}"),
                }
                break;
            },
        }
    };

    if accepted {
        return Ok(Some(d));
    }
    if best_regs_set {
        // restore_best_regs
        st.regstart[1..num_regs].copy_from_slice(&st.best_regstart[1..num_regs]);
        st.regend[1..num_regs].copy_from_slice(&st.best_regend[1..num_regs]);
        return Ok(Some(match_end as usize));
    }
    Ok(None)
}

/// CHARSET_BITMAP_SIZE, local to the hot charset arm.
#[inline(always)]
fn charset_bitmap_size_of(buf: &[u8], p: usize) -> usize {
    usize::from(buf[p + 1] & 0x7F)
}

/// on_failure_jump_loop, also reached from a succeed_n whose count is
/// zero: P is just past the opcode.
#[inline]
fn on_failure_jump_loop(
    buf: &[u8],
    p: &mut usize,
    d: usize,
    st: &mut MatchState,
) -> Result<(), MatchError> {
    let mcnt = extract_number(buf, *p);
    *p += 2;
    if st.check_infinite_loop(*p - 3, d as isize) {
        // Leave the loop as if it had failed, keeping the empty match.
        *p = (*p as isize + mcnt as isize) as usize;
        Ok(())
    } else {
        st.push_failure_point(*p - 3, d as isize)
    }
}

/// memcmp of DCNT bytes at D and D2: whether they differ.
fn compare_bytes(rd: &mut Reader<'_>, mut d: usize, mut d2: usize, mut dcnt: usize) -> bool {
    while dcnt > 0 {
        let a = rd.rest(d);
        let n = a.len().min(dcnt);
        let a = &a[..n];
        let b = rd.rest(d2);
        let n = n.min(b.len());
        if a[..n] != b[..n] {
            return true;
        }
        d += n;
        d2 += n;
        dcnt -= n;
    }
    false
}

/// bcmp_translate over DCNT bytes at D and D2: whether the characters
/// differ after translation.
fn compare_translated<E: MatchEnv + ?Sized>(
    rd: &mut Reader<'_>,
    d: usize,
    d2: usize,
    dcnt: usize,
    translate: bool,
    has_side: bool,
    env: &mut E,
) -> bool {
    let (mut p1, mut p2) = (d, d2);
    let (p1_end, p2_end) = (d + dcnt, d2 + dcnt);
    while p1 < p1_end && p2 < p2_end {
        let (c1, len1) = code_at(rd, p1, has_side, env);
        let (c2, len2) = code_at(rd, p2, has_side, env);
        let (c1, c2) = if translate {
            (env.translate(c1), env.translate(c2))
        } else {
            (c1, c2)
        };
        if c1 != c2 {
            return true;
        }
        p1 += len1;
        p2 += len2;
    }
    p1 != p1_end || p2 != p2_end
}

/// re_match_2_internal's stores into REGS for a match ending at END that
/// began at POS: group 0 the whole match, each other group its extent or
/// unset.  REGS keeps its allocation from search to search, as
/// `search_regs' does.
fn store_registers(regs: &mut Registers, st: &MatchState, num_regs: usize, pos: usize, end: usize) {
    let regs = &mut regs.regs;
    regs.clear();
    regs.push(Some((pos, end)));
    for reg in 1..num_regs {
        if st.regend[reg] == UNSET {
            regs.push(None);
        } else {
            regs.push(Some((st.regstart[reg] as usize, st.regend[reg] as usize)));
        }
    }
}

/// re_match_2: match PATTERN at POS, not past STOP; the end of the match.
/// The registers go to REGS when given (GNU's REGS argument), and only on
/// a match.
pub(crate) fn re_match<E: MatchEnv + ?Sized>(
    pattern: &mut Pattern,
    text: &Text<'_>,
    pos: usize,
    stop: usize,
    regs: Option<&mut Registers>,
    env: &mut E,
) -> Result<Option<usize>, MatchError> {
    env.setup_syntax(pos);
    let num_regs = pattern.re_nsub + 1;
    let mut st = PooledState::new(num_regs);
    let mut rd = Reader::new(text);
    let end = re_match_2_internal(pattern, text, &mut rd, pos, stop, &mut st, env)?;
    if let (Some(end), Some(regs)) = (end, regs) {
        store_registers(regs, &st, num_regs, pos, end);
    }
    Ok(end)
}

/// Whether the character at D, translated, can start a match: the fastmap
/// test re_search_2 makes with a translation (or for a unibyte text).
#[inline]
fn fastmap_admits<E: MatchEnv + ?Sized>(
    pattern: &Pattern,
    rd: &mut Reader<'_>,
    d: usize,
    target_multibyte: bool,
    has_side: bool,
    env: &mut E,
) -> bool {
    let (code, _) = code_at(rd, d, has_side, env);
    code_admits(pattern, code, target_multibyte, env)
}

/// Whether character CODE of a text, translated, can start a match.
#[inline(always)]
fn code_admits<E: MatchEnv + ?Sized>(
    pattern: &Pattern,
    code: u32,
    target_multibyte: bool,
    env: &mut E,
) -> bool {
    let lead = if target_multibyte {
        let c = if pattern.translate {
            env.translate(code)
        } else {
            code
        };
        leading_byte(c)
    } else {
        let mut c = code;
        if !char_byte8_p(code) && pattern.translate {
            let translated = env.translate(code);
            if translated != code
                && let Some(byte) = char_to_unibyte(translated)
            {
                c = unibyte_to_char(byte);
            }
        }
        leading_byte(c)
    };
    pattern.fastmap[usize::from(lead)]
}

/// `code_admits' of each ASCII character, as PATTERN keeps it for the
/// current translation.
fn ascii_admits<E: MatchEnv + ?Sized>(
    pattern: &mut Pattern,
    target_multibyte: bool,
    env: &mut E,
) -> u128 {
    let stamp = env.translate_stamp();
    if let Some((at, multibyte, bits)) = pattern.ascii_admits
        && at == stamp
        && multibyte == target_multibyte
    {
        return bits;
    }
    // A search cache miss compiles the pattern afresh, so this runs once
    // a compile: a tight loop over the translation's ASCII entries.
    let mut bits = 0u128;
    for c in 0..128u32 {
        if code_admits(pattern, c, target_multibyte, env) {
            bits |= 1 << c;
        }
    }
    pattern.ascii_admits = Some((stamp, target_multibyte, bits));
    bits
}

/// re_search_2: the start of the first match of PATTERN starting between
/// STARTPOS and STARTPOS + RANGE (a backward search when RANGE is
/// negative), not running past STOP.  The registers go to REGS when given,
/// and only on a match.
pub(crate) fn re_search<E: MatchEnv + ?Sized>(
    pattern: &mut Pattern,
    text: &Text<'_>,
    startpos: usize,
    range: isize,
    stop: usize,
    regs: Option<&mut Registers>,
    env: &mut E,
) -> Result<Option<usize>, MatchError> {
    let total = text.len() as isize;
    let mut startpos = startpos as isize;
    let mut range = range;
    let target_multibyte = text.multibyte;
    let has_side = text.has_side_table;

    if startpos < 0 || startpos > total {
        return Ok(None);
    }
    let endpos = startpos + range;
    if endpos < 0 {
        range = -startpos;
    } else if endpos > total {
        range = total - startpos;
    }

    // A forward search for a pattern anchored at the beginning.
    let first_op = pattern.buffer.first().copied();
    if first_op == Some(op::BEGBUF) && range > 0 {
        if startpos > 0 {
            return Ok(None);
        }
        range = 0;
    }
    // A forward search for something that starts with \= stops at point.
    if first_op == Some(op::AT_DOT)
        && range > 0
        && let Some(point) = env.point()
    {
        range = point as isize - startpos;
        if range < 0 {
            return Ok(None);
        }
    }

    if !pattern.fastmap_accurate {
        re_compile_fastmap(pattern, env);
    }
    let anchored_start = first_op == Some(op::BEGLINE);
    env.setup_syntax(startpos as usize);
    let num_regs = pattern.re_nsub + 1;
    let mut st = PooledState::new(num_regs);
    let mut rd = Reader::new(text);
    // With no translation, the scan tests bytes: a fastmap entry is set
    // only for a character's first byte, never a continuation byte, and a
    // side-table character's lead is its stored form's.
    let plain_scan = !pattern.translate && !text.latin1_bytes;

    loop {
        'attempt: {
            if anchored_start && startpos > 0 && rd.byte(startpos as usize - 1) != b'\n' {
                break 'attempt;
            }

            if startpos < total && !pattern.can_be_null {
                if range > 0 {
                    // Skip quickly over characters that cannot start a match.
                    let mut d = startpos as usize;
                    let limit = (startpos + range) as usize;
                    if plain_scan {
                        while d < limit {
                            let rest = rd.rest(d);
                            let n = rest.len().min(limit - d);
                            let window = &rest[..n];
                            let found = match pattern.fastmap_leads {
                                FastmapLeads::One(byte) => memchr::memchr(byte, window),
                                FastmapLeads::None => None,
                                FastmapLeads::Many => {
                                    window.iter().position(|&b| pattern.fastmap[usize::from(b)])
                                }
                            };
                            match found {
                                Some(offset) => {
                                    d += offset;
                                    break;
                                }
                                None => d += n,
                            }
                        }
                    } else {
                        // Each character translated and tested, an ASCII
                        // one by its verdict.
                        let admits = ascii_admits(pattern, target_multibyte, env);
                        'scan: while d < limit {
                            let rest = rd.rest(d);
                            let n = rest.len().min(limit - d);
                            let mut i = 0;
                            while i < n {
                                let byte = rest[i];
                                if byte < 0x80 {
                                    if admits & (1 << byte) != 0 {
                                        d += i;
                                        break 'scan;
                                    }
                                    i += 1;
                                } else {
                                    if fastmap_admits(
                                        pattern,
                                        &mut rd,
                                        d + i,
                                        target_multibyte,
                                        has_side,
                                        env,
                                    ) {
                                        d += i;
                                        break 'scan;
                                    }
                                    i += bytes_by_char_head(byte);
                                }
                            }
                            d += i;
                        }
                    }
                    let skipped = d as isize - startpos;
                    range -= skipped;
                    startpos = d as isize;
                } else if !fastmap_admits(
                    pattern,
                    &mut rd,
                    startpos as usize,
                    target_multibyte,
                    has_side,
                    env,
                ) {
                    break 'attempt;
                }
            }

            // Nothing left but the end, which cannot match the null string.
            if range >= 0 && startpos == total && !pattern.can_be_null {
                return Ok(None);
            }

            let start = startpos as usize;
            if let Some(end) =
                re_match_2_internal(pattern, text, &mut rd, start, stop, &mut st, env)?
            {
                if let Some(regs) = regs {
                    store_registers(regs, &st, num_regs, start, end);
                }
                return Ok(Some(start));
            }
        }

        // advance
        if range == 0 {
            break;
        } else if range > 0 {
            let len = bytes_by_char_head(rd.byte(startpos as usize)) as isize;
            range -= len;
            if range < 0 {
                break;
            }
            startpos += len;
        } else {
            range += 1;
            startpos -= 1;
            // To the previous character boundary.
            let char_start = rd.prev_start(startpos as usize + 1) as isize;
            let len = startpos + 1 - char_start;
            range += len - 1;
            if range > 0 {
                break;
            }
            startpos -= len - 1;
        }
    }
    Ok(None)
}
