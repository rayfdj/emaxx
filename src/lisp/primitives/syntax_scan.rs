//! syntax.c's scanners: gl_state with its intervals and just-in-time
//! `syntax-propertize', char_quoted, find_defun_start, back_comment,
//! forw_comment, `forward-comment', scan_lists (`scan-lists',
//! `scan-sexps'), `backward-prefix-chars', skip_syntaxes and
//! scan_sexps_forward (`parse-partial-sexp').
//!
//! The text is a snapshot of the buffer's rope read in place, and
//! positions travel as GNU's (charpos, bytepos) pairs: INC_BOTH and
//! DEC_BOTH step both, FETCH_CHAR_AS_MULTIBYTE reads at the byte.  Byte
//! positions count Emaxx's UTF-8 storage rather than GNU's internal
//! encoding, and none leaves this module.  The snapshot stays valid
//! through the Lisp the scanners call (`internal--syntax-propertize',
//! `syntax-ppss'), since GNU signals when either changes the text.

use super::*;
use crate::buffer::{PropertyInterval, PropertyIntervals, PropertySpan};
use crate::lisp::regex_emacs as rx;
use crate::lisp::types::{CharTableRef, Kind};

// syntax.h's enum syntaxcode.
const SWHITESPACE: i32 = 0;
const SWORD: i32 = 2;
const SSYMBOL: i32 = 3;
const SOPEN: i32 = 4;
const SCLOSE: i32 = 5;
const SQUOTE: i32 = 6;
const SSTRING: i32 = 7;
const SMATH: i32 = 8;
const SESCAPE: i32 = 9;
const SCHARQUOTE: i32 = 10;
const SCOMMENT: i32 = 11;
const SENDCOMMENT: i32 = 12;
const SCOMMENT_FENCE: i32 = 14;
const SSTRING_FENCE: i32 = 15;
const SMAX: i32 = 16;

/// The comment style and string terminator of comments and strings a
/// comment fence or string fence opened.
const ST_COMMENT_STYLE: i32 = 256 + 1;
const ST_STRING_STYLE: i32 = 256 + 2;

/// 1 + the most intervals update_syntax_table walks to a property change.
const INTERVALS_AT_ONCE: usize = 10;

fn flags_comstart_first(flags: i32) -> bool {
    (flags >> 16) & 1 != 0
}
fn flags_comstart_second(flags: i32) -> bool {
    (flags >> 17) & 1 != 0
}
fn flags_comend_first(flags: i32) -> bool {
    (flags >> 18) & 1 != 0
}
fn flags_comend_second(flags: i32) -> bool {
    (flags >> 19) & 1 != 0
}
fn flags_comstartend_first(flags: i32) -> bool {
    flags & 0x50000 != 0
}
fn flags_prefix(flags: i32) -> bool {
    (flags >> 20) & 1 != 0
}
fn flags_comment_styleb(flags: i32) -> i32 {
    (flags >> 21) & 1
}
fn flags_comment_stylec2(flags: i32) -> i32 {
    (flags >> 22) & 2
}
fn flags_comment_nested(flags: i32) -> bool {
    (flags >> 22) & 1 != 0
}

/// SYNTAX_FLAGS_COMMENT_STYLE: FLAGS are the main character's (the
/// second of a comment starter, the first of an ender).
fn flags_comment_style(flags: i32, other_flags: i32) -> i32 {
    flags_comment_styleb(flags) | flags_comment_stylec2(flags) | flags_comment_stylec2(other_flags)
}

/// SYNTAX_WITH_FLAGS of a syntax-table entry: a descriptor's car, and
/// whitespace for anything that is not a cons.
#[inline]
pub(super) fn entry_flags(entry: Value) -> i32 {
    if !entry.is_cons() {
        return SWHITESPACE;
    }
    match entry.car().map(|car| car.kind()) {
        Ok(Kind::Integer(code)) => code as i32,
        _ => SWHITESPACE,
    }
}

fn error(message: &str) -> LispError {
    LispError::Signal(message.into())
}

fn scan_error(message: &str, last_good: i64, from: i64) -> LispError {
    LispError::SignalValue(Value::list([
        Value::Symbol("scan-error".into()),
        Value::String(message.into()),
        Value::Integer(last_good),
        Value::Integer(from),
    ]))
}

fn args_out_of_range(values: impl IntoIterator<Item = Value>) -> LispError {
    LispError::SignalValue(Value::list(
        std::iter::once(Value::symbol("args-out-of-range")).chain(values),
    ))
}

/// CHECK_FIXNUM.
pub(super) fn check_fixnum(value: &Value) -> Result<i64, LispError> {
    match value.kind() {
        Kind::Integer(n) => Ok(n),
        _ => Err(LispError::WrongTypeArgument("fixnump".into(), *value)),
    }
}

/// CHECK_FIXNUM_COERCE_MARKER.
fn fixnum_coerce_marker(interp: &Interpreter, value: &Value) -> Result<i64, LispError> {
    match value.kind() {
        Kind::Integer(n) => Ok(n),
        Kind::Marker(id) => interp
            .marker_position(id)
            .map(|pos| pos as i64)
            .ok_or_else(|| error("Marker does not point anywhere")),
        _ => Err(LispError::WrongTypeArgument(
            "integer-or-marker-p".into(),
            *value,
        )),
    }
}

/// The buffer's text as FETCH_CHAR_AS_MULTIBYTE, INC_BOTH and DEC_BOTH
/// read it, one rope chunk at a time.  Positions are 1-based like GNU's.
struct Text<'r> {
    rope: &'r ropey::Rope,
    len: usize,
    ascii: bool,
    /// The buffer is unibyte, holding the bytes 0x80 to 0xFF as U+0080 to
    /// U+00FF: those read as raw-byte characters.
    unibyte: bool,
    /// The side table's characters beyond Unicode, by character position.
    extended: Vec<(usize, u32)>,
    chunk: &'r [u8],
    chunk_start: usize,
}

impl<'r> Text<'r> {
    fn new(rope: &'r ropey::Rope, multibyte: bool, extended: Vec<(usize, u32)>) -> Self {
        let len = rope.len_bytes();
        Text {
            rope,
            len,
            ascii: len == rope.len_chars(),
            unibyte: !multibyte,
            extended,
            chunk: &[],
            chunk_start: 0,
        }
    }

    /// CHAR_TO_BYTE.
    fn char_to_byte(&self, charpos: usize) -> usize {
        if self.ascii {
            charpos
        } else {
            self.rope
                .char_to_byte((charpos - 1).min(self.rope.len_chars()))
                + 1
        }
    }

    #[inline]
    fn byte_at(&mut self, index: usize) -> u8 {
        if index.wrapping_sub(self.chunk_start) >= self.chunk.len() {
            self.load(index);
        }
        self.chunk[index - self.chunk_start]
    }

    #[cold]
    fn load(&mut self, index: usize) {
        let (chunk, start, _, _) = self.rope.chunk_at_byte(index);
        self.chunk = chunk.as_bytes();
        self.chunk_start = start;
    }

    /// FETCH_CHAR_AS_MULTIBYTE.
    #[inline]
    fn fetch(&mut self, bytepos: usize) -> u32 {
        let index = bytepos - 1;
        if index >= self.len {
            return 0;
        }
        let lead = self.byte_at(index);
        if lead < 0x80 {
            return u32::from(lead);
        }
        let code = self.stored_code(index);
        if self.unibyte && code < 0x100 {
            rx::unibyte_to_char(code)
        } else {
            code
        }
    }

    /// FETCH_CHAR: a unibyte buffer's byte as itself.
    #[inline]
    fn fetch_char(&mut self, bytepos: usize) -> u32 {
        let index = bytepos - 1;
        if index >= self.len {
            return 0;
        }
        let lead = self.byte_at(index);
        if lead < 0x80 {
            return u32::from(lead);
        }
        self.stored_code(index)
    }

    /// The Emacs character stored at byte INDEX, whose lead byte is not
    /// ASCII; a unibyte buffer's byte is its value, whether stored as
    /// U+0080..U+00FF or as a raw-byte marker.
    fn stored_code(&mut self, index: usize) -> u32 {
        let (ch, _) = rx::decode_stored(&self.chunk[index - self.chunk_start..]);
        if self.unibyte {
            let code = rx::code_of_stored(ch);
            return if rx::char_byte8_p(code) {
                code - rx::BYTE8_BASE
            } else {
                code
            };
        }
        if ch == rx::EXTENDED_PLACEHOLDER && !self.extended.is_empty() {
            let charpos = self.rope.byte_to_char(index) + 1;
            if let Ok(found) = self
                .extended
                .binary_search_by_key(&charpos, |(pos, _)| *pos)
            {
                return self.extended[found].1;
            }
        }
        rx::code_of_stored(ch)
    }

    /// INC_BOTH.
    #[inline]
    fn inc(&mut self, pos: &mut usize, byte: &mut usize) {
        let index = *byte - 1;
        let len = if self.ascii || index >= self.len {
            1
        } else {
            rx::bytes_by_char_head(self.byte_at(index))
        };
        *pos += 1;
        *byte += len;
    }

    /// DEC_BOTH.
    #[inline]
    fn dec(&mut self, pos: &mut usize, byte: &mut usize) {
        *pos -= 1;
        *byte = self.dec_bytepos(*byte);
    }

    /// dec_bytepos: the byte position of the character before BYTEPOS.
    #[inline]
    fn dec_bytepos(&mut self, bytepos: usize) -> usize {
        if self.ascii {
            return bytepos - 1;
        }
        let mut index = bytepos - 2;
        while self.byte_at(index) & 0xC0 == 0x80 {
            index -= 1;
        }
        index + 1
    }
}

/// gl_state.object: what the scan's text and its properties belong to.
#[derive(Clone, Copy)]
pub(super) enum GlObject {
    /// nil: the current buffer, propertized just in time.
    Buffer,
    /// A string, with its own text properties.
    String(Value),
    /// t: text without properties (a C string).
    Plain,
}

/// syntax.h's gl_state, with SYNTAX's ASCII entries of the table in use.
/// An interval is held by its bounds.
pub(super) struct GlState {
    object: GlObject,
    start: usize,
    stop: usize,
    use_global: bool,
    /// SYNTAX_WITH_FLAGS of `global_code', the syntax-table property
    /// descriptor in force.
    global_code: i32,
    current_syntax_table: CharTableRef,
    old_prop: Value,
    b_property: usize,
    e_property: usize,
    e_property_truncated: bool,
    forward_i: Option<(usize, usize)>,
    backward_i: Option<(usize, usize)>,
    /// The buffer's own syntax table (BVAR (current_buffer, syntax_table)).
    buffer_table: CharTableRef,
    /// parse_sexp_lookup_properties.
    lookup_properties: bool,
    /// syntax_propertize__done.
    propertize_done: i64,
    /// Whether `internal--syntax-propertize' has run since this was last
    /// cleared: what a scan read of Lisp state is then stale.
    ran_lisp: bool,
    /// SYNTAX_WITH_FLAGS of the ASCII characters in `current_syntax_table'
    /// read so far.  Entries change only through Lisp, after which these
    /// are cleared.
    ascii: [i32; 128],
    ascii_known: u128,
}

thread_local! {
    /// The gl_state the last regexp match used, kept for the next one as
    /// GNU keeps its one gl_state: setting it up for a match clears what
    /// it held, without a fresh allocation and copy a match.
    static REGEXP_GL_STATE: std::cell::Cell<Option<Box<GlState>>> =
        const { std::cell::Cell::new(None) };
}

impl GlState {
    fn new(interp: &Interpreter) -> Self {
        let buffer_table = interp.current_syntax_table_id();
        let mut gl = GlState {
            object: GlObject::Buffer,
            start: 0,
            stop: 0,
            use_global: false,
            global_code: SWHITESPACE,
            current_syntax_table: buffer_table,
            old_prop: Value::Nil,
            b_property: 0,
            e_property: 0,
            e_property_truncated: false,
            forward_i: None,
            backward_i: None,
            buffer_table,
            lookup_properties: false,
            propertize_done: -1,
            ran_lisp: false,
            ascii: [0; 128],
            ascii_known: 0,
        };
        gl.read_state(interp);
        gl
    }

    /// What `new' makes for a regexp match in OBJECT while
    /// `parse-sexp-lookup-properties' is set, in place: the ASCII entries
    /// need no clearing, as `ascii_known' forgets them, and
    /// `syntax-propertize--done' is read for a buffer alone, the one
    /// object `syntax-propertize' runs on.
    fn reset_for_regexp(&mut self, interp: &Interpreter, object: GlObject) {
        let buffer_table = interp.current_syntax_table_id();
        self.object = object;
        self.start = 0;
        self.stop = 0;
        self.use_global = false;
        self.global_code = SWHITESPACE;
        self.current_syntax_table = buffer_table;
        self.old_prop = Value::Nil;
        self.b_property = 0;
        self.e_property = 0;
        self.e_property_truncated = false;
        self.forward_i = None;
        self.backward_i = None;
        self.ran_lisp = false;
        self.buffer_table = buffer_table;
        self.ascii_known = 0;
        self.lookup_properties = true;
        self.propertize_done = match object {
            GlObject::Buffer => interp
                .symbol_value_key(cached_symbol!("syntax-propertize--done"))
                .and_then(|value| value.integer_value())
                .unwrap_or(-1),
            _ => -1,
        };
    }

    /// Read what Lisp may have changed: the buffer's syntax table and the
    /// C variables; SYNTAX's entries are read afresh.
    fn read_state(&mut self, interp: &Interpreter) {
        self.buffer_table = interp.current_syntax_table_id();
        self.ascii_known = 0;
        self.lookup_properties = interp
            .symbol_value_key(cached_symbol!("parse-sexp-lookup-properties"))
            .is_some_and(|value| value.is_truthy());
        self.propertize_done = interp
            .symbol_value_key(cached_symbol!("syntax-propertize--done"))
            .and_then(|value| value.integer_value())
            .unwrap_or(-1);
    }

    fn set_table(&mut self, table: CharTableRef) {
        if table != self.current_syntax_table {
            self.current_syntax_table = table;
            self.ascii_known = 0;
        }
    }

    /// SETUP_BUFFER_SYNTAX_TABLE.
    fn setup_buffer_syntax_table(&mut self) {
        self.use_global = false;
        self.e_property_truncated = false;
        let table = self.buffer_table;
        self.set_table(table);
    }

    /// SYNTAX_WITH_FLAGS through gl_state.
    #[inline(always)]
    fn syntax_with_flags(&mut self, c: u32) -> i32 {
        if self.use_global {
            return self.global_code;
        }
        if c < 128 && self.ascii_known & (1u128 << c) != 0 {
            return self.ascii[c as usize];
        }
        self.table_syntax_with_flags(c)
    }

    /// SYNTAX_WITH_FLAGS read from the table, an ASCII character's
    /// remembered.
    #[inline(never)]
    fn table_syntax_with_flags(&mut self, c: u32) -> i32 {
        let flags = entry_flags(self.current_syntax_table.get(c));
        if c < 128 {
            self.ascii[c as usize] = flags;
            self.ascii_known |= 1u128 << c;
        }
        flags
    }

    /// SETUP_SYNTAX_TABLE: gl_state for a scan of the current buffer from
    /// FROM in the direction of COUNT.
    fn setup_syntax_table(
        &mut self,
        interp: &mut Interpreter,
        env: &mut Env,
        from: usize,
        count: i64,
    ) -> Result<(), LispError> {
        self.setup_buffer_syntax_table();
        let (begv, zv) = {
            let buffer = interp.buffer.borrow();
            (buffer.point_min(), buffer.point_max())
        };
        self.b_property = begv;
        self.e_property = zv + 1;
        self.object = GlObject::Buffer;
        if self.lookup_properties {
            if count > 0 {
                self.update_syntax_table_forward(interp, env, from, true)?;
            } else if from > begv {
                self.update_syntax_table(interp, from - 1, count, true)?;
                self.parse_sexp_propertize(interp, env, from - 1)?;
            }
        }
        Ok(())
    }

    /// update_syntax_table_forward.
    fn update_syntax_table_forward(
        &mut self,
        interp: &mut Interpreter,
        env: &mut Env,
        charpos: usize,
        init: bool,
    ) -> Result<(), LispError> {
        if self.e_property_truncated {
            self.parse_sexp_propertize(interp, env, charpos)
        } else {
            self.update_syntax_table(interp, charpos, 1, init)?;
            if matches!(self.object, GlObject::Buffer)
                && self.e_property as i64 > self.propertize_done
            {
                self.parse_sexp_propertize(interp, env, charpos)?;
            }
            Ok(())
        }
    }

    /// parse_sexp_propertize: run `internal--syntax-propertize' when
    /// CHARPOS is past what `syntax-propertize' has done, else keep
    /// gl_state from reading past it.
    fn parse_sexp_propertize(
        &mut self,
        interp: &mut Interpreter,
        env: &mut Env,
        charpos: usize,
    ) -> Result<(), LispError> {
        let zv = interp.buffer.borrow().point_max() as i64;
        let pos = charpos as i64;
        if self.propertize_done <= pos && self.propertize_done < zv {
            // A runtime without syntax.el has nothing to propertize with.
            if !interp.has_lisp_function("internal--syntax-propertize") {
                self.propertize_done = i64::MAX;
                return Ok(());
            }
            let modiffs = interp.buffer.borrow().chars_modified_tick();
            // safe_calln: an error in the propertizer is not the scan's.
            let _ = interp.call_function_value(
                Value::Symbol("internal--syntax-propertize".into()),
                Some("internal--syntax-propertize"),
                &[Value::Integer(zv.min(1 + pos))],
                env,
            );
            self.ran_lisp = true;
            if modiffs != interp.buffer.borrow().chars_modified_tick() {
                return Err(error("internal--syntax-propertize modified the buffer!"));
            }
            self.read_state(interp);
            if self.propertize_done <= pos && self.propertize_done < zv {
                return Err(error(
                    "internal--syntax-propertize did not move syntax-propertize--done",
                ));
            }
            self.setup_syntax_table(interp, env, charpos, 1)
        } else if self.e_property as i64 > self.propertize_done {
            self.e_property = self.propertize_done.max(0) as usize;
            self.e_property_truncated = true;
            Ok(())
        } else if self.e_property_truncated && (self.e_property as i64) < self.propertize_done {
            // When moving backward, e_property might be set without
            // resetting e_property_truncated, so the flag may occasionally
            // be left raised spuriously.
            self.e_property_truncated = false;
            self.update_syntax_table_forward(interp, env, charpos, false)
        } else {
            Ok(())
        }
    }

    /// update_syntax_table: make gl_state good for CHARPOS, which lies
    /// in the direction of COUNT from the interval held or in it; with
    /// INIT, start from the bounds alone.
    fn update_syntax_table(
        &mut self,
        interp: &Interpreter,
        charpos: usize,
        count: i64,
        init: bool,
    ) -> Result<(), LispError> {
        match self.object {
            GlObject::Buffer => {
                let buffer = interp.buffer.borrow();
                let bounds = (buffer.point_min(), buffer.point_max());
                let intervals = buffer.property_intervals();
                self.update_in(interp, intervals, Some(bounds), charpos, count, init)
            }
            GlObject::String(string) => {
                let Kind::StringObject(state) = string.kind() else {
                    unreachable!("gl_state's string object is a string");
                };
                let state = state.borrow();
                let len = state.len();
                let intervals = (state.props.has_intervals() && len > 0)
                    .then(|| PropertyIntervals::new(&state.props[..], 0, len));
                self.update_in(interp, intervals, Some((0, len)), charpos, count, init)
            }
            GlObject::Plain => self.update_in(
                interp,
                None::<PropertyIntervals<'_, crate::buffer::TextPropertySpan>>,
                None,
                charpos,
                count,
                init,
            ),
        }
    }

    /// update_syntax_table over the object's INTERVALS, BOUNDS its
    /// accessible text (none for t, which has no intervals).
    fn update_in<S: PropertySpan>(
        &mut self,
        interp: &Interpreter,
        intervals: Option<PropertyIntervals<'_, S>>,
        bounds: Option<(usize, usize)>,
        charpos: usize,
        count: i64,
        init: bool,
    ) -> Result<(), LispError> {
        let mut invalidate = true;
        let mut i;
        if init {
            self.old_prop = Value::Nil;
            self.start = self.b_property;
            self.stop = self.e_property;
            // interval_of.
            let found = match bounds {
                None => None,
                Some((beg, end)) => {
                    if !(beg <= charpos && charpos <= end) {
                        return Err(args_out_of_range([
                            Value::Integer(charpos as i64),
                            Value::Integer(charpos as i64),
                        ]));
                    }
                    intervals
                        .as_ref()
                        .filter(|_| beg != end)
                        .map(|intervals| intervals.find(charpos))
                }
            };
            self.forward_i = found.as_ref().and_then(bounds_of);
            self.backward_i = self.forward_i;
            invalidate = false;
            let Some(found) = found else {
                return Ok(());
            };
            i = found;
            self.b_property = i.start;
            self.e_property = i.end;
        } else {
            let held = if count > 0 {
                self.forward_i
            } else {
                self.backward_i
            };
            let (Some((held_start, _)), Some(intervals)) = (held, intervals.as_ref()) else {
                return Err(error(
                    "Error in syntax_table logic for to-the-end intervals",
                ));
            };
            i = intervals.find(held_start);
            if charpos < i.start {
                if count > 0 {
                    return Err(error("Error in syntax_table logic for intervals <-"));
                }
                i = intervals.find(charpos);
                if i.end != self.b_property {
                    invalidate = false;
                    self.forward_i = bounds_of(&i);
                    self.e_property = i.end;
                }
            } else if charpos >= i.end {
                if count < 0 {
                    return Err(error("Error in syntax_table logic for intervals ->"));
                }
                i = intervals.find(charpos);
                if i.start != self.e_property {
                    invalidate = false;
                    self.backward_i = bounds_of(&i);
                    self.b_property = i.start;
                }
            }
        }

        let tmp_table = textget(interp, i.plist);

        if invalidate {
            invalidate = tmp_table.word() != self.old_prop.word();
        }
        if invalidate {
            if count > 0 {
                self.backward_i = bounds_of(&i);
                self.b_property = i.start;
            } else {
                self.forward_i = bounds_of(&i);
                self.e_property = i.end;
            }
        }

        if tmp_table.word() != self.old_prop.word() {
            self.old_prop = tmp_table;
            match tmp_table.kind() {
                Kind::CharTable(table) if table.has_purpose("syntax-table") => {
                    self.use_global = false;
                    self.set_table(table);
                }
                _ if tmp_table.is_cons() => {
                    self.use_global = true;
                    self.global_code = entry_flags(tmp_table);
                }
                _ => {
                    self.use_global = false;
                    let table = self.buffer_table;
                    self.set_table(table);
                }
            }
        }

        let intervals = intervals.expect("an interval was found");
        let mut cnt = 0;
        let mut walk = Some(i);
        while let Some(i) = walk {
            if cnt != 0 && textget(interp, i.plist).word() != tmp_table.word() {
                if count > 0 {
                    self.e_property = i.start;
                    self.forward_i = bounds_of(&i);
                } else {
                    self.b_property = i.end;
                    self.backward_i = bounds_of(&i);
                }
                return Ok(());
            } else if cnt == INTERVALS_AT_ONCE {
                if count > 0 {
                    // e_property at the end is ZV + 1, not ZV, so that
                    // INC(from);UPDATE_SYNTAX_TABLE_FORWARD needs no check
                    // for the end in between.
                    self.e_property = i.end + usize::from(intervals.next(&i).is_none());
                    self.forward_i = bounds_of(&i);
                } else {
                    self.b_property = i.start;
                    self.backward_i = bounds_of(&i);
                }
                return Ok(());
            }
            cnt += 1;
            walk = if count > 0 {
                intervals.next(&i)
            } else {
                intervals.previous(&i)
            };
        }
        // This property goes to the end.
        if count > 0 {
            self.e_property = self.stop;
            self.forward_i = None;
        } else {
            self.b_property = self.start;
        }
        Ok(())
    }

    /// RE_SETUP_SYNTAX_TABLE_FOR_OBJECT while `parse-sexp-lookup-properties'
    /// is set (without it the matcher reads the buffer's table alone):
    /// gl_state for a regexp match in OBJECT starting at the character
    /// position CHARPOS gives (RE_SYNTAX_TABLE_BYTE_TO_CHAR).
    pub(super) fn for_regexp(
        interp: &Interpreter,
        object: GlObject,
        charpos: impl FnOnce() -> usize,
    ) -> (Box<Self>, Option<LispError>) {
        let mut gl = REGEXP_GL_STATE
            .take()
            .unwrap_or_else(|| Box::new(GlState::new(interp)));
        gl.reset_for_regexp(interp, object);
        gl.setup_buffer_syntax_table();
        (gl.b_property, gl.e_property) = match object {
            GlObject::Buffer => (1, interp.buffer.borrow().point_max()),
            GlObject::Plain => (0, usize::MAX),
            GlObject::String(string) => (0, 1 + string_chars(&string)),
        };
        let error = gl.update_syntax_table(interp, charpos(), 1, true).err();
        (gl, error)
    }

    /// Give back a regexp match's gl_state for the next match.
    pub(super) fn release_for_regexp(gl: Box<Self>) {
        REGEXP_GL_STATE.set(Some(gl));
    }

    /// Whether `parse-sexp-lookup-properties' is in force.
    #[inline]
    pub(super) fn lookup_properties(&self) -> bool {
        self.lookup_properties
    }

    /// UPDATE_SYNTAX_TABLE: make gl_state good for CHARPOS.  After an
    /// error no more properties are looked up.
    pub(super) fn update(
        &mut self,
        interp: &mut Interpreter,
        env: &mut Env,
        charpos: usize,
    ) -> Result<(), LispError> {
        let result = if self.lookup_properties && charpos < self.b_property {
            self.update_syntax_table(interp, charpos, -1, false)
        } else {
            Ok(())
        };
        let result = result.and_then(|()| {
            if self.lookup_properties && charpos >= self.e_property {
                self.update_syntax_table_forward(interp, env, charpos, false)
            } else {
                Ok(())
            }
        });
        if result.is_err() {
            self.lookup_properties = false;
        }
        result
    }

    /// SYNTAX through gl_state.
    #[inline]
    pub(super) fn syntax(&mut self, c: u32) -> u8 {
        (self.syntax_with_flags(c) & 0xff) as u8
    }
}

/// SCHARS of a string object.
fn string_chars(string: &Value) -> usize {
    match string.kind() {
        Kind::StringObject(state) => state.borrow().len(),
        _ => 0,
    }
}

/// An interval as gl_state holds it.
fn bounds_of(i: &PropertyInterval<'_>) -> Option<(usize, usize)> {
    Some((i.start, i.end))
}

/// textget of `syntax-table' in an interval's plist.
fn textget(interp: &Interpreter, plist: &[(crate::lisp::types::PropertyName, Value)]) -> Value {
    crate::lisp::primitives::strings::property_from_props_with_category(
        interp,
        plist,
        cached_symbol!("syntax-table"),
    )
    .unwrap_or(Value::Nil)
}

/// How scan_words ended: at a position (None for GNU's 0, the accessible
/// text ending first), or with COUNT words left from FROM after a Lisp call
/// changed the text read.
enum Words {
    Done(Option<usize>),
    Changed { from: usize, count: i64 },
}

/// lisp_parse_state.
#[derive(Clone)]
struct ParseState {
    depth: i64,
    /// -1 outside a string, else the character that ends it or
    /// ST_STRING_STYLE.
    instring: i32,
    /// 0 outside a comment, -1 in an unnestable one, else the nesting.
    incomment: i64,
    comstyle: i32,
    quoted: bool,
    mindepth: i64,
    thislevelstart: i64,
    prevlevelstart: i64,
    location: usize,
    location_byte: usize,
    comstr_start: i64,
    levelstarts: Value,
    prev_syntax: i32,
}

/// What forw_comment found: the comment's end, or where it stopped.
struct ForwComment {
    found: bool,
    pos: usize,
    byte: usize,
    /// The nesting where an unfinished comment stopped.
    incomment: Option<i64>,
    last_syntax: i32,
}

/// find_defun_start's cache: the place it was asked for and the start it
/// found, valid while the buffer, BEGV and MODIFF are the same.
#[derive(Clone, Copy)]
struct FindStart {
    buffer: u64,
    pos: usize,
    value: usize,
    value_byte: usize,
    begv: usize,
    modiff: crate::buffer::ModCount,
}

thread_local! {
    static FIND_START: std::cell::Cell<Option<FindStart>> = const { std::cell::Cell::new(None) };
}

/// One scan of the current buffer, with the C variables it reads.  Those
/// change only through Lisp, so they are read again after each call the
/// scan makes.
struct Scanner<'a, 'r> {
    interp: &'a mut Interpreter,
    env: &'a mut Env,
    text: Text<'r>,
    gl: GlState,
    begv: usize,
    begv_byte: usize,
    zv: usize,
    zv_byte: usize,
    parse_sexp_ignore_comments: Option<bool>,
    comment_end_can_be_escaped: Option<bool>,
    multibyte_syntax_as_symbol: Option<bool>,
    open_paren_in_column_0_is_defun_start: Option<bool>,
    comment_use_syntax_ppss: Option<bool>,
    /// The current category table and the variables word_boundary_p
    /// reads.
    categories: Option<(CharTableRef, super::search::WordBoundaryVars)>,
    quit_count: u16,
}

impl<'a, 'r> Scanner<'a, 'r> {
    fn new(interp: &'a mut Interpreter, env: &'a mut Env, rope: &'r ropey::Rope) -> Self {
        let buffer = interp.buffer.borrow();
        let multibyte = buffer.is_multibyte();
        let z = buffer.size_total() + 1;
        let extended = if buffer.has_extended_chars_in(1, z) {
            buffer
                .substring_extended_chars(1, z)
                .into_iter()
                .map(|(offset, code)| (offset + 1, code))
                .collect()
        } else {
            Vec::new()
        };
        drop(buffer);
        let gl = GlState::new(interp);
        let mut scanner = Scanner {
            interp,
            env,
            text: Text::new(rope, multibyte, extended),
            gl,
            begv: 0,
            begv_byte: 0,
            zv: 0,
            zv_byte: 0,
            parse_sexp_ignore_comments: None,
            comment_end_can_be_escaped: None,
            multibyte_syntax_as_symbol: None,
            open_paren_in_column_0_is_defun_start: None,
            comment_use_syntax_ppss: None,
            categories: None,
            quit_count: 0,
        };
        scanner.read_state();
        scanner
    }

    /// Read what Lisp may have changed: the bounds, the buffer's syntax
    /// table, the C variables; SYNTAX's entries are read afresh.
    fn read_state(&mut self) {
        let (begv, zv) = {
            let buffer = self.interp.buffer.borrow();
            (buffer.point_min(), buffer.point_max())
        };
        if begv != self.begv {
            self.begv = begv;
            self.begv_byte = self.text.char_to_byte(begv);
        }
        if zv != self.zv {
            self.zv = zv;
            self.zv_byte = self.text.char_to_byte(zv);
        }
        self.gl.read_state(self.interp);
        self.parse_sexp_ignore_comments = None;
        self.comment_end_can_be_escaped = None;
        self.multibyte_syntax_as_symbol = None;
        self.open_paren_in_column_0_is_defun_start = None;
        self.comment_use_syntax_ppss = None;
        self.categories = None;
    }

    fn bool_variable(&self, symbol: crate::lisp::types::symbols::SymbolKey) -> bool {
        self.interp
            .symbol_value_key(symbol)
            .is_some_and(|value| value.is_truthy())
    }

    fn parse_sexp_ignore_comments(&mut self) -> bool {
        match self.parse_sexp_ignore_comments {
            Some(value) => value,
            None => {
                let value = self.bool_variable(cached_symbol!("parse-sexp-ignore-comments"));
                self.parse_sexp_ignore_comments = Some(value);
                value
            }
        }
    }

    fn comment_end_can_be_escaped(&mut self) -> bool {
        match self.comment_end_can_be_escaped {
            Some(value) => value,
            None => {
                let value = self.bool_variable(cached_symbol!("comment-end-can-be-escaped"));
                self.comment_end_can_be_escaped = Some(value);
                value
            }
        }
    }

    fn multibyte_syntax_as_symbol(&mut self) -> bool {
        match self.multibyte_syntax_as_symbol {
            Some(value) => value,
            None => {
                let value = self.bool_variable(cached_symbol!("multibyte-syntax-as-symbol"));
                self.multibyte_syntax_as_symbol = Some(value);
                value
            }
        }
    }

    fn open_paren_in_column_0_is_defun_start(&mut self) -> bool {
        match self.open_paren_in_column_0_is_defun_start {
            Some(value) => value,
            None => {
                let value =
                    self.bool_variable(cached_symbol!("open-paren-in-column-0-is-defun-start"));
                self.open_paren_in_column_0_is_defun_start = Some(value);
                value
            }
        }
    }

    /// `comment-use-syntax-ppss', taken as nil by a runtime without
    /// syntax.el's `syntax-ppss'.
    fn comment_use_syntax_ppss(&mut self) -> bool {
        match self.comment_use_syntax_ppss {
            Some(value) => value,
            None => {
                let value = self.bool_variable(cached_symbol!("comment-use-syntax-ppss"))
                    && self.interp.has_lisp_function("syntax-ppss");
                self.comment_use_syntax_ppss = Some(value);
                value
            }
        }
    }

    fn point(&self) -> usize {
        self.interp.buffer.borrow().point()
    }

    /// SET_PT_BOTH.
    fn set_point(&mut self, pos: usize) {
        self.interp.buffer.borrow_mut().goto_char(pos);
    }

    #[inline]
    fn rarely_quit(&mut self) -> Result<(), LispError> {
        self.quit_count = self.quit_count.wrapping_add(1);
        if self.quit_count == 0 {
            self.interp.maybe_quit(self.env)?;
        }
        Ok(())
    }

    #[inline]
    fn fetch(&mut self, bytepos: usize) -> u32 {
        self.text.fetch(bytepos)
    }

    #[inline]
    fn inc(&mut self, pos: &mut usize, byte: &mut usize) {
        self.text.inc(pos, byte);
    }

    #[inline]
    fn dec(&mut self, pos: &mut usize, byte: &mut usize) {
        self.text.dec(pos, byte);
    }

    #[inline]
    fn syntax_with_flags(&mut self, c: u32) -> i32 {
        self.gl.syntax_with_flags(c)
    }

    #[inline]
    fn syntax(&mut self, c: u32) -> i32 {
        self.gl.syntax_with_flags(c) & 0xff
    }

    /// syntax_multibyte: Ssymbol for a non-ASCII C when
    /// MULTIBYTE_SYMBOL_P.
    #[inline]
    fn syntax_multibyte(&mut self, c: u32, multibyte_symbol_p: bool) -> i32 {
        if c < 0x80 || !multibyte_symbol_p {
            self.syntax(c)
        } else {
            SSYMBOL
        }
    }

    /// UPDATE_SYNTAX_TABLE_FORWARD.
    #[inline]
    fn update_forward(&mut self, charpos: usize) -> Result<(), LispError> {
        if self.gl.lookup_properties && charpos >= self.gl.e_property {
            self.update_syntax_table_forward(charpos, false)?;
        }
        Ok(())
    }

    /// UPDATE_SYNTAX_TABLE_BACKWARD.
    #[inline]
    fn update_backward(&mut self, charpos: usize) -> Result<(), LispError> {
        if self.gl.lookup_properties && charpos < self.gl.b_property {
            self.gl
                .update_syntax_table(&*self.interp, charpos, -1, false)?;
        }
        Ok(())
    }

    /// UPDATE_SYNTAX_TABLE.
    fn update(&mut self, charpos: usize) -> Result<(), LispError> {
        self.update_backward(charpos)?;
        self.update_forward(charpos)
    }

    /// SETUP_SYNTAX_TABLE.
    fn setup_syntax_table(&mut self, from: usize, count: i64) -> Result<(), LispError> {
        let result = self
            .gl
            .setup_syntax_table(self.interp, self.env, from, count);
        self.after_propertize();
        result
    }

    /// update_syntax_table_forward.
    fn update_syntax_table_forward(&mut self, charpos: usize, init: bool) -> Result<(), LispError> {
        let result = self
            .gl
            .update_syntax_table_forward(self.interp, self.env, charpos, init);
        self.after_propertize();
        result
    }

    /// What the scan read of Lisp state, read again once the propertizer
    /// has run.
    fn after_propertize(&mut self) {
        if std::mem::take(&mut self.gl.ran_lisp) {
            self.read_state();
        }
    }

    /// char_quoted: whether the character at CHARPOS follows an odd
    /// number of escape and character-quote characters.  gl_state is good
    /// at CHARPOS afterwards.
    fn char_quoted(&mut self, mut charpos: usize, mut bytepos: usize) -> Result<bool, LispError> {
        let beg = self.begv;
        let mut quoted = false;
        let orig = charpos;
        while charpos > beg {
            self.dec(&mut charpos, &mut bytepos);
            self.update_backward(charpos)?;
            let c = self.fetch(bytepos);
            let code = self.syntax(c);
            if !(code == SCHARQUOTE || code == SESCAPE) {
                break;
            }
            quoted = !quoted;
        }
        self.update(orig)?;
        Ok(quoted)
    }

    /// prev_char_comend_first.
    fn prev_char_comend_first(
        &mut self,
        mut pos: usize,
        mut pos_byte: usize,
    ) -> Result<bool, LispError> {
        self.dec(&mut pos, &mut pos_byte);
        self.update_backward(pos)?;
        let c = self.text.fetch_char(pos_byte);
        let val = flags_comend_first(self.syntax_with_flags(c));
        self.update_forward(pos + 1)?;
        Ok(val)
    }

    /// find_newline backwards from START for the COUNTth newline, at
    /// least 1, as scan_newline leaves point: after it, or at BEGV.
    fn line_start_backward(
        &mut self,
        start: usize,
        start_byte: usize,
        count: usize,
    ) -> (usize, usize) {
        let (mut pos, mut byte) = (start, start_byte);
        let mut counted = 0;
        while pos > self.begv {
            self.dec(&mut pos, &mut byte);
            if self.text.fetch_char(byte) == u32::from(b'\n') {
                counted += 1;
                if counted == count {
                    return (pos + 1, byte + 1);
                }
            }
        }
        (self.begv, self.begv_byte)
    }

    /// find_defun_start: a defun start at or not too far before POS.
    /// gl_state is not good anywhere in particular afterwards.
    fn find_defun_start(
        &mut self,
        pos: usize,
        pos_byte: usize,
    ) -> Result<(usize, usize), LispError> {
        let buffer_id = self.interp.current_buffer_id();
        let modiff = self.interp.buffer.borrow().modified_tick();
        // Reuse the defun start even if POS is a little farther on: it
        // may be in the next defun, which is all right.
        if let Some(found) = FIND_START.get()
            && found.buffer == buffer_id
            && pos <= found.pos + 1000
            && pos >= found.value
            && self.begv == found.begv
            && modiff == found.modiff
        {
            return Ok((found.value, found.value_byte));
        }

        let (value, value_byte) = if self.comment_use_syntax_ppss() {
            let modiffs = self.interp.buffer.borrow().chars_modified_tick();
            let opoint = self.point();
            let ppss = self.interp.call_function_value(
                Value::Symbol("syntax-ppss".into()),
                Some("syntax-ppss"),
                &[Value::Integer(pos as i64)],
                self.env,
            )?;
            if modiffs != self.interp.buffer.borrow().chars_modified_tick() {
                return Err(error("syntax-ppss modified the buffer!"));
            }
            self.set_point(opoint);
            self.read_state();
            let mut boc = ppss;
            for _ in 0..8 {
                boc = boc.cdr()?;
            }
            match boc.car()?.kind() {
                Kind::Integer(start) => {
                    let start = start as usize;
                    (start, self.text.char_to_byte(start))
                }
                _ => (pos, pos_byte),
            }
        } else if !self.open_paren_in_column_0_is_defun_start() {
            (self.begv, self.begv_byte)
        } else {
            // Back up to the start of the line.
            let (mut pt, mut pt_byte) = self.line_start_backward(pos, pos_byte, 1);
            // Only a `^\s(' good in the buffer's table and under the
            // text's properties counts, so the properties are looked up
            // only for a candidate.
            self.gl.setup_buffer_syntax_table();
            while pt > self.begv {
                let c = self.fetch(pt_byte);
                if self.syntax(c) == SOPEN {
                    self.setup_syntax_table(pt + 1, -1)?;
                    let c = self.fetch(pt_byte);
                    if self.syntax(c) == SOPEN {
                        break;
                    }
                    self.gl.setup_buffer_syntax_table();
                }
                // Move to the start of the previous line.
                (pt, pt_byte) = self.line_start_backward(pt, pt_byte, 2);
            }
            (pt, pt_byte)
        };
        FIND_START.set(Some(FindStart {
            buffer: self.interp.current_buffer_id(),
            pos,
            value,
            value_byte,
            begv: self.begv,
            modiff: self.interp.buffer.borrow().modified_tick(),
        }));
        Ok((value, value_byte))
    }

    /// back_comment: whether FROM ends a comment of COMSTYLE and
    /// COMNESTED, looking no further back than STOP; the comment's start
    /// when it does.  gl_state is good for a backward scan from the
    /// result, or from FROM.
    fn back_comment(
        &mut self,
        from0: usize,
        from_byte0: usize,
        stop: usize,
        comnested: bool,
        comstyle: i32,
    ) -> Result<Option<(usize, usize)>, LispError> {
        let (mut from, mut from_byte) = (from0, from_byte0);
        // Presumed outside of any string.
        let mut string_style: i32 = -1;
        let mut string_lossage = false;
        // A matching comment starter plus a non-matching comment ender
        // were passed, so a matching starter seen later may be hidden
        // inside another comment: { a (* b } c (* d *)
        let mut comment_lossage = false;
        let comment_end = from;
        let comment_end_byte = from_byte;
        let mut comstart_pos = 0usize;
        let mut comstart_byte = 0usize;
        // Where the containing defun starts, or 0 if not come across.
        let mut defun_start = 0usize;
        let mut defun_start_byte = 0usize;
        let mut nesting: i64 = 1;
        let mut syntax: i32 = 0;
        let comment_end_can_be_escaped = self.comment_end_can_be_escaped();

        // At the beginning of the range to scan we are outside strings,
        // which sets quote parity to the comment end.
        let lossage = 'scan: {
            while from != stop {
                self.rarely_quit()?;

                // Move back and examine a character.
                self.dec(&mut from, &mut from_byte);
                self.update_backward(from)?;

                let prev_syntax = syntax;
                let mut c = self.fetch(from_byte) as i32;
                syntax = self.syntax_with_flags(c as u32);
                let mut code = syntax & 0xff;

                // Check for 2-char comment markers.
                let com2start = flags_comstart_first(syntax)
                    && flags_comstart_second(prev_syntax)
                    && comstyle == flags_comment_style(prev_syntax, syntax)
                    && (flags_comment_nested(prev_syntax) || flags_comment_nested(syntax))
                        == comnested;
                let mut com2end = flags_comend_first(syntax) && flags_comend_second(prev_syntax);
                let comstart = com2start || code == SCOMMENT;

                // A 2-char comment sequence partly overlapping another is
                // not worth being clever about: |*| in C, or }% in modes
                // with %..\n and %{..}%.
                if from > stop && (com2end || comstart) {
                    let (mut next, mut next_byte) = (from, from_byte);
                    self.dec(&mut next, &mut next_byte);
                    self.update_backward(next)?;
                    let next_c = self.fetch(next_byte);
                    let next_syntax = self.syntax_with_flags(next_c);
                    if ((comstart || comnested)
                        && flags_comend_second(syntax)
                        && flags_comend_first(next_syntax))
                        || ((com2end || comnested)
                            && flags_comstart_second(syntax)
                            && comstyle == flags_comment_style(syntax, prev_syntax)
                            && flags_comstart_first(next_syntax))
                    {
                        break 'scan true;
                    }
                }

                if com2start && comstart_pos == 0 {
                    // A comment starter that may also be an ender (as in
                    // snmp-mode): the first time it is a starter, later an
                    // ender.
                    com2end = false;
                }

                // Turn a 2-char comment sequence into its syntax.
                if com2end {
                    code = SENDCOMMENT;
                } else if com2start {
                    code = SCOMMENT;
                } else if code == SCOMMENT
                    && (comstyle != flags_comment_style(syntax, 0)
                        || flags_comment_nested(syntax) != comnested)
                {
                    // Ignore comment starters of a different style.
                    continue;
                }

                // Ignore escaped characters, except comment enders that
                // cannot be escaped.
                if (comment_end_can_be_escaped || code != SENDCOMMENT)
                    && self.char_quoted(from, from_byte)?
                {
                    continue;
                }

                match code {
                    SSTRING_FENCE | SCOMMENT_FENCE | SSTRING => {
                        if code != SSTRING {
                            c = if code == SSTRING_FENCE {
                                ST_STRING_STYLE
                            } else {
                                ST_COMMENT_STYLE
                            };
                        }
                        // Track parity of quotes.
                        if string_style == -1 {
                            // Entering a string.
                            string_style = c;
                        } else if string_style == c {
                            // Leaving the string.
                            string_style = -1;
                        } else {
                            // Two kinds of string delimiters, which a
                            // backward scan cannot make sense of.
                            string_lossage = true;
                        }
                    }
                    SCOMMENT => {
                        // Already checked to be the relevant style.
                        if string_style != -1 || comment_lossage || string_lossage {
                            // Odd string quotes are involved, so be
                            // careful.  Pascal: " { " a { " }
                            break 'scan true;
                        }
                        if !comnested {
                            // Record the best comment starter so far.
                            comstart_pos = from;
                            comstart_byte = from_byte;
                        } else {
                            nesting -= 1;
                            if nesting <= 0 {
                                // Nested comments are balanced, so no
                                // earlier one need be looked for: followed
                                // by uniformly paired string quotes, this
                                // starter is outside strings, or the ender
                                // would be inside one.
                                return Ok((from != comment_end).then_some((from, from_byte)));
                            }
                        }
                    }
                    SENDCOMMENT => {
                        if flags_comment_style(syntax, 0) == comstyle
                            && ((com2end && flags_comment_nested(prev_syntax))
                                || flags_comment_nested(syntax))
                                == comnested
                        {
                            // The same style of comment ender as ours.
                            if comnested {
                                nesting += 1;
                            } else {
                                // Anything before it would match this
                                // ender rather than ours.
                                from = stop;
                            }
                        } else if comstart_pos != 0 || c != i32::from(b'\n') {
                            // Mixing comment styles: be careful.  Not quite
                            // right (comment_lossage always would be), but
                            // otherwise every multi-line C comment would go
                            // through lossage.  Fails in rare cases like
                            // { (* } *)
                            comment_lossage = true;
                        }
                    }
                    // Assume a defun start is outside of strings.
                    SOPEN
                        if self.open_paren_in_column_0_is_defun_start()
                            && !self.comment_use_syntax_ppss()
                            && (from == stop || {
                                let before = self.text.dec_bytepos(from_byte);
                                self.text.fetch_char(before) == u32::from(b'\n')
                            }) =>
                    {
                        defun_start = from;
                        defun_start_byte = from_byte;
                        from = stop;
                    }
                    _ => {}
                }
            }
            false
        };

        if !lossage {
            if comstart_pos == 0 {
                from = comment_end;
                from_byte = comment_end_byte;
                self.update_forward(comment_end)?;
            } else {
                // The start of the non-nested comment.
                from = comstart_pos;
                from_byte = comstart_byte;
                self.update_forward(from - 1)?;
            }
        } else {
            // Two kinds of string delimiters mixed up together: decode
            // going forwards from a known safe place (the defun start) to
            // the one in question, which records where a comment starter
            // was last passed.
            let mut adjusted = true;
            if defun_start == 0 {
                (defun_start, defun_start_byte) =
                    self.find_defun_start(comment_end, comment_end_byte)?;
                adjusted = defun_start > self.begv;
            }
            loop {
                let mut state = internalize_parse_state(Value::Nil)?;
                self.scan_sexps_forward(
                    &mut state,
                    defun_start,
                    defun_start_byte,
                    comment_end,
                    i64::MIN,
                    false,
                    0,
                )?;
                defun_start = comment_end;
                if !adjusted {
                    adjusted = true;
                    if let Some(mut found) = FIND_START.get() {
                        found.value = match state.levelstarts.car().map(|car| car.kind()) {
                            Ok(Kind::Integer(start)) if state.levelstarts.is_cons() => {
                                start as usize
                            }
                            _ if state.thislevelstart >= 0 => state.thislevelstart as usize,
                            _ => found.value,
                        };
                        found.value_byte = self.text.char_to_byte(found.value);
                        FIND_START.set(Some(found));
                    }
                }
                if state.incomment == if comnested { 1 } else { -1 } && state.comstyle == comstyle {
                    from = state.comstr_start as usize;
                } else {
                    from = comment_end;
                    if state.incomment != 0 {
                        // COMMENT_END is inside some other comment; ours may
                        // be nested, so try again from within the
                        // surrounding one: { a (* " *)
                        defun_start = (state.comstr_start + 2) as usize;
                        defun_start_byte = self.text.char_to_byte(defun_start);
                    }
                }
                self.rarely_quit()?;
                if defun_start >= comment_end {
                    break;
                }
            }
            from_byte = self.text.char_to_byte(from);
            self.update_forward(from - 1)?;
        }

        Ok((from != comment_end).then_some((from, from_byte)))
    }

    /// forw_comment: skip a comment of STYLE from FROM, just after its
    /// starter, to STOP at most.  NESTING is positive for a nested
    /// comment's depth; PREV_SYNTAX is the previous character's syntax
    /// when the scan may start inside a two-character ender, else 0.
    #[allow(clippy::too_many_arguments)]
    fn forw_comment(
        &mut self,
        mut from: usize,
        mut from_byte: usize,
        stop: usize,
        mut nesting: i64,
        style: i32,
        prev_syntax: i32,
    ) -> Result<ForwComment, LispError> {
        let comment_end_can_be_escaped = self.comment_end_can_be_escaped();
        if nesting <= 0 {
            nesting = -1;
        }
        // Enter the loop in the middle, to find a 2-char comment ender
        // when the scan starts inside one.
        let mut syntax = prev_syntax;
        let mut code = syntax & 0xff;
        let mut prev_syntax;
        let mut middle = syntax != 0 && from < stop;
        loop {
            if !middle {
                if from == stop {
                    let last_syntax = if code == SESCAPE
                        || code == SCHARQUOTE
                        || flags_comend_first(syntax)
                        || (nesting > 0 && flags_comstart_first(syntax))
                    {
                        syntax
                    } else {
                        SMAX
                    };
                    return Ok(ForwComment {
                        found: false,
                        pos: from,
                        byte: from_byte,
                        incomment: Some(nesting),
                        last_syntax,
                    });
                }
                let c = self.fetch(from_byte);
                prev_syntax = syntax;
                syntax = self.syntax_with_flags(c);
                code = syntax & 0xff;
                if code == SENDCOMMENT
                    && flags_comment_style(syntax, 0) == style
                    && (if flags_comment_nested(syntax) {
                        nesting > 0 && {
                            nesting -= 1;
                            nesting == 0
                        }
                    } else {
                        nesting < 0
                    })
                    && !(comment_end_can_be_escaped
                        && ((prev_syntax & 0xff) == SESCAPE || (prev_syntax & 0xff) == SCHARQUOTE))
                {
                    // A comment end of the style that began this comment.
                    break;
                }
                if code == SCOMMENT_FENCE && style == ST_COMMENT_STYLE {
                    break;
                }
                if nesting > 0
                    && code == SCOMMENT
                    && flags_comment_nested(syntax)
                    && flags_comment_style(syntax, 0) == style
                {
                    // A nested comment of the same style.
                    nesting += 1;
                }
                if comment_end_can_be_escaped && (code == SESCAPE || code == SCHARQUOTE) {
                    self.inc(&mut from, &mut from_byte);
                    self.update_forward(from)?;
                    if from == stop {
                        continue;
                    }
                    // The escaped character is used up: as the comment
                    // continues, it has no syntax.
                    syntax = SMAX;
                    code = syntax;
                }
                self.inc(&mut from, &mut from_byte);
                self.update_forward(from)?;
            }
            middle = false;

            // forw_incomment:
            if from < stop && flags_comend_first(syntax) {
                let c1 = self.fetch(from_byte);
                let other_syntax = self.syntax_with_flags(c1);
                if flags_comend_second(other_syntax)
                    && flags_comment_style(syntax, other_syntax) == style
                    && (if flags_comment_nested(syntax) || flags_comment_nested(other_syntax) {
                        nesting > 0
                    } else {
                        nesting < 0
                    })
                {
                    // So that "|#" (Lisp) cannot return the syntax of "#"
                    // as the last syntax.
                    syntax = SMAX;
                    nesting -= 1;
                    if nesting <= 0 {
                        // A comment end of the style that began this one.
                        break;
                    }
                    self.inc(&mut from, &mut from_byte);
                    self.update_forward(from)?;
                }
            }
            if nesting > 0 && from < stop && flags_comstart_first(syntax) {
                let c1 = self.fetch(from_byte);
                let other_syntax = self.syntax_with_flags(c1);
                if flags_comment_style(other_syntax, syntax) == style
                    && flags_comstart_second(other_syntax)
                    && (flags_comment_nested(syntax) || flags_comment_nested(other_syntax))
                {
                    // A nested comment of the same style; so that "#|#"
                    // is not also an ender.
                    syntax = SMAX;
                    self.inc(&mut from, &mut from_byte);
                    self.update_forward(from)?;
                    nesting += 1;
                }
            }

            self.rarely_quit()?;
        }
        // Any syntactic power the last character had is used up.
        Ok(ForwComment {
            found: true,
            pos: from,
            byte: from_byte,
            incomment: None,
            last_syntax: SMAX,
        })
    }

    /// Fforward_comment.
    fn forward_comment(&mut self, count: i64) -> Result<Value, LispError> {
        let mut count1 = count;
        let stop = if count1 > 0 { self.zv } else { self.begv };
        let mut from = self.point();
        let mut from_byte = self.text.char_to_byte(from);
        self.setup_syntax_table(from, count1)?;
        while count1 > 0 {
            let mut c;
            let mut code;
            let mut comstyle;
            let mut comnested;
            loop {
                if from == stop {
                    self.set_point(from);
                    return Ok(Value::Nil);
                }
                c = self.fetch(from_byte);
                let syntax = self.syntax_with_flags(c);
                code = syntax & 0xff;
                let comstart_first = flags_comstart_first(syntax);
                comnested = flags_comment_nested(syntax);
                comstyle = flags_comment_style(syntax, 0);
                self.inc(&mut from, &mut from_byte);
                self.update_forward(from)?;
                if from < stop && comstart_first {
                    let c1 = self.fetch(from_byte);
                    let other_syntax = self.syntax_with_flags(c1);
                    if flags_comstart_second(other_syntax) {
                        // A comment start sequence: only an end of the
                        // style it begins ends the comment.
                        code = SCOMMENT;
                        comstyle = flags_comment_style(other_syntax, syntax);
                        comnested |= flags_comment_nested(other_syntax);
                        self.inc(&mut from, &mut from_byte);
                        self.update_forward(from)?;
                    }
                }
                self.rarely_quit()?;
                if !(code == SWHITESPACE || (code == SENDCOMMENT && c == u32::from(b'\n'))) {
                    break;
                }
            }

            if code == SCOMMENT_FENCE {
                comstyle = ST_COMMENT_STYLE;
            } else if code != SCOMMENT {
                self.dec(&mut from, &mut from_byte);
                self.set_point(from);
                return Ok(Value::Nil);
            }
            // At the start of a comment.
            let found =
                self.forw_comment(from, from_byte, stop, i64::from(comnested), comstyle, 0)?;
            from = found.pos;
            from_byte = found.byte;
            if !found.found {
                self.set_point(from);
                return Ok(Value::Nil);
            }
            self.inc(&mut from, &mut from_byte);
            self.update_forward(from)?;
            // One comment skipped.
            count1 -= 1;
        }

        while count1 < 0 {
            loop {
                if from <= stop {
                    self.set_point(self.begv);
                    return Ok(Value::Nil);
                }
                self.dec(&mut from, &mut from_byte);
                // char_quoted does UPDATE_SYNTAX_TABLE_BACKWARD (from).
                let quoted = self.char_quoted(from, from_byte)?;
                let c = self.fetch(from_byte);
                let syntax = self.syntax_with_flags(c);
                let mut code = syntax & 0xff;
                let mut comstyle = 0;
                let mut comnested = flags_comment_nested(syntax);
                if code == SENDCOMMENT {
                    comstyle = flags_comment_style(syntax, 0);
                }
                if from > stop
                    && flags_comend_second(syntax)
                    && self.prev_char_comend_first(from, from_byte)?
                    && !{
                        let before = self.text.dec_bytepos(from_byte);
                        self.char_quoted(from - 1, before)?
                    }
                {
                    // Record the comment style, to match only a comment
                    // starter of the same style.
                    self.dec(&mut from, &mut from_byte);
                    code = SENDCOMMENT;
                    // char_quoted set gl_state up at the new FROM.
                    let c1 = self.fetch(from_byte);
                    let other_syntax = self.syntax_with_flags(c1);
                    comstyle = flags_comment_style(other_syntax, syntax);
                    comnested |= flags_comment_nested(other_syntax);
                }

                let leave = if code == SCOMMENT_FENCE {
                    // Skip to the first preceding unquoted comment fence.
                    let mut fence_found = false;
                    let (ini, ini_byte) = (from, from_byte);
                    if from > stop {
                        loop {
                            self.dec(&mut from, &mut from_byte);
                            self.update_backward(from)?;
                            let c = self.fetch(from_byte);
                            if self.syntax(c) == SCOMMENT_FENCE
                                && !self.char_quoted(from, from_byte)?
                            {
                                fence_found = true;
                                break;
                            } else if from == stop {
                                break;
                            }
                            self.rarely_quit()?;
                        }
                    }
                    if !fence_found {
                        // Set point to INI + 1.
                        from = ini;
                        from_byte = ini_byte;
                        true
                    } else {
                        // One comment skipped.
                        break;
                    }
                } else if code == SENDCOMMENT {
                    let found = if !quoted || !self.comment_end_can_be_escaped() {
                        self.back_comment(from, from_byte, stop, comnested, comstyle)?
                    } else {
                        None
                    };
                    match found {
                        Some((pos, byte)) => {
                            // One comment skipped.
                            from = pos;
                            from_byte = byte;
                            break;
                        }
                        // An end of line that ends no comment is
                        // whitespace; CC mode (and maybe others) relies on
                        // this.
                        None if c == u32::from(b'\n') => false,
                        None => {
                            // Go back to the end of this not-quite comment
                            // ender.
                            if self.syntax(c) != code {
                                // It was a two-char ender.
                                self.inc(&mut from, &mut from_byte);
                            }
                            true
                        }
                    }
                } else {
                    code != SWHITESPACE || quoted
                };
                if leave {
                    self.inc(&mut from, &mut from_byte);
                    self.set_point(from);
                    return Ok(Value::Nil);
                }
                self.rarely_quit()?;
            }
            count1 += 1;
        }

        self.set_point(from);
        Ok(Value::T)
    }

    /// scan_lists.
    fn scan_lists(
        &mut self,
        from0: i64,
        mut count: i64,
        mut depth: i64,
        sexpflag: bool,
    ) -> Result<Value, LispError> {
        let stop = if count > 0 { self.zv } else { self.begv };
        let mut mathexit = false;
        // Signal if the depth gets less than this.
        let mut min_depth = depth;
        let mut last_good = from0;
        let multibyte_symbol_p = sexpflag && self.multibyte_syntax_as_symbol();
        if depth > 0 {
            min_depth = 0;
        }
        let mut from = from0.clamp(self.begv as i64, self.zv as i64) as usize;
        let mut from_byte = self.text.char_to_byte(from);

        self.interp.maybe_quit(self.env)?;

        let lose = |last_good: i64, from: usize| {
            scan_error("Unbalanced parentheses", last_good, from as i64)
        };
        let premature = |last_good: i64, from: usize| {
            scan_error(
                "Containing expression ends prematurely",
                last_good,
                from as i64,
            )
        };

        self.setup_syntax_table(from, count)?;
        while count > 0 {
            'done: {
                while from < stop {
                    self.rarely_quit()?;
                    self.update_forward(from)?;
                    let mut c = self.fetch(from_byte);
                    let syntax = self.syntax_with_flags(c);
                    let mut code = self.syntax_multibyte(c, multibyte_symbol_p);
                    let comstart_first = flags_comstart_first(syntax);
                    let mut comnested = flags_comment_nested(syntax);
                    let mut comstyle = flags_comment_style(syntax, 0);
                    let prefix = flags_prefix(syntax);
                    if depth == min_depth {
                        last_good = from as i64;
                    }
                    self.inc(&mut from, &mut from_byte);
                    self.update_forward(from)?;
                    if from < stop && comstart_first {
                        c = self.fetch(from_byte);
                        let other_syntax = self.syntax_with_flags(c);
                        if flags_comstart_second(other_syntax) && self.parse_sexp_ignore_comments()
                        {
                            // A comment start sequence while comments are
                            // ignored: only an end of its style ends it.
                            code = SCOMMENT;
                            comstyle = flags_comment_style(other_syntax, syntax);
                            comnested |= flags_comment_nested(other_syntax);
                            self.inc(&mut from, &mut from_byte);
                            self.update_forward(from)?;
                        }
                    }

                    if prefix {
                        continue;
                    }

                    let mut close = false;
                    match code {
                        SESCAPE | SCHARQUOTE | SWORD | SSYMBOL => {
                            if code == SESCAPE || code == SCHARQUOTE {
                                if from == stop {
                                    return Err(lose(last_good, from));
                                }
                                // The following character is a word
                                // constituent.
                                self.inc(&mut from, &mut from_byte);
                            }
                            if depth != 0 || !sexpflag {
                                continue;
                            }
                            // This word is a sexp: return at its end.
                            while from < stop {
                                self.update_forward(from)?;
                                let c = self.fetch(from_byte);
                                match self.syntax_multibyte(c, multibyte_symbol_p) {
                                    SCHARQUOTE | SESCAPE => {
                                        self.inc(&mut from, &mut from_byte);
                                        if from == stop {
                                            return Err(lose(last_good, from));
                                        }
                                    }
                                    SWORD | SSYMBOL | SQUOTE => {}
                                    _ => break 'done,
                                }
                                self.inc(&mut from, &mut from_byte);
                                self.rarely_quit()?;
                            }
                            break 'done;
                        }
                        SCOMMENT_FENCE | SCOMMENT => {
                            if code == SCOMMENT_FENCE {
                                comstyle = ST_COMMENT_STYLE;
                            }
                            if !self.parse_sexp_ignore_comments() {
                                continue;
                            }
                            self.update_forward(from)?;
                            let found = self.forw_comment(
                                from,
                                from_byte,
                                stop,
                                i64::from(comnested),
                                comstyle,
                                0,
                            )?;
                            from = found.pos;
                            from_byte = found.byte;
                            if !found.found {
                                if depth == 0 {
                                    break 'done;
                                }
                                return Err(lose(last_good, from));
                            }
                            self.inc(&mut from, &mut from_byte);
                            self.update_forward(from)?;
                        }
                        SMATH | SOPEN => {
                            if code == SMATH {
                                if !sexpflag {
                                    continue;
                                }
                                if from != stop && c == self.fetch(from_byte) {
                                    self.inc(&mut from, &mut from_byte);
                                }
                                if mathexit {
                                    mathexit = false;
                                    close = true;
                                } else {
                                    mathexit = true;
                                }
                            }
                            if !close {
                                depth += 1;
                                if depth == 0 {
                                    break 'done;
                                }
                            }
                        }
                        SCLOSE => close = true,
                        SSTRING | SSTRING_FENCE => {
                            let temp_pos = self.text.dec_bytepos(from_byte);
                            let stringterm = self.fetch(temp_pos);
                            loop {
                                if from >= stop {
                                    return Err(lose(last_good, from));
                                }
                                self.update_forward(from)?;
                                let c = self.fetch(from_byte);
                                let c_code = self.syntax_multibyte(c, multibyte_symbol_p);
                                if if code == SSTRING {
                                    c == stringterm && c_code == SSTRING
                                } else {
                                    c_code == SSTRING_FENCE
                                } {
                                    break;
                                }
                                if c_code == SCHARQUOTE || c_code == SESCAPE {
                                    self.inc(&mut from, &mut from_byte);
                                }
                                self.inc(&mut from, &mut from_byte);
                                self.rarely_quit()?;
                            }
                            self.inc(&mut from, &mut from_byte);
                            if depth == 0 && sexpflag {
                                break 'done;
                            }
                        }
                        // Whitespace, punctuation, quote, endcomment.
                        _ => {}
                    }
                    if close {
                        depth -= 1;
                        if depth == 0 {
                            break 'done;
                        }
                        if depth < min_depth {
                            return Err(premature(last_good, from));
                        }
                    }
                }

                // At the end of the buffer: an error inside an object, nil
                // between objects.
                if depth != 0 {
                    return Err(lose(last_good, from));
                }
                return Ok(Value::Nil);
            }
            count -= 1;
        }

        while count < 0 {
            'done2: {
                while from > stop {
                    self.rarely_quit()?;
                    self.dec(&mut from, &mut from_byte);
                    self.update_backward(from)?;
                    let c = self.fetch(from_byte);
                    let syntax = self.syntax_with_flags(c);
                    let mut code = self.syntax_multibyte(c, multibyte_symbol_p);
                    if depth == min_depth {
                        last_good = from as i64;
                    }
                    let mut comstyle = 0;
                    let mut comnested = flags_comment_nested(syntax);
                    if code == SENDCOMMENT {
                        comstyle = flags_comment_style(syntax, 0);
                    }
                    if from > stop
                        && flags_comend_second(syntax)
                        && self.prev_char_comend_first(from, from_byte)?
                        && self.parse_sexp_ignore_comments()
                    {
                        // Record the comment style, to match only a
                        // comment starter of the same style.
                        self.dec(&mut from, &mut from_byte);
                        self.update_backward(from)?;
                        code = SENDCOMMENT;
                        let c2 = self.fetch(from_byte);
                        let other_syntax = self.syntax_with_flags(c2);
                        comstyle = flags_comment_style(other_syntax, syntax);
                        comnested |= flags_comment_nested(other_syntax);
                    }

                    // Quoting turns anything but a comment ender into a word
                    // character (never true if FROM was decremented above).
                    if code != SENDCOMMENT && self.char_quoted(from, from_byte)? {
                        self.dec(&mut from, &mut from_byte);
                        code = SWORD;
                    } else if flags_prefix(syntax) {
                        continue;
                    }

                    let mut open = false;
                    match code {
                        SWORD | SSYMBOL | SESCAPE | SCHARQUOTE => {
                            if depth != 0 || !sexpflag {
                                continue;
                            }
                            // This word is a sexp: count it finished after
                            // passing it.
                            while from > stop {
                                let mut temp_pos = self.text.dec_bytepos(from_byte);
                                self.update_backward(from - 1)?;
                                let c1 = self.fetch(temp_pos);
                                // Don't allow a comment end to be quoted.
                                if self.syntax_multibyte(c1, multibyte_symbol_p) == SENDCOMMENT {
                                    break 'done2;
                                }
                                let quoted = self.char_quoted(from - 1, temp_pos)?;
                                if quoted {
                                    self.dec(&mut from, &mut from_byte);
                                    temp_pos = self.text.dec_bytepos(temp_pos);
                                    self.update_backward(from - 1)?;
                                }
                                let c1 = self.fetch(temp_pos);
                                if !quoted
                                    && !matches!(
                                        self.syntax_multibyte(c1, multibyte_symbol_p),
                                        SWORD | SSYMBOL | SQUOTE
                                    )
                                {
                                    break 'done2;
                                }
                                self.dec(&mut from, &mut from_byte);
                                self.rarely_quit()?;
                            }
                            break 'done2;
                        }
                        SMATH | SCLOSE => {
                            let mut opened = false;
                            if code == SMATH {
                                if !sexpflag {
                                    continue;
                                }
                                if from > self.begv {
                                    let temp_pos = self.text.dec_bytepos(from_byte);
                                    self.update_backward(from - 1)?;
                                    if from != stop && c == self.fetch(temp_pos) {
                                        self.dec(&mut from, &mut from_byte);
                                    }
                                }
                                if mathexit {
                                    mathexit = false;
                                    opened = true;
                                } else {
                                    mathexit = true;
                                }
                            }
                            if opened {
                                open = true;
                            } else {
                                depth += 1;
                                if depth == 0 {
                                    break 'done2;
                                }
                            }
                        }
                        SOPEN => open = true,
                        SENDCOMMENT => {
                            if !self.parse_sexp_ignore_comments() {
                                continue;
                            }
                            // When nothing is found it was no comment end;
                            // a single-character one is just skipped, and a
                            // two-character one is not worth retrying as
                            // two separate characters.
                            if let Some((pos, byte)) =
                                self.back_comment(from, from_byte, stop, comnested, comstyle)?
                            {
                                from = pos;
                                from_byte = byte;
                            }
                        }
                        SCOMMENT_FENCE | SSTRING_FENCE => {
                            loop {
                                if from == stop {
                                    return Err(lose(last_good, from));
                                }
                                self.dec(&mut from, &mut from_byte);
                                self.update_backward(from)?;
                                if !self.char_quoted(from, from_byte)? {
                                    let c = self.fetch(from_byte);
                                    if self.syntax_multibyte(c, multibyte_symbol_p) == code {
                                        break;
                                    }
                                }
                                self.rarely_quit()?;
                            }
                            if code == SSTRING_FENCE && depth == 0 && sexpflag {
                                break 'done2;
                            }
                        }
                        SSTRING => {
                            let stringterm = self.fetch(from_byte);
                            loop {
                                if from == stop {
                                    return Err(lose(last_good, from));
                                }
                                self.dec(&mut from, &mut from_byte);
                                self.update_backward(from)?;
                                if !self.char_quoted(from, from_byte)? {
                                    let c = self.fetch(from_byte);
                                    if c == stringterm
                                        && self.syntax_multibyte(c, multibyte_symbol_p) == SSTRING
                                    {
                                        break;
                                    }
                                }
                                self.rarely_quit()?;
                            }
                            if depth == 0 && sexpflag {
                                break 'done2;
                            }
                        }
                        // Whitespace, punctuation, quote, endcomment.
                        _ => {}
                    }
                    if open {
                        depth -= 1;
                        if depth == 0 {
                            break 'done2;
                        }
                        if depth < min_depth {
                            return Err(premature(last_good, from));
                        }
                    }
                }

                // At the start of the buffer: an error inside an object,
                // nil between objects.
                if depth != 0 {
                    return Err(lose(last_good, from));
                }
                return Ok(Value::Nil);
            }
            count += 1;
        }

        Ok(Value::Integer(from as i64))
    }

    /// `words-include-escapes'.
    fn words_include_escapes(&self) -> bool {
        self.bool_variable(cached_symbol!("words-include-escapes"))
    }

    /// The `find-word-boundary-function-table' function for C, when
    /// `fboundp' (which checks it is a symbol) finds one.
    fn word_boundary_function(&mut self, c: u32) -> Result<Option<Value>, LispError> {
        let table = self
            .interp
            .symbol_value_key(cached_symbol!("find-word-boundary-function-table"));
        let func = match table.map(|table| table.kind()) {
            Some(Kind::CharTable(table)) => table.get(c),
            _ => Value::Nil,
        };
        // nil's function cell is always void.
        if func.is_nil() {
            return Ok(None);
        }
        let bound = super::call(self.interp, "fboundp", &[func], self.env)?;
        Ok(bound.is_truthy().then_some(func))
    }

    /// word_boundary_p with the current category table.
    fn word_boundary_p(&mut self, c1: u32, c2: u32) -> bool {
        let (table, vars) = match self.categories {
            Some(read) => read,
            None => {
                let read = (
                    super::search::current_category_table(self.interp),
                    super::search::WordBoundaryVars::read(self.interp),
                );
                self.categories = Some(read);
                read
            }
        };
        super::search::word_boundary_p_with(self.interp, vars, table, c1, c2)
    }

    /// scan_words: COUNT words from FROM.  A word-boundary function that
    /// changes the text ends the scan with what remains of it.
    fn scan_words(&mut self, from: usize, count: i64) -> Result<Words, LispError> {
        let beg = self.begv;
        let end = self.zv;
        let mut from = from;
        let mut from_byte = self.text.char_to_byte(from);
        let mut count = count;
        let escape = |escapes: bool, code: i32| escapes && (code == SESCAPE || code == SCHARQUOTE);

        self.setup_syntax_table(from, count)?;
        let mut escapes = self.words_include_escapes();

        while count > 0 {
            let mut ch0;
            loop {
                if from == end {
                    return Ok(Words::Done(None));
                }
                self.update_forward(from)?;
                ch0 = self.fetch(from_byte);
                let code = self.syntax(ch0);
                self.inc(&mut from, &mut from_byte);
                if escape(escapes, code) || code == SWORD {
                    break;
                }
                self.rarely_quit()?;
            }
            // Now CH0 is a character which begins a word and FROM is the
            // position of the next character.
            if let Some(func) = self.word_boundary_function(ch0)? {
                let modiff = self.interp.buffer.borrow().chars_modified_tick();
                let pos = self.interp.call_function_value(
                    func,
                    None,
                    &[Value::Integer(from as i64 - 1), Value::Integer(end as i64)],
                    self.env,
                )?;
                let zv = self.interp.buffer.borrow().point_max();
                if let Kind::Integer(pos) = pos.kind()
                    && (from as i64) < pos
                    && pos <= zv as i64
                {
                    from = pos as usize;
                }
                count -= 1;
                if modiff != self.interp.buffer.borrow().chars_modified_tick() {
                    return Ok(Words::Changed { from, count });
                }
                self.read_state();
                escapes = self.words_include_escapes();
                from_byte = self.text.char_to_byte(from);
                continue;
            }
            loop {
                if from == end {
                    break;
                }
                self.update_forward(from)?;
                let ch1 = self.fetch(from_byte);
                let code = self.syntax(ch1);
                if (code != SWORD && !escape(escapes, code)) || self.word_boundary_p(ch0, ch1) {
                    break;
                }
                self.inc(&mut from, &mut from_byte);
                ch0 = ch1;
                self.rarely_quit()?;
            }
            count -= 1;
        }
        while count < 0 {
            let mut ch1;
            loop {
                if from == beg {
                    return Ok(Words::Done(None));
                }
                self.dec(&mut from, &mut from_byte);
                self.update_backward(from)?;
                ch1 = self.fetch(from_byte);
                let code = self.syntax(ch1);
                if escape(escapes, code) || code == SWORD {
                    break;
                }
                self.rarely_quit()?;
            }
            // Now CH1 is a character which ends a word and FROM is the
            // position of it.
            if let Some(func) = self.word_boundary_function(ch1)? {
                let modiff = self.interp.buffer.borrow().chars_modified_tick();
                let pos = self.interp.call_function_value(
                    func,
                    None,
                    &[Value::Integer(from as i64), Value::Integer(beg as i64)],
                    self.env,
                )?;
                let begv = self.interp.buffer.borrow().point_min();
                if let Kind::Integer(pos) = pos.kind()
                    && begv as i64 <= pos
                    && pos < from as i64
                {
                    from = pos as usize;
                }
                count += 1;
                if modiff != self.interp.buffer.borrow().chars_modified_tick() {
                    return Ok(Words::Changed { from, count });
                }
                self.read_state();
                escapes = self.words_include_escapes();
                from_byte = self.text.char_to_byte(from);
                continue;
            }
            loop {
                if from == beg {
                    break;
                }
                self.dec(&mut from, &mut from_byte);
                self.update_backward(from)?;
                let ch0 = self.fetch(from_byte);
                let code = self.syntax(ch0);
                if (code != SWORD && !escape(escapes, code)) || self.word_boundary_p(ch0, ch1) {
                    self.inc(&mut from, &mut from_byte);
                    break;
                }
                ch1 = ch0;
                self.rarely_quit()?;
            }
            count += 1;
        }
        Ok(Words::Done(Some(from)))
    }

    /// Fbackward_prefix_chars.
    fn backward_prefix_chars(&mut self) -> Result<Value, LispError> {
        let beg = self.begv;
        let mut opoint = self.point();
        let mut pos = opoint;
        let mut pos_byte = self.text.char_to_byte(pos);
        if pos <= beg {
            return Ok(Value::Nil);
        }
        self.setup_syntax_table(pos, -1)?;
        self.dec(&mut pos, &mut pos_byte);
        // char_quoted sets gl_state up at POS.
        while !self.char_quoted(pos, pos_byte)? && {
            let c = self.fetch(pos_byte);
            let flags = self.syntax_with_flags(c);
            flags & 0xff == SQUOTE || flags_prefix(flags)
        } {
            opoint = pos;
            if pos <= beg {
                break;
            }
            self.dec(&mut pos, &mut pos_byte);
            if pos.trailing_zeros() >= 16 {
                self.interp.maybe_quit(self.env)?;
            }
        }
        self.set_point(opoint);
        Ok(Value::Nil)
    }

    /// skip_syntaxes over the syntax classes FASTMAP holds, towards LIM.
    fn skip_syntaxes(
        &mut self,
        forward: bool,
        fastmap: &[bool; 256],
        lim: usize,
    ) -> Result<Value, LispError> {
        let start_point = self.point();
        let mut pos = start_point;
        let mut pos_byte = self.text.char_to_byte(pos);
        self.setup_syntax_table(pos, if forward { 1 } else { -1 })?;
        let unibyte = self.text.unibyte;
        let fetch = |text: &mut Text<'_>, byte: usize| {
            if unibyte {
                text.fetch_char(byte)
            } else {
                text.fetch(byte)
            }
        };
        if forward {
            loop {
                loop {
                    if pos >= lim {
                        self.set_point(pos);
                        return Ok(Value::Integer(pos as i64 - start_point as i64));
                    }
                    let c = fetch(&mut self.text, pos_byte);
                    if !fastmap[self.syntax(c) as usize & 0xff] {
                        self.set_point(pos);
                        return Ok(Value::Integer(pos as i64 - start_point as i64));
                    }
                    self.inc(&mut pos, &mut pos_byte);
                    if pos.trailing_zeros() >= 16 {
                        self.interp.maybe_quit(self.env)?;
                    }
                    if self.gl.lookup_properties && pos >= self.gl.e_property {
                        break;
                    }
                }
                self.update_syntax_table_forward(pos, false)?;
            }
        } else {
            loop {
                if pos <= lim {
                    break;
                }
                self.update_backward(pos - 1)?;
                let prev_byte = self.text.dec_bytepos(pos_byte);
                let c = fetch(&mut self.text, prev_byte);
                if !fastmap[self.syntax(c) as usize & 0xff] {
                    break;
                }
                pos -= 1;
                pos_byte = prev_byte;
                if pos.trailing_zeros() >= 16 {
                    self.interp.maybe_quit(self.env)?;
                }
            }
            self.set_point(pos);
            Ok(Value::Integer(pos as i64 - start_point as i64))
        }
    }

    /// in_2char_comment_start: whether the character at FROM_BYTE
    /// completes a comment starter whose first character, at PREV_FROM,
    /// had PREV_FROM_SYNTAX; STATE enters the comment when it does.
    fn in_2char_comment_start(
        &mut self,
        state: &mut ParseState,
        prev_from_syntax: i32,
        prev_from: usize,
        from_byte: usize,
    ) -> bool {
        if !flags_comstart_first(prev_from_syntax) {
            return false;
        }
        let c1 = self.fetch(from_byte);
        let syntax = self.syntax_with_flags(c1);
        if !flags_comstart_second(syntax) {
            return false;
        }
        // Only a comment end of the style entered ends the comment.
        state.comstyle = flags_comment_style(syntax, prev_from_syntax);
        let comnested = flags_comment_nested(prev_from_syntax) | flags_comment_nested(syntax);
        state.incomment = if comnested { 1 } else { -1 };
        state.comstr_start = prev_from as i64;
        true
    }

    /// scan_sexps_forward: parse from FROM, in STATE, to END.  STOPBEFORE
    /// stops at the start of an atom; COMMENTSTOP 1 at the start of a
    /// comment, -1 at the start or end of a comment or string.
    // INC_FROM sets the previous syntax, which some steps then mark used
    // up, as GNU's macro does.
    #[allow(clippy::too_many_arguments, unused_assignments)]
    fn scan_sexps_forward(
        &mut self,
        state: &mut ParseState,
        mut from: usize,
        mut from_byte: usize,
        end: usize,
        targetdepth: i64,
        stopbefore: bool,
        commentstop: i32,
    ) -> Result<(), LispError> {
        #[derive(Clone, Copy)]
        struct Level {
            last: i64,
            prev: i64,
        }
        #[derive(Clone, Copy)]
        enum At {
            Top,
            StartQuoted,
            SymStarted,
            Comment,
            StartInComment,
            StartInString,
            StartQuotedInString,
            StringEnd,
        }
        enum Exit {
            Done,
            Stop,
            EndQuoted,
        }
        const LEVELS: usize = 100;
        let mut levels = [Level { last: -1, prev: -1 }; LEVELS];
        let mut curlevel = 0usize;
        let boundary_stop = commentstop == -1;

        let mut prev_from = from;
        let mut prev_from_byte = from_byte;
        if from != self.begv {
            self.dec(&mut prev_from, &mut prev_from_byte);
        }

        self.interp.maybe_quit(self.env)?;

        let mut depth = state.depth;
        let start_quoted = state.quoted;
        let mut prev_prev_from_syntax = SMAX;
        let mut prev_from_syntax = state.prev_syntax;

        // The second and further enclosing sexps.
        let mut tem = state.levelstarts;
        while !tem.is_nil() {
            let temhd = tem.car()?;
            if let Kind::Integer(start) = temhd.kind() {
                levels[curlevel].last = start;
            }
            curlevel += 1;
            if curlevel == LEVELS {
                curlevel -= 1;
            }
            levels[curlevel] = Level { last: -1, prev: -1 };
            tem = tem.cdr()?;
        }
        levels[curlevel] = Level { last: -1, prev: -1 };

        state.quoted = false;
        let mut mindepth = depth;

        self.setup_syntax_table(from, 1)?;

        // INC_FROM, instead of `from++'.
        macro_rules! inc_from {
            () => {{
                prev_from = from;
                prev_from_byte = from_byte;
                let temp = self.fetch(prev_from_byte);
                prev_prev_from_syntax = prev_from_syntax;
                prev_from_syntax = self.syntax_with_flags(temp);
                self.inc(&mut from, &mut from_byte);
                if from < end {
                    self.update_forward(from)?;
                }
            }};
        }

        // Enter the loop where the initial state belongs.
        let mut at = if state.incomment != 0 {
            At::StartInComment
        } else if state.instring >= 0 {
            if start_quoted {
                At::StartQuotedInString
            } else {
                At::StartInString
            }
        } else if start_quoted {
            At::StartQuoted
        } else if from < end
            && self.in_2char_comment_start(state, prev_from_syntax, prev_from, from_byte)
        {
            inc_from!();
            // The syntax has already been used up.
            prev_from_syntax = SMAX;
            At::Comment
        } else {
            At::Top
        };

        let exit = 'parse: loop {
            match at {
                At::Top => {
                    if from >= end {
                        break 'parse Exit::Done;
                    }
                    self.rarely_quit()?;
                    inc_from!();

                    if from < end
                        && self.in_2char_comment_start(
                            state,
                            prev_from_syntax,
                            prev_from,
                            from_byte,
                        )
                    {
                        inc_from!();
                        prev_from_syntax = SMAX;
                        at = At::Comment;
                        continue;
                    }

                    if flags_prefix(prev_from_syntax) {
                        continue;
                    }
                    let code = prev_from_syntax & 0xff;
                    match code {
                        SESCAPE | SCHARQUOTE => {
                            if stopbefore {
                                break 'parse Exit::Stop;
                            }
                            levels[curlevel].last = prev_from as i64;
                            at = At::StartQuoted;
                        }
                        SWORD | SSYMBOL => {
                            if stopbefore {
                                break 'parse Exit::Stop;
                            }
                            levels[curlevel].last = prev_from as i64;
                            at = At::SymStarted;
                        }
                        SCOMMENT_FENCE => {
                            // Only a comment end of the style entered ends
                            // the comment.
                            state.comstyle = ST_COMMENT_STYLE;
                            state.incomment = -1;
                            state.comstr_start = prev_from as i64;
                            at = At::Comment;
                        }
                        SCOMMENT => {
                            state.comstyle = flags_comment_style(prev_from_syntax, 0);
                            state.incomment = if flags_comment_nested(prev_from_syntax) {
                                1
                            } else {
                                -1
                            };
                            state.comstr_start = prev_from as i64;
                            at = At::Comment;
                        }
                        SOPEN => {
                            if stopbefore {
                                break 'parse Exit::Stop;
                            }
                            depth += 1;
                            levels[curlevel].last = prev_from as i64;
                            curlevel += 1;
                            if curlevel == LEVELS {
                                curlevel -= 1;
                            }
                            levels[curlevel] = Level { last: -1, prev: -1 };
                            if targetdepth == depth {
                                break 'parse Exit::Done;
                            }
                        }
                        SCLOSE => {
                            depth -= 1;
                            if depth < mindepth {
                                mindepth = depth;
                            }
                            curlevel = curlevel.saturating_sub(1);
                            levels[curlevel].prev = levels[curlevel].last;
                            if targetdepth == depth {
                                break 'parse Exit::Done;
                            }
                        }
                        SSTRING | SSTRING_FENCE => {
                            state.comstr_start = from as i64 - 1;
                            if stopbefore {
                                break 'parse Exit::Stop;
                            }
                            levels[curlevel].last = prev_from as i64;
                            state.instring = if code == SSTRING {
                                self.fetch(prev_from_byte) as i32
                            } else {
                                ST_STRING_STYLE
                            };
                            if boundary_stop {
                                break 'parse Exit::Done;
                            }
                            at = At::StartInString;
                        }
                        // Math is not dealt with; whitespace, punctuation,
                        // quote and endcomment are ignored.
                        _ => {}
                    }
                }
                At::StartQuoted => {
                    if from == end {
                        break 'parse Exit::EndQuoted;
                    }
                    inc_from!();
                    at = At::SymStarted;
                }
                At::SymStarted => {
                    while from < end {
                        if self.in_2char_comment_start(
                            state,
                            prev_from_syntax,
                            prev_from,
                            from_byte,
                        ) {
                            inc_from!();
                            prev_from_syntax = SMAX;
                            at = At::Comment;
                            continue 'parse;
                        }
                        let symchar = self.fetch(from_byte);
                        match self.syntax(symchar) {
                            SCHARQUOTE | SESCAPE => {
                                inc_from!();
                                if from == end {
                                    break 'parse Exit::EndQuoted;
                                }
                            }
                            SWORD | SSYMBOL | SQUOTE => {}
                            _ => break,
                        }
                        inc_from!();
                        self.rarely_quit()?;
                    }
                    // symdone:
                    levels[curlevel].prev = levels[curlevel].last;
                    at = At::Top;
                }
                At::Comment => {
                    if commentstop != 0 || boundary_stop {
                        break 'parse Exit::Done;
                    }
                    at = At::StartInComment;
                }
                At::StartInComment => {
                    // Entering in the middle, a 2-char comment ender is
                    // found even when the scan starts inside one; not at
                    // the very beginning of a comment ((*) ... (*)).
                    let found = self.forw_comment(
                        from,
                        from_byte,
                        end,
                        state.incomment,
                        state.comstyle,
                        if from == self.begv {
                            0
                        } else {
                            prev_from_syntax
                        },
                    )?;
                    from = found.pos;
                    from_byte = found.byte;
                    if let Some(incomment) = found.incomment {
                        state.incomment = incomment;
                    }
                    prev_from_syntax = found.last_syntax;
                    // PREV_FROM and its byte are invalid now; `done' does
                    // not use them and INC_FROM sets them afresh.
                    if !found.found {
                        if (prev_from_syntax & 0xff) == SESCAPE
                            || (prev_from_syntax & 0xff) == SCHARQUOTE
                        {
                            break 'parse Exit::EndQuoted;
                        }
                        break 'parse Exit::Done;
                    }
                    inc_from!();
                    state.incomment = 0;
                    // Reset the comment style.
                    state.comstyle = 0;
                    // For the comment closer.
                    prev_from_syntax = SMAX;
                    if boundary_stop {
                        break 'parse Exit::Done;
                    }
                    at = At::Top;
                }
                At::StartInString | At::StartQuotedInString => {
                    let nofence = state.instring != ST_STRING_STYLE;
                    if let At::StartQuotedInString = at {
                        if from >= end {
                            break 'parse Exit::EndQuoted;
                        }
                        inc_from!();
                        self.rarely_quit()?;
                    }
                    loop {
                        if from >= end {
                            break 'parse Exit::Done;
                        }
                        let c = self.fetch(from_byte);
                        let c_code = self.syntax(c);
                        // C_CODE too, so that a character whose
                        // syntax-table property says it is not a string
                        // character does not end the string.
                        if nofence && c as i32 == state.instring && c_code == SSTRING {
                            break;
                        }
                        match c_code {
                            SSTRING_FENCE if !nofence => break,
                            SCHARQUOTE | SESCAPE => {
                                inc_from!();
                                // startquotedinstring:
                                if from >= end {
                                    break 'parse Exit::EndQuoted;
                                }
                            }
                            _ => {}
                        }
                        inc_from!();
                        self.rarely_quit()?;
                    }
                    at = At::StringEnd;
                }
                At::StringEnd => {
                    state.instring = -1;
                    levels[curlevel].prev = levels[curlevel].last;
                    inc_from!();
                    if boundary_stop {
                        break 'parse Exit::Done;
                    }
                    at = At::Top;
                }
            }
        };

        match exit {
            Exit::Done => {}
            Exit::Stop => {
                // Stopping before the start of a sexp: the character that
                // starts it was just fetched, so return the position
                // before it.
                from = prev_from;
                from_byte = prev_from_byte;
                prev_from_syntax = prev_prev_from_syntax;
            }
            Exit::EndQuoted => state.quoted = true,
        }

        state.depth = depth;
        state.mindepth = mindepth;
        state.thislevelstart = levels[curlevel].prev;
        state.prevlevelstart = if curlevel == 0 {
            -1
        } else {
            levels[curlevel - 1].last
        };
        state.location = from;
        state.location_byte = from_byte;
        let mut levelstarts = Value::Nil;
        while curlevel > 0 {
            curlevel -= 1;
            levelstarts = Value::cons(Value::Integer(levels[curlevel].last), levelstarts);
        }
        state.levelstarts = levelstarts;
        state.prev_syntax = if flags_comstartend_first(prev_from_syntax) || state.quoted {
            prev_from_syntax
        } else {
            SMAX
        };
        Ok(())
    }
}

/// internalize_parse_state: a parse state list (OLDSTATE) as
/// scan_sexps_forward's state.
fn internalize_parse_state(external: Value) -> Result<ParseState, LispError> {
    let mut state = ParseState {
        depth: 0,
        instring: -1,
        incomment: 0,
        comstyle: 0,
        quoted: false,
        mindepth: 0,
        thislevelstart: -1,
        prevlevelstart: -1,
        location: 0,
        location_byte: 0,
        // No comment or string seen.
        comstr_start: -1,
        levelstarts: Value::Nil,
        prev_syntax: SMAX,
    };
    if external.is_nil() {
        return Ok(state);
    }
    let mut external = external;
    let tem = external.car()?;
    state.depth = match tem.kind() {
        Kind::Integer(depth) => depth,
        _ => 0,
    };

    external = external.cdr()?.cdr()?.cdr()?;
    let tem = external.car()?;
    // Inside a string-fence-style string?
    state.instring = match tem.kind() {
        Kind::Nil => -1,
        Kind::Integer(c) if (0..=i64::from(rx::MAX_CHAR)).contains(&c) => c as i32,
        _ => ST_STRING_STYLE,
    };

    external = external.cdr()?;
    let tem = external.car()?;
    state.incomment = match tem.kind() {
        Kind::Nil => 0,
        Kind::Integer(nesting) => nesting,
        _ => -1,
    };

    external = external.cdr()?;
    state.quoted = !external.car()?.is_nil();

    // The eighth element: nil for comment style a, else the style.
    external = external.cdr()?.cdr()?;
    let tem = external.car()?;
    state.comstyle = match tem.kind() {
        Kind::Nil => 0,
        Kind::Integer(style) if (0..=i64::from(ST_COMMENT_STYLE)).contains(&style) => style as i32,
        _ => ST_COMMENT_STYLE,
    };

    external = external.cdr()?;
    state.comstr_start = match external.car()?.kind() {
        Kind::Integer(start) => start,
        _ => -1,
    };

    external = external.cdr()?;
    state.levelstarts = external.car()?;

    external = external.cdr()?;
    let tem = external.car()?;
    state.prev_syntax = match tem.kind() {
        Kind::Nil => SMAX,
        Kind::Integer(syntax) => syntax as i32,
        _ => return Err(LispError::WrongTypeArgument("fixnump".into(), tem)),
    };
    Ok(state)
}

/// `forward-comment'.
pub(super) fn forward_comment(
    interp: &mut Interpreter,
    env: &mut Env,
    count: &Value,
) -> Result<Value, LispError> {
    let count = check_fixnum(count)?;
    let rope = interp.buffer.borrow().text_rope();
    Scanner::new(interp, env, &rope).forward_comment(count)
}

/// `scan-lists'.
pub(super) fn scan_lists(
    interp: &mut Interpreter,
    env: &mut Env,
    from: &Value,
    count: &Value,
    depth: &Value,
) -> Result<Value, LispError> {
    let from = check_fixnum(from)?;
    let count = check_fixnum(count)?;
    let depth = check_fixnum(depth)?;
    let rope = interp.buffer.borrow().text_rope();
    Scanner::new(interp, env, &rope).scan_lists(from, count, depth, false)
}

/// `scan-sexps'.
pub(super) fn scan_sexps(
    interp: &mut Interpreter,
    env: &mut Env,
    from: &Value,
    count: &Value,
) -> Result<Value, LispError> {
    let from = check_fixnum(from)?;
    let count = check_fixnum(count)?;
    let rope = interp.buffer.borrow().text_rope();
    Scanner::new(interp, env, &rope).scan_lists(from, count, 0, true)
}

/// scan_words: the position COUNT words from FROM, None (GNU's 0) when
/// the accessible text ends first.
pub(super) fn scan_words(
    interp: &mut Interpreter,
    env: &mut Env,
    from: usize,
    count: i64,
) -> Result<Option<usize>, LispError> {
    let (mut from, mut count) = (from, count);
    loop {
        let rope = interp.buffer.borrow().text_rope();
        match Scanner::new(interp, env, &rope).scan_words(from, count)? {
            Words::Done(pos) => return Ok(pos),
            Words::Changed {
                from: at,
                count: left,
            } => (from, count) = (at, left),
        }
    }
}

/// `forward-word'.
pub(super) fn forward_word(
    interp: &mut Interpreter,
    env: &mut Env,
    arg: Option<&Value>,
) -> Result<Value, LispError> {
    let count = match arg {
        Some(arg) if !arg.is_nil() => check_fixnum(arg)?,
        _ => 1,
    };
    let point = interp.buffer.borrow().point();
    let orig_val = scan_words(interp, env, point, count)?;
    let val = orig_val.unwrap_or_else(|| {
        let buffer = interp.buffer.borrow();
        if count > 0 {
            buffer.point_max()
        } else {
            buffer.point_min()
        }
    });
    // Avoid jumping out of an input field.
    let tmp = super::call(
        interp,
        "constrain-to-field",
        &[
            Value::Integer(val as i64),
            Value::Integer(point as i64),
            Value::Nil,
            Value::Nil,
            Value::Nil,
        ],
        env,
    )?;
    let val = check_fixnum(&tmp)? as usize;
    interp.buffer.borrow_mut().goto_char(val);
    Ok(if Some(val) == orig_val {
        Value::T
    } else {
        Value::Nil
    })
}

/// `backward-prefix-chars'.
pub(super) fn backward_prefix_chars(
    interp: &mut Interpreter,
    env: &mut Env,
) -> Result<Value, LispError> {
    let rope = interp.buffer.borrow().text_rope();
    Scanner::new(interp, env, &rope).backward_prefix_chars()
}

/// `skip-syntax-forward' and `skip-syntax-backward'.
pub(super) fn skip_syntaxes(
    interp: &mut Interpreter,
    env: &mut Env,
    forward: bool,
    string: &Value,
    lim: Option<&Value>,
) -> Result<Value, LispError> {
    let spec = string_text(string)?;
    let (begv, zv, point) = {
        let buffer = interp.buffer.borrow();
        (
            buffer.point_min() as i64,
            buffer.point_max() as i64,
            buffer.point() as i64,
        )
    };
    let lim = match lim {
        None => {
            if forward {
                zv
            } else {
                begv
            }
        }
        Some(value) if value.is_nil() => {
            if forward {
                zv
            } else {
                begv
            }
        }
        Some(value) => fixnum_coerce_marker(interp, value)?,
    };
    // Don't scan outside the accessible text.
    let lim = lim.clamp(begv, zv);
    if if forward { point >= lim } else { point <= lim } {
        return Ok(Value::Integer(0));
    }

    let mut fastmap = [false; 256];
    let mut chars = spec.chars().peekable();
    let negate = chars.next_if_eq(&'^').is_some();
    // Find the syntaxes specified; the spec is ASCII, and a multibyte
    // one is made unibyte.
    for c in chars {
        let byte = if (c as u32) < 0x100 {
            c as u32
        } else {
            rx::code_of_stored(c) & 0xff
        };
        fastmap[usize::from(rx::syntax_spec_code(byte))] = true;
    }
    if negate {
        for entry in &mut fastmap {
            *entry = !*entry;
        }
    }
    let rope = interp.buffer.borrow().text_rope();
    Scanner::new(interp, env, &rope).skip_syntaxes(forward, &fastmap, lim as usize)
}

/// `parse-partial-sexp'.
pub(super) fn parse_partial_sexp(
    interp: &mut Interpreter,
    env: &mut Env,
    args: &[Value],
) -> Result<Value, LispError> {
    let arg = |index: usize| args.get(index).copied().unwrap_or(Value::Nil);
    let (from, to) = (arg(0), arg(1));
    let targetdepth = arg(2);
    let target = if targetdepth.is_nil() {
        // A depth never reached.
        i64::MIN
    } else {
        check_fixnum(&targetdepth)?
    };

    if fix_position(interp, &to)? < fix_position(interp, &from)? {
        return Err(error("End position is smaller than start position"));
    }

    let (from, to) = validate_region(interp, &from, &to)?;

    let mut state = internalize_parse_state(arg(4))?;
    let commentstop = match arg(5) {
        value if value.is_nil() => 0,
        value if matches!(value.kind(), Kind::Symbol(name) if name == "syntax-table") => -1,
        _ => 1,
    };
    let rope = interp.buffer.borrow().text_rope();
    let mut scanner = Scanner::new(interp, env, &rope);
    let from_byte = scanner.text.char_to_byte(from);
    scanner.scan_sexps_forward(
        &mut state,
        from,
        from_byte,
        to,
        target,
        !arg(3).is_nil(),
        commentstop,
    )?;
    scanner.set_point(state.location);

    let position = |pos: i64| {
        if pos < 0 {
            Value::Nil
        } else {
            Value::Integer(pos)
        }
    };
    Ok(Value::list([
        Value::Integer(state.depth),
        position(state.prevlevelstart),
        position(state.thislevelstart),
        if state.instring >= 0 {
            if state.instring == ST_STRING_STYLE {
                Value::T
            } else {
                Value::Integer(i64::from(state.instring))
            }
        } else {
            Value::Nil
        },
        if state.incomment < 0 {
            Value::T
        } else if state.incomment == 0 {
            Value::Nil
        } else {
            Value::Integer(state.incomment)
        },
        if state.quoted { Value::T } else { Value::Nil },
        Value::Integer(state.mindepth),
        if state.comstyle != 0 {
            if state.comstyle == ST_COMMENT_STYLE {
                Value::symbol("syntax-table")
            } else {
                Value::Integer(i64::from(state.comstyle))
            }
        } else {
            Value::Nil
        },
        if state.incomment != 0 || state.instring >= 0 {
            Value::Integer(state.comstr_start)
        } else {
            Value::Nil
        },
        state.levelstarts,
        if state.prev_syntax == SMAX {
            Value::Nil
        } else {
            Value::Integer(i64::from(state.prev_syntax))
        },
    ]))
}
