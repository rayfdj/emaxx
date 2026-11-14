//! print.c's printer proper: `print', `print_preprocess' and
//! `print_object'.  An object is walked with print.c's explicit stack
//! (prstack) and written where print.c writes it: printchar a character
//! and strout a block, into print_buffer, onto stdout, into the echo
//! area or to a function called with each character where the walk
//! stands.  `print-circle''s labels live in `print-number-table', read
//! and written as print.c reads and writes it.

use super::*;
use crate::lisp::types::StringObjectRef;

/// print.c:63 `PRINT_CIRCLE'.
pub(crate) const PRINT_CIRCLE: usize = 200;

/// lisp.h's MOST_POSITIVE_FIXNUM.
const MOST_POSITIVE_FIXNUM: i64 = (1_i64 << 61) - 1;

/// What the walk keeps for stdout before it hands it over: the stream
/// makes the same writes of the same fwrites whenever they come, so they
/// may come a stdio buffer's worth at a time.
const STDOUT_PENDING: usize = 4096;

/// print.c's static printer state.  A printer run from a callback inside
/// another (`print-unreadable-function', a function printed to) shares
/// it, as C statics are.
#[derive(Clone)]
pub(crate) struct PrintGlobals {
    /// `print_depth': signed as in C, where a print run from a callback
    /// resets it under the run that called it.
    depth: isize,
    /// `being_printed': without `print-circle', the object being printed
    /// at each depth.
    being_printed: [Value; PRINT_CIRCLE],
    /// `new_backquote_output'.
    new_backquote_output: i64,
    /// `print_buffer': the text of the print into a buffer under way
    /// (none while `print_buffer.buffer' is NULL).  The walk takes it to
    /// write into and gives it back before any Lisp runs.
    pub(crate) buffer: Option<PrintOutput>,
    /// The insertions of print_finish waiting on their
    /// prepare_to_modify_buffer.
    pub(crate) inserting: usize,
    /// print_buffer's bytes as its memory holds them while an insertion
    /// waits and a print run by its hooks has written over them.
    pub(crate) memory: Option<Vec<u8>>,
    /// Room for what a print holds for stdout or the echo area before it
    /// hands it over, kept from one print to the next: empty between
    /// them.
    pending: PrintOutput,
}

impl PrintGlobals {
    /// An empty output for stdout or the echo area, with the room the
    /// last one left.
    pub(crate) fn take_pending(&mut self) -> PrintOutput {
        std::mem::take(&mut self.pending)
    }

    /// OUTPUT's room kept for the next print, unless it grew large.
    pub(crate) fn give_pending(&mut self, mut output: PrintOutput) {
        if output.room() <= 2 * STDOUT_PENDING {
            output.clear();
            self.pending = output;
        }
    }
}

impl Default for PrintGlobals {
    fn default() -> Self {
        Self {
            depth: 0,
            being_printed: [Value::Nil; PRINT_CIRCLE],
            new_backquote_output: 0,
            buffer: None,
            inserting: 0,
            memory: None,
            pending: PrintOutput::default(),
        }
    }
}

/// print.c's `print_stack_entry'.
#[derive(Clone, Copy)]
enum PrintEntry {
    /// PE_list: after "(" ELEM (* " " ELEM): the cons whose car was just
    /// printed, the elements left to print, and Brent's cycle detection.
    List {
        last: Value,
        maxlen: i64,
        tortoise: Value,
        n: i64,
        m: i64,
        tortoise_idx: i64,
    },
    /// PE_rbrac: ")" after " . " ELEM.
    Rbrac,
    /// PE_vector: the rest of a vectorlike's slots.
    Vector {
        obj: Value,
        size: usize,
        idx: usize,
        end: &'static str,
        truncated: bool,
    },
    /// PE_hash: the rest of a hash table's keys and values.
    Hash {
        obj: Value,
        nobjs: usize,
        idx: usize,
        printed: usize,
        truncated: bool,
    },
}

thread_local! {
    /// print.c's prstack: the one print stack of every print, as GNU's
    /// static is.  Each print_object works on the entries above the
    /// depth it found (its base_sp), and nothing takes entries off when
    /// a print is left by a throw or an error: entries of a print run
    /// inside another (by a function it prints to, or by
    /// `print-unreadable-function') and left that way are on top when
    /// the outer print goes on, and it finishes them, as GNU's does.
    /// The buffer is scanned for the values the entries hold.
    static PRSTACK: std::cell::RefCell<crate::lisp::alloc::RootedVec<PrintEntry>> =
        const { std::cell::RefCell::new(crate::lisp::alloc::RootedVec::new()) };
}

thread_local! {
    /// The print_objects running.  With none, no print reads the entries
    /// a print left by a throw or an error put there (each starts above
    /// what it finds), and they go: print.c's stack is no root of its
    /// collector, where this one is scanned.
    static PRINT_OBJECTS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// A print_object begins: prstack.sp, its base_sp.
fn prstack_enter() -> usize {
    PRINT_OBJECTS.with(|running| {
        if running.get() == 0 {
            PRSTACK.with_borrow_mut(|stack| stack.clear());
        }
        running.set(running.get() + 1);
    });
    PRSTACK.with_borrow(|stack| stack.len())
}

/// A print_object is over, returned from or left.
fn prstack_leave() {
    PRINT_OBJECTS.with(|running| {
        running.set(running.get().saturating_sub(1));
        if running.get() == 0 {
            PRSTACK.with_borrow_mut(|stack| stack.clear());
        }
    });
}

/// print_stack_push.
fn prstack_push(entry: PrintEntry) {
    PRSTACK.with_borrow_mut(|stack| stack.push(entry));
}

/// `prstack.sp--': the top entry goes, whichever print pushed it.
fn prstack_pop() {
    PRSTACK.with_borrow_mut(|stack| {
        stack.pop();
    });
}

/// The innermost entry above BASE_SP, and where it is.
fn prstack_top(base_sp: usize) -> Option<(usize, PrintEntry)> {
    PRSTACK.with_borrow_mut(|stack| {
        let entries = stack.as_mut_slice();
        let sp = entries.len();
        (sp > base_sp).then(|| (sp - 1, entries[sp - 1]))
    })
}

/// The entry at INDEX rewritten, as print.c writes through its pointer
/// to the entry.
fn prstack_set(index: usize, entry: PrintEntry) {
    PRSTACK.with_borrow_mut(|stack| {
        if let Some(slot) = stack.as_mut_slice().get_mut(index) {
            *slot = entry;
        }
    });
}

/// How print_object goes on after an object's own case.
enum Step {
    /// `break': the object is done, print_depth back down.
    Done,
    /// `goto next_obj': a stack entry carries on (or the depth is down).
    Next,
    /// `goto print_obj' with this object at the current depth.
    Print(Value),
}

struct Printer {
    /// What the walk has written and not handed over: print_buffer
    /// itself for a buffer (`held'), up to STDOUT_PENDING bytes for
    /// stdout, the echo area's text until the next flush, nothing for a
    /// function.
    out: PrintOutput,
    /// `out' is print_buffer, taken from the interpreter.
    held: bool,
    sink: PrintSink,
    options: PrintOptions,
    escapes: StringEscapes,
    /// The variables read since Lisp last ran (the bits of
    /// `read_print_variables'): each is read where the printer first
    /// needs it, and again once Lisp could have changed it.
    read: u16,
}

/// print.c:print: reset the numbering unless it continues, fill
/// `print-number-table' for `print-circle' and drop what appears once,
/// then print OBJ onto SINK.
pub(crate) fn print(
    interp: &mut Interpreter,
    env: &mut Env,
    obj: Value,
    escape: bool,
    sink: PrintSink,
) -> Result<(), LispError> {
    interp.print_globals.new_backquote_output = 0;
    prepare_print_numbering(interp, env);
    if print_flag(interp, CIRCLE) {
        print_preprocess(interp, env, obj)?;
        if let Some(table) = number_table(interp) {
            let once = table
                .entries()
                .filter(|(_, _, state)| crate::lisp::types::eq(*state, Value::T))
                .map(|(slot, _, _)| slot)
                .collect::<Vec<_>>();
            for slot in once {
                table.remove(slot);
            }
        }
    }
    interp.print_globals.depth = 0;
    let (out, held) = match sink {
        PrintSink::Buffer => (
            interp
                .print_globals
                .buffer
                .take()
                .unwrap_or_else(PrintOutput::plain),
            true,
        ),
        PrintSink::Stdout | PrintSink::Echo => (interp.print_globals.take_pending(), false),
        PrintSink::Function(_) => (PrintOutput::plain(), false),
    };
    let mut printer = Printer {
        out,
        held,
        sink,
        options: PrintOptions::default(),
        escapes: StringEscapes::default(),
        read: 0,
    };
    let walked = printer.print_object(interp, env, obj, escape);
    // What the walk wrote before it stopped is the output's first, and
    // so is an error of the output's own.
    let flushed = printer.flush(interp, env);
    if matches!(sink, PrintSink::Stdout | PrintSink::Echo) {
        interp.print_globals.give_pending(printer.out);
    }
    flushed.and(walked)
}

/// print.c:PRINT_CIRCLE_CANDIDATE_P.
fn circle_candidate(interp: &Interpreter, obj: &Value, gensym: bool) -> bool {
    match obj.kind() {
        Kind::StringObject(_)
        | Kind::Cons(_)
        | Kind::Vector(_)
        | Kind::Closure(_)
        | Kind::CharTable(_)
        | Kind::SubCharTable(_)
        | Kind::HashTable(_)
        | Kind::LispRecord(_) => true,
        Kind::Record(id) => interp
            .find_record(id)
            .is_some_and(|record| record.kind == crate::lisp::eval::RecordKind::Font),
        Kind::Symbol(symbol) => gensym && symbol.cell().is_uninterned(),
        _ => false,
    }
}

fn number_table(interp: &Interpreter) -> Option<crate::lisp::types::HashTableRef> {
    match interp
        .symbol_value_key(cached_symbol!("print-number-table"))?
        .kind()
    {
        Kind::HashTable(table) => Some(table),
        _ => None,
    }
}

/// Vprint_number_table as Fgethash and Fputhash check it.
fn checked_number_table(
    interp: &Interpreter,
) -> Result<crate::lisp::types::HashTableRef, LispError> {
    let table = interp
        .symbol_value_key(cached_symbol!("print-number-table"))
        .unwrap_or(Value::Nil);
    match table.kind() {
        Kind::HashTable(table) => Ok(table),
        _ => Err(LispError::WrongTypeArgument("hash-table-p".into(), table)),
    }
}

/// Fgethash (OBJ, Vprint_number_table, Qnil).
fn number_table_get(
    interp: &mut Interpreter,
    env: &mut Env,
    obj: &Value,
) -> Result<Value, LispError> {
    let table = checked_number_table(interp)?;
    let (_, slot) = hash_table_lookup(interp, table, obj, env)?;
    Ok(slot
        .and_then(|slot| table.entry(slot))
        .map_or(Value::Nil, |(_, state)| state))
}

/// Fputhash (OBJ, STATE, Vprint_number_table).
fn number_table_put(
    interp: &mut Interpreter,
    env: &mut Env,
    obj: Value,
    state: Value,
) -> Result<(), LispError> {
    let table = checked_number_table(interp)?;
    hash_table_put(interp, table, obj, state, env)
}

/// print.c:print_preprocess: OBJ's circle candidates into
/// `print-number-table' in print order, t when first seen and, seen
/// again, the negated next `print_number_index'.
pub(crate) fn print_preprocess(
    interp: &mut Interpreter,
    env: &mut Env,
    obj: Value,
) -> Result<(), LispError> {
    let gensym = print_flag(interp, GENSYM);
    let continuous_numbering = print_continuous_numbering(interp);
    // The objects still to visit: the collector scans them, since a
    // `print-number-table' whose test is Lisp's runs Lisp between them.
    let mut pending = crate::lisp::alloc::RootedVec::new();
    let mut obj = obj;
    loop {
        if circle_candidate(interp, &obj, gensym) {
            if number_table(interp).is_none() {
                let table = json::make_hash_table(interp, "eq", Vec::new());
                interp.set_variable_key(cached_symbol!("print-number-table"), table, env);
            }
            let num = number_table_get(interp, env, &obj)?;
            // byte-compile-output-docform's gensyms always get a number.
            let numbered_gensym = continuous_numbering
                && matches!(obj.kind(), Kind::Symbol(symbol) if symbol.cell().is_uninterned());
            if !num.is_nil() || numbered_gensym {
                if matches!(num.kind(), Kind::Nil | Kind::T | Kind::Symbol(_)) {
                    interp.print_number_index += 1;
                    let number = i64::try_from(interp.print_number_index).unwrap_or(i64::MAX);
                    number_table_put(interp, env, obj, Value::Integer(-number))?;
                }
            } else {
                number_table_put(interp, env, obj, Value::T)?;
                match obj.kind() {
                    // print_preprocess_string: each interval's plist.
                    Kind::StringObject(state) => {
                        let mut index = 0;
                        loop {
                            let plist = {
                                let state = state.borrow();
                                state
                                    .props
                                    .intervals()
                                    .filter(|intervals| index < intervals.count())
                                    .map(|intervals| intervals.plist(index))
                            };
                            let Some(plist) = plist else {
                                break;
                            };
                            index += 1;
                            if !plist.is_nil() {
                                print_preprocess(interp, env, plist)?;
                            }
                        }
                    }
                    Kind::Cons(_) => {
                        let (car, cdr) = obj.cons_values().expect("a cons");
                        if !cdr.is_nil() {
                            pending.push(cdr);
                        }
                        obj = car;
                        continue;
                    }
                    Kind::Vector(vector) => pending.extend(vector.slots().rev()),
                    Kind::LispRecord(record) => pending.extend(record.slots().rev()),
                    Kind::Closure(closure) => pending.extend(closure.slots().rev()),
                    Kind::CharTable(table) => pending.extend(table.slots().rev()),
                    Kind::SubCharTable(table) => pending.extend(table.slots().rev()),
                    Kind::HashTable(table) => {
                        let entries = table.entries().collect::<Vec<_>>();
                        for (_, key, value) in entries.into_iter().rev() {
                            pending.push(value);
                            pending.push(key);
                        }
                    }
                    Kind::Record(id) => {
                        if let Some(record) = interp.find_record(id) {
                            pending.extend(record.slots.iter().rev().copied());
                        }
                    }
                    _ => {}
                }
            }
        }
        match pending.pop() {
            Some(next) => obj = next,
            None => return Ok(()),
        }
    }
}

/// AREF of a vectorlike the printer walks; a sub char-table's slots start
/// after SUB_CHAR_TABLE_OFFSET, its packed depth and minimum character.
fn vectorlike_slot(obj: &Value, index: usize) -> Value {
    match obj.kind() {
        Kind::Vector(vector) => vector.get(index),
        Kind::LispRecord(record) => record.get(index),
        Kind::Closure(closure) => closure.get(index),
        Kind::CharTable(table) => (index < table.slot_count()).then(|| table.slot(index)),
        Kind::SubCharTable(table) => index
            .checked_sub(1)
            .filter(|index| *index < table.slots().len())
            .map(|index| table.slot(index)),
        _ => None,
    }
    .unwrap_or(Value::Nil)
}

/// fixnum_to_string: N's decimal digits, at the end of DIGITS.
fn fixnum_digits(n: i64, digits: &mut [u8; 20]) -> &[u8] {
    let mut at = digits.len();
    let mut rest = n.unsigned_abs();
    loop {
        at -= 1;
        digits[at] = b'0' + (rest % 10) as u8;
        rest /= 10;
        if rest == 0 {
            break;
        }
    }
    if n < 0 {
        at -= 1;
        digits[at] = b'-';
    }
    &digits[at..]
}

fn is_symbol_key(value: &Value, key: crate::lisp::types::symbols::SymbolKey) -> bool {
    matches!(value.kind(), Kind::Symbol(symbol) if key.with(|wanted| *wanted == symbol))
}

/// print.c:named_escape.
fn named_escape(code: u32) -> Option<char> {
    match code {
        0x08 => Some('b'),
        0x09 => Some('t'),
        0x0a => Some('n'),
        0x0c => Some('f'),
        0x0d => Some('r'),
        0x20 => Some('s'),
        // \a, \v, \e and \d are excluded from printing as escapes since
        // they are somewhat rare as characters and more likely to be
        // plain integers.
        _ => None,
    }
}

/// Whether print_object's symbol case sets CONFUSING for the name BYTES:
/// it looks like a number, or starts with `?' or `.'.
fn symbol_name_confusing(bytes: &[u8]) -> bool {
    let Some(&first) = bytes.first() else {
        return false;
    };
    if first == b'?' || first == b'.' {
        return true;
    }
    let signed = usize::from(first == b'-' || first == b'+');
    bytes
        .get(signed)
        .is_some_and(|byte| byte.is_ascii_digit() || *byte == b'.')
        && std::str::from_utf8(bytes).is_ok_and(symbol_name_looks_like_number)
}

/// The characters print_object's symbol case quotes under `escapeflag'.
#[inline]
fn symbol_char_quoted(code: u32) -> bool {
    matches!(
        code,
        0x22 | 0x5c | 0x27 | 0x3b | 0x23 | 0x28 | 0x29 | 0x2c | 0x60 | 0x5b | 0x5d | 0xa0
    ) || code <= 0o40
}

/// fetch_string_char_advance of the string STATE at byte OFFSET: the
/// character there and its bytes; past the string's bytes, the
/// terminating NUL.
#[inline]
fn string_char_at(state: &crate::lisp::types::SharedStringState, offset: usize) -> (u32, usize) {
    let bytes = state.bytes();
    let byte = bytes.get(offset).copied().unwrap_or(0);
    if byte >= 0x80 && state.is_multibyte() {
        crate::lisp::types::string_data::decode_character(&bytes[offset..])
            .unwrap_or((u32::from(byte), 1))
    } else {
        (u32::from(byte), 1)
    }
}

/// bool_vector_uchar_data (VECTOR)[INDEX]: the words' bytes in memory
/// order.
fn bool_vector_byte(vector: crate::lisp::types::BoolVectorRef, index: usize) -> u8 {
    const WORD: usize = std::mem::size_of::<u64>();
    vector
        .words()
        .get(index / WORD)
        .map_or(0, |word| (word.get() as u64).to_le_bytes()[index % WORD])
}

impl Printer {
    /// Lisp has run: every variable is read again where it is next
    /// needed.
    #[inline]
    fn lisp_ran(&mut self) {
        self.read = 0;
    }

    /// The variables of BITS as they are now, unless read since Lisp
    /// last ran.  (Inlined where BITS is known, so that each place reads
    /// its own variables and tests for no others.)
    #[inline(always)]
    fn vars(&mut self, interp: &Interpreter, bits: u16) {
        let unread = bits & !self.read;
        if unread != 0 {
            read_print_variables(interp, unread, &mut self.options, &mut self.escapes);
            self.read |= unread;
        }
    }

    /// What the walk has written is handed over: print_buffer back to
    /// the interpreter, stdout's and the echo area's text to them.
    fn flush(&mut self, interp: &mut Interpreter, env: &mut Env) -> Result<(), LispError> {
        match self.sink {
            PrintSink::Buffer => {
                if self.held {
                    interp.print_globals.buffer = Some(std::mem::take(&mut self.out));
                    self.held = false;
                }
                Ok(())
            }
            PrintSink::Stdout | PrintSink::Echo => {
                if !self.out.written() {
                    return Ok(());
                }
                let delivered = deliver(interp, env, self.sink, &self.out);
                self.out.clear();
                delivered
            }
            PrintSink::Function(_) => Ok(()),
        }
    }

    /// print_buffer taken again once Lisp has run: as the prints it ran
    /// left it.
    fn resume(&mut self, interp: &mut Interpreter) {
        if matches!(self.sink, PrintSink::Buffer) && !self.held {
            self.out = interp
                .print_globals
                .buffer
                .take()
                .unwrap_or_else(PrintOutput::plain);
            self.held = true;
        }
    }

    /// stdout's text handed over once there is a stdio buffer's worth.
    #[inline]
    fn sync(&mut self, interp: &mut Interpreter, env: &mut Env) -> Result<(), LispError> {
        if self.out.len() >= STDOUT_PENDING && matches!(self.sink, PrintSink::Stdout) {
            return self.flush(interp, env);
        }
        Ok(())
    }

    /// printchar's `call1 (fun, make_fixnum (ch))'.
    fn call_function(
        &mut self,
        interp: &mut Interpreter,
        env: &mut Env,
        function: Value,
        code: u32,
    ) -> Result<(), LispError> {
        self.read = 0;
        interp
            .call_function_value(function, None, &[Value::Integer(i64::from(code))], env)
            .map(drop)
    }

    /// print.c:printchar of the character CODE.
    #[inline]
    fn printchar_code(
        &mut self,
        interp: &mut Interpreter,
        env: &mut Env,
        code: u32,
    ) -> Result<(), LispError> {
        if let PrintSink::Function(function) = self.sink {
            return self.call_function(interp, env, function, code);
        }
        self.out.push_code(code);
        Ok(())
    }

    #[inline]
    fn printchar(
        &mut self,
        interp: &mut Interpreter,
        env: &mut Env,
        character: char,
    ) -> Result<(), LispError> {
        self.printchar_code(interp, env, character as u32)
    }

    /// printchar of each of TEXT's characters.
    fn printchars(
        &mut self,
        interp: &mut Interpreter,
        env: &mut Env,
        text: &str,
    ) -> Result<(), LispError> {
        for character in text.chars() {
            self.printchar(interp, env, character)?;
        }
        Ok(())
    }

    /// print.c:strout of TEXT, a C string sprintf filled or
    /// print_c_string was given: one block, a function called with each
    /// character.
    fn strout(
        &mut self,
        interp: &mut Interpreter,
        env: &mut Env,
        text: &str,
    ) -> Result<(), LispError> {
        if let PrintSink::Function(function) = self.sink {
            for character in text.chars() {
                self.call_function(interp, env, function, character as u32)?;
            }
            return Ok(());
        }
        self.out.push_str(text);
        Ok(())
    }

    /// strout of ASCII the printer made itself (a number's digits).
    fn strout_ascii(
        &mut self,
        interp: &mut Interpreter,
        env: &mut Env,
        ascii: &[u8],
    ) -> Result<(), LispError> {
        if let PrintSink::Function(function) = self.sink {
            for &byte in ascii {
                self.call_function(interp, env, function, u32::from(byte))?;
            }
            return Ok(());
        }
        self.out.strout_ascii(ascii);
        Ok(())
    }

    /// print.c:print_c_string of the C string OCTETS: one strout whose
    /// bytes are a character each, a function handed each as the `char'
    /// it is.
    fn c_string(
        &mut self,
        interp: &mut Interpreter,
        env: &mut Env,
        octets: &[u8],
    ) -> Result<(), LispError> {
        if let PrintSink::Function(function) = self.sink {
            for &byte in octets {
                self.call_function(interp, env, function, output::c_char_argument(byte))?;
            }
            return Ok(());
        }
        self.out.push_octets(octets);
        Ok(())
    }

    /// A pseudovector's text host code rendered whole: print.c writes
    /// all of them but the closing `>' with strout, and that with
    /// printchar.
    fn strout_unreadable(
        &mut self,
        interp: &mut Interpreter,
        env: &mut Env,
        text: &str,
    ) -> Result<(), LispError> {
        match text.strip_suffix('>') {
            Some(head) => {
                self.strout(interp, env, head)?;
                self.printchar(interp, env, '>')
            }
            None => self.strout(interp, env, text),
        }
    }

    /// print.c:print_string of STRING: one strout of its text (as
    /// `princ' prints it) into a buffer, onto stdout or into the echo
    /// area; a printchar a character to a function, which reads the
    /// string again for each.
    fn print_string(
        &mut self,
        interp: &mut Interpreter,
        env: &mut Env,
        string: StringObjectRef,
    ) -> Result<(), LispError> {
        if let PrintSink::Function(function) = self.sink {
            self.read = 0;
            return print_string_to_function(interp, env, string, function);
        }
        strout_princ_string(interp, &string.borrow(), &mut self.out);
        self.sync(interp, env)
    }

    /// print_string of NAME when it is a string (a buffer's, a
    /// process's, a thread's name).
    fn print_name(
        &mut self,
        interp: &mut Interpreter,
        env: &mut Env,
        name: Value,
    ) -> Result<(), LispError> {
        match name.kind() {
            Kind::StringObject(string) => self.print_string(interp, env, string),
            _ => Ok(()),
        }
    }

    /// print.c:strout of a Lisp string's data as it is (a string found in
    /// `print-number-table'): its bytes one block, each a character when
    /// the string has as many characters as bytes (to a function, each as
    /// the C `char' it is); a multibyte string's characters otherwise.
    fn strout_string(
        &mut self,
        interp: &mut Interpreter,
        env: &mut Env,
        text: StringObjectRef,
    ) -> Result<(), LispError> {
        if let PrintSink::Function(function) = self.sink {
            // strout is given the data's address once: the bytes as
            // they are now.
            let (octets, bytes) = {
                let state = text.borrow();
                (state.len() == state.bytes().len(), state.bytes().to_vec())
            };
            let mut rest = &bytes[..];
            while let Some(&byte) = rest.first() {
                let (code, width) = if octets {
                    (output::c_char_argument(byte), 1)
                } else {
                    crate::lisp::types::string_data::decode_character(rest)
                        .unwrap_or((u32::from(byte), 1))
                };
                rest = &rest[width..];
                self.call_function(interp, env, function, code)?;
            }
            return Ok(());
        }
        {
            let state = text.borrow();
            let bytes = state.bytes();
            if state.len() == bytes.len() {
                self.out.push_octets(bytes);
            } else {
                self.out.strout_block(|block| {
                    let mut rest = bytes;
                    while let Some(&byte) = rest.first() {
                        let (code, width) = crate::lisp::types::string_data::decode_character(rest)
                            .unwrap_or((u32::from(byte), 1));
                        block.push_code(code);
                        rest = &rest[width..];
                    }
                });
            }
        }
        self.sync(interp, env)
    }

    /// Fgethash (OBJ, Vprint_number_table, Qnil).  A table whose test is
    /// Lisp's runs Lisp.
    fn number_get(
        &mut self,
        interp: &mut Interpreter,
        env: &mut Env,
        obj: &Value,
    ) -> Result<Value, LispError> {
        let lisp = checked_number_table(interp)?
            .test()
            .standard_test()
            .is_none();
        if lisp {
            self.flush(interp, env)?;
        }
        let num = number_table_get(interp, env, obj);
        if lisp {
            self.resume(interp);
            self.lisp_ran();
        }
        num
    }

    /// Fputhash (OBJ, STATE, Vprint_number_table).
    fn number_put(
        &mut self,
        interp: &mut Interpreter,
        env: &mut Env,
        obj: Value,
        state: Value,
    ) -> Result<(), LispError> {
        let lisp = checked_number_table(interp)?
            .test()
            .standard_test()
            .is_none();
        if lisp {
            self.flush(interp, env)?;
        }
        let put = number_table_put(interp, env, obj, state);
        if lisp {
            self.resume(interp);
            self.lisp_ran();
        }
        put
    }

    /// print.c:print_object: OBJ and, through the stack entries it pushes,
    /// everything inside it, before returning to the entries below.
    fn print_object(
        &mut self,
        interp: &mut Interpreter,
        env: &mut Env,
        obj: Value,
        escape: bool,
    ) -> Result<(), LispError> {
        let base_sp = prstack_enter();
        let walked = self.walk(interp, env, obj, escape, base_sp);
        prstack_leave();
        walked
    }

    /// print_object's loop over OBJ and the entries above BASE_SP.
    fn walk(
        &mut self,
        interp: &mut Interpreter,
        env: &mut Env,
        obj: Value,
        escape: bool,
        base_sp: usize,
    ) -> Result<(), LispError> {
        let mut obj = obj;
        loop {
            if let Some(next) = self.print_obj(interp, env, obj, escape)? {
                obj = next;
                continue;
            }
            match self.next_obj(interp, env, base_sp)? {
                Some(next) => obj = next,
                None => return Ok(()),
            }
        }
    }

    /// print_object's `print_obj': circle detection, then OBJ's own case.
    /// The object to print next, or none to go on with the stack.
    fn print_obj(
        &mut self,
        interp: &mut Interpreter,
        env: &mut Env,
        obj: Value,
        escape: bool,
    ) -> Result<Option<Value>, LispError> {
        self.sync(interp, env)?;
        interp.maybe_quit(env)?;
        self.vars(interp, CIRCLE);
        if !self.options.circle {
            // Simple but incomplete way.  (Below zero, where a print
            // run by Lisp left the depth of the print that ran it,
            // nothing is compared, and what C stores lands before the
            // array.)
            if let Ok(depth) = usize::try_from(interp.print_globals.depth) {
                if depth >= PRINT_CIRCLE {
                    return Err(LispError::Signal(
                        "Apparently circular structure being printed".into(),
                    ));
                }
                if let Some(index) = interp.print_globals.being_printed[..depth]
                    .iter()
                    .position(|outer| outer.word() == obj.word())
                {
                    self.strout(interp, env, &format!("#{index}"))?;
                    return Ok(None);
                }
                interp.print_globals.being_printed[depth] = obj;
            }
        } else {
            // PRINT_CIRCLE_CANDIDATE_P, which reads `print-gensym'.
            self.vars(interp, GENSYM);
            if circle_candidate(interp, &obj, self.options.gensym) {
                // With the print-circle feature.
                let num = self.number_get(interp, env, &obj)?;
                match num.kind() {
                    // Not printed yet: label it (the label written
                    // first), and remember it is printed.
                    Kind::Integer(n) if n < 0 => {
                        self.strout(interp, env, &format!("#{}=", -n))?;
                        // `make_fixnum (n)' of the negated number: the
                        // most negative fixnum's is no fixnum and wraps
                        // back to it.
                        let label = if -n > MOST_POSITIVE_FIXNUM {
                            (-n).wrapping_sub(1 << 62)
                        } else {
                            -n
                        };
                        self.number_put(interp, env, obj, Value::Integer(label))?;
                    }
                    // Just print #n# if OBJ has already been printed.
                    Kind::Integer(n) => {
                        self.strout(interp, env, &format!("#{n}#"))?;
                        return Ok(None);
                    }
                    Kind::StringObject(text) => {
                        self.strout_string(interp, env, text)?;
                        return Ok(None);
                    }
                    _ => {}
                }
            }
        }
        interp.print_globals.depth += 1;
        match self.print_case(interp, env, obj, escape)? {
            Step::Done => {
                interp.print_globals.depth -= 1;
                Ok(None)
            }
            Step::Next => Ok(None),
            Step::Print(next) => Ok(Some(next)),
        }
    }

    fn print_case(
        &mut self,
        interp: &mut Interpreter,
        env: &mut Env,
        obj: Value,
        escape: bool,
    ) -> Result<Step, LispError> {
        match obj.kind() {
            Kind::Integer(n) => self.print_fixnum(interp, env, n, escape)?,
            // print_bignum.
            Kind::BigInteger(_) => self.strout(interp, env, &obj.to_string())?,
            Kind::Float(f) => {
                let text = float_to_string(interp, f.get());
                self.strout(interp, env, &text)?;
            }
            Kind::StringObject(string) => {
                if escape {
                    self.print_string_literal(interp, env, string)?;
                } else {
                    self.print_string(interp, env, string)?;
                }
            }
            // The symbols they are: a printchar a character.
            Kind::Nil => self.printchars(interp, env, "nil")?,
            Kind::T => self.printchars(interp, env, "t")?,
            Kind::Symbol(symbol) => self.print_symbol(interp, env, &symbol, escape)?,
            Kind::Cons(_) => return self.print_cons(interp, env, obj, escape),
            Kind::Vector(vector) => {
                self.push_vector(interp, env, ("[", "]"), obj, 0, vector.len())?;
                return Ok(Step::Next);
            }
            Kind::LispRecord(record) => {
                self.push_vector(interp, env, ("#s(", ")"), obj, 0, record.len())?;
                return Ok(Step::Next);
            }
            Kind::Closure(closure) => {
                self.push_vector(interp, env, ("#[", "]"), obj, 0, closure.public_len())?;
                return Ok(Step::Next);
            }
            Kind::CharTable(table) => {
                self.push_vector(interp, env, ("#^[", "]"), obj, 0, table.slot_count())?;
                return Ok(Step::Next);
            }
            Kind::SubCharTable(table) => {
                self.strout(interp, env, "#^^[")?;
                let head = format!("{} {}", table.depth(), table.min_char());
                self.strout(interp, env, &head)?;
                self.push_vector(interp, env, ("", "]"), obj, 1, 1 + table.slots().len())?;
                return Ok(Step::Next);
            }
            Kind::HashTable(_) => return self.print_hash_table(interp, env, obj, escape),
            Kind::BoolVector(vector) => self.print_bool_vector(interp, env, vector)?,
            Kind::Unbound | Kind::ReaderForm(_) => self.strout(interp, env, &obj.to_string())?,
            _ => self.print_vectorlike_unreadable(interp, env, obj, escape)?,
        }
        Ok(Step::Done)
    }

    /// print_object's fixnum case: `?' and the character while
    /// print_integers_as_characters is set, when named_escape names it
    /// or graphic_base_p admits it, each a printchar; else
    /// fixnum_to_string's digits, one strout.
    fn print_fixnum(
        &mut self,
        interp: &mut Interpreter,
        env: &mut Env,
        n: i64,
        escape: bool,
    ) -> Result<(), LispError> {
        self.vars(interp, INTEGERS_AS_CHARACTERS);
        if self.options.integers_as_characters
            && let Some(code) = u32::try_from(n).ok().filter(|code| *code <= 0x10ffff)
        {
            let named = named_escape(code);
            if named.is_some() || super::search::graphic_base_p(interp, code) {
                self.printchar(interp, env, '?')?;
                let code = match named {
                    Some(name) => {
                        self.printchar(interp, env, '\\')?;
                        name as u32
                    }
                    None => {
                        if escape
                            && matches!(
                                char::from_u32(code),
                                Some(';' | '"' | '\'' | '\\' | '(' | ')' | '{' | '}' | '[' | ']')
                            )
                        {
                            self.printchar(interp, env, '\\')?;
                        }
                        code
                    }
                };
                return self.printchar_code(interp, env, code);
            }
        }
        let mut digits = [0u8; 20];
        let text = fixnum_digits(n, &mut digits);
        self.strout_ascii(interp, env, text)
    }

    /// print_object's Lisp_Symbol case: `#:' or `##', then the name, read
    /// with fetch_string_char_advance for each character.
    fn print_symbol(
        &mut self,
        interp: &mut Interpreter,
        env: &mut Env,
        symbol: &crate::lisp::types::SymbolName,
        escape: bool,
    ) -> Result<(), LispError> {
        let Kind::StringObject(name) = symbol.lisp_name().kind() else {
            let visible = crate::lisp::types::visible_symbol_name(symbol.as_str()).to_owned();
            return self.printchars(interp, env, &visible);
        };
        let (size_byte, mut confusing) = {
            let state = name.borrow();
            (state.bytes().len(), symbol_name_confusing(state.bytes()))
        };
        self.vars(interp, GENSYM);
        // `#:' for a symbol not interned in the initial obarray.
        if self.options.gensym
            && (symbol.cell().is_uninterned()
                || crate::lisp::types::is_private_obarray_symbol(symbol.as_str()))
        {
            self.strout(interp, env, "#:")?;
        } else if size_byte == 0 {
            return self.strout(interp, env, "##");
        }
        if let PrintSink::Function(function) = self.sink {
            let mut offset = 0;
            while offset < size_byte {
                let (code, width) = {
                    let state = name.borrow();
                    // The name as it is now; nothing past its
                    // terminating NUL is the name's.
                    if offset > state.bytes().len() {
                        break;
                    }
                    string_char_at(&state, offset)
                };
                offset += width;
                if escape && (symbol_char_quoted(code) || confusing) {
                    self.call_function(interp, env, function, u32::from(b'\\'))?;
                    confusing = false;
                }
                self.call_function(interp, env, function, code)?;
            }
            return Ok(());
        }
        let state = name.borrow();
        let mut offset = 0;
        while offset < size_byte {
            let (code, width) = string_char_at(&state, offset);
            offset += width;
            if escape && (symbol_char_quoted(code) || confusing) {
                self.out.push('\\');
                confusing = false;
            }
            self.out.push_code(code);
        }
        Ok(())
    }

    /// print_object's Lisp_Cons case.
    fn print_cons(
        &mut self,
        interp: &mut Interpreter,
        env: &mut Env,
        obj: Value,
        escape: bool,
    ) -> Result<Step, LispError> {
        // If deeper than spec'd depth, print placeholder.
        self.vars(interp, LEVEL | QUOTED);
        if self
            .options
            .level
            .is_some_and(|level| interp.print_globals.depth as i64 > level)
        {
            self.strout(interp, env, "...")?;
            return Ok(Step::Done);
        }
        let (car, cdr) = obj.cons_values().expect("a cons");
        if self.options.quoted
            && let Some((_, rest)) = cdr.cons_values()
            && rest.is_nil()
        {
            // The quoted object as it is once the quote is written.
            let quoted = |obj: Value| {
                obj.cons_values()
                    .and_then(|(_, cdr)| cdr.cons_values())
                    .map_or(Value::Nil, |(inner, _)| inner)
            };
            if is_symbol_key(&car, cached_symbol!("quote")) {
                self.printchar(interp, env, '\'')?;
                // Tail recursion.
                interp.print_globals.depth -= 1;
                return Ok(Step::Print(quoted(obj)));
            }
            if is_symbol_key(&car, cached_symbol!("function")) {
                self.strout(interp, env, "#'")?;
                interp.print_globals.depth -= 1;
                return Ok(Step::Print(quoted(obj)));
            }
            if is_symbol_key(&car, cached_symbol!("`")) {
                self.printchar(interp, env, '`')?;
                interp.print_globals.new_backquote_output += 1;
                self.print_object(interp, env, quoted(obj), escape)?;
                interp.print_globals.new_backquote_output -= 1;
                return Ok(Step::Done);
            }
            if (is_symbol_key(&car, cached_symbol!(","))
                || is_symbol_key(&car, cached_symbol!(",@")))
                && interp.print_globals.new_backquote_output != 0
            {
                self.print_object(interp, env, car, false)?;
                interp.print_globals.new_backquote_output -= 1;
                self.print_object(interp, env, quoted(obj), escape)?;
                interp.print_globals.new_backquote_output += 1;
                return Ok(Step::Done);
            }
        }
        self.printchar(interp, env, '(')?;
        // Negative values of print-length are invalid in CL.  Treat them
        // like nil, as CMUCL does.
        self.vars(interp, LENGTH);
        let print_length = self
            .options
            .length
            .map_or(i64::MAX, |length| i64::try_from(length).unwrap_or(i64::MAX));
        if print_length == 0 {
            self.strout(interp, env, "...)")?;
            return Ok(Step::Done);
        }
        prstack_push(PrintEntry::List {
            last: obj,
            maxlen: print_length,
            tortoise: obj,
            n: 2,
            m: 2,
            tortoise_idx: 0,
        });
        // Print the car, as it is now.
        Ok(Step::Print(
            obj.cons_values().map_or(Value::Nil, |(car, _)| car),
        ))
    }

    /// print.c:print_stack_push_vector: the bracket, then `print-length'
    /// read, then the entry.
    fn push_vector(
        &mut self,
        interp: &mut Interpreter,
        env: &mut Env,
        (lbrac, rbrac): (&str, &'static str),
        obj: Value,
        start: usize,
        size: usize,
    ) -> Result<(), LispError> {
        self.strout(interp, env, lbrac)?;
        self.vars(interp, LENGTH);
        let print_size = self.options.length.map_or(size, |length| length.min(size));
        prstack_push(PrintEntry::Vector {
            obj,
            size: print_size,
            idx: start,
            end: rbrac,
            truncated: print_size < size,
        });
        Ok(())
    }

    /// print_object's PVEC_HASH_TABLE case: a readable
    /// `#s(hash-table test equal data (k1 v1 k2 v2))'.
    fn print_hash_table(
        &mut self,
        interp: &mut Interpreter,
        env: &mut Env,
        obj: Value,
        escape: bool,
    ) -> Result<Step, LispError> {
        let Kind::HashTable(table) = obj.kind() else {
            return Ok(Step::Done);
        };
        self.strout(interp, env, "#s(hash-table")?;
        let test = table.test_name();
        // `!BASE_EQ (h->test->name, Qeql)'.
        if !matches!(test.kind(), Kind::Symbol(name) if name.is_named("eql")) {
            self.strout(interp, env, " test ")?;
            self.print_object(interp, env, test, escape)?;
        }
        let weakness = hash_table_weakness_value(table);
        if !weakness.is_nil() {
            self.strout(interp, env, " weakness ")?;
            self.print_object(interp, env, weakness, escape)?;
        }
        if table.purecopy() {
            self.strout(interp, env, " purecopy t")?;
        }
        // `h->count' as it is once all of that is written.
        let count = table.count();
        if count > 0 {
            self.strout(interp, env, " data (")?;
            // Don't print more elements than the specified maximum.
            self.vars(interp, LENGTH);
            let size = self
                .options
                .length
                .map_or(count, |length| length.min(count));
            prstack_push(PrintEntry::Hash {
                obj,
                nobjs: size * 2,
                idx: 0,
                printed: 0,
                truncated: size < table.count(),
            });
        } else {
            // Empty table: we can omit the data entirely.
            self.printchar(interp, env, ')')?;
            interp.print_globals.depth -= 1;
        }
        Ok(Step::Next)
    }

    /// print.c:print_bool_vector: `#&N"', then the bits packed eight to
    /// a byte, low-order bit first, each byte written with the escape
    /// rules octalout applies and the variables read for each.
    fn print_bool_vector(
        &mut self,
        interp: &mut Interpreter,
        env: &mut Env,
        vector: crate::lisp::types::BoolVectorRef,
    ) -> Result<(), LispError> {
        let size = vector.len();
        let real_size_in_bytes = size.div_ceil(8);
        self.strout(interp, env, &format!("#&{size}\""))?;
        // Don't print more bytes than the specified maximum.
        self.vars(interp, LENGTH);
        let size_in_bytes = self
            .options
            .length
            .map_or(real_size_in_bytes, |limit| limit.min(real_size_in_bytes));
        for index in 0..size_in_bytes {
            interp.maybe_quit(env)?;
            let byte = bool_vector_byte(vector, index);
            self.vars(
                interp,
                match byte {
                    b'\n' | 0x0c => ESCAPE_NEWLINES | ESCAPE_CONTROL,
                    0..=0x1f | 0x7f => ESCAPE_CONTROL,
                    _ => 0,
                },
            );
            if byte == b'\n' && self.escapes.newlines {
                self.strout(interp, env, "\\n")?;
            } else if byte == 0x0c && self.escapes.newlines {
                self.strout(interp, env, "\\f")?;
            } else if byte > 0o177 || (self.escapes.control && byte.is_ascii_control()) {
                // Use octal escapes to avoid encoding issues.
                let next_is_octal = index + 1 < size_in_bytes
                    && (b'0'..=b'7').contains(&bool_vector_byte(vector, index + 1));
                let digits = octal_digits(u32::from(byte), next_is_octal);
                self.printchar(interp, env, '\\')?;
                for shift in (0..digits).rev() {
                    self.printchar_code(
                        interp,
                        env,
                        u32::from(b'0') + ((u32::from(byte) >> (3 * shift)) & 7),
                    )?;
                }
            } else {
                if byte == b'"' || byte == b'\\' {
                    self.printchar(interp, env, '\\')?;
                }
                self.printchar_code(interp, env, u32::from(byte))?;
            }
        }
        if size_in_bytes < real_size_in_bytes {
            self.strout(interp, env, " ...")?;
        }
        self.printchar(interp, env, '"')
    }

    /// print_object's `next_obj': carry on with the innermost entry above
    /// BASE_SP.  The object to print next, or none when they are done.
    /// The entry is the top one whichever print pushed it, stepped where
    /// it lies; `--prstack.sp' takes the top one off, which a print run
    /// by a function printed to and left by a throw may have put above.
    fn next_obj(
        &mut self,
        interp: &mut Interpreter,
        env: &mut Env,
        base_sp: usize,
    ) -> Result<Option<Value>, LispError> {
        while let Some((index, entry)) = prstack_top(base_sp) {
            match entry {
                PrintEntry::List {
                    last,
                    maxlen,
                    tortoise,
                    n,
                    m,
                    tortoise_idx,
                } => {
                    let next = last.cons_values().map_or(Value::Nil, |(_, cdr)| cdr);
                    if next.is_nil() {
                        // End of list: print ")".
                        self.printchar(interp, env, ')')?;
                    } else if next.cons_values().is_some() {
                        self.vars(interp, CIRCLE);
                        if self.options.circle {
                            // With the print-circle feature.
                            let num = self.number_get(interp, env, &next)?;
                            if !(num.is_nil() || crate::lisp::types::eq(num, Value::T)) {
                                self.strout(interp, env, " . ")?;
                                prstack_set(index, PrintEntry::Rbrac);
                                return Ok(Some(next));
                            }
                        }
                        // List continues: print " " ELEM ...
                        self.printchar(interp, env, ' ')?;
                        let maxlen = maxlen - 1;
                        if maxlen <= 0 {
                            prstack_set(
                                index,
                                PrintEntry::List {
                                    last,
                                    maxlen,
                                    tortoise,
                                    n,
                                    m,
                                    tortoise_idx,
                                },
                            );
                            self.strout(interp, env, "...)")?;
                        } else {
                            let n = n - 1;
                            if n == 0 {
                                // Double tortoise update period and
                                // teleport it.
                                prstack_set(
                                    index,
                                    PrintEntry::List {
                                        last: next,
                                        maxlen,
                                        tortoise: next,
                                        n: m << 1,
                                        m: m << 1,
                                        tortoise_idx: tortoise_idx + m,
                                    },
                                );
                            } else {
                                prstack_set(
                                    index,
                                    PrintEntry::List {
                                        last: next,
                                        maxlen,
                                        tortoise,
                                        n,
                                        m,
                                        tortoise_idx,
                                    },
                                );
                                if next.word() == tortoise.word() {
                                    self.strout(interp, env, &format!(". #{tortoise_idx})"))?;
                                    prstack_pop();
                                    interp.print_globals.depth -= 1;
                                    continue;
                                }
                            }
                            // The element as it is once the space is
                            // written.
                            return Ok(Some(next.cons_values().map_or(Value::Nil, |(car, _)| car)));
                        }
                    } else {
                        // Non-nil ending: print " . " ELEM ")".
                        self.strout(interp, env, " . ")?;
                        prstack_set(index, PrintEntry::Rbrac);
                        return Ok(Some(next));
                    }
                }
                PrintEntry::Rbrac => self.printchar(interp, env, ')')?,
                PrintEntry::Vector {
                    obj,
                    size,
                    idx,
                    end,
                    truncated,
                } => {
                    if idx < size {
                        if idx > 0 {
                            self.printchar(interp, env, ' ')?;
                        }
                        let item = vectorlike_slot(&obj, idx);
                        prstack_set(
                            index,
                            PrintEntry::Vector {
                                obj,
                                size,
                                idx: idx + 1,
                                end,
                                truncated,
                            },
                        );
                        return Ok(Some(item));
                    }
                    if truncated {
                        if idx > 0 {
                            self.printchar(interp, env, ' ')?;
                        }
                        self.strout(interp, env, "...")?;
                    }
                    self.strout(interp, env, end)?;
                }
                PrintEntry::Hash {
                    obj,
                    nobjs,
                    idx,
                    printed,
                    truncated,
                } => {
                    if printed < nobjs {
                        if printed > 0 {
                            self.printchar(interp, env, ' ')?;
                        }
                        let table = match obj.kind() {
                            Kind::HashTable(table) => Some(table),
                            _ => None,
                        };
                        let (item, idx) = if printed % 2 == 0 {
                            match table.and_then(|table| table.entry_at_or_after(idx)) {
                                Some((slot, key, _)) => (key, slot),
                                None => (Value::Nil, idx),
                            }
                        } else {
                            let value = table
                                .and_then(|table| table.entry(idx))
                                .map_or(Value::Nil, |(_, value)| value);
                            (value, idx + 1)
                        };
                        prstack_set(
                            index,
                            PrintEntry::Hash {
                                obj,
                                nobjs,
                                idx,
                                printed: printed + 1,
                                truncated,
                            },
                        );
                        return Ok(Some(item));
                    }
                    if truncated {
                        if printed > 0 {
                            self.printchar(interp, env, ' ')?;
                        }
                        self.strout(interp, env, "...")?;
                    }
                    self.strout(interp, env, "))")?;
                }
            }
            prstack_pop();
            interp.print_globals.depth -= 1;
        }
        Ok(None)
    }

    /// print_object's Lisp_String case under `escapeflag': the literal,
    /// and with text properties (`print-charset-text-property' pruning
    /// `charset') `#(' and each interval as print_interval writes it.
    fn print_string_literal(
        &mut self,
        interp: &mut Interpreter,
        env: &mut Env,
        string: StringObjectRef,
    ) -> Result<(), LispError> {
        // `bool multibyte = STRING_MULTIBYTE (obj)', then `obj =
        // print_prune_string_charset (obj)': from here on the walk reads
        // that string, the copy before anything is written.
        let multibyte = string.borrow().is_multibyte();
        let printed = print_pruned_string(interp, string)?;
        // `if (string_intervals (obj))'.
        if printed.borrow().props.has_intervals() {
            self.strout(interp, env, "#(")?;
        }
        if let PrintSink::Function(function) = self.sink {
            self.call_function(interp, env, function, u32::from(b'"'))?;
            let size_byte = printed.borrow().bytes().len();
            let mut offset = 0;
            let mut need_nonhex = false;
            let mut turn = PrintOutput::plain();
            while offset < size_byte {
                {
                    let printed = printed.borrow();
                    // To the `size_byte' read at the start: a string the
                    // function made shorter is read at its terminating
                    // NUL next; nothing past that is the string's.
                    if offset > printed.bytes().len() {
                        break;
                    }
                    // The escape variables this character's turn reads.
                    self.vars(
                        interp,
                        match printed.bytes().get(offset).copied().unwrap_or(0) {
                            b'\n' | 0x0c => ESCAPE_NEWLINES | ESCAPE_CONTROL,
                            0..=0x1f | 0x7f => ESCAPE_CONTROL,
                            0x80.. if multibyte => ESCAPE_MULTIBYTE,
                            0x80.. => ESCAPE_NONASCII,
                            _ => 0,
                        },
                    );
                    turn.clear();
                    prin1_string_char(
                        self.escapes,
                        multibyte,
                        &printed,
                        &mut offset,
                        &mut need_nonhex,
                        &mut turn,
                    );
                }
                for code in turn.codes() {
                    self.call_function(interp, env, function, code)?;
                }
            }
            self.call_function(interp, env, function, u32::from(b'"'))?;
        } else {
            self.vars(interp, ESCAPES);
            render_prin1_string(self.escapes, multibyte, &printed.borrow(), &mut self.out);
            self.sync(interp, env)?;
        }
        // `if (string_intervals (obj))' again (a string printed itself
        // may have lost its properties to Lisp the print ran), then
        // traverse_intervals with print_interval: each interval read from
        // the string when its turn comes, one with no plist skipped.
        if !printed.borrow().props.has_intervals() {
            return Ok(());
        }
        let mut index = 0;
        loop {
            let interval = {
                let printed = printed.borrow();
                printed
                    .props
                    .intervals()
                    .filter(|intervals| index < intervals.count())
                    .map(|intervals| intervals.bounds(index))
            };
            let Some((start, end, plist)) = interval else {
                break;
            };
            index += 1;
            if plist.is_nil() {
                continue;
            }
            self.printchar(interp, env, ' ')?;
            self.print_object(interp, env, Value::Integer(start as i64), true)?;
            self.printchar(interp, env, ' ')?;
            self.print_object(interp, env, Value::Integer(end as i64), true)?;
            self.printchar(interp, env, ' ')?;
            self.print_object(interp, env, plist, true)?;
        }
        self.printchar(interp, env, ')')
    }

    /// print_vectorlike_unreadable's head: `print-unreadable-function',
    /// bound to nil while it runs, prints OBJ when it returns non-nil (a
    /// string as print_string writes it).
    fn print_unreadable_function(
        &mut self,
        interp: &mut Interpreter,
        env: &mut Env,
        obj: &Value,
        escape: bool,
    ) -> Result<bool, LispError> {
        let key = cached_symbol!("print-unreadable-function");
        let function = interp.symbol_value_key(key).unwrap_or(Value::Nil);
        if function.is_nil() || !function_value_p(interp, &function, env) {
            return Ok(false);
        }
        // Lisp runs: what is written is where it goes by now.
        self.flush(interp, env)?;
        let called: Result<Value, LispError> = (|| {
            let restore = key.with(|symbol| interp.bind_special_symbol(symbol, Value::Nil, env))?;
            let flag = if escape { Value::T } else { Value::Nil };
            // The function runs in print--unreadable-callback-buffer, the
            // buffer current when the print began, not the secret
            // " prin1" buffer prin1-to-string prints into ("this will
            // make Emacs segfault"); record_unwind_current_buffer, when
            // that buffer lives, makes the buffer current before current
            // again, whatever the function selected.
            let current = interp.current_buffer_id();
            let callback = interp
                .print_unreadable_callback_buffer
                .filter(|callback| interp.has_buffer_id(*callback));
            let selected = match callback {
                Some(callback) if callback != current => interp.set_current_buffer_id(callback),
                _ => Ok(()),
            };
            let result = selected
                .and_then(|()| interp.call_function_value(function, None, &[*obj, flag], env));
            let returned = if callback.is_some()
                && interp.current_buffer_id() != current
                && interp.has_buffer_id(current)
            {
                interp.set_current_buffer_id(current)
            } else {
                Ok(())
            };
            let restored = interp.restore_special_dynamic(restore, env);
            let result = result?;
            returned?;
            restored?;
            Ok(result)
        })();
        self.resume(interp);
        self.lisp_ran();
        let result = called?;
        if result.is_nil() {
            return Ok(false);
        }
        if let Kind::StringObject(text) = result.kind() {
            self.print_string(interp, env, text)?;
        }
        // It's handled, so stop processing here.
        Ok(true)
    }

    /// print.c:print_vectorlike_unreadable.
    fn print_vectorlike_unreadable(
        &mut self,
        interp: &mut Interpreter,
        env: &mut Env,
        obj: Value,
        escape: bool,
    ) -> Result<(), LispError> {
        if self.print_unreadable_function(interp, env, &obj, escape)? {
            return Ok(());
        }
        match obj.kind() {
            Kind::Marker(marker) => {
                self.strout(interp, env, "#<marker ")?;
                if marker.insertion_type() {
                    self.strout(interp, env, "(moves after insertion) ")?;
                }
                match marker.buffer() {
                    None => self.strout(interp, env, "in no buffer")?,
                    Some(_) => {
                        let at = format!("at {} in ", marker.last_position());
                        self.strout(interp, env, &at)?;
                        // `XMARKER (obj)->buffer' again: the buffer the
                        // marker is in once that is written.
                        let name = marker
                            .buffer()
                            .map_or(Value::Nil, |buffer| buffer.borrow().name());
                        self.print_name(interp, env, name)?;
                    }
                }
                self.printchar(interp, env, '>')?;
            }
            Kind::SymbolWithPos(object) => {
                self.vars(interp, SYMBOLS_BARE);
                if self.options.symbols_bare {
                    self.print_object(interp, env, object.symbol(), escape)?;
                } else {
                    self.strout(interp, env, "#<symbol ")?;
                    self.print_object(interp, env, object.symbol(), escape)?;
                    self.strout(interp, env, " at ")?;
                    self.print_object(interp, env, object.position(), escape)?;
                    self.printchar(interp, env, '>')?;
                }
            }
            Kind::Overlay(overlay) => {
                self.strout(interp, env, "#<overlay ")?;
                match overlay.buffer() {
                    None => self.strout(interp, env, "in no buffer")?,
                    Some(_) => {
                        let (start, end) = overlay.bounds();
                        self.strout(interp, env, &format!("from {start} to {end} in "))?;
                        // `OVERLAY_BUFFER (obj)' again.
                        let name = overlay
                            .buffer()
                            .map_or(Value::Nil, |buffer| buffer.borrow().name());
                        self.print_name(interp, env, name)?;
                    }
                }
                self.printchar(interp, env, '>')?;
            }
            Kind::BuiltinFunc(name) => {
                let name = name.to_string();
                self.strout(interp, env, "#<subr ")?;
                self.c_string(interp, env, name.as_bytes())?;
                self.printchar(interp, env, '>')?;
            }
            Kind::NativeFunction(function) => {
                let name = function.name().as_bytes().to_vec();
                self.strout(interp, env, "#<subr ")?;
                self.c_string(interp, env, &name)?;
                self.printchar(interp, env, '>')?;
            }
            Kind::Terminal(terminal) => {
                self.strout(interp, env, &format!("#<terminal {}", terminal.id))?;
                let name = {
                    let state = terminal.borrow();
                    state.live.then(|| state.name.clone())
                };
                if let Some(name) = name {
                    self.strout(interp, env, " on ")?;
                    self.c_string(interp, env, name.as_bytes())?;
                }
                self.printchar(interp, env, '>')?;
            }
            Kind::Buffer(buffer) => {
                // print_string (BVAR (XBUFFER (obj), name)).
                let name = |interp: &Interpreter| {
                    interp
                        .get_buffer_by_id(buffer.id)
                        .map_or(Value::Nil, |live| live.name())
                };
                if !interp.has_buffer_id(buffer.id) {
                    self.strout(interp, env, "#<killed buffer>")?;
                } else if escape {
                    self.strout(interp, env, "#<buffer ")?;
                    // The name as it is once that is written.
                    let name = name(interp);
                    self.print_name(interp, env, name)?;
                    self.printchar(interp, env, '>')?;
                } else {
                    let name = name(interp);
                    self.print_name(interp, env, name)?;
                }
            }
            Kind::Frame(frame) => {
                let id = frame.identity();
                // `frame_name = XFRAME (obj)->name' before anything is
                // written.
                let name = frame.name.get();
                let live = if interp.frame_is_live(frame) {
                    "#<frame "
                } else {
                    "#<dead frame "
                };
                self.strout(interp, env, live)?;
                match name.kind() {
                    Kind::StringObject(name) => self.print_string(interp, env, name)?,
                    _ => {
                        let name = format!("F{}", frame.borrow().id);
                        self.strout(interp, env, &name)?;
                    }
                }
                self.strout(interp, env, &format!(" 0x{id:x}>"))?;
            }
            Kind::Finalizer(finalizer) => {
                self.strout(interp, env, "#<finalizer")?;
                if finalizer.function().is_nil() {
                    self.strout(interp, env, " used")?;
                }
                self.printchar(interp, env, '>')?;
            }
            Kind::NativeCompUnit(unit) => {
                self.strout(interp, env, "#<native compilation unit: ")?;
                self.print_object(interp, env, unit.field(0), escape)?;
                self.printchar(interp, env, ' ')?;
                self.print_object(interp, env, unit.field(1), escape)?;
                self.printchar(interp, env, '>')?;
            }
            Kind::Record(id) => self.print_record_unreadable(interp, env, &obj, id, escape)?,
            _ => self.strout(interp, env, &obj.to_string())?,
        }
        Ok(())
    }

    /// `#<KIND NAME>' of a thread, mutex or condition variable:
    /// print_string of its name string, else the object's address (Emaxx
    /// quotes its own object identity in the same syntax).
    fn print_named(
        &mut self,
        interp: &mut Interpreter,
        env: &mut Env,
        head: &str,
        name: Value,
        identity: usize,
    ) -> Result<(), LispError> {
        self.strout(interp, env, head)?;
        match name.kind() {
            Kind::StringObject(name) => self.print_string(interp, env, name)?,
            _ => self.strout(interp, env, &format!("0x{identity:x}"))?,
        }
        self.printchar(interp, env, '>')
    }

    /// print_vectorlike_unreadable for the pseudovectors kept as records.
    fn print_record_unreadable(
        &mut self,
        interp: &mut Interpreter,
        env: &mut Env,
        obj: &Value,
        id: crate::lisp::types::RecordRef,
        escape: bool,
    ) -> Result<(), LispError> {
        use crate::lisp::eval::RecordKind;
        let Some(kind) = interp.find_record(id).map(|record| record.kind) else {
            return self.strout(interp, env, &obj.to_string());
        };
        match kind {
            RecordKind::ModuleFunction => {
                let rendered = crate::lisp::modules::print_function(interp, id.id);
                self.strout_unreadable(interp, env, &rendered)
            }
            RecordKind::UserPointer => {
                let rendered = crate::lisp::modules::print_user_pointer(interp, id.id);
                self.strout_unreadable(interp, env, &rendered)
            }
            RecordKind::Thread => {
                let name = interp.thread_name(id.id);
                self.print_named(interp, env, "#<thread ", name, id.identity())
            }
            RecordKind::Mutex => {
                let name = interp.mutex_name(id.id);
                self.print_named(interp, env, "#<mutex ", name, id.identity())
            }
            RecordKind::ConditionVariable => {
                let name = interp.condition_variable_name(id.id);
                self.print_named(interp, env, "#<condvar ", name, id.identity())
            }
            // `#<process NAME>', or the bare name when `princ' clears
            // escapeflag.
            RecordKind::Process => {
                if escape {
                    self.strout(interp, env, "#<process ")?;
                }
                match interp.process_name(id.id).map(|name| name.kind()) {
                    Some(Kind::StringObject(name)) => self.print_string(interp, env, name)?,
                    _ => self.strout(interp, env, &format!("0x{:x}", id.identity()))?,
                }
                if escape {
                    self.printchar(interp, env, '>')?;
                }
                Ok(())
            }
            RecordKind::Obarray => {
                let count = crate::lisp::primitives::completion::obarray_symbols(interp, obj)
                    .map(|symbols| symbols.len())
                    .unwrap_or(0);
                self.strout(interp, env, &format!("#<obarray n={count}>"))
            }
            RecordKind::Window => {
                let head = format!(
                    "#<window {}",
                    interp.window_sequence_number(id.id).unwrap_or(0)
                );
                self.strout(interp, env, &head)?;
                let buffer_name = |interp: &Interpreter| {
                    crate::lisp::primitives::window_buffer_id(interp, obj)
                        .and_then(|buffer_id| interp.get_buffer_by_id(buffer_id))
                        .map(|buffer| buffer.name())
                };
                // `BUFFERP (XWINDOW (obj)->contents)', then the name of
                // the buffer the window shows once " on " is written.
                if buffer_name(interp).is_some() {
                    self.strout(interp, env, " on ")?;
                    let name = buffer_name(interp).unwrap_or(Value::Nil);
                    self.print_name(interp, env, name)?;
                }
                self.printchar(interp, env, '>')
            }
            RecordKind::WindowConfiguration => self.strout(interp, env, "#<window-configuration>"),
            RecordKind::Sqlite => {
                let rendered = crate::lisp::sqlite::print_sqlite(interp, id.id);
                self.strout_unreadable(interp, env, &rendered)
            }
            // print.c PVEC_FONT: `#<font-spec' or `#<font-entity' and the
            // FONT_SPEC_MAX slots (an entity's FONT_EXTRA_INDEX, private
            // to its driver, left out), or `#<font-object NAME>'.
            RecordKind::Font => {
                const FONT_EXTRA_INDEX: usize = 12;
                const FONT_SPEC_MAX: usize = 13;
                const FONT_NAME_INDEX: usize = 14;
                let kind_name = interp
                    .find_record(id.id)
                    .and_then(|record| record.symbol_type_name())
                    .unwrap_or("font-spec")
                    .to_string();
                // `AREF (obj, i)' when slot I's turn comes: what Lisp the
                // print ran has stored there by then.
                let slot = |interp: &Interpreter, index: usize| {
                    interp
                        .find_record(id.id)
                        .and_then(|record| record.slots.get(index).copied())
                        .unwrap_or(Value::Nil)
                };
                if kind_name == "font-object" {
                    self.strout(interp, env, "#<font-object ")?;
                    let name = slot(interp, FONT_NAME_INDEX);
                    self.print_object(interp, env, name, escape)?;
                } else {
                    let entity = kind_name == "font-entity";
                    self.strout(
                        interp,
                        env,
                        if entity {
                            "#<font-entity"
                        } else {
                            "#<font-spec"
                        },
                    )?;
                    for index in 0..FONT_SPEC_MAX {
                        if index != FONT_EXTRA_INDEX || !entity {
                            self.printchar(interp, env, ' ')?;
                            // The style slots' numbers as their names
                            // (font_style_symbolic).
                            let value = slot(interp, index);
                            let value = if (crate::lisp::primitives::font::FONT_WEIGHT_INDEX
                                ..=crate::lisp::primitives::font::FONT_WIDTH_INDEX)
                                .contains(&index)
                            {
                                crate::lisp::primitives::font::style_symbolic_of(
                                    interp, &value, index, false,
                                )?
                            } else {
                                value
                            };
                            self.print_object(interp, env, value, escape)?;
                        }
                    }
                }
                self.printchar(interp, env, '>')
            }
            RecordKind::TreeSitterParser
            | RecordKind::TreeSitterNode
            | RecordKind::TreeSitterCompiledQuery => {
                // Still printed by their record slots.
                // The fields where the collector scans them while Lisp
                // the print runs can collect.
                let fields = crate::lisp::alloc::RootedVec::from_vec(
                    record_prin1_fields(interp, id.id).unwrap_or_default(),
                );
                self.strout(interp, env, "#s(")?;
                for index in 0..fields.len() {
                    if index > 0 {
                        self.printchar(interp, env, ' ')?;
                    }
                    let field = fields[index];
                    self.print_object(interp, env, field, escape)?;
                }
                self.strout(interp, env, ")")
            }
        }
    }
}
