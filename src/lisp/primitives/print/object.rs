//! print.c's printer proper: `print', `print_preprocess' and
//! `print_object'.  An object is walked with print.c's explicit stack
//! (prstack) and written to one output, so nesting costs neither host
//! stack nor a copy of the inner output at every level.  `print-circle''s
//! labels live in `print-number-table', read and written as print.c reads
//! and writes it.

use super::*;
use crate::lisp::types::HashTableRef;

/// print.c:63 `PRINT_CIRCLE'.
pub(crate) const PRINT_CIRCLE: usize = 200;

/// print.c's static printer state.  A printer run from a callback inside
/// another (`print-unreadable-function') shares it, as C statics are.
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
}

impl Default for PrintGlobals {
    fn default() -> Self {
        Self {
            depth: 0,
            being_printed: [Value::Nil; PRINT_CIRCLE],
            new_backquote_output: 0,
        }
    }
}

/// print.c's `print_stack_entry'.
enum PrintEntry {
    /// PE_list: after "(" ELEM (* " " ELEM): the cons whose car was just
    /// printed, the elements left to print, and Brent's cycle detection.
    List {
        last: Value,
        maxlen: usize,
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
        table: HashTableRef,
        nobjs: usize,
        idx: usize,
        printed: usize,
        truncated: bool,
    },
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
    out: PrintOutput,
    options: PrintOptions,
    /// The escape variables as print.c reads them for each string; only
    /// Lisp run from the printer (`print-unreadable-function') can change
    /// them, and they are read again after it.
    escapes: StringEscapes,
    stack: Vec<PrintEntry>,
}

/// print.c:print: reset the numbering unless it continues, fill
/// `print-number-table' for `print-circle' and drop what appears once,
/// then print OBJ.
pub(crate) fn print(
    interp: &mut Interpreter,
    env: &mut Env,
    obj: Value,
    options: PrintOptions,
) -> Result<PrintOutput, LispError> {
    interp.print_globals.new_backquote_output = 0;
    prepare_print_numbering(interp, env, options);
    if options.circle {
        print_preprocess(interp, env, obj, options)?;
        if let Some(table) = number_table(interp) {
            let once = table
                .entries()
                .filter(|(_, _, state)| state.word() == Value::T.word())
                .map(|(slot, _, _)| slot)
                .collect::<Vec<_>>();
            for slot in once {
                table.remove(slot);
            }
        }
    }
    interp.print_globals.depth = 0;
    let mut printer = Printer {
        out: PrintOutput::default(),
        options,
        escapes: StringEscapes::current(interp),
        stack: Vec::new(),
    };
    printer.print_object(interp, env, obj, options.escape)?;
    Ok(printer.out)
}

/// print.c:PRINT_CIRCLE_CANDIDATE_P.
fn circle_candidate(interp: &Interpreter, obj: &Value, options: PrintOptions) -> bool {
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
        Kind::Symbol(symbol) => options.gensym && symbol.cell().is_uninterned(),
        _ => false,
    }
}

fn number_table(interp: &Interpreter) -> Option<HashTableRef> {
    match interp
        .symbol_value_key(cached_symbol!("print-number-table"))?
        .kind()
    {
        Kind::HashTable(table) => Some(table),
        _ => None,
    }
}

/// Vprint_number_table as Fgethash and Fputhash check it.
fn checked_number_table(interp: &Interpreter) -> Result<HashTableRef, LispError> {
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
    options: PrintOptions,
) -> Result<(), LispError> {
    let mut pending = Vec::new();
    let mut obj = obj;
    loop {
        if circle_candidate(interp, &obj, options) {
            if number_table(interp).is_none() {
                let table = json::make_hash_table(interp, "eq", Vec::new());
                interp.set_variable("print-number-table", table, env);
            }
            let num = number_table_get(interp, env, &obj)?;
            // byte-compile-output-docform's gensyms always get a number.
            let numbered_gensym = options.continuous_numbering
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
                        let spans = state.borrow().props.to_vec();
                        for span in spans.into_iter().filter(|span| !span.props.is_empty()) {
                            print_preprocess(interp, env, plist_value(&span.props), options)?;
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

/// print_object's fixnum case: fixnum_to_string's decimal digits.
fn push_fixnum(out: &mut PrintOutput, n: i64) {
    let mut digits = [0u8; 20];
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
        out.push('-');
    }
    for &digit in &digits[at..] {
        out.push_code(u32::from(digit));
    }
}

fn is_symbol_key(value: &Value, key: crate::lisp::types::symbols::SymbolKey) -> bool {
    matches!(value.kind(), Kind::Symbol(symbol) if key.with(|wanted| *wanted == symbol))
}

impl Printer {
    /// print.c:print_object: OBJ and, through the stack entries it pushes,
    /// everything inside it, before returning to the entries below.
    fn print_object(
        &mut self,
        interp: &mut Interpreter,
        env: &mut Env,
        obj: Value,
        escape: bool,
    ) -> Result<(), LispError> {
        let base_sp = self.stack.len();
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
        interp.maybe_quit(env)?;
        if !self.options.circle {
            // Simple but incomplete way.
            let depth = usize::try_from(interp.print_globals.depth).unwrap_or(0);
            if depth >= PRINT_CIRCLE {
                return Err(LispError::Signal(
                    "Apparently circular structure being printed".into(),
                ));
            }
            if let Some(index) = interp.print_globals.being_printed[..depth]
                .iter()
                .position(|outer| outer.word() == obj.word())
            {
                self.out.push_str(&format!("#{index}"));
                return Ok(None);
            }
            interp.print_globals.being_printed[depth] = obj;
        } else if circle_candidate(interp, &obj, self.options) {
            let num = number_table_get(interp, env, &obj)?;
            match num.kind() {
                // Not printed yet: label it, and remember it is printed.
                Kind::Integer(n) if n < 0 => {
                    self.out.push_str(&format!("#{}=", -n));
                    number_table_put(interp, env, obj, Value::Integer(-n))?;
                }
                Kind::Integer(n) => {
                    self.out.push_str(&format!("#{n}#"));
                    return Ok(None);
                }
                Kind::StringObject(text) => {
                    self.out
                        .append(&render_princ_string(interp, &text.borrow(), env, false));
                    return Ok(None);
                }
                _ => {}
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
            Kind::Integer(n) => {
                match self
                    .options
                    .integers_as_characters
                    .then(|| render_integer_as_character(&obj, escape))
                    .flatten()
                {
                    Some(character) => self.out.push_str(&character),
                    None => push_fixnum(&mut self.out, n),
                }
            }
            Kind::BigInteger(_) => self.out.push_str(&obj.to_string()),
            Kind::Float(_) => self.out.push_str(&obj.to_string()),
            Kind::StringObject(state) => {
                if escape {
                    self.print_string_literal(interp, env, state)?;
                } else {
                    self.out.append(&render_princ_string(
                        interp,
                        &state.borrow(),
                        env,
                        self.options.output_is_function,
                    ));
                }
            }
            Kind::Nil => self.out.push_str("nil"),
            Kind::T => self.out.push_str("t"),
            Kind::Symbol(symbol) => {
                let options = PrintOptions {
                    escape,
                    ..self.options
                };
                render_prin1_symbol(&symbol, options, &mut self.out);
            }
            Kind::Cons(_) => return self.print_cons(interp, env, obj, escape),
            Kind::Vector(vector) => {
                self.push_vector("[", "]", obj, 0, vector.len());
                return Ok(Step::Next);
            }
            Kind::LispRecord(record) => {
                self.push_vector("#s(", ")", obj, 0, record.len());
                return Ok(Step::Next);
            }
            Kind::Closure(closure) => {
                self.push_vector("#[", "]", obj, 0, closure.public_len());
                return Ok(Step::Next);
            }
            Kind::CharTable(table) => {
                self.push_vector("#^[", "]", obj, 0, table.slot_count());
                return Ok(Step::Next);
            }
            Kind::SubCharTable(table) => {
                self.out.push_str("#^^[");
                self.out
                    .push_str(&format!("{} {}", table.depth(), table.min_char()));
                self.push_vector("", "]", obj, 1, 1 + table.slots().len());
                return Ok(Step::Next);
            }
            Kind::HashTable(table) => return self.print_hash_table(interp, env, table, escape),
            Kind::BoolVector(vector) => {
                self.out
                    .push_str(&render_bool_vector_prin1(interp, env, vector, self.options));
            }
            Kind::Unbound | Kind::ReaderForm(_) => self.out.push_str(&obj.to_string()),
            _ => self.print_vectorlike_unreadable(interp, env, obj, escape)?,
        }
        Ok(Step::Done)
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
        if self.options.level.is_some_and(|level| {
            usize::try_from(interp.print_globals.depth).is_ok_and(|depth| depth > level)
        }) {
            self.out.push_str("...");
            return Ok(Step::Done);
        }
        let (car, cdr) = obj.cons_values().expect("a cons");
        if self.options.quoted
            && let Some((inner, rest)) = cdr.cons_values()
            && rest.is_nil()
        {
            if is_symbol_key(&car, cached_symbol!("quote")) {
                self.out.push('\'');
                // Tail recursion.
                interp.print_globals.depth -= 1;
                return Ok(Step::Print(inner));
            }
            if is_symbol_key(&car, cached_symbol!("function")) {
                self.out.push_str("#'");
                interp.print_globals.depth -= 1;
                return Ok(Step::Print(inner));
            }
            if is_symbol_key(&car, cached_symbol!("`")) {
                self.out.push('`');
                interp.print_globals.new_backquote_output += 1;
                self.print_object(interp, env, inner, escape)?;
                interp.print_globals.new_backquote_output -= 1;
                return Ok(Step::Done);
            }
            if (is_symbol_key(&car, cached_symbol!(","))
                || is_symbol_key(&car, cached_symbol!(",@")))
                && interp.print_globals.new_backquote_output != 0
            {
                self.print_object(interp, env, car, false)?;
                interp.print_globals.new_backquote_output -= 1;
                self.print_object(interp, env, inner, escape)?;
                interp.print_globals.new_backquote_output += 1;
                return Ok(Step::Done);
            }
        }
        self.out.push('(');
        // Negative values of print-length are invalid in CL.  Treat them
        // like nil, as CMUCL does.
        let print_length = self.options.length.unwrap_or(usize::MAX);
        if print_length == 0 {
            self.out.push_str("...)");
            return Ok(Step::Done);
        }
        self.stack.push(PrintEntry::List {
            last: obj,
            maxlen: print_length,
            tortoise: obj,
            n: 2,
            m: 2,
            tortoise_idx: 0,
        });
        Ok(Step::Print(car))
    }

    /// print.c:print_stack_push_vector.
    fn push_vector(
        &mut self,
        lbrac: &str,
        rbrac: &'static str,
        obj: Value,
        start: usize,
        size: usize,
    ) {
        self.out.push_str(lbrac);
        let print_size = self.options.length.map_or(size, |length| length.min(size));
        self.stack.push(PrintEntry::Vector {
            obj,
            size: print_size,
            idx: start,
            end: rbrac,
            truncated: print_size < size,
        });
    }

    /// print_object's PVEC_HASH_TABLE case: a readable
    /// `#s(hash-table test equal data (k1 v1 k2 v2))'.
    fn print_hash_table(
        &mut self,
        interp: &mut Interpreter,
        env: &mut Env,
        table: HashTableRef,
        escape: bool,
    ) -> Result<Step, LispError> {
        self.out.push_str("#s(hash-table");
        let test = table.test_name();
        if !matches!(test.kind(), Kind::Symbol(name) if name == "eql") {
            self.out.push_str(" test ");
            self.print_object(interp, env, test, escape)?;
        }
        let weakness = hash_table_weakness_value(table);
        if !weakness.is_nil() {
            self.out.push_str(" weakness ");
            self.print_object(interp, env, weakness, escape)?;
        }
        if table.purecopy() {
            self.out.push_str(" purecopy t");
        }
        let count = table.count();
        if count > 0 {
            self.out.push_str(" data (");
            // Don't print more elements than the specified maximum.
            let size = self
                .options
                .length
                .map_or(count, |length| length.min(count));
            self.stack.push(PrintEntry::Hash {
                table,
                nobjs: size * 2,
                idx: 0,
                printed: 0,
                truncated: size < count,
            });
        } else {
            // Empty table: we can omit the data entirely.
            self.out.push(')');
            interp.print_globals.depth -= 1;
        }
        Ok(Step::Next)
    }

    /// print_object's `next_obj': carry on with the innermost entry above
    /// BASE_SP.  The object to print next, or none when they are done.
    fn next_obj(
        &mut self,
        interp: &mut Interpreter,
        env: &mut Env,
        base_sp: usize,
    ) -> Result<Option<Value>, LispError> {
        while self.stack.len() > base_sp {
            let entry = self.stack.last_mut().expect("an entry above BASE_SP");
            let next = match entry {
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
                        self.out.push(')');
                        None
                    } else if let Some((next_car, _)) = next.cons_values() {
                        let mut labelled = false;
                        if self.options.circle {
                            let num = number_table_get(interp, env, &next)?;
                            labelled = !(num.is_nil() || num.word() == Value::T.word());
                        }
                        if labelled {
                            self.out.push_str(" . ");
                            *entry = PrintEntry::Rbrac;
                            Some(next)
                        } else {
                            // List continues: print " " ELEM ...
                            self.out.push(' ');
                            *maxlen -= 1;
                            if *maxlen == 0 {
                                self.out.push_str("...)");
                                None
                            } else {
                                *last = next;
                                *n -= 1;
                                if *n == 0 {
                                    // Double tortoise update period and
                                    // teleport it.
                                    *tortoise_idx += *m;
                                    *m <<= 1;
                                    *n = *m;
                                    *tortoise = next;
                                    Some(next_car)
                                } else if next.word() == tortoise.word() {
                                    self.out.push_str(&format!(". #{tortoise_idx})"));
                                    None
                                } else {
                                    Some(next_car)
                                }
                            }
                        }
                    } else {
                        // Non-nil ending: print " . " ELEM ")".
                        self.out.push_str(" . ");
                        *entry = PrintEntry::Rbrac;
                        Some(next)
                    }
                }
                PrintEntry::Rbrac => {
                    self.out.push(')');
                    None
                }
                PrintEntry::Vector {
                    obj,
                    size,
                    idx,
                    end,
                    truncated,
                } => {
                    if *idx >= *size {
                        if *truncated {
                            if *idx > 0 {
                                self.out.push(' ');
                            }
                            self.out.push_str("...");
                        }
                        self.out.push_str(end);
                        None
                    } else {
                        if *idx > 0 {
                            self.out.push(' ');
                        }
                        let item = vectorlike_slot(obj, *idx);
                        *idx += 1;
                        Some(item)
                    }
                }
                PrintEntry::Hash {
                    table,
                    nobjs,
                    idx,
                    printed,
                    truncated,
                } => {
                    if *printed >= *nobjs {
                        if *truncated {
                            if *printed > 0 {
                                self.out.push(' ');
                            }
                            self.out.push_str("...");
                        }
                        self.out.push_str("))");
                        None
                    } else {
                        if *printed > 0 {
                            self.out.push(' ');
                        }
                        let item = if *printed % 2 == 0 {
                            match table.entry_at_or_after(*idx) {
                                Some((slot, key, _)) => {
                                    *idx = slot;
                                    key
                                }
                                None => Value::Nil,
                            }
                        } else {
                            let value = table.entry(*idx).map_or(Value::Nil, |(_, value)| value);
                            *idx += 1;
                            value
                        };
                        *printed += 1;
                        Some(item)
                    }
                }
            };
            match next {
                Some(next) => return Ok(Some(next)),
                None => {
                    self.stack.pop();
                    interp.print_globals.depth -= 1;
                }
            }
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
        state: crate::lisp::types::StringObjectRef,
    ) -> Result<(), LispError> {
        let intervals = {
            let state = state.borrow();
            let intervals = if state.props.is_empty() {
                Vec::new()
            } else {
                let props = state.props.to_vec();
                let keep_charset = props
                    .iter()
                    .any(|span| span.props.iter().any(|(name, _)| name == "charset"))
                    && charset_text_properties_print(interp, env, &state.codes(), &props);
                props
                    .into_iter()
                    .filter_map(|span| {
                        let plist = span
                            .props
                            .iter()
                            .filter(|(name, _)| name != "charset" || keep_charset)
                            .cloned()
                            .collect::<Vec<_>>();
                        (!plist.is_empty()).then(|| (span.start, span.end, plist_value(&plist)))
                    })
                    .collect::<Vec<_>>()
            };
            if !intervals.is_empty() {
                self.out.push_str("#(");
            }
            render_prin1_string(self.escapes, &state, &mut self.out);
            intervals
        };
        if intervals.is_empty() {
            return Ok(());
        }
        for (start, end, plist) in intervals {
            self.out.push(' ');
            self.print_object(interp, env, Value::Integer(start as i64), true)?;
            self.out.push(' ');
            self.print_object(interp, env, Value::Integer(end as i64), true)?;
            self.out.push(' ');
            self.print_object(interp, env, plist, true)?;
        }
        self.out.push(')');
        Ok(())
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
        let restore = key.with(|symbol| interp.bind_special_symbol(symbol, Value::Nil, env))?;
        let flag = if escape { Value::T } else { Value::Nil };
        let result = call_function_value(interp, &function, &[*obj, flag], env);
        let restored = interp.restore_special_dynamic(restore, env);
        self.escapes = StringEscapes::current(interp);
        let result = result?;
        restored?;
        if result.is_nil() {
            return Ok(false);
        }
        if let Kind::StringObject(text) = result.kind() {
            self.out.append(&render_princ_string(
                interp,
                &text.borrow(),
                env,
                self.options.output_is_function,
            ));
        }
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
                self.out.push_str("#<marker ");
                if marker.insertion_type() {
                    self.out.push_str("(moves after insertion) ");
                }
                match marker.buffer() {
                    None => self.out.push_str("in no buffer"),
                    Some(buffer) => {
                        self.out
                            .push_str(&format!("at {} in ", marker.last_position()));
                        self.out.push_str(&buffer.borrow().name);
                    }
                }
                self.out.push('>');
            }
            Kind::SymbolWithPos(object) => {
                if self.options.symbols_bare {
                    self.print_object(interp, env, object.symbol(), escape)?;
                } else {
                    self.out.push_str("#<symbol ");
                    self.print_object(interp, env, object.symbol(), escape)?;
                    self.out.push_str(" at ");
                    self.print_object(interp, env, object.position(), escape)?;
                    self.out.push('>');
                }
            }
            Kind::Overlay(overlay) => {
                self.out.push_str("#<overlay ");
                match overlay.buffer() {
                    None => self.out.push_str("in no buffer"),
                    Some(buffer) => {
                        let (start, end) = overlay.bounds();
                        self.out.push_str(&format!("from {start} to {end} in "));
                        self.out.push_str(&buffer.borrow().name);
                    }
                }
                self.out.push('>');
            }
            Kind::BuiltinFunc(name) => self.out.push_str(&format!("#<subr {name}>")),
            Kind::NativeFunction(function) => {
                self.out.push_str("#<subr ");
                if self.options.output_is_function {
                    for byte in function.name().bytes() {
                        self.out.push_c_function_byte(byte);
                    }
                } else {
                    self.out.push_c_string(function.name());
                }
                self.out.push('>');
            }
            Kind::Terminal(terminal) => {
                let state = terminal.borrow();
                self.out.push_str(&format!("#<terminal {}", terminal.id));
                if state.live {
                    self.out.push_str(" on ");
                    self.out.push_str(&state.name);
                }
                self.out.push('>');
            }
            Kind::Buffer(buffer) => match interp.get_buffer_by_id(buffer.id) {
                None => self.out.push_str("#<killed buffer>"),
                Some(live) if escape => {
                    self.out.push_str("#<buffer ");
                    self.out.push_str(&live.name);
                    self.out.push('>');
                }
                Some(live) => self.out.push_str(&live.name),
            },
            Kind::Frame(frame) => {
                let id = frame.identity();
                let name = string_text(&frame.name.get())
                    .unwrap_or_else(|_| format!("F{}", frame.borrow().id));
                let live = if interp.frame_is_live(frame) {
                    "#<frame "
                } else {
                    "#<dead frame "
                };
                self.out.push_str(&format!("{live}{name} 0x{id:x}>"));
            }
            Kind::Finalizer(finalizer) => {
                self.out.push_str("#<finalizer");
                if finalizer.function().is_nil() {
                    self.out.push_str(" used");
                }
                self.out.push('>');
            }
            Kind::NativeCompUnit(unit) => {
                self.out.push_str("#<native compilation unit: ");
                self.print_object(interp, env, unit.field(0), escape)?;
                self.out.push(' ');
                self.print_object(interp, env, unit.field(1), escape)?;
                self.out.push('>');
            }
            Kind::Record(id) => self.print_record_unreadable(interp, env, &obj, id, escape)?,
            _ => self.out.push_str(&obj.to_string()),
        }
        Ok(())
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
            self.out.push_str(&obj.to_string());
            return Ok(());
        };
        let rendered = match kind {
            RecordKind::ModuleFunction => crate::lisp::modules::print_function(interp, id.id),
            RecordKind::UserPointer => crate::lisp::modules::print_user_pointer(interp, id.id),
            // print.c prints a thread, mutex or condition variable by
            // name, falling back to the object's address; Emaxx quotes
            // its own object identity in the same syntax.
            RecordKind::Thread => interp
                .thread_name(id.id)
                .map(|name| format!("#<thread {name}>"))
                .unwrap_or_else(|| format!("#<thread 0x{:x}>", id.identity())),
            RecordKind::Mutex => interp
                .mutex_name(id.id)
                .map(|name| format!("#<mutex {name}>"))
                .unwrap_or_else(|| format!("#<mutex 0x{:x}>", id.identity())),
            RecordKind::ConditionVariable => interp
                .condition_variable_name(id.id)
                .map(|name| format!("#<condvar {name}>"))
                .unwrap_or_else(|| format!("#<condvar 0x{:x}>", id.identity())),
            // `#<process NAME>', or the bare name when `princ' clears
            // escapeflag.
            RecordKind::Process => {
                let name = interp
                    .process_name(id.id)
                    .unwrap_or_else(|| format!("0x{:x}", id.identity()));
                if escape {
                    format!("#<process {name}>")
                } else {
                    name
                }
            }
            RecordKind::Obarray => {
                let count = crate::lisp::primitives::completion::obarray_symbols(interp, obj)
                    .map(|symbols| symbols.len())
                    .unwrap_or(0);
                format!("#<obarray n={count}>")
            }
            RecordKind::Window => {
                let mut rendered = format!(
                    "#<window {}",
                    interp.window_sequence_number(id.id).unwrap_or(0)
                );
                if let Some(buffer) = crate::lisp::primitives::window_buffer_id(interp, obj)
                    .and_then(|buffer_id| interp.get_buffer_by_id(buffer_id))
                {
                    rendered.push_str(" on ");
                    rendered.push_str(&buffer.name);
                }
                rendered.push('>');
                rendered
            }
            RecordKind::WindowConfiguration => "#<window-configuration>".into(),
            RecordKind::Font
            | RecordKind::TreeSitterParser
            | RecordKind::TreeSitterNode
            | RecordKind::TreeSitterCompiledQuery
            | RecordKind::Sqlite => {
                // Still printed by their record slots.
                let fields = record_prin1_fields(interp, id.id).unwrap_or_default();
                self.out.push_str("#s(");
                for (index, field) in fields.into_iter().enumerate() {
                    if index > 0 {
                        self.out.push(' ');
                    }
                    self.print_object(interp, env, field, escape)?;
                }
                self.out.push(')');
                return Ok(());
            }
        };
        self.out.push_str(&rendered);
        Ok(())
    }
}
