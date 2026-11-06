use super::*;
use crate::lisp::types::Kind;

pub fn buffer_undo_list_value(buffer: &crate::buffer::Buffer) -> Value {
    buffer.undo_list()
}

/// fns.c's equal_kind for the two walks Lisp asks for (equal_no_quit's
/// is not used here).
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum EqualKind {
    /// Fequal.
    Plain,
    /// Fequal_including_properties: strings' text properties too.
    IncludingProperties,
}

/// fns.c:Fequal.
pub(crate) fn values_equal(
    interp: &Interpreter,
    left: &Value,
    right: &Value,
) -> Result<bool, LispError> {
    internal_equal(interp, *left, *right, EqualKind::Plain, 0, &mut None)
}

/// fns.c:Fequal_including_properties.
pub(crate) fn values_equal_including_properties(
    interp: &Interpreter,
    left: &Value,
    right: &Value,
) -> Result<bool, LispError> {
    internal_equal(
        interp,
        *left,
        *right,
        EqualKind::IncludingProperties,
        0,
        &mut None,
    )
}

/// internal_equal's HT, made the first time the walk is more than 10
/// deep: each cons or vectorlike O1 compared there, with the objects O2 it
/// was compared with.  A pair met again is taken as equal, so structures
/// that lead back into themselves compare without end.
type EqualSeen = HashMap<usize, Vec<Value>>;

/// lisp.h's FOR_EACH_TAIL bookkeeping (struct for_each_tail_internal):
/// Brent's tortoise, which moves to the tail in hand after MAX steps
/// (Q counts them, with N counting each 65,536), MAX doubling each time.
pub(crate) struct ForEachTail {
    tortoise: Value,
    max: isize,
    n: isize,
    q: u16,
}

impl ForEachTail {
    pub(crate) fn new(list: Value) -> Self {
        ForEachTail {
            tortoise: list,
            max: 2,
            n: 0,
            q: 2,
        }
    }

    /// The step after TAIL = XCDR (TAIL): whether TAIL is the tortoise,
    /// closing a cycle.
    #[inline]
    pub(crate) fn step(&mut self, tail: Value) -> bool {
        self.q = self.q.wrapping_sub(1);
        if self.q != 0 {
            return tail.eq_value(self.tortoise);
        }
        self.n -= 1;
        if 0 < self.n {
            return tail.eq_value(self.tortoise);
        }
        self.max <<= 1;
        self.q = self.max as u16;
        self.n = self.max >> 16;
        self.tortoise = tail;
        false
    }
}

/// maybe_remove_pos_from_symbol: the bare symbol of a symbol with
/// position while `symbols-with-pos-enabled' is set.
#[inline(always)]
pub(crate) fn bare_symbol(interp: &Interpreter, value: Value) -> Value {
    // The flag first, as lisp.h tests symbols_with_pos_enabled before
    // SYMBOL_WITH_POS_P: no object is looked into while it is off.
    if !interp.symbols_with_positions_enabled() || !value.is_vectorlike() {
        return value;
    }
    match value.kind() {
        Kind::SymbolWithPos(symbol) => symbol.symbol(),
        _ => value,
    }
}

/// fns.c:internal_equal's first tests, ahead of the walk below: above
/// depth 10, where HT is not consulted yet, EQ objects are equal, and
/// objects of two XTYPEs or two symbols or fixnums that are not EQ are
/// not (the switch has no case for them).  A fixnum and a vectorlike go
/// on, as a bignum Emaxx did not normalize could be one's value.  Two
/// strings compare here too, but for `equal-including-properties'.
#[inline(always)]
fn internal_equal(
    interp: &Interpreter,
    o1: Value,
    o2: Value,
    kind: EqualKind,
    depth: usize,
    ht: &mut Option<EqualSeen>,
) -> Result<bool, LispError> {
    if depth <= 10 {
        let (o1, o2) = (bare_symbol(interp, o1), bare_symbol(interp, o2));
        if o1.eq_value(o2) {
            return Ok(true);
        }
        let xtype = o1.xtype();
        if xtype != o2.xtype() {
            if !(o1.is_fixnum() && o2.is_vectorlike() || o2.is_fixnum() && o1.is_vectorlike()) {
                return Ok(false);
            }
        } else if o1.is_symbol() || o1.is_fixnum() {
            return Ok(false);
        } else if kind == EqualKind::Plain
            && let (Kind::StringObject(left), Kind::StringObject(right)) = (o1.kind(), o2.kind())
        {
            // The Lisp_String case: SCHARS, SBYTES and the bytes.
            return Ok(left.borrow().contents_equal(&right.borrow()));
        }
    }
    internal_equal_1(interp, o1, o2, kind, depth, ht)
}

/// fns.c:internal_equal: O1 and O2 compared DEPTH levels down.  Past
/// depth 10 HT remembers the pairs compared; past depth 200 the walk
/// signals instead of recursing further.  A list's cdrs are walked in a
/// loop that signals `circular-list' with the tail that closes a cycle;
/// its last cdr, and an overlay's plist, are compared one level down by
/// going round again (tail_recurse).
fn internal_equal_1(
    interp: &Interpreter,
    mut o1: Value,
    mut o2: Value,
    kind: EqualKind,
    mut depth: usize,
    ht: &mut Option<EqualSeen>,
) -> Result<bool, LispError> {
    loop {
        if depth > 10 {
            if depth > 200 {
                return Err(LispError::Signal("Stack overflow in equal".into()));
            }
            if o1.is_cons() || o1.is_vectorlike() {
                let o2s = ht
                    .get_or_insert_with(HashMap::new)
                    .entry(o1.word())
                    .or_default();
                if o2s.iter().any(|seen| seen.eq_value(o2)) {
                    return Ok(true);
                }
                o2s.push(o2);
            }
        }
        // A symbol with position compares the contained symbol, and is
        // `equal' to the corresponding ordinary symbol.
        o1 = bare_symbol(interp, o1);
        o2 = bare_symbol(interp, o2);
        if o1.eq_value(o2) {
            return Ok(true);
        }
        // XTYPE at tail_recurse, at any depth: objects of two types are
        // not equal, nor two symbols or two fixnums that are not EQ (the
        // switch has no case for them), so a list's last cdr is settled
        // here; a fixnum and a vectorlike go on, as in `internal_equal'.
        if o1.xtype() != o2.xtype() {
            if !(o1.is_fixnum() && o2.is_vectorlike() || o2.is_fixnum() && o1.is_vectorlike()) {
                return Ok(false);
            }
        } else if o1.is_symbol() || o1.is_fixnum() {
            return Ok(false);
        }
        if o1.is_cons() && o2.is_cons() {
            let mut tail = ForEachTail::new(o1);
            while let Some(cell) = o1.cons_ref() {
                let Some(other) = o2.cons_ref() else {
                    return Ok(false);
                };
                if !internal_equal(interp, cell.car.get(), other.car.get(), kind, depth + 1, ht)? {
                    return Ok(false);
                }
                o2 = other.cdr.get();
                o1 = cell.cdr.get();
                // EQ (XCDR (o1), o2).
                if o1.eq_value(o2) || bare_symbol(interp, o1).eq_value(bare_symbol(interp, o2)) {
                    return Ok(true);
                }
                if tail.step(o1) {
                    return Err(LispError::SignalValue(Value::list([
                        Value::Symbol("circular-list".into()),
                        o1,
                    ])));
                }
            }
            depth += 1;
            continue;
        }
        match (o1.kind(), o2.kind()) {
            (Kind::Overlay(left), Kind::Overlay(right)) => {
                if !overlay_extents_equal(left, right) {
                    return Ok(false);
                }
                o1 = left.plist();
                o2 = right.plist();
                depth += 1;
            }
            _ => return equal_atoms(interp, o1, o2, kind, depth, ht),
        }
    }
}

/// internal_equal for O1 and O2 that are not both conses or overlays:
/// numbers, strings and the other vectorlike objects.  Bignums, markers,
/// bool vectors, tree-sitter nodes and symbols with position compare
/// their values; vectors, closures, char-tables and records their slots;
/// every other pseudovector is equal only to itself.
fn equal_atoms(
    interp: &Interpreter,
    o1: Value,
    o2: Value,
    kind: EqualKind,
    depth: usize,
    ht: &mut Option<EqualSeen>,
) -> Result<bool, LispError> {
    let mut slots = |left: &mut dyn Iterator<Item = Value>,
                     right: &mut dyn Iterator<Item = Value>|
     -> Result<bool, LispError> {
        for (left, right) in left.zip(right) {
            if !internal_equal(interp, left, right, kind, depth + 1, ht)? {
                return Ok(false);
            }
        }
        Ok(true)
    };
    Ok(match (o1.kind(), o2.kind()) {
        (Kind::Float(a), Kind::Float(b)) => a.to_bits() == b.to_bits(),
        (Kind::Integer(a), Kind::Integer(b)) => a == b,
        (Kind::BigInteger(a), Kind::BigInteger(b)) => a == b,
        (Kind::Integer(a), Kind::BigInteger(b)) | (Kind::BigInteger(b), Kind::Integer(a)) => {
            BigInt::from(a) == *b
        }
        (Kind::StringObject(left), Kind::StringObject(right)) => {
            let (left, right) = (left.borrow(), right.borrow());
            left.contents_equal(&right)
                && (kind == EqualKind::Plain || string_intervals_equal(interp, &left, &right)?)
        }
        (Kind::Marker(left), Kind::Marker(right)) => markers_equal(left, right),
        (Kind::SymbolWithPos(left), Kind::SymbolWithPos(right)) => {
            left.symbol().eq_value(right.symbol()) && left.position().eq_value(right.position())
        }
        (Kind::Vector(left), Kind::Vector(right)) => {
            left.len() == right.len() && slots(&mut left.slots(), &mut right.slots())?
        }
        (Kind::Closure(left), Kind::Closure(right)) => {
            left.public_len() == right.public_len() && slots(&mut left.slots(), &mut right.slots())?
        }
        (Kind::CharTable(left), Kind::CharTable(right)) => {
            left.slot_count() == right.slot_count() && slots(&mut left.slots(), &mut right.slots())?
        }
        (Kind::SubCharTable(left), Kind::SubCharTable(right)) => {
            left.depth() == right.depth()
                && left.min_char() == right.min_char()
                && slots(&mut left.slots(), &mut right.slots())?
        }
        (Kind::LispRecord(left), Kind::LispRecord(right)) => {
            left.len() == right.len() && slots(&mut left.slots(), &mut right.slots())?
        }
        // fns.c:internal_equal: the lengths, then the bits' bytes.
        (Kind::BoolVector(left), Kind::BoolVector(right)) => {
            left.len() == right.len()
                && left
                    .words()
                    .iter()
                    .zip(right.words())
                    .all(|(left, right)| left.get() == right.get())
        }
        // A record and the reader's form of a record literal.
        (Kind::LispRecord(record), _) | (_, Kind::LispRecord(record)) => {
            let form = if matches!(o1.kind(), Kind::LispRecord(_)) {
                o2
            } else {
                o1
            };
            match record_literal_items(&form) {
                Some(items) => {
                    let fields = &items[1..];
                    record.len() == fields.len()
                        && slots(
                            &mut record.slots(),
                            &mut fields.iter().map(record_literal_slot_data),
                        )?
                }
                None => false,
            }
        }
        (Kind::Record(left_id), Kind::Record(right_id)) => {
            let (Some(left), Some(right)) =
                (interp.find_record(left_id), interp.find_record(right_id))
            else {
                return Ok(false);
            };
            if left.kind != right.kind {
                return Ok(false);
            }
            use crate::lisp::eval::RecordKind;
            match left.kind {
                RecordKind::TreeSitterNode => {
                    let left = interp.treesit_node_state(&o1);
                    let right = interp.treesit_node_state(&o2);
                    matches!((left, right), (Some(left), Some(right))
                        if left.parser_id == right.parser_id
                            && left.generation == right.generation
                            && left.node_id == right.node_id)
                }
                // Pseudovectors below PVEC_CLOSURE: equal only when eq.
                RecordKind::Process
                | RecordKind::Obarray
                | RecordKind::Window
                | RecordKind::WindowConfiguration
                | RecordKind::Thread
                | RecordKind::Mutex
                | RecordKind::ConditionVariable
                | RecordKind::ModuleFunction
                | RecordKind::UserPointer
                | RecordKind::TreeSitterParser
                | RecordKind::TreeSitterCompiledQuery
                | RecordKind::Sqlite => false,
                _ => {
                    left.slots.len() == right.slots.len()
                        && slots(
                            &mut std::iter::once(left.type_tag).chain(left.slots.iter().copied()),
                            &mut std::iter::once(right.type_tag).chain(right.slots.iter().copied()),
                        )?
                }
            }
        }
        _ => false,
    })
}

/// The overlays' buffers and extents, which internal_equal compares
/// before their plists.
fn overlay_extents_equal(
    left: crate::overlay::OverlayRef,
    right: crate::overlay::OverlayRef,
) -> bool {
    let same_buffer = match (left.buffer(), right.buffer()) {
        (None, None) => true,
        (Some(a), Some(b)) => a.ptr_eq(&b),
        _ => false,
    };
    // Detached overlays all have GNU's public -1 endpoints, regardless of
    // the last interval coordinates retained in their unlinked node.
    same_buffer && (left.is_dead() || left.bounds() == right.bounds())
}

/// intervals.c:compare_string_intervals: the strings' properties agree
/// at every position.  Interval boundaries do not matter, and each
/// position's plists compare as sets, their values by `equal'
/// (intervals_equal_1 with USE_EQUAL).
fn string_intervals_equal(
    interp: &Interpreter,
    left: &crate::lisp::types::SharedStringState,
    right: &crate::lisp::types::SharedStringState,
) -> Result<bool, LispError> {
    let mut failure = None;
    let mut equal = |a: Value, b: Value| match values_equal(interp, &a, &b) {
        Ok(same) => same,
        Err(error) => {
            failure.get_or_insert(error);
            false
        }
    };
    let same = crate::intervals::compare_string_intervals(
        left.props.intervals(),
        right.props.intervals(),
        left.len(),
        &mut equal,
    );
    match failure {
        Some(error) => Err(error),
        None => Ok(same),
    }
}

/// fns.c:Feql with no symbol reckoned its position's: a float is eql to
/// a float of the same bits (same_float), a bignum to a bignum of the
/// same value, any other object to itself (BASE_EQ).
pub(crate) fn values_eql(left: &Value, right: &Value) -> bool {
    left.word() == right.word() || eql_numbers(left, right)
}

/// Feql's FLOATP and BIGNUMP arms for two objects of different words.
#[inline]
fn eql_numbers(left: &Value, right: &Value) -> bool {
    if left.is_float() {
        return match (left.kind(), right.kind()) {
            // same_float: the bits, so a NaN is eql to its own pattern
            // and 0.0 is not eql to -0.0.
            (Kind::Float(a), Kind::Float(b)) => a.to_bits() == b.to_bits(),
            _ => false,
        };
    }
    left.is_vectorlike()
        && right.is_vectorlike()
        && matches!((left.kind(), right.kind()), (Kind::BigInteger(a), Kind::BigInteger(b)) if a == b)
}

/// fns.c:Feql: the float and bignum arms, then EQ, which reckons a
/// symbol with position its bare symbol while `symbols-with-pos-enabled'
/// is set (a float or a bignum is never EQ to another word).
pub(crate) fn values_eql_in_env(
    interp: &Interpreter,
    left: &Value,
    right: &Value,
    env: &Env,
) -> bool {
    values_eq_in_env(interp, left, right, env) || eql_numbers(left, right)
}

#[inline]
pub(crate) fn values_eq_in_env(
    interp: &Interpreter,
    left: &Value,
    right: &Value,
    _env: &Env,
) -> bool {
    lisp_eq(interp, left, right)
}

/// lisp.h:EQ: BASE_EQ, or while `symbols-with-pos-enabled' the bare
/// symbols' BASE_EQ.
#[inline]
pub(crate) fn lisp_eq(interp: &Interpreter, left: &Value, right: &Value) -> bool {
    left.word() == right.word()
        || ((left.is_vectorlike() || right.is_vectorlike())
            && crate::lisp::primitives::symbols_with_pos_flag(interp)
            && bare_symbols_eq(left, right))
}

/// EQ with the flag set: BASE_EQ of the bare symbols of LEFT and RIGHT
/// (XSYMBOL_WITH_POS_SYM of a symbol with position, any other object
/// itself).
#[inline(never)]
fn bare_symbols_eq(left: &Value, right: &Value) -> bool {
    let bare = |value: &Value| match value.kind() {
        Kind::SymbolWithPos(symbol) => symbol.symbol(),
        _ => *value,
    };
    bare(left).word() == bare(right).word()
}

/// lisp.h:BASE_EQ: the same word.  Each object has one word: its tagged
/// address, or the immediate value of a fixnum.
#[inline(always)]
pub(crate) fn values_eq_plain(left: &Value, right: &Value) -> bool {
    left.word() == right.word()
}

pub(crate) fn plist_type_error(plist: &Value) -> LispError {
    LispError::SignalValue(Value::list([
        Value::Symbol("wrong-type-argument".into()),
        Value::Symbol("plistp".into()),
        *plist,
    ]))
}

/// fns.c:Fsafe_length: the steps FOR_EACH_TAIL_SAFE takes, which stops
/// at a non-cons or where it finds a cycle.
pub(crate) fn safe_list_length(list: &Value) -> i64 {
    let mut len = 0i64;
    let mut tail = *list;
    let mut walk = crate::lisp::types::TailWalk::new(tail);
    while let Some(cell) = tail.cons_ref() {
        len += 1;
        tail = cell.cdr.get();
        if walk.cycled_no_quit(tail) {
            break;
        }
    }
    len
}

pub(crate) fn nthcdr_value(
    interp: &mut Interpreter,
    env: &mut Env,
    count: &Value,
    list: &Value,
) -> Result<Value, LispError> {
    let mut tail = *list;
    // fns.c:Fnthcdr counts down in an EMACS_INT.  A positive bignum counts
    // down from LARGE_NUM instead: no list or cycle is that long, and the
    // cycle reduction below adds back what the substitution left out.
    const LARGE_NUM: i64 = i64::MAX;
    let mut num = match count.kind() {
        Kind::Integer(n) => {
            // A small fixnum count walks without circularity or quit
            // checks (SMALL_LIST_LEN_MAX).
            if n <= 127 {
                for _ in 0..n.max(0) {
                    match tail.kind() {
                        Kind::Cons(cell) => tail = cell.cdr.get(),
                        Kind::Nil => return Ok(Value::Nil),
                        _ => return Err(wrong_type_argument("listp", *list)),
                    }
                }
                return Ok(tail);
            }
            n
        }
        Kind::BigInteger(n) => {
            if BigInt::from(n).is_negative() {
                return Ok(tail);
            }
            LARGE_NUM
        }
        _ => {
            return Err(LispError::WrongTypeArgument("integerp".into(), *count));
        }
    };

    // FOR_EACH_TAIL_SAFE: Brent's tortoise, counted as lisp.h counts
    // `li.q' and `li.n'.
    let mut tortoise = tail;
    let mut maximum = 2_isize;
    let mut remaining = 0_isize;
    let mut quit_count = 2_u16;
    let mut tortoise_num = num;
    let mut saved_tail = tail;
    let mut cycle = false;
    while let Kind::Cons(cell) = tail.kind() {
        // If the tortoise just jumped (which is rare), update TORTOISE_NUM
        // accordingly.
        if tail.word() == tortoise.word() {
            tortoise_num = num;
        }
        saved_tail = cell.cdr.get();
        num -= 1;
        if num == 0 {
            return Ok(saved_tail);
        }
        rarely_quit(interp, env, num)?;
        tail = saved_tail;
        quit_count = quit_count.wrapping_sub(1);
        let compare = if quit_count != 0 {
            true
        } else {
            remaining = remaining.wrapping_sub(1);
            remaining > 0
        };
        if compare {
            if tail.word() == tortoise.word() {
                cycle = true;
                break;
            }
        } else {
            maximum = maximum.wrapping_shl(1);
            quit_count = maximum as u16;
            remaining = maximum >> u16::BITS;
            tortoise = tail;
        }
    }

    tail = saved_tail;
    if !cycle {
        // CHECK_LIST_END reports the original list.  A count that stops
        // exactly at an improper tail returned that tail above.
        if !tail.is_nil() {
            return Err(wrong_type_argument("listp", *list));
        }
        return Ok(Value::Nil);
    }

    // TAIL is part of a cycle.  Reduce NUM modulo the cycle length to avoid
    // going around this cycle repeatedly.
    let cycle_length = tortoise_num - num;
    if let Kind::BigInteger(n) = count.kind() {
        // Undo the error introduced when LARGE_NUM was substituted for N:
        // the steps left are N less the steps taken, modulo CYCLE_LENGTH.
        let taken = BigInt::from(LARGE_NUM - num);
        let left = (BigInt::from(n) - taken) % BigInt::from(cycle_length);
        num = left
            .to_i64()
            .expect("a remainder is less than the cycle length");
    } else {
        num %= cycle_length;
    }

    // One last time through the cycle.
    while num > 0 {
        let Kind::Cons(cell) = tail.kind() else {
            unreachable!("a cycle's tails are conses");
        };
        tail = cell.cdr.get();
        rarely_quit(interp, env, num)?;
        num -= 1;
    }
    Ok(tail)
}

/// lisp.h:rarely_quit: maybe_quit once each QUIT_COUNT_HEURISTIC (2^16)
/// counts.
#[inline]
fn rarely_quit(interp: &mut Interpreter, env: &mut Env, count: i64) -> Result<(), LispError> {
    if count & 0xffff == 0 {
        interp.maybe_quit(env)?;
    }
    Ok(())
}

pub(crate) fn sequence_length_value(value: &Value) -> Result<i64, LispError> {
    match value.kind() {
        // fns.c:Flength reads SCHARS: the character count in place, no
        // copy of the text.
        Kind::StringObject(state) => Ok(state.borrow().len() as i64),
        Kind::Nil => Ok(0),
        // fns.c:Flength reads ASIZE directly; taking the size must not
        // clone or traverse the vector's elements.
        Kind::Vector(vector) => Ok(vector.len() as i64),
        // fns.c Flength: a char-table's length is MAX_CHAR (0x3FFFFF),
        // not the number of covered codepoints.
        Kind::CharTable(_) => Ok(0x3f_ffff),
        Kind::BoolVector(vector) => Ok(vector.len() as i64),
        Kind::Closure(lambda) => Ok(lambda.public_len() as i64),
        Kind::LispRecord(record) => Ok(record.len() as i64),
        // The reader's record literal: its slots, without a copy of them.
        Kind::ReaderForm(form) => match form.as_ref() {
            crate::lisp::types::ReaderForm::Record { slots } => Ok(slots.len() as i64),
            _ => Err(LispError::WrongTypeArgument("sequencep".into(), *value)),
        },
        // fns.c:list_length counts the tails in place (FOR_EACH_TAIL and
        // CHECK_LIST_END), without a copy of the elements.
        Kind::Cons(_) => {
            let mut length = 0_i64;
            value.visit_list_elements(|_| {
                length += 1;
                Ok(())
            })?;
            Ok(length)
        }
        _ => Err(LispError::WrongTypeArgument("sequencep".into(), *value)),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ValueOrder {
    Less,
    Equal,
    Greater,
    Unordered,
}

pub(crate) fn order_from_ordering(ordering: Ordering) -> ValueOrder {
    match ordering {
        Ordering::Less => ValueOrder::Less,
        Ordering::Equal => ValueOrder::Equal,
        Ordering::Greater => ValueOrder::Greater,
    }
}

pub(crate) fn order_from_option(ordering: Option<Ordering>) -> ValueOrder {
    ordering
        .map(order_from_ordering)
        .unwrap_or(ValueOrder::Unordered)
}

pub(crate) fn type_mismatch_signal(left: &Value, right: &Value) -> LispError {
    LispError::SignalValue(Value::list([
        Value::Symbol("type-mismatch".into()),
        *left,
        *right,
    ]))
}

pub(crate) fn circular_signal(value: &Value) -> LispError {
    LispError::SignalValue(Value::list([Value::Symbol("circular".into()), *value]))
}

pub(crate) fn is_number_value(value: &Value) -> bool {
    matches!(
        value.kind(),
        Kind::Integer(_) | Kind::BigInteger(_) | Kind::Float(_)
    )
}

pub(crate) fn plain_symbol_name(value: &Value) -> Option<&str> {
    match value.kind() {
        Kind::Nil => Some("nil"),
        Kind::T => Some("t"),
        Kind::Symbol(_) => value.symbol_name_by_tag(),
        _ => None,
    }
}

pub(crate) fn compare_plain_symbol_names(left: &str, right: &str) -> ValueOrder {
    let left_visible = crate::lisp::types::visible_symbol_name(left);
    let right_visible = crate::lisp::types::visible_symbol_name(right);
    match left_visible.cmp(right_visible) {
        Ordering::Less => ValueOrder::Less,
        Ordering::Greater => ValueOrder::Greater,
        Ordering::Equal => {
            if left == right {
                ValueOrder::Equal
            } else {
                ValueOrder::Unordered
            }
        }
    }
}

pub(crate) fn compare_sequence_values(
    interp: &Interpreter,
    left: &[Value],
    right: &[Value],
    env: &Env,
    seen_lists: &mut HashSet<(usize, usize)>,
) -> Result<ValueOrder, LispError> {
    for (left_item, right_item) in left.iter().zip(right.iter()) {
        match value_ordering(interp, left_item, right_item, env, seen_lists)? {
            ValueOrder::Less => return Ok(ValueOrder::Less),
            ValueOrder::Greater => return Ok(ValueOrder::Greater),
            ValueOrder::Equal | ValueOrder::Unordered => {}
        }
    }
    Ok(order_from_ordering(left.len().cmp(&right.len())))
}

pub(crate) fn compare_symbol_values(
    interp: &Interpreter,
    left: &Value,
    right: &Value,
    _env: &Env,
) -> Result<Option<ValueOrder>, LispError> {
    let symbols_with_pos_enabled = interp.symbols_with_positions_enabled();
    let left_with_pos = symbol_with_pos_parts(interp, left);
    let right_with_pos = symbol_with_pos_parts(interp, right);

    if symbols_with_pos_enabled {
        let left_base = left_with_pos
            .as_ref()
            .map(|(symbol, _)| symbol)
            .unwrap_or(left);
        let right_base = right_with_pos
            .as_ref()
            .map(|(symbol, _)| symbol)
            .unwrap_or(right);
        if let (Some(left_name), Some(right_name)) =
            (plain_symbol_name(left_base), plain_symbol_name(right_base))
        {
            return Ok(Some(compare_plain_symbol_names(left_name, right_name)));
        }
        if left_with_pos.is_some() || right_with_pos.is_some() {
            return Err(type_mismatch_signal(left, right));
        }
    } else {
        match (left_with_pos.as_ref(), right_with_pos.as_ref()) {
            (Some((left_symbol, _left_pos)), Some((right_symbol, _right_pos))) => {
                let Some(left_name) = plain_symbol_name(left_symbol) else {
                    return Err(type_mismatch_signal(left, right));
                };
                let Some(right_name) = plain_symbol_name(right_symbol) else {
                    return Err(type_mismatch_signal(left, right));
                };
                return Ok(Some(compare_plain_symbol_names(left_name, right_name)));
            }
            (Some(_), None) | (None, Some(_)) => return Err(type_mismatch_signal(left, right)),
            (None, None) => {}
        }
    }

    match (plain_symbol_name(left), plain_symbol_name(right)) {
        (Some(left_name), Some(right_name)) => {
            Ok(Some(compare_plain_symbol_names(left_name, right_name)))
        }
        (Some(_), None) | (None, Some(_)) => Err(type_mismatch_signal(left, right)),
        (None, None) => Ok(None),
    }
}

fn compare_buffer_objects(
    left: crate::lisp::types::BufferRef,
    right: crate::lisp::types::BufferRef,
) -> ValueOrder {
    // fns.c:value_cmp(PVEC_BUFFER) compares the Fbuffer_name strings
    // (string_cmp), killed buffers (nil names) before live ones.
    // Allocation IDs are not order.
    let (left, right) = (left.borrow().name(), right.borrow().name());
    match (left.kind(), right.kind()) {
        (Kind::StringObject(left), Kind::StringObject(right)) => {
            match left.borrow().compare_contents(&right.borrow()) {
                Ordering::Less => ValueOrder::Less,
                Ordering::Equal => ValueOrder::Equal,
                Ordering::Greater => ValueOrder::Greater,
            }
        }
        (Kind::StringObject(_), _) => ValueOrder::Greater,
        (_, Kind::StringObject(_)) => ValueOrder::Less,
        _ => ValueOrder::Equal,
    }
}

pub(crate) fn compare_record_values(
    interp: &Interpreter,
    left: &Value,
    right: &Value,
    env: &Env,
    seen_lists: &mut HashSet<(usize, usize)>,
) -> Result<Option<ValueOrder>, LispError> {
    match (left.kind(), right.kind()) {
        (Kind::LispRecord(a), Kind::LispRecord(b)) => {
            if a.ptr_eq(&b) {
                return Ok(Some(ValueOrder::Equal));
            }
            if !seen_lists.insert((a.identity(), b.identity())) {
                return Err(LispError::Signal(
                    "Maximum depth exceeded in comparison".into(),
                ));
            }
            let result = (|| {
                for (x, y) in a.slots().zip(b.slots()) {
                    match value_ordering(interp, &x, &y, env, seen_lists)? {
                        ValueOrder::Less => return Ok(Some(ValueOrder::Less)),
                        ValueOrder::Greater => return Ok(Some(ValueOrder::Greater)),
                        ValueOrder::Equal | ValueOrder::Unordered => {}
                    }
                }
                Ok(Some(order_from_ordering(a.len().cmp(&b.len()))))
            })();
            seen_lists.remove(&(a.identity(), b.identity()));
            return result;
        }
        (Kind::LispRecord(_), _) | (_, Kind::LispRecord(_)) => {
            return Err(type_mismatch_signal(left, right));
        }
        (Kind::BoolVector(a), Kind::BoolVector(b)) => return Ok(Some(bool_vector_cmp(a, b))),
        (Kind::BoolVector(_), _) | (_, Kind::BoolVector(_)) => {
            return Err(type_mismatch_signal(left, right));
        }
        _ => {}
    }
    match (left.kind(), right.kind()) {
        (Kind::Record(_), _) | (_, Kind::Record(_)) => {}
        _ => return Ok(None),
    }

    let (Kind::Record(left_id), Kind::Record(right_id)) = (left.kind(), right.kind()) else {
        return Err(type_mismatch_signal(left, right));
    };
    let Some(left_record) = interp.find_record(left_id) else {
        return Ok(Some(order_from_ordering(
            left_id.identity().cmp(&right_id.identity()),
        )));
    };
    let Some(right_record) = interp.find_record(right_id) else {
        return Ok(Some(order_from_ordering(
            left_id.identity().cmp(&right_id.identity()),
        )));
    };

    if left_record.kind != right_record.kind {
        return Err(type_mismatch_signal(left, right));
    }

    match left_record.kind {
        crate::lisp::eval::RecordKind::Process => Ok(Some(
            // fns.c:value_cmp(PVEC_PROCESS): the names, as strings.
            match (
                interp.process_name(left_id.id).map(|name| name.kind()),
                interp.process_name(right_id.id).map(|name| name.kind()),
            ) {
                (Some(Kind::StringObject(left)), Some(Kind::StringObject(right))) => {
                    match left.borrow().compare_contents(&right.borrow()) {
                        Ordering::Less => ValueOrder::Less,
                        Ordering::Equal => ValueOrder::Equal,
                        Ordering::Greater => ValueOrder::Greater,
                    }
                }
                _ => ValueOrder::Unordered,
            },
        )),
        crate::lisp::eval::RecordKind::Font
        | crate::lisp::eval::RecordKind::Obarray
        | crate::lisp::eval::RecordKind::Window
        | crate::lisp::eval::RecordKind::WindowConfiguration
        | crate::lisp::eval::RecordKind::Thread
        | crate::lisp::eval::RecordKind::Mutex
        | crate::lisp::eval::RecordKind::ConditionVariable
        | crate::lisp::eval::RecordKind::ModuleFunction
        | crate::lisp::eval::RecordKind::UserPointer
        | crate::lisp::eval::RecordKind::TreeSitterParser
        | crate::lisp::eval::RecordKind::TreeSitterNode
        | crate::lisp::eval::RecordKind::TreeSitterCompiledQuery
        | crate::lisp::eval::RecordKind::Sqlite => Ok(Some(ValueOrder::Unordered)),
    }
}

pub(crate) fn value_ordering(
    interp: &Interpreter,
    left: &Value,
    right: &Value,
    env: &Env,
    seen_lists: &mut HashSet<(usize, usize)>,
) -> Result<ValueOrder, LispError> {
    if is_number_value(left) || is_number_value(right) {
        if is_number_value(left) && is_number_value(right) {
            return Ok(order_from_option(value_cmp_numeric_ordering(left, right)));
        }
        // value_cmp answers a bignum against what is not vectorlike (a
        // symbol, a string or a cons) as -value_cmp (b, a), whose mismatch
        // names the other value first.
        if matches!(left.kind(), Kind::BigInteger(_))
            && matches!(
                right.kind(),
                Kind::Nil | Kind::T | Kind::Symbol(_) | Kind::StringObject(_) | Kind::Cons(_)
            )
        {
            return Err(type_mismatch_signal(right, left));
        }
        return Err(type_mismatch_signal(left, right));
    }

    if let Some(order) = compare_record_values(interp, left, right, env, seen_lists)? {
        return Ok(order);
    }

    if is_vector_value(left) || is_vector_value(right) {
        if is_vector_value(left) && is_vector_value(right) {
            return compare_sequence_values(
                interp,
                &vector_items(left)?,
                &vector_items(right)?,
                env,
                seen_lists,
            );
        }
        return Err(type_mismatch_signal(left, right));
    }

    // fns.c:value_cmp gives all PVEC_SUBR objects the same unordered
    // relation, including a native subr compared with a static DEFUN subr.
    if matches!(left.kind(), Kind::BuiltinFunc(_) | Kind::NativeFunction(_))
        || matches!(right.kind(), Kind::BuiltinFunc(_) | Kind::NativeFunction(_))
    {
        return if matches!(left.kind(), Kind::BuiltinFunc(_) | Kind::NativeFunction(_))
            && matches!(right.kind(), Kind::BuiltinFunc(_) | Kind::NativeFunction(_))
        {
            Ok(if left.eq_value(*right) {
                ValueOrder::Equal
            } else {
                ValueOrder::Unordered
            })
        } else {
            Err(type_mismatch_signal(left, right))
        };
    }

    // fns.c:value_cmp treats distinct pseudovectors without a dedicated
    // ordering (including hash and character tables) as unordered. Their
    // contents do not order them; different pseudovector types still signal.
    if matches!(
        left.kind(),
        Kind::CharTable(_) | Kind::HashTable(_) | Kind::NativeCompUnit(_)
    ) || matches!(
        right.kind(),
        Kind::CharTable(_) | Kind::HashTable(_) | Kind::NativeCompUnit(_)
    ) {
        return if matches!(
            (left.kind(), right.kind()),
            (Kind::CharTable(_), Kind::CharTable(_))
                | (Kind::HashTable(_), Kind::HashTable(_))
                | (Kind::NativeCompUnit(_), Kind::NativeCompUnit(_))
        ) {
            Ok(ValueOrder::Unordered)
        } else {
            Err(type_mismatch_signal(left, right))
        };
    }

    if matches!(left.kind(), Kind::Buffer(_)) || matches!(right.kind(), Kind::Buffer(_)) {
        return match (left.kind(), right.kind()) {
            (Kind::Buffer(left), Kind::Buffer(right)) => Ok(compare_buffer_objects(left, right)),
            _ => Err(type_mismatch_signal(left, right)),
        };
    }

    if matches!(left.kind(), Kind::Marker(_)) || matches!(right.kind(), Kind::Marker(_)) {
        return match (left.kind(), right.kind()) {
            (Kind::Marker(left_id), Kind::Marker(right_id)) => {
                let (left_buffer, right_buffer) = match (left_id.buffer(), right_id.buffer()) {
                    (Some(left), Some(right)) => (left, right),
                    (None, Some(_)) => return Ok(ValueOrder::Less),
                    (Some(_), None) => return Ok(ValueOrder::Greater),
                    (None, None) => return Ok(ValueOrder::Equal),
                };
                let buffer_order = compare_buffer_objects(left_buffer, right_buffer);
                if buffer_order != ValueOrder::Equal {
                    return Ok(buffer_order);
                }
                Ok(order_from_ordering(
                    left_id.last_position().cmp(&right_id.last_position()),
                ))
            }
            _ => Err(type_mismatch_signal(left, right)),
        };
    }

    if matches!(left.kind(), Kind::Cons(_)) || matches!(right.kind(), Kind::Cons(_)) {
        return match (left.kind(), right.kind()) {
            (Kind::Nil, Kind::Nil) => Ok(ValueOrder::Equal),
            (Kind::Nil, Kind::Cons(_)) => Ok(ValueOrder::Less),
            (Kind::Cons(_), Kind::Nil) => Ok(ValueOrder::Greater),
            (Kind::Cons(left_cell), Kind::Cons(right_cell)) => {
                let key = (
                    crate::lisp::types::ConsCell::identity(&left_cell),
                    crate::lisp::types::ConsCell::identity(&right_cell),
                );
                if !seen_lists.insert(key) {
                    return Err(circular_signal(left));
                }
                let result = (|| {
                    let left_head = left_cell.car.get();
                    let left_tail = left_cell.cdr.get();
                    let right_head = right_cell.car.get();
                    let right_tail = right_cell.cdr.get();
                    let head_order =
                        value_ordering(interp, &left_head, &right_head, env, seen_lists)?;
                    if matches!(head_order, ValueOrder::Less | ValueOrder::Greater) {
                        return Ok(head_order);
                    }
                    value_ordering(interp, &left_tail, &right_tail, env, seen_lists)
                })();
                seen_lists.remove(&key);
                result
            }
            _ => Err(type_mismatch_signal(left, right)),
        };
    }

    if let Some(order) = compare_symbol_values(interp, left, right, env)? {
        return Ok(order);
    }

    if let (Some(left_string), Some(right_string)) = (string_like(left), string_like(right)) {
        return Ok(order_from_ordering(
            left_string
                .character_codes()
                .cmp(&right_string.character_codes()),
        ));
    }
    if string_like(left).is_some() || string_like(right).is_some() {
        return Err(type_mismatch_signal(left, right));
    }

    Err(type_mismatch_signal(left, right))
}

/// fns.c value_cmp's numeric rules, which are deliberately NOT
/// arithcompare's: a fixnum against a float compares after C promotion to
/// double (`ia < XFLOAT_DATA (b)`), so a fixnum a double cannot represent
/// exactly can compare unordered against the neighbouring float where `<'
/// still orders them; a fixnum against a bignum compares by the bignum's
/// sign alone; a float against a bignum compares exactly (mpz_cmp_d), with
/// NaN unordered.
fn value_cmp_numeric_ordering(left: &Value, right: &Value) -> Option<std::cmp::Ordering> {
    use std::cmp::Ordering;
    fn float_cmp(a: f64, b: f64) -> Option<Ordering> {
        if a < b {
            Some(Ordering::Less)
        } else if a > b {
            Some(Ordering::Greater)
        } else if a == b {
            Some(Ordering::Equal)
        } else {
            None
        }
    }
    fn bignum_sign(value: &crate::lisp::types::SharedBigInt) -> Ordering {
        use num_bigint::Sign;
        match value.sign() {
            Sign::Minus => Ordering::Less,
            Sign::NoSign => Ordering::Equal,
            Sign::Plus => Ordering::Greater,
        }
    }
    fn bignum_vs_float(bignum: &crate::lisp::types::SharedBigInt, float: f64) -> Option<Ordering> {
        if float.is_nan() {
            return None;
        }
        if float.is_infinite() {
            return Some(if float.is_sign_positive() {
                Ordering::Less
            } else {
                Ordering::Greater
            });
        }
        // mpz_cmp_d compares the integer with the double exactly: decompose
        // the double into mantissa * 2^exponent and compare as integers,
        // scaling whichever side needs it.
        let bits = float.to_bits();
        let sign = if bits >> 63 == 0 { 1i64 } else { -1i64 };
        let biased_exponent = ((bits >> 52) & 0x7ff) as i64;
        let raw_mantissa = bits & 0xf_ffff_ffff_ffff;
        let (mantissa, exponent) = if biased_exponent == 0 {
            (raw_mantissa as i64, -1074i64)
        } else {
            ((raw_mantissa | (1 << 52)) as i64, biased_exponent - 1075)
        };
        let mantissa = num_bigint::BigInt::from(sign * mantissa);
        let big = &**bignum;
        let cmp = if exponent >= 0 {
            big.cmp(&(mantissa << exponent as usize))
        } else {
            (big.clone() << (-exponent) as usize).cmp(&mantissa)
        };
        Some(cmp)
    }
    match (left.kind(), right.kind()) {
        (Kind::Integer(a), Kind::Integer(b)) => Some(a.cmp(&b)),
        (Kind::Integer(a), Kind::Float(b)) => float_cmp(a as f64, b.get()),
        (Kind::Float(a), Kind::Integer(b)) => float_cmp(a.get(), b as f64),
        (Kind::Float(a), Kind::Float(b)) => float_cmp(a.get(), b.get()),
        (Kind::Integer(_), Kind::BigInteger(b)) => Some(bignum_sign(&b).reverse()),
        (Kind::BigInteger(a), Kind::Integer(_)) => Some(bignum_sign(&a)),
        (Kind::BigInteger(a), Kind::BigInteger(b)) => Some((*a).cmp(&*b)),
        (Kind::BigInteger(a), Kind::Float(b)) => bignum_vs_float(&a, b.get()),
        (Kind::Float(a), Kind::BigInteger(b)) => {
            bignum_vs_float(&b, a.get()).map(Ordering::reverse)
        }
        _ => None,
    }
}

pub(crate) fn value_less(
    interp: &Interpreter,
    left: &Value,
    right: &Value,
    env: &Env,
) -> Result<bool, LispError> {
    Ok(matches!(
        value_ordering(interp, left, right, env, &mut HashSet::new())?,
        ValueOrder::Less
    ))
}

/// fns.c:Fproper_list_p's count: the length of a proper list, None for a
/// dotted or circular one.  FOR_EACH_TAIL_SAFE counts the tails in place;
/// copying the elements made the predicate allocate a list's length.
pub(crate) fn proper_list_length(value: &Value) -> Option<usize> {
    let mut len = 0;
    let mut tail = *value;
    let mut seen = crate::lisp::types::CycleGuard::new();
    while let Some(cell) = tail.cons_ref() {
        if seen.step(crate::lisp::types::ConsCell::identity(&cell)) {
            return None;
        }
        len += 1;
        tail = cell.cdr.get();
    }
    tail.is_nil().then_some(len)
}

/// lisp.h:check_integer_range: an integer from LO to HI, `integerp' for
/// anything else, `args-out-of-range' VALUE LO HI for an integer (a
/// bignum included) outside the range.
pub(crate) fn check_integer_range(value: Value, lo: i64, hi: i64) -> Result<i64, LispError> {
    match value.kind() {
        Kind::Integer(n) if (lo..=hi).contains(&n) => Ok(n),
        Kind::Integer(_) | Kind::BigInteger(_) => Err(LispError::SignalValue(Value::list([
            Value::symbol("args-out-of-range"),
            value,
            Value::Integer(lo),
            Value::Integer(hi),
        ]))),
        _ => Err(LispError::WrongTypeArgument("integerp".into(), value)),
    }
}

/// eval.c:signal_error: `(error S . ARG)', ARG made a one-element list
/// unless it already is a proper list.
pub(crate) fn signal_error(message: &str, arg: Value) -> LispError {
    let arg = if proper_list_length(&arg).is_some() {
        arg
    } else {
        Value::list([arg])
    };
    LispError::SignalValue(Value::cons(
        Value::symbol("error"),
        Value::cons(Value::string(message), arg),
    ))
}

pub(crate) fn remove_equal(
    interp: &Interpreter,
    elt: &Value,
    sequence: &Value,
) -> Result<Value, LispError> {
    // fns.c:Fdelete: a string loses the characters ELT names (none when
    // ELT is not a character), a vector the elements `equal' to ELT; a
    // sequence that loses nothing comes back itself.
    if let Kind::StringObject(state) = sequence.kind() {
        let Kind::Integer(target) = elt.kind() else {
            return Ok(*sequence);
        };
        if !(0..=0x3F_FFFF).contains(&target) {
            return Ok(*sequence);
        }
        let (codes, multibyte) = {
            let state = state.borrow();
            (state.codes(), state.is_multibyte())
        };
        let kept = codes
            .iter()
            .copied()
            .filter(|&code| i64::from(code) != target)
            .collect::<Vec<_>>();
        if kept.len() == codes.len() {
            return Ok(*sequence);
        }
        let mut bytes = Vec::with_capacity(kept.len());
        for code in kept {
            if multibyte {
                let (encoded, width) = crate::lisp::types::string_data::encode_character(code)?;
                bytes.extend_from_slice(&encoded[..width]);
            } else {
                bytes.push(code as u8);
            }
        }
        return make_specified_string(bytes, multibyte)
            .map_err(|error| LispError::Signal(error.to_string()));
    }

    if is_vector_value(sequence) {
        let items = vector_items(sequence)?;
        let mut filtered = Vec::with_capacity(items.len());
        for item in &items {
            if !values_equal(interp, item, elt)? {
                filtered.push(*item);
            }
        }
        if filtered.len() == items.len() {
            return Ok(*sequence);
        }
        return Ok(Value::vector(filtered));
    }

    match sequence.kind() {
        Kind::Nil | Kind::Cons(_) => {
            let mut kept = Vec::new();
            for item in sequence.to_vec()? {
                if !values_equal(interp, &item, elt)? {
                    kept.push(item);
                }
            }
            Ok(Value::list(kept))
        }
        _ => Err(LispError::WrongTypeArgument("sequencep".into(), *sequence)),
    }
}

/// fns.c:Fnconc: each argument but the last, unless nil, has its last cons
/// (FOR_EACH_TAIL's, QUIT where it calls maybe_quit) set to the next
/// argument, and a nil next argument stands for that cons in its turn.
pub(crate) fn nconc_values(
    args: &[Value],
    mut quit: impl FnMut() -> Result<(), LispError>,
) -> Result<Value, LispError> {
    let mut val = Value::Nil;
    // `if (NILP (tem)) args[argnum + 1] = tail;'
    let mut carried = None;
    for (argnum, arg) in args.iter().enumerate() {
        let tem = carried.take().unwrap_or(*arg);
        if tem.is_nil() {
            continue;
        }
        if val.is_nil() {
            val = tem;
        }
        let Some(next) = args.get(argnum + 1) else {
            break;
        };
        if !tem.is_cons() {
            return Err(LispError::WrongTypeArgument("consp".into(), tem));
        }
        let mut tail = tem;
        let mut cursor = tem;
        let mut walk = crate::lisp::types::TailWalk::new(tem);
        while let Some(cell) = cursor.cons_ref() {
            tail = cursor;
            cursor = cell.cdr.get();
            if walk.cycled(cursor, &mut quit)? {
                return Err(crate::lisp::types::circular_list(cursor));
            }
        }
        tail.set_cdr(*next)?;
        if next.is_nil() {
            carried = Some(tail);
        }
    }
    Ok(val)
}

pub(crate) fn copy_alist_value(value: &Value) -> Result<Value, LispError> {
    if string_like(value).is_some() || is_vector_value(value) {
        return Err(LispError::WrongTypeArgument("listp".into(), *value));
    }
    let items = value.to_vec()?;
    let mut copied = Vec::with_capacity(items.len());
    for item in items {
        if let Some((car, cdr)) = item.cons_values() {
            copied.push(Value::cons(car, cdr));
        } else {
            copied.push(item);
        }
    }
    Ok(Value::list(copied))
}

/// fns.c:Fnconc's walk of one argument: CHECK_CONS, then the last cons
/// FOR_EACH_TAIL reaches.
/// fns.c:bool_vector_cmp: the lexicographic order of the bits, the equal
/// words skipped, a proper prefix first.
fn bool_vector_cmp(
    a: crate::lisp::types::BoolVectorRef,
    b: crate::lisp::types::BoolVectorRef,
) -> ValueOrder {
    use crate::lisp::alloc::vectors::bool_vectors::BITS_PER_BITS_WORD;
    let (mut na, mut nb) = (a.len(), b.len());
    let words_min = na.min(nb) / BITS_PER_BITS_WORD;
    let (ad, bd) = (a.words(), b.words());
    let mut i = 0;
    while i < words_min && ad[i].get() == bd[i].get() {
        i += 1;
    }
    na -= i * BITS_PER_BITS_WORD;
    nb -= i * BITS_PER_BITS_WORD;
    if nb == 0 {
        return if na != 0 {
            ValueOrder::Greater
        } else {
            ValueOrder::Equal
        };
    }
    if na == 0 {
        return ValueOrder::Less;
    }
    let (aw, bw) = (ad[i].get(), bd[i].get());
    let xw = aw ^ bw;
    if xw == 0 {
        return order_from_ordering(na.cmp(&nb));
    }
    // The first difference.
    if xw.isolate_lowest_one() & aw != 0 {
        ValueOrder::Greater
    } else {
        ValueOrder::Less
    }
}

pub(crate) fn markers_equal(
    left: crate::lisp::types::MarkerRef,
    right: crate::lisp::types::MarkerRef,
) -> bool {
    match (left.buffer(), right.buffer()) {
        (None, None) => true,
        (Some(a), Some(b)) => a.ptr_eq(&b) && left.byte_position() == right.byte_position(),
        _ => false,
    }
}

pub(crate) fn resolve_callable(
    interp: &Interpreter,
    value: &Value,
    env: &Env,
) -> Result<Value, LispError> {
    let callable = if symbols_with_pos_enabled(interp, env) {
        symbol_with_pos_parts(interp, value)
            .map(|(symbol, _)| symbol)
            .unwrap_or_else(|| *value)
    } else {
        *value
    };
    match callable.kind() {
        Kind::Symbol(name) => interp.lookup_function_symbol(&name, env),
        // `t' is a symbol whose function cell `fset' may fill.
        Kind::T => {
            interp.lookup_function_symbol(&crate::lisp::types::symbols::builtin_boolean(true), env)
        }
        _ => Ok(callable),
    }
}

pub(crate) fn is_lambda_expression(interp: &Interpreter, value: &Value, env: &Env) -> bool {
    value.to_vec().ok().is_some_and(|items| {
        items.first().is_some_and(|head| {
            matches!(head.kind(), Kind::Symbol(name) if name.is_named("lambda"))
                || (symbols_with_pos_enabled(interp, env)
                    && matches!(
                        symbol_with_pos_parts(interp, head).map(|(a0, a1)| (a0.kind(), a1)),
                        Some((Kind::Symbol(name), _)) if name.is_named("lambda")
                    ))
        }) && items.get(1).is_some()
    })
}

pub(crate) fn callable_value_p(interp: &Interpreter, value: &Value, env: &Env) -> bool {
    matches!(
        value.kind(),
        Kind::BuiltinFunc(_) | Kind::NativeFunction(_) | Kind::Closure(_)
    ) || is_lambda_expression(interp, value, env)
        || matches!(
            value.kind(),
            Kind::Record(id)
                if interp.find_record(id).is_some_and(|record| matches!(
                    record.kind,
                    crate::lisp::eval::RecordKind::ModuleFunction
                ))
        )
}

/// eval.c:FUNCTIONP.  Native callers use this helper directly so
/// redefining the Lisp symbol `functionp' cannot change C-level argument
/// validation.
pub(crate) fn function_value_p(interp: &Interpreter, value: &Value, env: &Env) -> bool {
    let mut object = *value;
    // SYMBOLP (object) && !NILP (Ffboundp (object)): CHECK_SYMBOL and
    // XSYMBOL take a symbol with position, while `symbols-with-pos-enabled'
    // makes it a symbol, as its bare symbol.
    let symbol = if object.is_symbol() {
        Some(object)
    } else if symbols_with_pos_enabled(interp, env) {
        symbol_with_pos_parts(interp, &object).map(|(symbol, _)| symbol)
    } else {
        None
    };
    if let Some(symbol) = symbol {
        // Findirect_function: nil for a void cell, so for an unbound
        // symbol, and for a chain that ends in one.
        object = match symbol.kind() {
            Kind::Symbol(name) => interp.lookup_function_symbol(&name, env),
            Kind::T => interp
                .lookup_function_symbol(&crate::lisp::types::symbols::builtin_boolean(true), env),
            _ => Ok(Value::Nil),
        }
        .unwrap_or(Value::Nil);
        let autoload = cached_symbol!("autoload").with(|symbol| Value::Symbol(*symbol));
        if let Some((head, _)) = object.cons_values()
            && values_eq_in_env(interp, &head, &autoload, env)
        {
            // Autoloaded symbols are functions, except if they load
            // macros or keymaps.
            for _ in 0..4 {
                let Some((_, tail)) = object.cons_values() else {
                    break;
                };
                object = tail;
            }
            return !object.cons_values().is_some_and(|(kind, _)| !kind.is_nil());
        }
    }
    match object.kind() {
        Kind::BuiltinFunc(subr) => !matches!(
            subr.descriptor().max_args(),
            crate::lisp::native_comp::abi::NativeMaxArgs::Unevalled
        ),
        Kind::NativeFunction(_) | Kind::Closure(_) => true,
        Kind::Record(id) => interp.find_record(id).is_some_and(|record| {
            matches!(record.kind, crate::lisp::eval::RecordKind::ModuleFunction)
        }),
        Kind::Cons(cell) => {
            let lambda = cached_symbol!("lambda").with(|symbol| Value::Symbol(*symbol));
            values_eq_in_env(interp, &cell.car.get(), &lambda, env)
        }
        _ => false,
    }
}

pub(crate) fn invoke_function_value(
    interp: &mut Interpreter,
    func: &Value,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    interp.call_function_value(*func, None, args, env)
}

// keymap.c:Fmake_sparse_keymap/Fmake_keymap: the Lisp list owns every
// binding. There is no private record, reverse index or public projection.
pub(crate) fn make_runtime_keymap(_interp: &mut Interpreter, prompt: Value) -> Value {
    let tail = if prompt.is_nil() {
        Value::Nil
    } else {
        Value::list([prompt])
    };
    Value::cons(keymap_symbol(), tail)
}

pub(crate) fn make_runtime_full_keymap(interp: &mut Interpreter, prompt: Value) -> Value {
    let table = interp.make_char_table(Some("keymap".into()), Value::Nil);
    let tail = if prompt.is_nil() {
        Value::Nil
    } else {
        Value::list([prompt])
    };
    Value::cons(keymap_symbol(), Value::cons(table, tail))
}

/// Qkeymap: the cached symbol, not an intern of its name.
#[inline]
fn keymap_symbol() -> Value {
    cached_symbol!("keymap").with(|symbol| Value::Symbol(*symbol))
}

pub(crate) fn is_keymap_placeholder(value: &Value) -> bool {
    value
        .cons_values()
        .is_some_and(|(head, _)| head.eq_value(keymap_symbol()))
}

pub(crate) fn is_keymap_value(_interp: &Interpreter, value: &Value) -> bool {
    is_keymap_placeholder(value)
}

/// The first character table in this map's own list, before its parent.
#[cfg(test)]
pub(crate) fn keymap_char_table_value(_interp: &Interpreter, keymap: &Value) -> Option<Value> {
    let view = *keymap;
    if !matches!(view.car_if_list()?.kind(), Kind::Symbol(name) if name.is_named("keymap")) {
        return None;
    }
    let mut tail = view.cdr_if_list()?;
    let mut cycle = crate::lisp::types::CycleGuard::new();
    while let Kind::Cons(cell) = tail.kind() {
        if cycle.step(crate::lisp::types::ConsCell::identity(&cell)) {
            break;
        }
        let item = cell.car.get();
        if matches!(item.kind(), Kind::Symbol(name) if name.is_named("keymap")) {
            break; // The inherited parent's cells are not this map's table.
        }
        if matches!(item.kind(), Kind::CharTable(_)) {
            return Some(item);
        }
        tail = cell.cdr.get();
    }
    None
}

/// This map's own sparse bindings as (EVENT . DEFINITION) pairs.
#[cfg(test)]
pub(crate) fn keymap_direct_bindings(
    interp: &Interpreter,
    keymap: &Value,
) -> Result<Vec<(Value, Value)>, LispError> {
    if !is_keymap_value(interp, keymap) {
        return Ok(Vec::new());
    }
    let (items, _) = keymap_public_view_own_items(keymap)?;
    Ok(items
        .into_iter()
        .filter(|entry| !is_keymap_value(interp, entry))
        .filter_map(|entry| entry.cons_values())
        .collect())
}

/// The child's own cells of a public view (after the `keymap' head) and the
/// parent spliced in as its tail (a cell whose car is the symbol `keymap',
/// as Fset_keymap_parent leaves it), or nil.
#[cfg(test)]
fn keymap_public_view_own_items(view: &Value) -> Result<(Vec<Value>, Value), LispError> {
    let mut items = Vec::new();
    let mut seen = HashSet::new();
    let mut tail = view.cdr()?;
    loop {
        let Kind::Cons(cell) = tail.kind() else {
            if !tail.is_nil() {
                return Err(wrong_type_argument("listp", tail));
            }
            return Ok((items, Value::Nil));
        };
        if !seen.insert(crate::lisp::types::ConsCell::identity(&cell)) {
            return Ok((items, Value::Nil));
        }
        let item = cell.car.get();
        if matches!(item.kind(), Kind::Symbol(name) if name.is_named("keymap")) {
            return Ok((items, tail));
        }
        items.push(item);
        let next = cell.cdr.get();
        tail = next;
    }
}

#[cfg(test)]
pub(crate) fn keymap_parent_values(interp: &Interpreter, keymap: &Value) -> Vec<Value> {
    let view = *keymap;
    let Ok((items, parent)) = keymap_public_view_own_items(&view) else {
        return Vec::new();
    };
    items
        .into_iter()
        .filter(|item| is_keymap_value(interp, item))
        .chain(parent.is_truthy().then_some(parent))
        .collect()
}

pub(crate) type KeymapEntryCallback<'a> =
    dyn FnMut(&mut Interpreter, Value, Value, &mut Env) -> Result<(), LispError> + 'a;

/// keymap.c:map_keymap_internal reads the actual list cells and returns the
/// parent tail. Callbacks may change a later entry or the current cell's cdr.
/// Root the cursor and its current vector/table across those callbacks.
pub(crate) fn map_keymap_own_entries(
    interp: &mut Interpreter,
    keymap: Value,
    function: &mut KeymapEntryCallback<'_>,
    env: &mut Env,
) -> Result<Value, LispError> {
    let map = keymap;
    let mut tail = if map.car().is_ok_and(|head| head.eq_value(keymap_symbol())) {
        map.cdr()?
    } else {
        map
    };
    let mut roots = crate::lisp::alloc::RootedVec::from_vec(vec![map, tail, Value::Nil]);
    // GNU walks the cells as they are, with no test for a circular list.
    while let Some((entry, _)) = tail.cons_values() {
        if entry.eq_value(keymap_symbol()) || is_keymap_value(interp, &entry) {
            break;
        }
        roots[2] = entry;
        match entry.kind() {
            Kind::Cons(_) => {
                let value = entry.cdr()?;
                function(
                    interp,
                    entry.car()?,
                    if value == Value::T { Value::Nil } else { value },
                    env,
                )?;
            }
            Kind::Vector(vector) => {
                for index in 0..vector.len() {
                    let value = vector.get(index).expect("keymap vector slot");
                    function(
                        interp,
                        Value::Integer(index as i64),
                        if value == Value::T { Value::Nil } else { value },
                        env,
                    )?;
                }
            }
            Kind::CharTable(table) => {
                super::dispatch::map_char_table_with(
                    interp,
                    &mut |interp, key, value, _, _, env| {
                        // map_keymap_char_table_item copies the walker's
                        // reused range and map_keymap_item unwraps t.
                        let key = match key.cons_values() {
                            Some((start, end)) => Value::cons(start, end),
                            None => key,
                        };
                        function(
                            interp,
                            key,
                            if value == Value::T { Value::Nil } else { value },
                            env,
                        )
                    },
                    table,
                    env,
                )?;
            }
            _ => {}
        }
        tail = tail.cdr()?;
        roots[1] = tail;
    }
    Ok(tail)
}

/// keymap.c:access_keymap (MAP, EVENT, T_OK, false, true): None for no
/// binding at all, as access_keymap_1's Qunbound.
pub(crate) fn keymap_access_event(
    interp: &mut Interpreter,
    map: Value,
    event: Value,
    accept_default: bool,
    env: &mut Env,
) -> Result<Option<Value>, LispError> {
    let value = access_keymap_1(interp, map, event, accept_default, false, true, env)?;
    Ok((!value.eq_value(Value::Unbound)).then_some(value))
}

pub(crate) fn keymap_lookup_sequence_value_with_default(
    interp: &mut Interpreter,
    keymap_or_maps: &Value,
    key_parts: &[Value],
    accept_default: bool,
    env: &mut Env,
) -> Result<Value, LispError> {
    interp.with_lisp_stack_roots(&(keymap_or_maps, key_parts), |interp| {
        keymap_lookup_events(
            interp,
            *keymap_or_maps,
            key_parts.len(),
            accept_default,
            |_, index, _| Ok(key_parts[index]),
            env,
        )
    })
}

/// keymap.c:lookup_key_1 calls Faref immediately before each access. A menu
/// filter can mutate later events, so copying or normalizing the entire
/// sequence before traversing it changes the meaning of ordinary lookup.
pub(crate) fn keymap_lookup_live_sequence_value(
    interp: &mut Interpreter,
    keymap: &Value,
    sequence: Value,
    accept_default: bool,
    env: &mut Env,
) -> Result<Value, LispError> {
    let length = sequence_length_value(&sequence)? as usize;
    interp.with_lisp_stack_roots(&(keymap, sequence), |interp| {
        keymap_lookup_events(
            interp,
            *keymap,
            length,
            accept_default,
            |interp, index, env| {
                let mut event = match sequence.kind() {
                    Kind::Vector(vector) => vector.get(index),
                    _ => string_char_code_at_in_place(&sequence, index).map(Value::Integer),
                }
                .ok_or_else(|| {
                    LispError::SignalValue(Value::list([
                        Value::symbol("args-out-of-range"),
                        sequence,
                        Value::Integer(index as i64),
                    ]))
                })?;
                if lucid_event_type_list_p(&event) {
                    event = event_convert_list_value(interp, &event)?;
                }
                if symbols_with_pos_enabled(interp, env)
                    && let Some((symbol, _)) = symbol_with_pos_parts(interp, &event)
                {
                    event = symbol;
                }
                if sequence.is_string()
                    && !string_argument_multibyte(&sequence)
                    && let Kind::Integer(code) = event.kind()
                    && code & 0x80 != 0
                {
                    event = Value::Integer((code | KEY_DESCRIPTION_META_BIT) & !0x80);
                }
                Ok(event)
            },
            env,
        )
    })
}

fn keymap_lookup_events(
    interp: &mut Interpreter,
    keymap: Value,
    length: usize,
    accept_default: bool,
    read_event: impl FnMut(&mut Interpreter, usize, &mut Env) -> Result<Value, LispError>,
    env: &mut Env,
) -> Result<Value, LispError> {
    lookup_key_1(interp, keymap, length, accept_default, read_event, env)
}

pub(crate) fn keymap_get_keyelt(
    interp: &mut Interpreter,
    object: &Value,
    autoload: bool,
    env: &mut Env,
) -> Result<Value, LispError> {
    get_keyelt(interp, *object, autoload, env)
}

// keyboard.c follow_key resolves autoloaded keymap symbols before deciding
// whether another event is required. Loading may collect or signal.
pub(crate) fn load_autoloaded_prefix_map(
    interp: &mut Interpreter,
    binding: &Value,
    env: &Env,
) -> Result<(), LispError> {
    let Kind::Symbol(name) = binding.kind() else {
        return Ok(());
    };
    let Ok(function) = interp.lookup_function_symbol(&name, env) else {
        return Ok(());
    };
    if let Some((_, _, Kind::Symbol(kind))) =
        autoload_parts(&function).map(|(a0, a1, a2)| (a0, a1, a2.kind()))
        && kind.is_named("keymap")
    {
        interp.autoload_do_load(function, *binding, Value::Nil, &mut env.clone())?;
    }
    Ok(())
}

/// CONSP (get_keymap (VALUE, false, false)): VALUE's keymap list when it is
/// one or a symbol whose function indirection is one.  An autoloaded
/// keymap is not loaded, and is not a list.
pub(crate) fn keymap_reference_map(
    interp: &Interpreter,
    value: &Value,
    env: &Env,
) -> Option<Value> {
    let map = get_keymap_noload(interp, *value, env);
    map.is_cons().then_some(map)
}

/// keymap.c:Fdescribe_buffer_bindings selects the maps and their shadow order;
/// unchanged help.el owns traversal, filtering, ranges and presentation.
pub(crate) fn describe_buffer_bindings(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    if args.is_empty() || args.len() > 3 {
        return Err(LispError::WrongNumberOfArgs(
            "describe-buffer-bindings".into(),
            args.len(),
        ));
    }
    let buffer = args[0];
    if !matches!(buffer.kind(), Kind::Buffer(_)) {
        return Err(LispError::WrongTypeArgument("bufferp".into(), buffer));
    }
    let prefix = args.get(1).copied().unwrap_or(Value::Nil);
    let nomenu = if args.get(2).is_some_and(Value::is_truthy) {
        Value::Nil
    } else {
        Value::T
    };
    // This is the actual argument vector for help--describe-map-tree. Keep
    // its maps, accumulated shadow list, prefix and buffer rooted across
    // arbitrary Lisp filters and insertion hooks.
    let mut description = crate::lisp::alloc::RootedVec::from_vec(vec![
        Value::Nil,
        Value::Nil,
        Value::Nil,
        prefix,
        Value::Nil,
        nomenu,
        Value::T,
        Value::Nil,
        Value::Nil,
        buffer,
    ]);
    let translation = interp
        .lookup_var_key(cached_symbol!("keyboard-translate-table"), env)
        .unwrap_or(Value::Nil);
    if !prefix.is_nil()
        && let Some(table) = string_like(&translation)
    {
        let size = table.text.chars().count();
        let mut heading = true;
        for index in 0..size {
            // GNU reacquires the table after insertions, which can run Lisp.
            let table = interp
                .lookup_var_key(cached_symbol!("keyboard-translate-table"), env)
                .unwrap_or(Value::Nil);
            let Some(table) = string_like(&table) else {
                break;
            };
            let bytes = internal_string_bytes(&table)?;
            let Some(&translated) = bytes.get(index) else {
                break;
            };
            if usize::from(translated) == index {
                continue;
            }
            if heading {
                call(interp, "insert", &[Value::String("Keyboard translations:\n\nYou type        Translation\n--------        -----------\n".into())], env)?;
                heading = false;
            }
            let key = single_key_description_text(&Value::Integer(i64::from(translated)), false)?;
            call(interp, "insert", &[Value::String(key.into())], env)?;
            call(
                interp,
                "indent-to",
                &[Value::Integer(16), Value::Integer(1)],
                env,
            )?;
            let key = single_key_description_text(&Value::Integer(index as i64), false)?;
            call(
                interp,
                "insert",
                &[Value::String(key.into()), Value::String("\n".into())],
                env,
            )?;
        }
        call(interp, "insert", &[Value::String("\n".into())], env)?;
    }
    let translations = interp
        .lookup_var_key(cached_symbol!("key-translation-map"), env)
        .unwrap_or(Value::Nil);
    if !translations.is_nil() {
        insert_keymap_description(
            interp,
            &mut description,
            translations,
            "Key translations".into(),
            env,
        )?;
    }
    description[1] = Value::T;
    description[6] = Value::Nil;
    let terminal = interp
        .lookup_var_key(cached_symbol!("overriding-terminal-local-map"), env)
        .unwrap_or(Value::Nil);
    let overriding = if !terminal.is_nil() {
        insert_keymap_description(
            interp,
            &mut description,
            terminal,
            "\x0c\nOverriding Bindings".into(),
            env,
        )?;
        description[2] = Value::cons(description[0], description[2]);
        Value::Nil
    } else {
        interp
            .lookup_var_key(cached_symbol!("overriding-local-map"), env)
            .unwrap_or(Value::Nil)
    };
    if !overriding.is_nil() {
        insert_keymap_description(
            interp,
            &mut description,
            overriding,
            "\x0c\nOverriding Bindings".into(),
            env,
        )?;
        description[2] = Value::cons(description[0], description[2]);
    } else {
        let minor = with_keymap_description_buffer(interp, buffer, |interp| {
            current_minor_maps(interp, env).map(crate::lisp::alloc::RootedVec::from_vec)
        })?;
        // get_local_map (BUF_PT, buffer, Qkeymap).
        let property = with_keymap_description_buffer(interp, buffer, |interp| {
            let point = interp.buffer.borrow().point() as i64;
            get_local_map(interp, point, "keymap")
        })?;
        if !property.is_nil() {
            insert_keymap_description(
                interp,
                &mut description,
                property,
                "\x0c\n`keymap' Property Bindings".into(),
                env,
            )?;
            description[2] = Value::cons(description[0], description[2]);
        }
        for (mode, map) in minor.iter() {
            let mode = match mode.kind() {
                Kind::Symbol(name) => name.to_string(),
                _ => "t".to_string(),
            };
            insert_keymap_description(
                interp,
                &mut description,
                *map,
                format!("\x0c\n`{mode}' Minor Mode Bindings"),
                env,
            )?;
            description[2] = Value::cons(description[0], description[2]);
        }
        let (local, is_buffer_map, major) =
            with_keymap_description_buffer(interp, buffer, |interp| {
                // BVAR (current_buffer, keymap).
                let normal = interp.buffer.borrow().keymap();
                let point = interp.buffer.borrow().point() as i64;
                let local = get_local_map(interp, point, "local-map")?;
                let major = interp
                    .lookup_var_key(cached_symbol!("major-mode"), env)
                    .unwrap_or(Value::Nil);
                Ok((local, values_eq_in_env(interp, &local, &normal, env), major))
            })?;
        if !local.is_nil() {
            description[0] = local;
            let title = if is_buffer_map {
                let title = call(
                    interp,
                    "format",
                    &[
                        Value::String("\x0c\n`%s' Major Mode Bindings".into()),
                        major,
                    ],
                    env,
                )?;
                string_like(&title).expect("format returns a string").text
            } else {
                "\x0c\n`local-map' Property Bindings".into()
            };
            insert_keymap_description(interp, &mut description, local, title, env)?;
            description[2] = Value::cons(description[0], description[2]);
        }
    }
    description[7] = Value::T;
    let global = interp.current_global_map_value();
    insert_keymap_description(
        interp,
        &mut description,
        global,
        "\x0c\nGlobal Bindings".into(),
        env,
    )?;
    description[1] = Value::Nil;
    description[2] = Value::Nil;
    description[6] = Value::T;
    description[7] = Value::Nil;
    for (variable, title) in [
        (
            cached_symbol!("local-function-key-map"),
            "\x0c\nFunction key map translations",
        ),
        (
            cached_symbol!("input-decode-map"),
            "\x0c\nInput decoding map translations",
        ),
    ] {
        let map = interp.lookup_var_key(variable, env).unwrap_or(Value::Nil);
        if !map.is_nil() {
            insert_keymap_description(interp, &mut description, map, title.into(), env)?;
        }
    }
    Ok(Value::Nil)
}

fn insert_keymap_description(
    interp: &mut Interpreter,
    args: &mut crate::lisp::alloc::RootedVec<Value>,
    map: Value,
    title: String,
    env: &mut Env,
) -> Result<(), LispError> {
    args[0] = map;
    args[4] = Value::String(title.into());
    call_function_value(interp, &Value::symbol("help--describe-map-tree"), args, env)?;
    Ok(())
}

fn with_keymap_description_buffer<T>(
    interp: &mut Interpreter,
    buffer: Value,
    body: impl FnOnce(&Interpreter) -> Result<T, LispError>,
) -> Result<T, LispError> {
    let saved = interp.current_buffer_id();
    let target = interp.resolve_buffer_id(&buffer)?;
    interp.set_current_buffer_id(target)?;
    let result = body(interp);
    let restored = interp.set_current_buffer_id(saved);
    result.and_then(|value| restored.map(|()| value))
}

/// keymap.c:Fhelp__describe_vector and describe_vector with KEYMAP_P:
/// each run of a char-table's characters bound to one object
/// (char_table_ref_and_range) is looked up and described once, a vector's
/// consecutive `equal' elements are merged, and the char-table's default
/// is described last.
pub(crate) fn help_describe_vector(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    need_args("help--describe-vector", args, 7)?;
    let vector = args[0];
    let table = match vector.kind() {
        Kind::CharTable(table) => Some(table),
        Kind::Vector(_) => None,
        _ => {
            return Err(LispError::WrongTypeArgument(
                "vector-or-char-table-p".into(),
                vector,
            ));
        }
    };
    let output_buffer = Value::Buffer(interp.buffer);
    let restore = interp.bind_special_key(cached_symbol!("standard-output"), output_buffer, env)?;
    let mut result = describe_vector(interp, vector, table, args, env);
    if let Err(error) = interp.restore_special_binding(restore, env)
        && result.is_ok()
    {
        result = Err(error);
    }
    result.map(|()| Value::Nil)
}

/// shadow_lookup without remapping: KEY's binding in the keymaps SHADOW,
/// defaults accepted, nil for a key too long.
fn shadow_lookup(
    interp: &mut Interpreter,
    shadow: &Value,
    key: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    let value = keymap_lookup_sequence_value_with_default(interp, shadow, key, true, env)?;
    Ok(match value.kind() {
        Kind::Integer(n) if n >= 0 => Value::Nil,
        _ => value,
    })
}

fn describe_vector(
    interp: &mut Interpreter,
    vector: Value,
    table: Option<crate::lisp::types::CharTableRef>,
    args: &[Value],
    env: &mut Env,
) -> Result<(), LispError> {
    // MAX_5_BYTE_CHAR + 1 and MAX_CHAR + 1.
    const BYTE8_START: u32 = 0x3F_FF80;
    const END: u32 = 0x40_0000;
    let (prefix, describer) = (args[1], args[2]);
    let partial = args[3].is_truthy();
    let (shadow, entire_map) = (args[4], args[5]);
    let mention_shadow = args[6].is_truthy();
    let element = |index: u32| match vector.kind() {
        Kind::Vector(items) => items.get(index as usize).unwrap_or(Value::Nil),
        _ => Value::Nil,
    };
    let (mut stop, to) = match (table, vector.kind()) {
        (Some(_), _) => (BYTE8_START, END),
        (None, Kind::Vector(items)) => (items.len() as u32, items.len() as u32),
        (None, _) => (0, 0),
    };
    let mut first = true;
    let mut i = 0u32;
    loop {
        interp.maybe_quit(env)?;
        if i == stop {
            if i == to {
                break;
            }
            stop = to;
        }
        let starting_i = i;
        let mut range_beg = i;
        let val = match table {
            Some(table) => {
                let (val, from, end) = table.ref_and_range(i, i, stop - 1);
                range_beg = from;
                i = end;
                val
            }
            None => element(i),
        };
        let definition = keymap_get_keyelt(interp, &val, false, env)?;
        let suppressed = || {
            partial
                && matches!(definition.kind(), Kind::Symbol(symbol)
                    if interp.get_symbol_property_of(&symbol, &cached_symbol!("suppress-keymap").with(Clone::clone))
                        .is_some_and(|value| value.is_truthy()))
        };
        if definition.is_nil() || suppressed() {
            i += 1;
            continue;
        }
        let key = [Value::Integer(i64::from(starting_i))];
        let mut this_shadowed = false;
        let mut shadowed_by = Value::Nil;
        if !shadow.is_nil() {
            shadowed_by = shadow_lookup(interp, &shadow, &key, env)?;
            if !shadowed_by.is_nil() && !shadowed_by.eq_value(definition) {
                if mention_shadow {
                    this_shadowed = true;
                } else {
                    i += 1;
                    continue;
                }
            }
        }
        // Ignore a definition shadowed by an earlier one in the same map.
        if !entire_map.is_nil() {
            let tem =
                keymap_lookup_sequence_value_with_default(interp, &entire_map, &key, true, env)?;
            if !tem.eq_value(definition) {
                i += 1;
                continue;
            }
        }
        if first {
            super::dispatch::insert_string(interp, "\n", env)?;
            first = false;
        }
        insert_described_key_of(interp, starting_i, &prefix, env)?;
        if table.is_none() {
            while i + 1 < stop {
                let next = keymap_get_keyelt(interp, &element(i + 1), false, env)?;
                if next.is_nil() || !values_equal(interp, &next, &definition)? {
                    break;
                }
                i += 1;
            }
        }
        // Consecutive keys either not shadowed, or shadowed by the same
        // command.
        let check = interp
            .symbol_value_key(cached_symbol!(
                "describe-bindings-check-shadowing-in-ranges"
            ))
            .unwrap_or(Value::Nil);
        if !check.is_nil()
            && table.is_some()
            && i != starting_i
            && (!check.eq_value(Value::symbol("ignore-self-insert"))
                || !definition.eq_value(Value::symbol("self-insert-command")))
        {
            // C's `j <= i' reads the I the body may have lowered.
            let mut j = range_beg + 1;
            while j <= i {
                let tem = shadow_lookup(interp, &shadow, &[Value::Integer(i64::from(j))], env)?;
                if !values_equal(interp, &tem, &shadowed_by)? {
                    i = j - 1;
                }
                j += 1;
            }
        }
        if i != starting_i {
            super::dispatch::insert_string(interp, " .. ", env)?;
            insert_described_key_of(interp, i, &prefix, env)?;
        }
        call_function_value(interp, &describer, &[definition], env)?;
        if this_shadowed {
            let point = interp.buffer.borrow().point();
            interp.buffer.borrow_mut().goto_char(point - 1);
            if let Kind::Symbol(command) = shadowed_by.kind() {
                super::dispatch::insert_string(
                    interp,
                    &format!("  (currently shadowed by `{command}')"),
                    env,
                )?;
            } else {
                super::dispatch::insert_string(interp, "  (currently shadowed)", env)?;
            }
            let point = interp.buffer.borrow().point();
            interp.buffer.borrow_mut().goto_char(point + 1);
        }
        i += 1;
    }
    if let Some(table) = table
        && !table.default().is_nil()
    {
        super::dispatch::insert_string(interp, "default", env)?;
        call_function_value(interp, &describer, &[table.default()], env)?;
    }
    Ok(())
}

/// describe_key_maybe_fontify of the one-event key CODE after PREFIX.
fn insert_described_key_of(
    interp: &mut Interpreter,
    code: u32,
    prefix: &Value,
    env: &mut Env,
) -> Result<(), LispError> {
    let key = Value::vector([Value::Integer(i64::from(code))]);
    let description = call(interp, "key-description", &[key, *prefix], env)?;
    let length = call(interp, "length", &[description], env)?;
    call(
        interp,
        "add-text-properties",
        &[
            Value::Integer(0),
            length,
            Value::list([
                Value::symbol("font-lock-face"),
                Value::symbol("help-key-binding"),
            ]),
            description,
        ],
        env,
    )?;
    call(interp, "insert", &[description], env)?;
    Ok(())
}

/// keymap.c:current_minor_maps: the variables and maps of the active minor
/// modes, `emulation-mode-map-alists' first (a symbol in it read for its
/// alist), then `minor-mode-overriding-map-alist', then
/// `minor-mode-map-alist', whose entry for a variable the overriding alist
/// also names is skipped.  An entry counts when its car is a symbol whose
/// value (find_symbol_value) is bound and non-nil; its map is the
/// Findirect_function of the cdr, whatever that is, and a void one drops
/// the entry.
pub(crate) fn current_minor_maps(
    interp: &Interpreter,
    env: &Env,
) -> Result<Vec<(Value, Value)>, LispError> {
    let symbol_value = |symbol: &Value| -> Option<Value> {
        match symbol.kind() {
            Kind::Nil => Some(Value::Nil),
            Kind::T => Some(Value::T),
            _ => symbol
                .symbol_cell()
                .and_then(|symbol| interp.symbol_value_symbol(&symbol)),
        }
    };
    let lists = [
        interp
            .symbol_value_key(cached_symbol!("minor-mode-overriding-map-alist"))
            .unwrap_or(Value::Nil),
        interp
            .symbol_value_key(cached_symbol!("minor-mode-map-alist"))
            .unwrap_or(Value::Nil),
    ];
    let mut emulation_alists = interp
        .symbol_value_key(cached_symbol!("emulation-mode-map-alists"))
        .unwrap_or(Value::Nil);
    let mut maps = Vec::new();
    let mut list_number = 0i32;
    while list_number < 2 {
        let mut alist = if let Some((head, rest)) = emulation_alists.cons_values() {
            emulation_alists = rest;
            list_number = -1;
            if head.is_symbol() {
                symbol_value(&head).unwrap_or(Value::Nil)
            } else {
                head
            }
        } else {
            lists[list_number as usize]
        };
        while let Some((assoc, rest)) = alist.cons_values() {
            alist = rest;
            let Some((var, map)) = assoc.cons_values() else {
                continue;
            };
            if !var.is_symbol() || !symbol_value(&var).is_some_and(|value| value.is_truthy()) {
                continue;
            }
            // assq_no_quit (var, lists[0]).
            if list_number == 1 {
                let mut tail = lists[0];
                let mut overridden = false;
                while let Some((entry, rest)) = tail.cons_values() {
                    if entry
                        .cons_values()
                        .is_some_and(|(car, _)| car.eq_value(var))
                    {
                        overridden = true;
                        break;
                    }
                    tail = rest;
                }
                if overridden {
                    continue;
                }
            }
            let map = super::keymap::indirect_function(interp, map, env);
            if !map.is_nil() {
                maps.push((var, map));
            }
        }
        list_number += 1;
    }
    Ok(maps)
}

/// The maps of `current_minor_maps' alone.
pub(crate) fn active_minor_mode_maps(
    interp: &Interpreter,
    env: &Env,
) -> Result<Vec<Value>, LispError> {
    Ok(current_minor_maps(interp, env)?
        .into_iter()
        .map(|(_, map)| map)
        .collect())
}

/// intervals.c:get_local_map: the keymap that the TYPE property (`keymap'
/// or `local-map') of the current buffer gives at POSITION, clipped to
/// the accessible text and read with the narrowing lifted -- the
/// character after it by Fget_char_property, else by get_pos_property
/// past the first position -- when get_keymap (prop, 0, 0) makes a list
/// of it; else nil for `keymap' and the buffer's own map for `local-map'.
pub(crate) fn get_local_map(
    interp: &Interpreter,
    position: i64,
    type_: &str,
) -> Result<Value, LispError> {
    let (begv, zv) = interp.buffer.borrow().restriction();
    let position = position.clamp(begv as i64, zv as i64);
    let prop = crate::lisp::types::PropertyName::symbol(type_);
    let buffer = Value::Buffer(interp.buffer);
    interp.buffer.borrow_mut().widen();
    let found = super::textprop::get_char_property(interp, &Value::Integer(position), prop, buffer)
        .and_then(|found| {
            if found.is_nil() && position > 1 {
                super::textprop::get_pos_property(interp, &Value::Integer(position), prop, buffer)
            } else {
                Ok(found)
            }
        });
    interp.buffer.borrow_mut().restore_restriction(begv, zv);
    let map = super::keymap::get_keymap_noload(interp, found?, &Env::new());
    if map.is_cons() {
        Ok(map)
    } else if type_ == "keymap" {
        Ok(Value::Nil)
    } else {
        Ok(interp.buffer.borrow().keymap())
    }
}

/// keymap.c:click_position: POSITION's fixnum or marker position, else
/// point, which must lie in the accessible text.
fn click_position(interp: &Interpreter, position: &Value) -> Result<i64, LispError> {
    let pos = match position.kind() {
        Kind::Integer(_) | Kind::Marker(_) => {
            super::textprop::fixnum_coerce_marker(interp, position)?
        }
        _ => interp.buffer.borrow().point() as i64,
    };
    let (begv, zv) = interp.buffer.borrow().restriction();
    if !(begv as i64 <= pos && pos <= zv as i64) {
        return Err(LispError::SignalValue(Value::list([
            Value::symbol("args-out-of-range"),
            Value::Buffer(interp.buffer),
            *position,
        ])));
    }
    Ok(pos)
}

/// Fnth (N, POSN) as keyboard.h's POSN_STRING and POSN_BUFFER_POSN read
/// it: Fnthcdr's walk, which refuses a dotted tail, then Fcar.
fn posn_nth(posn: &Value, n: usize) -> Result<Value, LispError> {
    let mut tail = *posn;
    for _ in 0..n {
        match tail.kind() {
            Kind::Cons(cell) => tail = cell.cdr.get(),
            Kind::Nil => return Ok(Value::Nil),
            _ => return Err(wrong_type_argument("listp", *posn)),
        }
    }
    match tail.kind() {
        Kind::Cons(cell) => Ok(cell.car.get()),
        Kind::Nil => Ok(Value::Nil),
        _ => Err(wrong_type_argument("listp", tail)),
    }
}

/// keymap.c:1657 Fcurrent_active_maps: the active keymaps in precedence
/// order, the global map last.  A click POSITION's window's buffer is
/// made current while they are found.  With OLP, `overriding-local-map'
/// -- consulted only while `overriding-terminal-local-map' is nil -- stands
/// alone before the global map; otherwise the maps are, in order,
/// `overriding-terminal-local-map' (with OLP), the `keymap' property map,
/// the minor mode maps and the `local-map' property or buffer map, all at
/// click_position's position, or for a click the ones its buffer position
/// or string gives.  Neither overriding map is checked for being one.
pub(crate) fn current_active_maps(
    interp: &mut Interpreter,
    env: &Env,
    olp: bool,
    position: Option<&Value>,
) -> Result<Vec<Value>, LispError> {
    let position = position.copied().unwrap_or(Value::Nil);
    let saved = interp.current_buffer_id();
    let mut switched = false;
    if let Some((window, _)) = position.cons_values()
        && let Some(buffer) = super::window::window_buffer_id(interp, &window)
        && buffer != saved
    {
        // record_unwind_current_buffer; set_buffer_internal.
        interp.set_current_buffer_id(buffer)?;
        switched = true;
    }
    let maps = current_active_maps_1(interp, env, olp, position);
    if switched && interp.has_buffer_id(saved) {
        interp.set_current_buffer_id(saved)?;
    }
    maps
}

fn current_active_maps_1(
    interp: &Interpreter,
    env: &Env,
    olp: bool,
    position: Value,
) -> Result<Vec<Value>, LispError> {
    let global_map = interp.current_global_map_value();
    let otlp = interp
        .symbol_value_key(cached_symbol!("overriding-terminal-local-map"))
        .unwrap_or(Value::Nil);
    if olp
        && otlp.is_nil()
        && let Some(map) = interp
            .symbol_value_key(cached_symbol!("overriding-local-map"))
            .filter(Value::is_truthy)
    {
        return Ok(vec![map, global_map]);
    }
    let pt = click_position(interp, &position)?;
    let mut local_map = get_local_map(interp, pt, "local-map")?;
    let mut keymap = get_local_map(interp, pt, "keymap")?;
    if position.is_cons() {
        let string = posn_nth(&position, 4)?;
        if string.is_nil() {
            // POSN_INBUFFER_P: the clicked buffer position's maps.
            if let Kind::Integer(pos) = posn_nth(&position, 5)?.kind() {
                let z = interp.buffer.borrow().size_total() as i64 + 1;
                if (1..=z).contains(&pos) {
                    local_map = get_local_map(interp, pos, "local-map")?;
                    keymap = get_local_map(interp, pos, "keymap")?;
                }
            }
        }
        // A click on a string: its properties, which a mode or header
        // line click takes even when they are nil.
        if let Some((text, pos)) = string.cons_values()
            && let Kind::StringObject(state) = text.kind()
            && let chars = state.borrow().len()
            && let Kind::Integer(pos) = pos.kind()
            && pos >= 0
            && (pos as usize) < chars
        {
            let area = position
                .cdr_if_list()
                .and_then(|rest| rest.car_if_list())
                .unwrap_or(Value::Nil);
            let line_area = area.eq_value(Value::symbol("mode-line"))
                || area.eq_value(Value::symbol("header-line"));
            let map = super::textprop::get_text_property(
                interp,
                &Value::Integer(pos),
                crate::lisp::types::PropertyName::symbol("local-map"),
                text,
            )?;
            if map.is_truthy() || line_area {
                local_map = map;
            }
            let map = super::textprop::get_text_property(
                interp,
                &Value::Integer(pos),
                crate::lisp::types::PropertyName::symbol("keymap"),
                text,
            )?;
            if map.is_truthy() || line_area {
                keymap = map;
            }
        }
    }
    let mut maps = Vec::new();
    if olp && otlp.is_truthy() {
        maps.push(otlp);
    }
    if keymap.is_truthy() {
        maps.push(keymap);
    }
    maps.extend(active_minor_mode_maps(interp, env)?);
    if local_map.is_truthy() {
        maps.push(local_map);
    }
    maps.push(global_map);
    Ok(maps)
}

pub(crate) fn command_remapping(
    interp: &mut Interpreter,
    command: &Value,
    keymaps: Option<&Value>,
    env: &mut Env,
) -> Result<Value, LispError> {
    super::call(
        interp,
        "command-remapping",
        &[*command, Value::Nil, keymaps.copied().unwrap_or(Value::Nil)],
        env,
    )
}

pub(crate) fn locale_uses_utf8() -> bool {
    // GNU calls setlocale(LC_ALL, "") in main before testing (emacs.c:1657-
    // 1663) -- though it SKIPS that call when LC_ALL is exactly "C", leaving
    // the process in the startup "C" locale, which fails the test the same
    // way.  Both branches therefore agree, and POSIX gives LC_ALL precedence
    // inside setlocale(LC_CTYPE, "") anyway.  Without some such call the
    // process stays in "C" and the test is meaningless.
    //
    // Note this mutates process-global locale state, lazily, on whichever
    // thread first needs a quoting style.  Nothing else in Emaxx calls
    // setlocale, so there is no one to race with, but the timing differs
    // from GNU's once-in-main.
    // `mbstate_t' is opaque and not exposed by the libc crate on every
    // target; it is at most this large on the platforms Emaxx builds for,
    // and only ever needs to be zeroed.
    #[repr(C, align(8))]
    struct MbState([u8; 128]);

    unsafe extern "C" {
        fn mbrtowc(
            pwc: *mut libc::wchar_t,
            s: *const libc::c_char,
            n: libc::size_t,
            ps: *mut MbState,
        ) -> libc::size_t;
    }

    static UTF8: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *UTF8.get_or_init(|| {
        // SAFETY: `setlocale' with an empty name selects the environment's
        // locale, exactly as GNU's startup does, and `mbrtowc' is handed a
        // two-byte buffer with a matching length and a zeroed state.
        unsafe {
            libc::setlocale(libc::LC_CTYPE, c"".as_ptr());
            let mut wc: libc::wchar_t = 0;
            let mut state = MbState([0; 128]);
            let bytes: [libc::c_char; 2] = [0xc4u8 as libc::c_char, 0x80u8 as libc::c_char];
            mbrtowc(&mut wc, bytes.as_ptr(), 2, &mut state) == 2 && wc == 0x100
        }
    })
}

/// The characters doprnt.c:490 puts for a format's grave accent and
/// apostrophe under the effective `text-quoting-style'.
pub(crate) fn doprnt_quote_pair(interp: &Interpreter) -> (char, char) {
    match effective_text_quoting_style(interp) {
        "curve" => ('\u{2018}', '\u{2019}'),
        "straight" => ('\'', '\''),
        _ => ('`', '\''),
    }
}

/// eval.c:error, through verror and vformat_string to doprnt: FORMAT's
/// own grave accents and apostrophes requoted as `text-quoting-style'
/// has them (curve: U+2018 and U+2019, straight: apostrophes, grave: as
/// written), each `{}' replaced by the next of ARGS as it is, unrequoted,
/// as doprnt copies a `%s' argument.
pub(crate) fn doprnt_error(
    interp: &Interpreter,
    format: &str,
    args: &[&dyn std::fmt::Display],
) -> LispError {
    use std::fmt::Write;
    let (open, close) = doprnt_quote_pair(interp);
    let mut message = String::with_capacity(format.len());
    let mut args = args.iter();
    let mut chars = format.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '{' if chars.peek() == Some(&'}') => {
                chars.next();
                if let Some(arg) = args.next() {
                    let _ = write!(message, "{arg}");
                }
            }
            '`' => message.push(open),
            '\'' => message.push(close),
            c => message.push(c),
        }
    }
    LispError::Signal(message)
}

/// `doprnt_error' whose `{}' each copy a string's SDATA, as a `%s' given
/// SDATA (STRING) copies it: its bytes up to the first NUL, the message
/// made from all the bytes by alloc.c:make_string (multibyte only when
/// every byte is part of a valid multibyte form).
pub(crate) fn doprnt_error_sdata(
    quotes: Option<&Interpreter>,
    format: &str,
    args: &[Value],
) -> LispError {
    let (open, close) = quotes.map_or(('`', '\''), doprnt_quote_pair);
    let mut bytes = Vec::with_capacity(format.len());
    let mut args = args.iter();
    let mut chars = format.chars().peekable();
    let mut buffer = [0u8; 4];
    while let Some(c) = chars.next() {
        let c = match c {
            '{' if chars.peek() == Some(&'}') => {
                chars.next();
                if let Some(Kind::StringObject(state)) = args.next().map(|value| value.kind()) {
                    let state = state.borrow();
                    let data = state.bytes();
                    let end = data
                        .iter()
                        .position(|&byte| byte == 0)
                        .unwrap_or(data.len());
                    bytes.extend_from_slice(&data[..end]);
                }
                continue;
            }
            '`' => open,
            '\'' => close,
            c => c,
        };
        bytes.extend_from_slice(c.encode_utf8(&mut buffer).as_bytes());
    }
    match crate::lisp::primitives::strings::make_string(bytes) {
        Ok(message) => LispError::SignalValue(Value::list([Value::symbol("error"), message])),
        Err(_) => LispError::Signal(format.into()),
    }
}

/// GNU `doc.c:Ftext_quoting_style' collapses the Lisp variable to one of
/// three effective styles.  A nil variable consults
/// `default_to_grave_quoting_style' (doc.c:653), whose first test is the
/// locale flag above.
///
/// If the flag is set, the second test reads `standard-display-table' and
/// answers grave when U+2018 displays as a one-element vector holding ?`.
/// This plain Lisp-variable read is observable in batch as well as in the
/// interactive startup path that forces the locale flag to t.
pub(crate) fn effective_text_quoting_style(interp: &Interpreter) -> &'static str {
    // doc.c reads the C variable Vtext_quoting_style itself, which a
    // `makunbound' of the symbol leaves untouched.
    match interp
        .forwarded_c_value_key(cached_symbol!("text-quoting-style"))
        .map(|v| v.kind())
    {
        Some(Kind::Symbol(style)) if style.is_named("grave") => "grave",
        Some(Kind::Symbol(style)) if style.is_named("straight") => "straight",
        Some(Kind::Symbol(style)) if style.is_named("curve") => "curve",
        // doc.c:679 consults the locale flag for NIL alone; every other
        // non-nil value falls through to `curve' ("Any other value is
        // treated as `curve'").  Routing unmatched values through the flag
        // made a bogus style answer grave in a non-UTF-8 locale.
        Some(Kind::Nil) | None => {
            // The two C variables, by the symbols interned once.
            let utf8 = interp
                .symbol_value_key(cached_symbol!("internal--text-quoting-flag"))
                .map_or_else(locale_uses_utf8, |flag| flag.is_truthy());
            if !utf8 {
                return "grave";
            }
            // doc.c:658 default_to_grave_quoting_style's second test: a
            // `standard-display-table' that displays U+2018 as the
            // one-element vector [?`] means grave even in a UTF-8 locale.
            // It is a plain variable read, observable in batch.
            if let Some(Kind::CharTable(id)) = interp
                .symbol_value_key(cached_symbol!("standard-display-table"))
                .map(|v| v.kind())
                && id.has_purpose("display-table")
                && let Some(entry) = interp.char_table_get(id, 0x2018)
                && crate::lisp::primitives::interactive::is_vector_value(&entry)
                && crate::lisp::primitives::buffers::vector_items(&entry)
                    .is_ok_and(|items| items.len() == 1 && items[0] == Value::Integer(96))
            {
                return "grave";
            }
            "curve"
        }
        _ => "curve",
    }
}

/// alloc.c:allocate_record counts the type field in PSEUDOVECTOR_SIZE_MASK.
/// Check before creating either the Lisp allocation or a temporary slot vector.
pub(crate) fn check_record_data_slots(data_slots: usize) -> Result<(), LispError> {
    let max = crate::lisp::alloc::vectors::generic_records::MAX_RECORD_SLOTS;
    if data_slots >= max {
        return Err(LispError::Signal(format!(
            "Attempt to allocate a record of {} slots; max is {max}",
            data_slots.saturating_add(1)
        )));
    }
    Ok(())
}
