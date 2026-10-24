use super::*;
use crate::lisp::types::Kind;
use crate::lisp::types::PropertyName;

pub fn buffer_undo_list_value(buffer: &crate::buffer::Buffer) -> Value {
    buffer.undo_list_value()
}

pub(crate) fn values_equal(interp: &Interpreter, left: &Value, right: &Value) -> bool {
    // Scalar fast paths: identical outcome to the recursive walk below, but
    // without paying for a fresh seen-set and the aggregate-type probes.
    match (left.kind(), right.kind()) {
        (Kind::Integer(a), Kind::Integer(b)) => return a == b,
        (Kind::Symbol(a), Kind::Symbol(b)) => return a == b,
        (Kind::Nil, Kind::Nil) | (Kind::T, Kind::T) => return true,
        (Kind::Nil | Kind::T, Kind::Integer(_)) | (Kind::Integer(_), Kind::Nil | Kind::T) => {
            return false;
        }
        _ => {}
    }
    values_equal_recursive_with_env(interp, left, right, &mut HashSet::new(), None)
}

pub(crate) fn values_equal_in_env(
    interp: &Interpreter,
    left: &Value,
    right: &Value,
    env: &Env,
) -> bool {
    values_equal_recursive_with_env(interp, left, right, &mut HashSet::new(), Some(env))
}

/// fns.c's Fequal cons walk: FOR_EACH_TAIL iterates the left spine with
/// cycle detection and signals `(circular-list LIST)' on a loop, after
/// taking the shared-tail EQ escape.  The internal non-signaling
/// `values_equal' instead answers `t' for isomorphic cycles, which
/// testcover relies on GNU NOT doing (its circular-list marks come from
/// the ignored signal).
pub(crate) fn values_equal_signaling(
    interp: &Interpreter,
    left: &Value,
    right: &Value,
    env: &Env,
) -> Result<bool, LispError> {
    values_equal_signaling_depth(interp, left, right, Some(env), 0, &mut HashMap::new())
}

fn values_equal_signaling_depth(
    interp: &Interpreter,
    left: &Value,
    right: &Value,
    env: Option<&Env>,
    depth: usize,
    memo: &mut HashMap<usize, Vec<usize>>,
) -> Result<bool, LispError> {
    fn plain_cons(value: &Value) -> bool {
        matches!(value.kind(), Kind::Cons(cell)
            if !matches!(cell.car.get().kind(), Kind::Symbol(tag) if tag == "vector-literal"))
    }
    // internal_equal: BASE_EQ objects are equal, before any walk.
    if left.eq_value(*right) {
        return Ok(true);
    }
    if !plain_cons(left) || !plain_cons(right) {
        // Leaves take the same env-aware walk `equal' uses everywhere
        // else: internal_equal's EQ sees through `symbol-with-pos'
        // wrappers while `symbols-with-pos-enabled' is set, at any depth.
        return Ok(values_equal_recursive_with_env(
            interp,
            left,
            right,
            &mut HashSet::new(),
            env,
        ));
    }
    // fns.c internal_equal: past depth 10 a seen-pair table answers true
    // for a revisited (o1, o2) pair -- how car-circular graphs compare
    // equal -- and depth 200 is a hard error, not a crash.
    if depth > 10 {
        if depth > 200 {
            return Err(LispError::Signal("Stack overflow in equal".into()));
        }
        if let (Kind::Cons(lc), Kind::Cons(rc)) = (left.kind(), right.kind()) {
            let left_key = lc.as_ptr() as usize;
            let right_key = rc.as_ptr() as usize;
            let entry = memo.entry(left_key).or_default();
            if entry.contains(&right_key) {
                return Ok(true);
            }
            entry.push(right_key);
        }
    }
    let mut l = *left;
    let mut r = *right;
    let mut tortoise = *left;
    let mut steps = 0usize;
    let mut limit = 2usize;
    while let Kind::Cons(lc) = l.kind() {
        let Kind::Cons(rc) = r.kind() else {
            return Ok(false);
        };
        let left_car = lc.car.get();
        let right_car = rc.car.get();
        if !values_equal_signaling_depth(interp, &left_car, &right_car, env, depth + 1, memo)? {
            return Ok(false);
        }
        let left_cdr = lc.cdr.get();
        let right_cdr = rc.cdr.get();
        if let (Kind::Cons(a), Kind::Cons(b)) = (left_cdr.kind(), right_cdr.kind())
            && crate::lisp::types::SharedCons::ptr_eq(&a, &b)
        {
            return Ok(true);
        }
        l = left_cdr;
        r = right_cdr;
        if let (Kind::Cons(current), Kind::Cons(lagging)) = (l.kind(), tortoise.kind())
            && crate::lisp::types::SharedCons::ptr_eq(&current, &lagging)
        {
            return Err(LispError::SignalValue(Value::list([
                Value::Symbol("circular-list".into()),
                *left,
            ])));
        }
        steps += 1;
        if steps == limit {
            tortoise = l;
            steps = 0;
            limit *= 2;
        }
    }
    Ok(values_equal_recursive_with_env(
        interp,
        &l,
        &r,
        &mut HashSet::new(),
        env,
    ))
}

fn char_tables_equal(
    interp: &Interpreter,
    left: crate::lisp::types::CharTableRef,
    right: crate::lisp::types::CharTableRef,
    seen: &mut HashSet<(usize, usize)>,
    env: Option<&Env>,
) -> bool {
    if left == right || !seen.insert((left.identity(), right.identity())) {
        return true;
    }
    left.slot_count() == right.slot_count()
        && left
            .slots()
            .zip(right.slots())
            .all(|(a, b)| values_equal_recursive_with_env(interp, &a, &b, seen, env))
}

pub(crate) fn record_equals_record_literal_form(
    interp: &Interpreter,
    record: crate::lisp::types::LispRecordRef,
    form: &Value,
    seen: &mut HashSet<(usize, usize)>,
    env: Option<&Env>,
) -> bool {
    let Some(items) = record_literal_items(form) else {
        return false;
    };
    if let Some((car, _)) = form.cons_cells() {
        let pair = (record.identity(), car.cell_id());
        if !seen.insert(pair) {
            return true;
        }
    }

    let actual_fields = &items[1..];

    record.len() == actual_fields.len()
        && record
            .slots()
            .zip(actual_fields.iter())
            .all(|(left, right)| {
                let right_value = record_literal_slot_data(right);
                values_equal_recursive_with_env(interp, &left, &right_value, seen, env)
            })
}

fn values_equal_recursive_with_env(
    interp: &Interpreter,
    left: &Value,
    right: &Value,
    seen: &mut HashSet<(usize, usize)>,
    env: Option<&Env>,
) -> bool {
    if let Some(env) = env
        && (matches!(left.kind(), Kind::SymbolWithPos(_))
            || matches!(right.kind(), Kind::SymbolWithPos(_)))
        && let Some(equal) = symbol_with_pos_equal_in_env(interp, left, right, env)
    {
        return equal;
    }
    // fns.c internal_equal: BASE_EQ objects are equal, before any walk.
    if left.eq_value(*right) {
        return true;
    }
    if let Some(equal) = crate::lisp::primitives::strings::string_texts_equal_in_place(left, right)
    {
        return equal;
    }
    if is_bool_vector_value(interp, left) && is_bool_vector_value(interp, right) {
        return bool_vector_values(interp, left).ok() == bool_vector_values(interp, right).ok();
    }
    let left_is_vector = is_vector_value(left);
    let right_is_vector = is_vector_value(right);
    if left_is_vector || right_is_vector {
        if !left_is_vector || !right_is_vector {
            return false;
        }
        let (Some(left_id), Some(right_id)) = (vector_identity(left), vector_identity(right))
        else {
            return false;
        };
        if !seen.insert((left_id, right_id)) {
            return true;
        }
        let (Ok(left_items), Ok(right_items)) = (vector_items(left), vector_items(right)) else {
            return false;
        };
        return left_items.len() == right_items.len()
            && left_items
                .iter()
                .zip(right_items.iter())
                .all(|(left, right)| {
                    values_equal_recursive_with_env(interp, left, right, seen, env)
                });
    }
    match (left.kind(), right.kind()) {
        (Kind::Nil, Kind::Nil) | (Kind::T, Kind::T) => true,
        (Kind::Integer(a), Kind::Integer(b)) => a == b,
        (Kind::BigInteger(a), Kind::BigInteger(b)) => a == b,
        (Kind::Integer(a), Kind::BigInteger(b)) | (Kind::BigInteger(b), Kind::Integer(a)) => {
            BigInt::from(a) == *b
        }
        // fns.c internal_equal: floats compare by representation
        // (same_float), like eql: NaN equals NaN, 0.0 differs from -0.0.
        (Kind::Float(a), Kind::Float(b)) => a.to_bits() == b.to_bits(),
        (Kind::StringObject(a), Kind::StringObject(b)) => a == b,
        (Kind::Symbol(a), Kind::Symbol(b)) => a == b,
        (Kind::BuiltinFunc(a), Kind::BuiltinFunc(b)) => a == b,
        (Kind::NativeFunction(a), Kind::NativeFunction(b)) => a.ptr_eq(&b),
        (Kind::NativeCompUnit(a), Kind::NativeCompUnit(b)) => a.ptr_eq(&b),
        (Kind::Buffer(a), Kind::Buffer(b)) => a.ptr_eq(&b),
        (Kind::Marker(a), Kind::Marker(b)) => markers_equal(a, b),
        (Kind::HashTable(a), Kind::HashTable(b)) => a == b,
        (Kind::Overlay(a), Kind::Overlay(b)) => overlays_equal(interp, a, b, seen, env),
        (Kind::CharTable(left_id), Kind::CharTable(right_id)) => {
            char_tables_equal(interp, left_id, right_id, seen, env)
        }
        (Kind::SubCharTable(left), Kind::SubCharTable(right)) => {
            left == right
                || (!seen.insert((left.identity(), right.identity()))
                    || (left.depth() == right.depth()
                        && left.min_char() == right.min_char()
                        && left.slots().zip(right.slots()).all(|(a, b)| {
                            values_equal_recursive_with_env(interp, &a, &b, seen, env)
                        })))
        }

        (Kind::Frame(left_id), Kind::Frame(right_id)) => left_id == right_id,
        (Kind::Terminal(left_id), Kind::Terminal(right_id)) => left_id.ptr_eq(&right_id),
        (Kind::SymbolWithPos(left), Kind::SymbolWithPos(right)) => {
            left.symbol().eq_value(right.symbol()) && left.position() == right.position()
        }
        (Kind::Record(left_id), Kind::Record(right_id)) => {
            if left_id.ptr_eq(&right_id) {
                return true;
            }
            let (Some(left_record), Some(right_record)) =
                (interp.find_record(left_id), interp.find_record(right_id))
            else {
                return false;
            };
            if left_record.kind != right_record.kind {
                return false;
            }
            // GNU only walks real records and the pseudovectors at or above
            // PVEC_CLOSURE.  Native handles below that boundary (threads,
            // synchronization objects, parsers, SQLite handles, and the
            // like) are opaque and compare by identity.
            if matches!(
                left_record.kind,
                crate::lisp::eval::RecordKind::Process
                    | crate::lisp::eval::RecordKind::Obarray
                    | crate::lisp::eval::RecordKind::Window
                    | crate::lisp::eval::RecordKind::WindowConfiguration
                    | crate::lisp::eval::RecordKind::Thread
                    | crate::lisp::eval::RecordKind::Mutex
                    | crate::lisp::eval::RecordKind::ConditionVariable
                    | crate::lisp::eval::RecordKind::ModuleFunction
                    | crate::lisp::eval::RecordKind::UserPointer
                    | crate::lisp::eval::RecordKind::TreeSitterParser
                    | crate::lisp::eval::RecordKind::TreeSitterCompiledQuery
                    | crate::lisp::eval::RecordKind::Sqlite
            ) {
                return false;
            }
            if left_record.kind == crate::lisp::eval::RecordKind::TreeSitterNode {
                let left = interp.treesit_node_state(left);
                let right = interp.treesit_node_state(right);
                return matches!((left, right), (Some(left), Some(right))
                    if left.parser_id == right.parser_id
                        && left.generation == right.generation
                        && left.node_id == right.node_id);
            }
            // GNU `equal' compares real records element-wise like vectors.
            let pair = (left_id.identity(), right_id.identity());
            if !seen.insert(pair) {
                return true;
            }
            values_equal_recursive_with_env(
                interp,
                &left_record.type_tag,
                &right_record.type_tag,
                seen,
                env,
            ) && left_record.slots.len() == right_record.slots.len()
                && left_record
                    .slots
                    .iter()
                    .zip(right_record.slots.iter())
                    .all(|(left, right)| {
                        values_equal_recursive_with_env(interp, left, right, seen, env)
                    })
        }
        (Kind::LispRecord(left), Kind::LispRecord(right)) => {
            left.ptr_eq(&right)
                || (left.len() == right.len()
                    && (!seen.insert((left.identity(), right.identity()))
                        || left.slots().zip(right.slots()).all(|(a, b)| {
                            values_equal_recursive_with_env(interp, &a, &b, seen, env)
                        })))
        }
        (Kind::LispRecord(record), _) if record_literal_items(right).is_some() => {
            record_equals_record_literal_form(interp, record, right, seen, env)
        }
        (_, Kind::LispRecord(record)) if record_literal_items(left).is_some() => {
            record_equals_record_literal_form(interp, record, left, seen, env)
        }
        (Kind::Cons(_), Kind::Cons(_)) => {
            let Some((left_car, _)) = left.cons_cells() else {
                return false;
            };
            let Some((right_car, _)) = right.cons_cells() else {
                return false;
            };
            let pair = (left_car.cell_id(), right_car.cell_id());
            if !seen.insert(pair) {
                return true;
            }
            let Some((a_car, a_cdr)) = left.cons_values() else {
                return false;
            };
            let Some((b_car, b_cdr)) = right.cons_values() else {
                return false;
            };
            values_equal_recursive_with_env(interp, &a_car, &b_car, seen, env)
                && values_equal_recursive_with_env(interp, &a_cdr, &b_cdr, seen, env)
        }
        (Kind::Closure(left), Kind::Closure(right)) => {
            let left_ptr = left.identity();
            let right_ptr = right.identity();
            if left_ptr == right_ptr || !seen.insert((left_ptr, right_ptr)) {
                return true;
            }
            left.public_len() == right.public_len()
                && left.slots().zip(right.slots()).all(|(left, right)| {
                    values_equal_recursive_with_env(interp, &left, &right, seen, env)
                })
        }
        _ => left == right,
    }
}

pub(crate) fn values_eql(left: &Value, right: &Value) -> bool {
    match (left.kind(), right.kind()) {
        (Kind::Nil, Kind::Nil) | (Kind::T, Kind::T) => true,
        (Kind::Integer(a), Kind::Integer(b)) => a == b,
        (Kind::BigInteger(a), Kind::BigInteger(b)) => a == b,
        // fns.c Feql via same_float: representation equality.  IEEE ==
        // said NaN != NaN, and every fixpoint loop keyed on eql/memql
        // diverged on forms containing a NaN literal -- eager
        // macroexpansion of cl-lib/data/fns/floatfns-tests never
        // terminated.  Bit equality also gives GNU's (eql 0.0 -0.0) nil.
        (Kind::Float(a), Kind::Float(b)) => a.to_bits() == b.to_bits(),
        (Kind::Symbol(a), Kind::Symbol(b)) => a == b,
        (Kind::BuiltinFunc(a), Kind::BuiltinFunc(b)) => a == b,
        (Kind::NativeFunction(a), Kind::NativeFunction(b)) => a.ptr_eq(&b),
        (Kind::NativeCompUnit(a), Kind::NativeCompUnit(b)) => a.ptr_eq(&b),
        (Kind::StringObject(left), Kind::StringObject(right)) => left.ptr_eq(&right),
        (Kind::Cons(left), Kind::Cons(right)) => {
            crate::lisp::types::SharedCons::ptr_eq(&left, &right)
        }
        (Kind::Vector(left), Kind::Vector(right)) => left.ptr_eq(&right),
        (Kind::Closure(left), Kind::Closure(right)) => left.ptr_eq(&right),
        (Kind::Buffer(left), Kind::Buffer(right)) => left.ptr_eq(&right),
        (Kind::Marker(left_id), Kind::Marker(right_id)) => left_id == right_id,
        (Kind::HashTable(a), Kind::HashTable(b)) => a == b,
        (Kind::Overlay(left_id), Kind::Overlay(right_id)) => left_id.ptr_eq(&right_id),
        (Kind::CharTable(left_id), Kind::CharTable(right_id)) => left_id == right_id,
        (Kind::SubCharTable(left), Kind::SubCharTable(right)) => left == right,
        (Kind::Frame(left_id), Kind::Frame(right_id)) => left_id == right_id,
        (Kind::Terminal(left_id), Kind::Terminal(right_id)) => left_id.ptr_eq(&right_id),
        (Kind::SymbolWithPos(left), Kind::SymbolWithPos(right)) => left.ptr_eq(&right),
        (Kind::Finalizer(left_id), Kind::Finalizer(right_id)) => left_id == right_id,
        (Kind::Record(left), Kind::Record(right)) => left.ptr_eq(&right),
        (Kind::LispRecord(left), Kind::LispRecord(right)) => left.ptr_eq(&right),
        // eql on non-numbers is eq; identity must be reflexive here too.
        (Kind::ReaderForm(left), Kind::ReaderForm(right)) => left.ptr_eq(&right),
        _ => false,
    }
}

/// fns.c:Feql handles floats and bignums specially, then delegates every
/// other representation to `EQ`.  That final path matters while
/// `symbols-with-pos-enabled` is dynamically non-nil: distinct positioned
/// wrappers for the same bare symbol must then be `eql` too.
pub(crate) fn values_eql_in_env(
    interp: &Interpreter,
    left: &Value,
    right: &Value,
    env: &Env,
) -> bool {
    match left.kind() {
        Kind::Float(_) | Kind::BigInteger(_) => values_eql(left, right),
        _ => values_eq_in_env(interp, left, right, env),
    }
}

#[inline]
pub(crate) fn values_eq_in_env(
    interp: &Interpreter,
    left: &Value,
    right: &Value,
    env: &Env,
) -> bool {
    // lisp.h:EQ: one word is one object.  A symbol with position, the one
    // object eq to a word other than its own, is vectorlike.
    if left.word() == right.word() {
        return true;
    }
    if !left.is_vectorlike() && !right.is_vectorlike() {
        return false;
    }
    values_eq_vectorlike_in_env(interp, left, right, env)
}

#[inline(never)]
fn values_eq_vectorlike_in_env(
    interp: &Interpreter,
    left: &Value,
    right: &Value,
    env: &Env,
) -> bool {
    // Only the positioned-symbol subtype needs the compatibility flag.
    // Ordinary values compare their words without probing symbol fields.
    if (matches!(left.kind(), Kind::SymbolWithPos(_))
        || matches!(right.kind(), Kind::SymbolWithPos(_)))
        && let Some(equal) = symbol_with_pos_eq_in_env(interp, left, right, env)
    {
        return equal;
    }
    values_eq_plain(left, right)
}

/// data.c:Feq on two objects neither of which is a symbol with position:
/// word identity, with the object kinds Emaxx addresses by id compared by
/// id.  Those are vectorlike, so a word that is not is the only word for
/// its object.
#[inline]
pub(crate) fn values_eq_plain(left: &Value, right: &Value) -> bool {
    if left.word() == right.word() {
        return true;
    }
    if !left.is_vectorlike() || !right.is_vectorlike() {
        return false;
    }
    values_eq_by_kind(left, right)
}

#[inline(never)]
fn values_eq_by_kind(left: &Value, right: &Value) -> bool {
    match (left.kind(), right.kind()) {
        (Kind::Nil, Kind::Nil) | (Kind::T, Kind::T) => true,
        (Kind::Integer(a), Kind::Integer(b)) => a == b,
        // lisp.h:EQ compares bignum object identities. Only Feql compares
        // their numeric payloads, just as it does for allocated floats.
        (Kind::BigInteger(a), Kind::BigInteger(b)) => a.ptr_eq(&b),
        // data.c:Feq compares the Lisp_Object words.  Float words point to
        // allocated Lisp_Float objects, so only the same allocation is eq.
        (Kind::Float(a), Kind::Float(b)) => a.ptr_eq(&b),
        (Kind::Symbol(a), Kind::Symbol(b)) => a == b,
        (Kind::BuiltinFunc(a), Kind::BuiltinFunc(b)) => a == b,
        (Kind::NativeFunction(a), Kind::NativeFunction(b)) => a.ptr_eq(&b),
        (Kind::NativeCompUnit(a), Kind::NativeCompUnit(b)) => a.ptr_eq(&b),
        (Kind::StringObject(left), Kind::StringObject(right)) => left.ptr_eq(&right),
        (Kind::Cons(left), Kind::Cons(right)) => {
            crate::lisp::types::SharedCons::ptr_eq(&left, &right)
        }
        (Kind::Vector(left), Kind::Vector(right)) => left.ptr_eq(&right),
        (Kind::Closure(left), Kind::Closure(right)) => left.ptr_eq(&right),
        (Kind::Buffer(left), Kind::Buffer(right)) => left.ptr_eq(&right),
        (Kind::Marker(left_id), Kind::Marker(right_id)) => left_id == right_id,
        (Kind::HashTable(a), Kind::HashTable(b)) => a == b,
        (Kind::Overlay(left_id), Kind::Overlay(right_id)) => left_id.ptr_eq(&right_id),
        (Kind::CharTable(left_id), Kind::CharTable(right_id)) => left_id == right_id,
        (Kind::SubCharTable(left), Kind::SubCharTable(right)) => left == right,
        (Kind::Frame(left_id), Kind::Frame(right_id)) => left_id == right_id,
        (Kind::Terminal(left_id), Kind::Terminal(right_id)) => left_id.ptr_eq(&right_id),
        (Kind::SymbolWithPos(left), Kind::SymbolWithPos(right)) => left.ptr_eq(&right),
        (Kind::Finalizer(left_id), Kind::Finalizer(right_id)) => left_id == right_id,
        (Kind::Record(left), Kind::Record(right)) => left.ptr_eq(&right),
        (Kind::LispRecord(left), Kind::LispRecord(right)) => left.ptr_eq(&right),
        // eq must be reflexive on every object: edebug-unwrap*'s fixed point
        // `(while (not (eq sexp (setq sexp (edebug-unwrap sexp)))))' spins
        // forever when an opaque form is never eq to itself.
        (Kind::ReaderForm(left), Kind::ReaderForm(right)) => left.ptr_eq(&right),
        _ => false,
    }
}

pub(crate) fn plist_type_error(plist: &Value) -> LispError {
    LispError::SignalValue(Value::list([
        Value::Symbol("wrong-type-argument".into()),
        Value::Symbol("plistp".into()),
        *plist,
    ]))
}

pub(crate) fn safe_list_length(list: &Value) -> i64 {
    let mut len = 0i64;
    let mut current = *list;
    let mut seen = crate::lisp::types::CycleGuard::new();
    loop {
        match current.kind() {
            Kind::Cons(cons_cell) => {
                let cdr = &cons_cell.cdr;
                let cell_id = crate::lisp::types::ConsCell::identity(&cons_cell);
                if seen.step(cell_id) {
                    return len;
                }
                len += 1;
                current = cdr.get();
            }
            Kind::Nil => return len,
            _ => return len,
        }
    }
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

pub(crate) fn sequence_length_value(interp: &Interpreter, value: &Value) -> Result<i64, LispError> {
    if let Some(items) = record_literal_items(value) {
        return Ok(items.len().saturating_sub(1) as i64);
    }
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
        item if is_bool_vector_value(interp, &item.value()) => {
            Ok(bool_vector_values(interp, &item.value())?.len() as i64)
        }
        Kind::Closure(lambda) => Ok(lambda.public_len() as i64),
        Kind::LispRecord(record) => Ok(record.len() as i64),
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

fn text_property_plists_equal(
    interp: &Interpreter,
    left: &[(PropertyName, Value)],
    right: &[(PropertyName, Value)],
    env: &Env,
) -> bool {
    left.len() == right.len()
        && left.iter().all(|(key, left_value)| {
            right.iter().any(|(right_key, right_value)| {
                // intervals.c:intervals_equal_1(..., true) uses Fequal
                // for property values, not Fequal_including_properties.
                right_key == key && values_equal_in_env(interp, left_value, right_value, env)
            })
        })
}

pub(crate) fn values_equal_including_properties(
    interp: &Interpreter,
    left: &Value,
    right: &Value,
    env: &Env,
) -> bool {
    values_equal_including_properties_recursive(interp, left, right, &mut HashSet::new(), env)
}

pub(crate) fn values_equal_including_properties_recursive(
    interp: &Interpreter,
    left: &Value,
    right: &Value,
    seen: &mut HashSet<(usize, usize)>,
    env: &Env,
) -> bool {
    // fns.c:internal_equal calls maybe_remove_pos_from_symbol before its
    // type/equality dispatch.  The switch is the dynamically bound
    // `symbols-with-pos-enabled' flag, so the including-properties variant
    // must consult the same evaluator environment as ordinary `equal'.
    if (matches!(left.kind(), Kind::SymbolWithPos(_))
        || matches!(right.kind(), Kind::SymbolWithPos(_)))
        && let Some(equal) = symbol_with_pos_equal_in_env(interp, left, right, env)
    {
        return equal;
    }
    if let (Kind::StringObject(left_string), Kind::StringObject(right_string)) =
        (left.kind(), right.kind())
    {
        // fns.c:internal_equal checks BASE_EQ before inspecting contents.
        if left_string.ptr_eq(&right_string) {
            return true;
        }
        let left_string = left_string.borrow();
        let right_string = right_string.borrow();
        if !left_string.contents_equal(&right_string) {
            return false;
        }
        // GNU's compare_string_intervals walks POSITIONS, so interval
        // segmentation is not significant, and plists within a span
        // compare as sets (intervals_equal in intervals.c).
        let len = left_string.len();
        let collect_props = |spans: &[StringPropertySpan], pos: usize| {
            let mut out: Vec<(PropertyName, Value)> = Vec::new();
            for span in spans {
                if span.start <= pos && pos < span.end {
                    for (key, value) in &span.props {
                        if !out.iter().any(|(existing, _)| existing == key) {
                            out.push((*key, *value));
                        }
                    }
                }
            }
            out
        };
        let mut bounds: Vec<usize> = vec![0, len];
        for span in left_string.props.iter().chain(right_string.props.iter()) {
            bounds.push(span.start.min(len));
            bounds.push(span.end.min(len));
        }
        bounds.sort_unstable();
        bounds.dedup();
        for window in bounds.windows(2) {
            let pos = window[0];
            if pos >= len {
                break;
            }
            if !text_property_plists_equal(
                interp,
                &collect_props(&left_string.props, pos),
                &collect_props(&right_string.props, pos),
                env,
            ) {
                return false;
            }
        }
        return true;
    }
    if let (Ok(left_items), Ok(right_items)) = (vector_items(left), vector_items(right))
        && matches!(left.kind(), Kind::Cons(_))
        && matches!(right.kind(), Kind::Cons(_))
    {
        return left_items.len() == right_items.len()
            && left_items
                .iter()
                .zip(right_items.iter())
                .all(|(left, right)| {
                    values_equal_including_properties_recursive(interp, left, right, seen, env)
                });
    }
    match (left.kind(), right.kind()) {
        (Kind::Nil, Kind::Nil) | (Kind::T, Kind::T) => true,
        (Kind::Integer(a), Kind::Integer(b)) => a == b,
        (Kind::BigInteger(a), Kind::BigInteger(b)) => a == b,
        (Kind::Integer(a), Kind::BigInteger(b)) | (Kind::BigInteger(b), Kind::Integer(a)) => {
            BigInt::from(a) == *b
        }
        (Kind::Float(a), Kind::Float(b)) => a.to_bits() == b.to_bits(),
        (Kind::Symbol(a), Kind::Symbol(b)) => a == b,
        (Kind::LispRecord(left), Kind::LispRecord(right)) => {
            left.ptr_eq(&right)
                || (left.len() == right.len()
                    && (!seen.insert((left.identity(), right.identity()))
                        || left.slots().zip(right.slots()).all(|(a, b)| {
                            values_equal_including_properties_recursive(interp, &a, &b, seen, env)
                        })))
        }
        (Kind::Record(left_id), Kind::Record(right_id)) => {
            if left_id.ptr_eq(&right_id) {
                return true;
            }
            let (Some(left_record), Some(right_record)) =
                (interp.find_record(left_id), interp.find_record(right_id))
            else {
                return false;
            };
            if left_record.kind != right_record.kind {
                return false;
            }
            // fns.c:internal_equal walks genuine records and pseudovectors
            // at or above PVEC_CLOSURE element-by-element.  In particular,
            // distinct but equal byte-code closures are keys in comp.el's
            // relocation tables.  Runtime handles below that boundary stay
            // identity-only.
            if matches!(
                left_record.kind,
                crate::lisp::eval::RecordKind::Process
                    | crate::lisp::eval::RecordKind::Obarray
                    | crate::lisp::eval::RecordKind::Window
                    | crate::lisp::eval::RecordKind::WindowConfiguration
                    | crate::lisp::eval::RecordKind::Thread
                    | crate::lisp::eval::RecordKind::Mutex
                    | crate::lisp::eval::RecordKind::ConditionVariable
                    | crate::lisp::eval::RecordKind::ModuleFunction
                    | crate::lisp::eval::RecordKind::UserPointer
                    | crate::lisp::eval::RecordKind::TreeSitterParser
                    | crate::lisp::eval::RecordKind::TreeSitterCompiledQuery
                    | crate::lisp::eval::RecordKind::Sqlite
            ) {
                return false;
            }
            if left_record.kind == crate::lisp::eval::RecordKind::TreeSitterNode {
                let left = interp.treesit_node_state(left);
                let right = interp.treesit_node_state(right);
                return matches!((left, right), (Some(left), Some(right))
                    if left.parser_id == right.parser_id
                        && left.generation == right.generation
                        && left.node_id == right.node_id);
            }
            let pair = (left_id.identity(), right_id.identity());
            if !seen.insert(pair) {
                return true;
            }
            values_equal_including_properties_recursive(
                interp,
                &left_record.type_tag,
                &right_record.type_tag,
                seen,
                env,
            ) && left_record.slots.len() == right_record.slots.len()
                && left_record
                    .slots
                    .iter()
                    .zip(&right_record.slots)
                    .all(|(left, right)| {
                        values_equal_including_properties_recursive(interp, left, right, seen, env)
                    })
        }
        (Kind::Cons(_), Kind::Cons(_)) => {
            let Some((left_car, _)) = left.cons_cells() else {
                return false;
            };
            let Some((right_car, _)) = right.cons_cells() else {
                return false;
            };
            let pair = (left_car.cell_id(), right_car.cell_id());
            if !seen.insert(pair) {
                return true;
            }
            let Some((a_car, a_cdr)) = left.cons_values() else {
                return false;
            };
            let Some((b_car, b_cdr)) = right.cons_values() else {
                return false;
            };
            values_equal_including_properties_recursive(interp, &a_car, &b_car, seen, env)
                && values_equal_including_properties_recursive(interp, &a_cdr, &b_cdr, seen, env)
        }
        _ => values_equal_in_env(interp, left, right, env),
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
    // fns.c:value_cmp(PVEC_BUFFER) compares current names, with killed
    // buffers (nil names) before live ones. Allocation IDs are not order.
    let left = left.borrow();
    let right = right.borrow();
    match (left.last_name.is_none(), right.last_name.is_none()) {
        (true, true) => compare_plain_symbol_names(&left.name, &right.name),
        (false, true) => ValueOrder::Less,
        (true, false) => ValueOrder::Greater,
        (false, false) => ValueOrder::Equal,
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
        crate::lisp::eval::RecordKind::BoolVector => {
            let left_bits = bool_vector_bits(interp, left)?;
            let right_bits = bool_vector_bits(interp, right)?;
            for (left_bit, right_bit) in left_bits.iter().zip(right_bits.iter()) {
                match left_bit.cmp(right_bit) {
                    Ordering::Less => return Ok(Some(ValueOrder::Less)),
                    Ordering::Greater => return Ok(Some(ValueOrder::Greater)),
                    Ordering::Equal => {}
                }
            }
            Ok(Some(order_from_ordering(
                left_bits.len().cmp(&right_bits.len()),
            )))
        }
        crate::lisp::eval::RecordKind::Process => Ok(Some(
            match (
                interp.process_name(left_id.id),
                interp.process_name(right_id.id),
            ) {
                (Some(left), Some(right)) => compare_plain_symbol_names(&left, &right),
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

pub(crate) fn circular_list_signal_p(value: &Value) -> bool {
    value
        .to_vec()
        .ok()
        .and_then(|items| items.first().cloned())
        .and_then(|head| head.symbol_str().map(str::to_string))
        .is_some_and(|symbol| symbol == "circular-list")
}

pub(crate) fn remove_equal(
    interp: &Interpreter,
    elt: &Value,
    sequence: &Value,
) -> Result<Value, LispError> {
    if let Some(string) = sequence_string_like(sequence) {
        let filtered = string
            .text
            .chars()
            .filter(|ch| !values_equal(interp, &string_sequence_value(&string, *ch), elt))
            .collect::<String>();
        return Ok(make_shared_string_value_with_multibyte(
            filtered,
            Vec::new(),
            string.multibyte,
        ));
    }

    if is_vector_value(sequence) {
        let filtered = vector_items(sequence)?
            .into_iter()
            .filter(|item| !values_equal(interp, item, elt))
            .collect::<Vec<_>>();
        let mut result = vec![Value::symbol("vector-literal")];
        result.extend(filtered);
        return Ok(Value::list(result));
    }

    match sequence.kind() {
        Kind::Nil | Kind::Cons(_) => Ok(Value::list(
            sequence
                .to_vec()?
                .into_iter()
                .filter(|item| !values_equal(interp, item, elt))
                .collect::<Vec<_>>(),
        )),
        _ => Err(LispError::WrongTypeArgument("sequencep".into(), *sequence)),
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum HashMode {
    Eq,
    Eql,
    Equal,
    EqualIncludingProperties,
}

pub(crate) fn nconc_values(args: &[Value]) -> Result<Value, LispError> {
    if args.is_empty() {
        return Ok(Value::Nil);
    }

    let mut head: Option<Value> = None;
    let mut tail: Option<Value> = None;

    for (index, value) in args.iter().enumerate() {
        let is_last = index + 1 == args.len();
        if is_last {
            if let Some(tail_cell) = tail {
                tail_cell.set_cdr(*value)?;
                return Ok(head.unwrap_or(*value));
            }
            return Ok(*value);
        }

        if value.is_nil() {
            continue;
        }

        let last_cell = last_nconc_cell(value)?;
        if let Some(tail_cell) = &tail {
            tail_cell.set_cdr(*value)?;
        } else {
            head = Some(*value);
        }
        tail = Some(last_cell);
    }

    Ok(head.unwrap_or(Value::Nil))
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

pub(crate) struct RemoteFileNameParts {
    pub(crate) prefix: String,
    pub(crate) method: String,
    pub(crate) localname: String,
}

pub(crate) fn parse_remote_file_name(path: &str) -> Option<RemoteFileNameParts> {
    if !path.starts_with('/') {
        return None;
    }
    let rest = &path[1..];
    let method_end = rest.find(':')?;
    if method_end == 0 {
        return None;
    }
    let method = rest[..method_end].to_string();
    // tramp-file-name-regexp: the method segment is `[^/|:]+' -- a local
    // file whose NAME contains a colon ("/tmp/dir/:foo:bar") must never
    // parse as remote.
    if method.contains('/') || method.contains('|') {
        return None;
    }
    let after_method = &rest[method_end + 1..];
    let host_end = after_method.find(':')?;
    let authority = &after_method[..host_end];
    // The user/host segment is `[^/|:]*' by the same regexp.
    if authority.contains('/') || authority.contains('|') {
        return None;
    }
    let localname = after_method[host_end + 1..].to_string();
    match authority.rsplit_once('@') {
        Some((_, host)) if !host.is_empty() => {}
        _ if authority.is_empty() && method == "mock" => {}
        _ if authority.is_empty() => return None,
        _ => {}
    }
    Some(RemoteFileNameParts {
        prefix: path[..1 + method_end + 1 + host_end + 1].to_string(),
        method,
        localname,
    })
}

pub(crate) fn last_nconc_cell(value: &Value) -> Result<Value, LispError> {
    let mut current = *value;
    let mut seen = crate::lisp::types::CycleGuard::new();
    loop {
        let Some((car, cdr)) = (current.clone()).cons_cells() else {
            return Err(LispError::WrongTypeArgument("consp".into(), current));
        };
        let cell_id = car.cell_id();
        if seen.step(cell_id) {
            return Err(LispError::SignalValue(Value::list([
                Value::symbol("circular-list"),
                Value::string("Circular list"),
            ])));
        }
        match cdr.get().kind() {
            Kind::Cons(_) => current = cdr.get(),
            _ => return Ok(current),
        }
    }
}

pub(crate) fn sxhash_value_in_env(
    interp: &Interpreter,
    value: &Value,
    mode: HashMode,
    env: &Env,
) -> i64 {
    sxhash_value_with_symbol_positions(interp, value, mode, symbols_with_pos_enabled(interp, env))
}

fn sxhash_value_with_symbol_positions(
    interp: &Interpreter,
    value: &Value,
    mode: HashMode,
    remove_symbol_positions: bool,
) -> i64 {
    let mut state = 0xcbf2_9ce4_8422_2325u64;
    hash_value_recursive_with_symbol_positions(
        interp,
        &mut state,
        value,
        mode,
        remove_symbol_positions,
    );
    (state & 0x7fff_ffff_ffff_ffff) as i64
}

/// Return the structural hash used to index a GNU `equal' hash table.
///
/// Some runtime objects can compare equal to a different representation
/// (keymap records and their public list projection, for example).  Keep
/// those values in a collision bucket.  The compatibility check follows only
/// the portion GNU's `sxhash_obj' will inspect: depth is capped at three and a
/// list contributes at most seven elements.  Walking the entire key first is
/// both unnecessary and unlike fns.c, especially for compiler IR lists.
pub(crate) fn equal_hash_table_key_hash_in_env(
    interp: &Interpreter,
    value: &Value,
    env: &Env,
) -> Option<i64> {
    fn indexable(value: &Value, depth: u32) -> bool {
        if depth > SXHASH_MAX_DEPTH {
            return true;
        }
        match value.kind() {
            Kind::Record(_)
            | Kind::LispRecord(_)
            | Kind::Buffer(_)
            | Kind::Marker(_)
            | Kind::Overlay(_)
            | Kind::CharTable(_)
            | Kind::Closure(_)
            | Kind::ReaderForm(_) => false,
            Kind::Cons(_) => {
                let mut tail = *value;
                let limit = if depth < SXHASH_MAX_DEPTH {
                    SXHASH_MAX_LEN
                } else {
                    1
                };
                for _ in 0..limit {
                    let Some((car, cdr)) = tail.cons_values() else {
                        break;
                    };
                    if matches!(
                            car.kind(),
                        Kind::Symbol(symbol) if symbol == "keymap"
                    ) {
                        return false;
                    }
                    if depth < SXHASH_MAX_DEPTH && !indexable(&car, depth + 1) {
                        return false;
                    }
                    tail = cdr;
                }
                if depth < SXHASH_MAX_DEPTH && !tail.is_nil() {
                    return indexable(&tail, depth + 1);
                }
                true
            }
            _ => true,
        }
    }

    indexable(value, 0).then(|| sxhash_value_in_env(interp, value, HashMode::Equal, env))
}

/// Bucket key for a runtime-accelerated hash table.  The invariant is that
/// keys the table's test considers the same always share a bucket; probing
/// a bucket still compares with the real test, so collisions are harmless.
///
/// The `eq'/`eql' hash therefore differs from `sxhash-eq' in three ways:
/// conses and lambdas hash by cell identity (mutation- and cycle-proof,
/// and `eq' only holds for the same cells), a symbol-with-pos hashes as
/// its bare symbol (the two can be `eq' under symbols-with-pos-enabled),
/// and -0.0 hashes as 0.0 (`eq'/`eql' compare floats by value here).
pub(crate) fn runtime_hash_bucket_key(
    interp: &Interpreter,
    test: crate::lisp::eval::RuntimeHashTest,
    value: &Value,
) -> Option<i64> {
    use crate::lisp::eval::RuntimeHashTest;
    if test == RuntimeHashTest::Equal {
        return equal_hash_table_key_hash_in_env(interp, value, &Env::new());
    }
    let mut state = 0xcbf2_9ce4_8422_2325u64;
    if let Kind::SymbolWithPos(object) = value.kind() {
        hash_value_eq(&mut state, &object.symbol());
        return Some(state as i64);
    }
    match value.kind() {
        Kind::Cons(cons_cell) => {
            hash_mix(&mut state, 16);
            hash_mix(
                &mut state,
                crate::lisp::types::ConsCell::identity(&cons_cell) as u64,
            );
        }
        Kind::Float(number) => {
            hash_mix(&mut state, 15);
            let normalized = if number.get() == 0.0 {
                0.0f64
            } else {
                number.get()
            };
            hash_mix(&mut state, normalized.to_bits());
        }
        Kind::BigInteger(number) => {
            hash_mix(&mut state, 14);
            hash_str(&mut state, &number.to_string());
        }
        other => match test {
            RuntimeHashTest::Eql => hash_value_eql(&mut state, &other.value()),
            _ => hash_value_eq(&mut state, &other.value()),
        },
    }
    Some(state as i64)
}

pub(crate) fn hash_mix(state: &mut u64, value: u64) {
    *state ^= value;
    *state = state.wrapping_mul(0x0000_0100_0000_01b3);
}

pub(crate) fn hash_str(state: &mut u64, text: &str) {
    for byte in text.as_bytes() {
        hash_mix(state, u64::from(*byte));
    }
}

/// fns.c:5336 `SXHASH_MAX_DEPTH' and fns.c:5341 `SXHASH_MAX_LEN'.  GNU stops
/// descending at depth 3 and folds in at most seven elements of a list or
/// vector, so hashing a cyclic structure terminates instead of recursing
/// forever: `cl-print' labels a closure `#<bytecode %#x>' by calling
/// `sxhash' on it, and a closure's constants routinely point back at the
/// closure (ert builds exactly that graph while reporting a failure).
const SXHASH_MAX_DEPTH: u32 = 3;
const SXHASH_MAX_LEN: usize = 7;

pub(crate) fn hash_props(
    interp: &Interpreter,
    state: &mut u64,
    props: &[StringPropertySpan],
    depth: u32,
    remove_symbol_positions: bool,
) {
    hash_mix(state, props.len() as u64);
    for span in props {
        hash_mix(state, span.start as u64);
        hash_mix(state, span.end as u64);
        hash_mix(state, span.props.len() as u64);
        for (key, value) in &span.props {
            // Property names compare with EQ (intervals_equal).
            hash_value_eq(state, &key.value());
            hash_value_equal_at(
                interp,
                state,
                value,
                // fns.c:hash_interval hashes the plist with sxhash_obj;
                // its property values use ordinary equal, like intervals.c.
                false,
                depth + 1,
                remove_symbol_positions,
            );
        }
    }
}

fn hash_value_recursive_with_symbol_positions(
    interp: &Interpreter,
    state: &mut u64,
    value: &Value,
    mode: HashMode,
    remove_symbol_positions: bool,
) {
    if remove_symbol_positions && let Some((symbol, _)) = symbol_with_pos_parts(interp, value) {
        return hash_value_recursive_with_symbol_positions(
            interp,
            state,
            &symbol,
            mode,
            remove_symbol_positions,
        );
    }
    match mode {
        HashMode::Eq => hash_value_eq(state, value),
        HashMode::Eql => hash_value_eql(state, value),
        HashMode::Equal | HashMode::EqualIncludingProperties => hash_value_equal_at(
            interp,
            state,
            value,
            mode == HashMode::EqualIncludingProperties,
            0,
            remove_symbol_positions,
        ),
    }
}

pub(crate) fn hash_value_eq(state: &mut u64, value: &Value) {
    match value.kind() {
        Kind::Nil => hash_mix(state, 0),
        Kind::T => hash_mix(state, 1),
        Kind::Integer(number) => {
            hash_mix(state, 2);
            hash_mix(state, number as u64);
        }
        Kind::Symbol(symbol) => {
            hash_mix(state, 3);
            hash_str(state, &symbol);
        }
        Kind::StringObject(shared) => {
            hash_mix(state, 4);
            hash_mix(state, shared.identity() as u64);
        }
        Kind::Vector(vector) => {
            hash_mix(state, 16);
            hash_mix(state, vector.identity() as u64);
        }
        Kind::NativeCompUnit(unit) => {
            hash_mix(state, 54);
            hash_mix(state, unit.identity() as u64);
        }
        Kind::NativeFunction(function) => {
            hash_mix(state, 53);
            hash_mix(state, function.identity() as u64);
        }
        Kind::BuiltinFunc(name) => {
            hash_mix(state, 6);
            hash_str(state, &name);
        }
        Kind::Closure(lambda_value) => {
            hash_mix(state, 7);
            hash_mix(state, lambda_value.identity() as u64);
        }
        Kind::Buffer(buffer_value) => {
            hash_mix(state, 8);
            hash_mix(state, buffer_value.identity() as u64);
        }
        Kind::Marker(id) => {
            hash_mix(state, 9);
            hash_mix(state, id.identity() as u64);
        }
        Kind::Overlay(overlay) => {
            hash_mix(state, 10);
            hash_mix(state, overlay.identity() as u64);
        }
        Kind::HashTable(table) => {
            hash_mix(state, 52);
            hash_mix(state, table.identity() as u64);
        }
        Kind::CharTable(table) => {
            hash_mix(state, 11);
            hash_mix(state, table.identity() as u64);
        }
        Kind::SubCharTable(table) => {
            hash_mix(state, 44);
            hash_mix(state, table.identity() as u64);
        }
        Kind::Frame(id) => {
            hash_mix(state, 18);
            hash_mix(state, id.identity() as u64);
        }
        Kind::Terminal(id) => {
            hash_mix(state, 19);
            hash_mix(state, id.identity() as u64);
        }
        Kind::SymbolWithPos(object) => {
            hash_mix(state, 21);
            hash_mix(state, object.identity() as u64);
        }
        Kind::LispRecord(record) => {
            hash_mix(state, 45);
            hash_mix(state, record.identity() as u64);
        }
        Kind::Record(id) => {
            hash_mix(state, 12);
            hash_mix(state, id.id);
        }
        Kind::Finalizer(object) => {
            hash_mix(state, 13);
            hash_mix(state, object.identity() as u64);
        }
        Kind::ReaderForm(form) => {
            hash_mix(state, 20);
            hash_mix(state, form.identity() as u64);
        }
        Kind::Unbound => {
            hash_mix(state, 17);
        }
        Kind::BigInteger(number) => {
            hash_mix(state, 14);
            hash_str(state, &number.to_string());
        }
        Kind::Float(number) => {
            hash_mix(state, 15);
            hash_mix(state, number.to_bits());
        }
        Kind::Cons(_) => {
            let Some((car, cdr)) = value.cons_values() else {
                return;
            };
            hash_mix(state, 16);
            hash_value_eq(state, &car);
            hash_value_eq(state, &cdr);
        }
    }
}

pub(crate) fn hash_value_eql(state: &mut u64, value: &Value) {
    match value.kind() {
        Kind::Float(number) => {
            hash_mix(state, 21);
            hash_mix(state, number.to_bits());
        }
        Kind::BigInteger(number) => {
            hash_mix(state, 22);
            hash_str(state, &number.to_string());
        }
        other => hash_value_eq(state, &other.value()),
    }
}

/// `sxhash_obj' (fns.c:5505) with GNU's depth bound: past `SXHASH_MAX_DEPTH'
/// every object hashes as 0 regardless of its contents.
pub(crate) fn hash_value_equal_at(
    interp: &Interpreter,
    state: &mut u64,
    value: &Value,
    include_properties: bool,
    depth: u32,
    remove_symbol_positions: bool,
) {
    if depth > SXHASH_MAX_DEPTH {
        hash_mix(state, 0);
        return;
    }
    if remove_symbol_positions && let Some((symbol, _)) = symbol_with_pos_parts(interp, value) {
        return hash_value_equal_at(
            interp,
            state,
            &symbol,
            include_properties,
            depth,
            remove_symbol_positions,
        );
    }
    match value.kind() {
        Kind::Nil => hash_mix(state, 30),
        Kind::T => hash_mix(state, 31),
        Kind::Integer(number) => {
            hash_mix(state, 32);
            hash_str(state, &number.to_string());
        }
        Kind::BigInteger(number) => {
            hash_mix(state, 32);
            hash_str(state, &number.to_string());
        }
        Kind::Float(number) => {
            hash_mix(state, 34);
            let bits = if number.get() == 0.0 {
                0.0f64.to_bits()
            } else if number.is_nan() {
                f64::NAN.to_bits()
            } else {
                number.to_bits()
            };
            hash_mix(state, bits);
        }
        Kind::StringObject(shared) => {
            hash_mix(state, 35);
            let shared = shared.borrow();
            // fns.c:sxhash_obj hashes SDATA/SBYTES, without converting
            // internal characters to a host text representation.
            for byte in shared.bytes() {
                hash_mix(state, u64::from(*byte));
            }
            if include_properties {
                hash_props(interp, state, &shared.props, depth, remove_symbol_positions);
            }
        }
        Kind::Symbol(symbol) => {
            hash_mix(state, 37);
            hash_str(state, &symbol);
        }
        Kind::Vector(vector) => {
            hash_mix(state, 40);
            for slot in vector.slots().take(SXHASH_MAX_LEN) {
                hash_value_equal_at(
                    interp,
                    state,
                    &slot,
                    include_properties,
                    depth + 1,
                    remove_symbol_positions,
                );
            }
        }
        Kind::Cons(_) => {
            hash_mix(state, 38);
            // `sxhash_list' (fns.c:5420): walk at most `SXHASH_MAX_LEN'
            // elements, and only while the structure is shallower than
            // `SXHASH_MAX_DEPTH'; whatever tail remains is folded in one
            // level deeper, which is what terminates a circular list.
            let mut tail = *value;
            if depth < SXHASH_MAX_DEPTH {
                for _ in 0..SXHASH_MAX_LEN {
                    let Some((car, cdr)) = tail.cons_values() else {
                        break;
                    };
                    hash_value_equal_at(
                        interp,
                        state,
                        &car,
                        include_properties,
                        depth + 1,
                        remove_symbol_positions,
                    );
                    tail = cdr;
                }
            }
            if !tail.is_nil() {
                hash_value_equal_at(
                    interp,
                    state,
                    &tail,
                    include_properties,
                    depth + 1,
                    remove_symbol_positions,
                );
            }
        }
        Kind::NativeCompUnit(unit) => {
            hash_mix(state, 54);
            hash_mix(state, unit.identity() as u64);
        }
        Kind::NativeFunction(function) => {
            hash_mix(state, 53);
            hash_mix(state, function.identity() as u64);
        }
        Kind::BuiltinFunc(name) => {
            hash_mix(state, 39);
            hash_str(state, &name);
        }
        Kind::Closure(lambda_value) => {
            hash_mix(state, 40);
            // `sxhash_vector' (fns.c:5447) bounds a closure the same way.
            for slot in lambda_value.slots().take(SXHASH_MAX_LEN) {
                hash_value_equal_at(
                    interp,
                    state,
                    &slot,
                    include_properties,
                    depth + 1,
                    remove_symbol_positions,
                );
            }
        }
        Kind::Buffer(buffer_value) => {
            hash_mix(state, 41);
            hash_mix(state, buffer_value.identity() as u64);
        }
        Kind::Marker(id) => {
            hash_marker_equal(state, id);
        }
        Kind::Overlay(overlay) => {
            hash_mix(state, 43);
            let (beg, end) = if overlay.is_dead() {
                (-1, -1)
            } else {
                overlay.bounds()
            };
            hash_mix(state, beg as u64);
            hash_mix(state, end as u64);
            hash_value_equal_at(
                interp,
                state,
                &overlay.plist(),
                include_properties,
                depth + 1,
                remove_symbol_positions,
            );
        }
        Kind::SubCharTable(_) => hash_mix(state, 42),
        Kind::HashTable(table) => {
            hash_mix(state, 52);
            hash_mix(state, table.identity() as u64);
        }
        Kind::CharTable(id) => {
            hash_char_table_equal(
                interp,
                state,
                id,
                include_properties,
                depth,
                remove_symbol_positions,
            );
        }
        Kind::Frame(id) => {
            hash_mix(state, 48);
            hash_mix(state, id.identity() as u64);
        }
        Kind::Terminal(id) => {
            hash_mix(state, 49);
            hash_mix(state, id.identity() as u64);
        }
        Kind::SymbolWithPos(object) => {
            // fns.c:sxhash_obj uses this address with positions disabled.
            // When enabled, the wrapper is stripped in the branch above.
            hash_mix(state, 51);
            hash_mix(state, object.identity() as u64);
        }
        Kind::LispRecord(record) => {
            hash_mix(state, 45);
            hash_mix(state, record.len() as u64);
            for field in record.slots().take(SXHASH_MAX_LEN) {
                hash_value_equal_at(
                    interp,
                    state,
                    &field,
                    include_properties,
                    depth + 1,
                    remove_symbol_positions,
                );
            }
        }
        Kind::Record(id) => {
            hash_record_equal(
                interp,
                state,
                id.id,
                include_properties,
                depth,
                remove_symbol_positions,
            );
        }
        Kind::Finalizer(object) => {
            hash_mix(state, 46);
            hash_mix(state, object.identity() as u64);
        }
        Kind::ReaderForm(form) => {
            hash_mix(state, 50);
            hash_mix(state, form.identity() as u64);
        }
        Kind::Unbound => {
            hash_mix(state, 47);
        }
    }
}

pub(crate) fn hash_marker_equal(state: &mut u64, marker: crate::lisp::types::MarkerRef) {
    hash_mix(state, 42);
    if let Some(buffer) = marker.buffer() {
        hash_mix(state, buffer.identity() as u64);
        hash_mix(state, marker.bytepos() as u64);
    }
}

pub(crate) fn hash_char_table_equal(
    interp: &Interpreter,
    state: &mut u64,
    table: crate::lisp::types::CharTableRef,
    include_properties: bool,
    depth: u32,
    remove_symbol_positions: bool,
) {
    hash_mix(state, 44);
    hash_mix(state, table.slot_count() as u64);
    for value in table.slots().take(SXHASH_MAX_LEN) {
        hash_value_equal_at(
            interp,
            state,
            &value,
            include_properties,
            depth + 1,
            remove_symbol_positions,
        );
    }
}

pub(crate) fn hash_record_equal(
    interp: &Interpreter,
    state: &mut u64,
    id: u64,
    include_properties: bool,
    depth: u32,
    remove_symbol_positions: bool,
) {
    hash_mix(state, 45);
    let Some(record) = interp.find_record(id) else {
        hash_mix(state, id);
        return;
    };

    match record.kind {
        crate::lisp::eval::RecordKind::BoolVector => {
            hash_str(state, "bool-vector");
            if let Ok(bits) = bool_vector_bits(interp, &interp.record_value(id)) {
                hash_mix(state, bits.len() as u64);
                for bit in bits {
                    hash_mix(state, u64::from(bit));
                }
            } else {
                hash_mix(state, id);
            }
        }
        crate::lisp::eval::RecordKind::Process
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
        | crate::lisp::eval::RecordKind::Sqlite => {
            hash_value_equal_at(
                interp,
                state,
                &record.type_tag,
                include_properties,
                depth + 1,
                remove_symbol_positions,
            );
            hash_mix(state, id);
        }
        crate::lisp::eval::RecordKind::Font => {
            hash_value_equal_at(
                interp,
                state,
                &record.type_tag,
                include_properties,
                depth + 1,
                remove_symbol_positions,
            );
            hash_mix(state, record.slots.len() as u64);
            // fns.c:5447 `sxhash_vector' hashes a record's leading slots only.
            for slot in record.slots.iter().take(SXHASH_MAX_LEN) {
                hash_value_equal_at(
                    interp,
                    state,
                    slot,
                    include_properties,
                    depth + 1,
                    remove_symbol_positions,
                );
            }
        }
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

pub(crate) fn overlays_equal(
    interp: &Interpreter,
    left: crate::overlay::OverlayRef,
    right: crate::overlay::OverlayRef,
    seen: &mut HashSet<(usize, usize)>,
    env: Option<&Env>,
) -> bool {
    // fns.c internal_equal memoizes objects it has descended into (the
    // depth>10 eq hash table) and assumes equality on revisit, so cyclic
    // overlay graphs -- semantic's tag<->overlay plists are exactly that --
    // terminate instead of recursing forever.  The pair-set mirrors GNU's
    // termination across repeated object pairs; without
    // it this comparison overflowed an 8 GiB stack on
    // test/lisp/cedet/semantic-utest-ia.el.
    let pair = (left.identity(), right.identity());
    if !seen.insert(pair) {
        return true;
    }
    let same_buffer = match (left.buffer(), right.buffer()) {
        (None, None) => true,
        (Some(a), Some(b)) => a.ptr_eq(&b),
        _ => false,
    };
    // Detached overlays all have GNU's public -1 endpoints, regardless of
    // the last interval coordinates retained in their unlinked node.
    let equal = same_buffer
        && (left.is_dead() || left.bounds() == right.bounds())
        && values_equal_recursive_with_env(interp, &left.plist(), &right.plist(), seen, env);
    seen.remove(&pair);
    equal
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
        Kind::Symbol(name) => interp.lookup_function(&name, env),
        _ => Ok(callable),
    }
}

pub(crate) fn is_lambda_expression(interp: &Interpreter, value: &Value, env: &Env) -> bool {
    value.to_vec().ok().is_some_and(|items| {
        items.first().is_some_and(|head| {
            matches!(head.kind(), Kind::Symbol(name) if name == "lambda")
                || (symbols_with_pos_enabled(interp, env)
                    && matches!(
                        symbol_with_pos_parts(interp, head).map(|(a0, a1)| (a0.kind(), a1)),
                        Some((Kind::Symbol(name), _)) if name == "lambda"
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

/// GNU's internal FUNCTIONP predicate.  Native callers use this helper
/// directly so redefining the Lisp symbol `functionp' cannot change C-level
/// argument validation.
pub(crate) fn function_value_p(interp: &Interpreter, value: &Value, env: &Env) -> bool {
    let symbol_value = if symbols_with_pos_enabled(interp, env) {
        symbol_with_pos_parts(interp, value)
            .map(|(symbol, _)| symbol)
            .unwrap_or_else(|| *value)
    } else {
        *value
    };
    let symbol = symbol_value.symbol_str();
    if symbol
        .is_some_and(|symbol| name_facts(symbol).special_form || interp.has_macro_binding(symbol))
    {
        return false;
    }
    let resolved = resolve_callable(interp, value, env).unwrap_or(*value);
    let autoloaded_function =
        symbol.is_some() && autoload_parts(&resolved).is_some_and(|(_, _, kind)| kind.is_nil());
    callable_value_p(interp, &resolved, env) || autoloaded_function
}

pub(crate) fn value_matches_with_test(
    interp: &mut Interpreter,
    left: &Value,
    right: &Value,
    testfn: Option<&Value>,
    env: &mut Env,
) -> Result<bool, LispError> {
    match testfn.filter(|value| !value.is_nil()).map(|v| v.kind()) {
        None => Ok(values_eq_in_env(interp, left, right, env)),
        Some(kind @ (Kind::Symbol(_) | Kind::BuiltinFunc(_))) => {
            let name = match &kind {
                Kind::Symbol(name) => name.as_str(),
                Kind::BuiltinFunc(subr) => subr.as_str(),
                _ => unreachable!("a symbol or a subr"),
            };
            match name {
                "eq" => Ok(values_eq_in_env(interp, left, right, env)),
                "eql" => Ok(values_eql(left, right)),
                "equal" => Ok(values_equal_in_env(interp, left, right, env)),
                _ => {
                    let func = resolve_callable(interp, testfn.expect("checked Some"), env)?;
                    Ok(invoke_function_value(interp, &func, &[*left, *right], env)?.is_truthy())
                }
            }
        }
        Some(other) => {
            let func = resolve_callable(interp, &other.value(), env)?;
            Ok(invoke_function_value(interp, &func, &[*left, *right], env)?.is_truthy())
        }
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

pub(crate) fn callable_name(original: &Value, resolved: &Value) -> Option<String> {
    match original.kind() {
        Kind::Symbol(name) => Some(name.to_string()),
        _ => match resolved.kind() {
            Kind::BuiltinFunc(name) => Some(name.to_string()),
            _ => None,
        },
    }
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
    if !matches!(view.car_if_list()?.kind(), Kind::Symbol(name) if name == "keymap") {
        return None;
    }
    let mut tail = view.cdr_if_list()?;
    let mut cycle = crate::lisp::types::CycleGuard::new();
    while let Kind::Cons(cell) = tail.kind() {
        if cycle.step(crate::lisp::types::ConsCell::identity(&cell)) {
            break;
        }
        let item = cell.car.get();
        if matches!(item.kind(), Kind::Symbol(name) if name == "keymap") {
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
        if matches!(item.kind(), Kind::Symbol(name) if name == "keymap") {
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
    let mut seen = HashSet::new();
    while let Some((entry, _)) = tail.cons_values() {
        if entry.eq_value(keymap_symbol()) || is_keymap_value(interp, &entry) {
            break;
        }
        if !seen.insert(tail.word()) {
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
    let length = sequence_length_value(interp, &sequence)? as usize;
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
    let Ok(function) = interp.lookup_function(&name, env) else {
        return Ok(());
    };
    if let Some((file, _, Kind::Symbol(kind))) =
        autoload_parts(&function).map(|(a0, a1, a2)| (a0, a1, a2.kind()))
        && kind == "keymap"
    {
        interp.load_autoload_target(&file, env)?;
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
        .lookup_var("keyboard-translate-table", env)
        .unwrap_or(Value::Nil);
    if !prefix.is_nil()
        && let Some(table) = string_like(&translation)
    {
        let size = table.text.chars().count();
        let mut heading = true;
        for index in 0..size {
            // GNU reacquires the table after insertions, which can run Lisp.
            let table = interp
                .lookup_var("keyboard-translate-table", env)
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
        .lookup_var("key-translation-map", env)
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
        .lookup_var("overriding-terminal-local-map", env)
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
            .lookup_var("overriding-local-map", env)
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
            active_minor_mode_bindings(interp, env).map(crate::lisp::alloc::RootedVec::from_vec)
        })?;
        let property = with_keymap_description_buffer(interp, buffer, |interp| {
            Ok(keymap_at_active_position(interp, None).unwrap_or(Value::Nil))
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
                let normal = interp
                    .lookup_var("current-local-map", env)
                    .unwrap_or(Value::Nil);
                let local = local_map_at_active_position(interp, None).unwrap_or(normal);
                let major = interp.lookup_var("major-mode", env).unwrap_or(Value::Nil);
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
            "local-function-key-map",
            "\x0c\nFunction key map translations",
        ),
        ("input-decode-map", "\x0c\nInput decoding map translations"),
    ] {
        let map = interp.lookup_var(variable, env).unwrap_or(Value::Nil);
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

pub(crate) fn reader_control_char(base: i64) -> Option<i64> {
    match base {
        0x20 | 0x40 => Some(0),
        0x3f => Some(0x7f),
        value if (i64::from(b'a')..=i64::from(b'z')).contains(&value) => Some(value - 0x60),
        value if (i64::from(b'A')..=i64::from(b'_')).contains(&value) => Some(value - 0x40),
        _ => None,
    }
}

pub(crate) fn reader_key_event_value(event: Value) -> Value {
    let Kind::Integer(code) = event.kind() else {
        return event;
    };
    let base = code & !KEY_DESCRIPTION_MODIFIER_MASK;
    let modifiers = code & KEY_DESCRIPTION_MODIFIER_MASK;
    if modifiers & KEY_DESCRIPTION_CTRL_BIT == 0 {
        return Value::Integer(code);
    }

    let other_modifiers = modifiers & !KEY_DESCRIPTION_CTRL_BIT;
    let Some(control) = reader_control_char(base) else {
        return Value::Integer(code);
    };
    Value::Integer(control | other_modifiers)
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
    let restore = interp.bind_special_variable("standard-output", output_buffer, env)?;
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
                    if interp.get_symbol_property(&symbol, "suppress-keymap")
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
            interp.insert_current_buffer("\n");
            first = false;
        }
        insert_described_key_of(interp, starting_i, &prefix, env)?;
        if table.is_none() {
            while i + 1 < stop {
                let next = keymap_get_keyelt(interp, &element(i + 1), false, env)?;
                if next.is_nil() || !values_equal(interp, &next, &definition) {
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
                if !values_equal(interp, &tem, &shadowed_by) {
                    i = j - 1;
                }
                j += 1;
            }
        }
        if i != starting_i {
            interp.insert_current_buffer(" .. ");
            insert_described_key_of(interp, i, &prefix, env)?;
        }
        call_function_value(interp, &describer, &[definition], env)?;
        if this_shadowed {
            let point = interp.buffer.borrow().point();
            interp.buffer.borrow_mut().goto_char(point - 1);
            if let Kind::Symbol(command) = shadowed_by.kind() {
                interp.insert_current_buffer(&format!("  (currently shadowed by `{command}')"));
            } else {
                interp.insert_current_buffer("  (currently shadowed)");
            }
            let point = interp.buffer.borrow().point();
            interp.buffer.borrow_mut().goto_char(point + 1);
        }
        i += 1;
    }
    if let Some(table) = table
        && !table.default().is_nil()
    {
        interp.insert_current_buffer("default");
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

/// Return GNU's current minor-mode stack as `(mode-variable, keymap)` pairs.
///
/// This ordering and replacement policy is shared by command lookup,
/// `current-active-maps`, `current-minor-mode-maps`, and
/// `minor-mode-key-binding`: emulation maps come first, overriding maps
/// replace same-mode entries in the ordinary alist, and prefix-command
/// symbols are resolved through their function cells.
pub(crate) fn active_minor_mode_bindings(
    interp: &Interpreter,
    env: &Env,
) -> Result<Vec<(String, Value)>, LispError> {
    let overriding = interp
        .lookup_var("minor-mode-overriding-map-alist", env)
        .unwrap_or(Value::Nil);
    let overridden_modes = overriding
        .to_vec()
        .unwrap_or_default()
        .into_iter()
        .filter_map(|entry| {
            let (mode, _) = entry.cons_values()?;
            match mode.kind() {
                Kind::Symbol(name) => Some(name.as_str().to_owned()),
                _ => None,
            }
        })
        .collect::<HashSet<_>>();

    let mut alists = Vec::new();
    if let Some(emulation_alists) = interp.lookup_var("emulation-mode-map-alists", env) {
        for element in emulation_alists.to_vec().unwrap_or_default() {
            alists.push(match element.kind() {
                Kind::Symbol(variable) => interp.lookup_var(&variable, env).unwrap_or(Value::Nil),
                other => other.value(),
            });
        }
    }
    alists.push(overriding);
    alists.push(
        interp
            .lookup_var("minor-mode-map-alist", env)
            .unwrap_or(Value::Nil),
    );

    let ordinary_index = alists.len().saturating_sub(1);
    let mut bindings = Vec::new();
    for (index, alist) in alists.into_iter().enumerate() {
        for entry in alist.to_vec().unwrap_or_default() {
            let Some((mode, map)) = entry.cons_values() else {
                continue;
            };
            let Kind::Symbol(mode_name) = mode.kind() else {
                continue;
            };
            if index == ordinary_index && overridden_modes.contains(mode_name.as_str()) {
                continue;
            }
            if !interp
                .lookup_var(&mode_name, env)
                .is_some_and(|value| value.is_truthy())
            {
                continue;
            }
            if let Some(map) = keymap_reference_map(interp, &map, env) {
                bindings.push((mode_name.to_string(), map));
            }
        }
    }
    Ok(bindings)
}

pub(crate) fn active_minor_mode_maps(
    interp: &Interpreter,
    env: &Env,
) -> Result<Vec<Value>, LispError> {
    Ok(active_minor_mode_bindings(interp, env)?
        .into_iter()
        .map(|(_, map)| map)
        .collect())
}

pub(crate) fn keymap_at_active_position(
    interp: &Interpreter,
    posn: Option<&Value>,
) -> Option<Value> {
    text_property_keymap_at_active_position(interp, posn, "keymap")
}

/// The map a `local-map' text property puts in place of the buffer's local
/// map (keymap.c:1707 reads it through get_local_map exactly where the
/// `keymap' property is read; both ride the same position rules).
pub(crate) fn local_map_at_active_position(
    interp: &Interpreter,
    posn: Option<&Value>,
) -> Option<Value> {
    text_property_keymap_at_active_position(interp, posn, "local-map")
}

fn text_property_keymap_at_active_position(
    interp: &Interpreter,
    posn: Option<&Value>,
    category: &str,
) -> Option<Value> {
    let buffer_keymap = |pos: usize| {
        buffer_property_at_with_category(interp, &interp.buffer.borrow(), pos, category)
            .filter(|value| is_keymap_value(interp, value))
    };

    let string_keymap = |value: &Value| {
        string_property_at_with_category(interp, value, 0, category)
            .filter(|value| is_keymap_value(interp, value))
    };

    match posn {
        None => buffer_keymap(interp.buffer.borrow().point()),
        Some(value) if value.is_nil() => buffer_keymap(interp.buffer.borrow().point()),
        Some(value) if string_like(value).is_some() => {
            string_keymap(value).or_else(|| buffer_keymap(interp.buffer.borrow().point()))
        }
        Some(value) => {
            let items = value.to_vec().ok()?;
            let area = items.get(1)?;
            let string = items.get(4).and_then(|value| value.car_if_list());
            if let Some(string) = string.as_ref().and_then(string_keymap) {
                return Some(string);
            }
            if !area.is_nil() {
                return None;
            }
            match items.get(5).map(|v| v.kind()) {
                Some(Kind::Integer(pos)) if pos > 0 => buffer_keymap(pos as usize),
                _ => buffer_keymap(interp.buffer.borrow().point()),
            }
        }
    }
}

/// keymap.c:1657 Fcurrent_active_maps: the active keymaps in precedence
/// order, global map last.  OLP says whether the overriding maps apply.
/// When it does, `overriding-local-map' -- consulted only while
/// `overriding-terminal-local-map' is nil ("it seems clearer to use just
/// one", keymap.c:1695) -- REPLACES the keymap-property, minor-mode and
/// local maps entirely; `overriding-terminal-local-map' replaces nothing
/// and rides on TOP of all of them.  A `local-map' text property stands in
/// for the buffer's local map (get_local_map).  subr.el's
/// `add-keymap-witness' marker is an inert keymap element here: keymap.c
/// never consults it.  (Finding 109: dispatch used to suppress local and
/// minor maps under overriding-terminal-local-map unless the witness was
/// present -- a rule GNU does not have in either direction.)
pub(crate) fn current_active_maps(
    interp: &Interpreter,
    env: &Env,
    olp: bool,
    posn: Option<&Value>,
) -> Result<Vec<Value>, LispError> {
    let mut maps = active_command_keymaps_for(interp, env, olp, posn)?;
    let global_map = interp.current_global_map_value();
    if is_keymap_value(interp, &global_map) {
        maps.push(global_map);
    }
    Ok(maps)
}

/// `current_active_maps' without the trailing global map, for the callers
/// that consult the global map through their own fallback path.
pub(crate) fn active_command_keymaps_for(
    interp: &Interpreter,
    env: &Env,
    olp: bool,
    posn: Option<&Value>,
) -> Result<Vec<Value>, LispError> {
    let mut maps = Vec::new();
    let otlp = interp
        .lookup_var("overriding-terminal-local-map", env)
        .filter(Value::is_truthy);
    if olp
        && otlp.is_none()
        && let Some(map) = interp
            .lookup_var("overriding-local-map", env)
            .filter(Value::is_truthy)
    {
        if is_keymap_value(interp, &map) {
            maps.push(map);
        }
        return Ok(maps);
    }
    if olp
        && let Some(map) = otlp
        && is_keymap_value(interp, &map)
    {
        maps.push(map);
    }
    if let Some(map) = keymap_at_active_position(interp, posn) {
        maps.push(map);
    }
    maps.extend(active_minor_mode_maps(interp, env)?);
    if let Some(map) = local_map_at_active_position(interp, posn) {
        maps.push(map);
    } else if let Some(map) = interp
        .lookup_var("current-local-map", env)
        .filter(Value::is_truthy)
        && is_keymap_value(interp, &map)
    {
        maps.push(map);
    }
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

/// GNU `doc.c:Ftext_quoting_style' collapses the Lisp variable to one of
/// three effective styles.  A nil variable consults
/// `default_to_grave_quoting_style' (doc.c:653), whose first test is the
/// locale flag above.
///
/// If the flag is set, the second test reads `standard-display-table' and
/// answers grave when U+2018 displays as a one-element vector holding ?`.
/// This plain Lisp-variable read is observable in batch as well as in the
/// interactive startup path that forces the locale flag to t.
pub(crate) fn effective_text_quoting_style(interp: &Interpreter, env: &Env) -> &'static str {
    // doc.c reads the C variable Vtext_quoting_style itself, which a
    // `makunbound' of the symbol leaves untouched.
    match interp
        .forwarded_c_value("text-quoting-style", env)
        .map(|v| v.kind())
    {
        Some(Kind::Symbol(style)) if style == "grave" => "grave",
        Some(Kind::Symbol(style)) if style == "straight" => "straight",
        Some(Kind::Symbol(style)) if style == "curve" => "curve",
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
