use super::*;
use crate::lisp::types::Kind;
use crate::lisp::types::obarray::{self, Buckets};

pub(crate) const OBARRAY_RECORD_TYPE: &str = "obarray";

pub(crate) fn ensure_interaction_allowed(interp: &Interpreter, env: &Env) -> Result<(), LispError> {
    if !interaction_allowed(interp, env) {
        // minibuf.c:barf_if_interaction_inhibited, xsignal0.
        return Err(LispError::SignalValue(Value::list([Value::Symbol(
            "inhibited-interaction".into(),
        )])));
    }
    Ok(())
}

pub(crate) fn interaction_allowed(interp: &Interpreter, env: &Env) -> bool {
    !interp
        .lookup_var_key(cached_symbol!("inhibit-interaction"), env)
        .is_some_and(|value| value.is_truthy())
}

pub(crate) fn is_window_value(interp: &Interpreter, value: &Value) -> bool {
    crate::lisp::types::eq_symbol_named(value, "window")
        || matches!(value.kind(), Kind::Record(id) if interp.find_record(id).is_some_and(|record|
            record.kind == crate::lisp::eval::RecordKind::Window))
}

#[cfg(test)]
pub(crate) fn make_obarray(interp: &mut Interpreter) -> Value {
    make_obarray_with_bits(interp, obarray::DEFAULT_BITS)
}

/// lread.c:make_obarray: an obarray of 2^BITS empty buckets.
pub(crate) fn make_obarray_with_bits(interp: &mut Interpreter, bits: u32) -> Value {
    interp.create_pseudovector(
        crate::lisp::eval::RecordKind::Obarray,
        OBARRAY_RECORD_TYPE,
        obarray::record_slots(bits),
    )
}

/// The bucket slots of the private obarray record ID.
fn private_obarray_slots(record: &crate::lisp::eval::RecordState) -> Option<&[Value]> {
    (record.has_symbol_type(OBARRAY_RECORD_TYPE) && obarray::is_record_slots(&record.slots))
        .then_some(record.slots.as_slice())
}

pub(crate) fn clear_obarray(interp: &mut Interpreter, obarray: &Value) -> Result<Value, LispError> {
    let Kind::Record(id) = obarray.kind() else {
        return Err(LispError::WrongTypeArgument("obarrayp".into(), *obarray));
    };
    let Some(record) = interp.find_record_mut(id) else {
        return Err(LispError::WrongTypeArgument("obarrayp".into(), *obarray));
    };
    if record.has_symbol_type(OBARRAY_RECORD_TYPE) {
        // Fobarray_clear.  Not GNU: the initial obarray keeps its symbols,
        // which this implementation's own code names by their text.
        if obarray::is_record_slots(&record.slots) {
            obarray::RecordBuckets(&mut record.slots).clear();
        }
    } else {
        return Err(LispError::WrongTypeArgument("obarrayp".into(), *obarray));
    }
    Ok(Value::Nil)
}

pub(crate) fn is_obarray_like_value(interp: &Interpreter, value: &Value) -> bool {
    let Kind::Record(id) = value.kind() else {
        return false;
    };
    interp
        .find_record(id)
        .is_some_and(|record| record.kind == crate::lisp::eval::RecordKind::Obarray)
}

pub(crate) fn obarray_symbols(
    interp: &Interpreter,
    obarray: &Value,
) -> Result<Vec<Value>, LispError> {
    // Read-only face of check_obarray_slow's legacy-vector rule: a vector
    // already carrying an obarray in slot 0 reads through it, and an
    // untouched one (slot 0 still the fixnum 0) reads as empty.
    if is_vector_value(obarray) {
        let slots = vector_items(obarray)?;
        if let Some(current) = slots.first() {
            if is_obarray_like_value(interp, current) {
                return obarray_symbols(interp, current);
            }
            if matches!(current.kind(), Kind::Integer(0)) {
                return Ok(Vec::new());
            }
        }
    }
    let Kind::Record(id) = obarray.kind() else {
        return Err(LispError::WrongTypeArgument("obarrayp".into(), *obarray));
    };
    if interp.is_standard_obarray_id(id.id) {
        return Ok(interp
            .known_symbols_shared()
            .iter()
            .cloned()
            .map(|symbol| match symbol.as_str() {
                "nil" => Value::Nil,
                "t" => Value::T,
                _ => Value::Symbol(symbol),
            })
            .collect());
    }
    let Some(record) = interp.find_record(id) else {
        return Err(LispError::WrongTypeArgument("obarrayp".into(), *obarray));
    };
    if let Some(slots) = private_obarray_slots(record) {
        // DOOBARRAY's walk.
        return Ok(obarray::RecordBucketsRef(slots)
            .symbols()
            .into_iter()
            .map(Value::Symbol)
            .collect());
    }
    Err(LispError::WrongTypeArgument("obarrayp".into(), *obarray))
}

pub(crate) fn obarray_symbol_matches(value: &Value, symbol_name: &str) -> bool {
    matches!(
        (value.kind(), symbol_name),
        (Kind::Nil, "nil") | (Kind::T, "t")
    ) || matches!(
        value.kind(),
        Kind::Symbol(name) if crate::lisp::types::visible_symbol_name(name.as_str()) == symbol_name
    )
}

/// Finternal__obarray_buckets: each bucket's symbols in chain order.
pub(crate) fn obarray_bucket_lists(
    interp: &mut Interpreter,
    obarray: &Value,
) -> Result<Value, LispError> {
    let obarray = &coerce_legacy_vector_obarray(interp, obarray)?;
    let Kind::Record(id) = obarray.kind() else {
        return Err(LispError::WrongTypeArgument("obarrayp".into(), *obarray));
    };
    let lists = if interp.is_standard_obarray_id(id.id) {
        interp.standard_obarray_bucket_lists()
    } else {
        let Some(record) = interp.find_record(id) else {
            return Err(LispError::WrongTypeArgument("obarrayp".into(), *obarray));
        };
        let Some(slots) = private_obarray_slots(record) else {
            return Err(LispError::WrongTypeArgument("obarrayp".into(), *obarray));
        };
        obarray::RecordBucketsRef(slots).bucket_lists()
    };
    Ok(Value::list(lists.into_iter().map(|bucket| {
        Value::list(bucket.into_iter().map(Value::Symbol))
    })))
}

/// lread.c check_obarray_slow: a legacy VECTOR obarray whose first slot
/// is the fixnum 0 receives a real obarray object stored there on first
/// use (the rest of the vector stays unused), and a vector already
/// carrying one answers with it.  Everything else of vector shape is
/// still not an obarray.
pub(crate) fn coerce_legacy_vector_obarray(
    interp: &mut Interpreter,
    obarray: &Value,
) -> Result<Value, LispError> {
    if !is_vector_value(obarray) {
        return Ok(*obarray);
    }
    let slots = vector_items(obarray)?;
    let Some(current) = slots.first().cloned() else {
        return Err(LispError::WrongTypeArgument("obarrayp".into(), *obarray));
    };
    if is_obarray_like_value(interp, &current) {
        return Ok(current);
    }
    if matches!(current.kind(), Kind::Integer(0)) {
        // make_obarray (0): one bucket.
        let fresh = make_obarray_with_bits(interp, 0);
        aset_vector_value(obarray, 0, fresh)?;
        return Ok(fresh);
    }
    Err(LispError::WrongTypeArgument("obarrayp".into(), *obarray))
}

pub(crate) fn intern_in_obarray(
    interp: &mut Interpreter,
    obarray: &Value,
    symbol_name: &str,
) -> Result<Value, LispError> {
    // check_obarray's common case, the initial obarray itself: oblookup,
    // and on a miss intern_driver with the name made only then.
    if let Kind::Record(id) = obarray.kind()
        && interp.is_standard_obarray_id(id.id)
    {
        return Ok(Value::Symbol(
            crate::lisp::types::SymbolName::intern_initial_with(symbol_name, || {
                Value::string(symbol_name)
            }),
        ));
    }
    intern_in_obarray_with_name(interp, obarray, symbol_name, None, |_| {
        Ok(Value::string(symbol_name))
    })
}

/// lread.c read0's intern of a symbol token NAME read from a source whose
/// characters readchar reported as MULTIBYTE (READCHAR_REPORT_MULTIBYTE):
/// oblookup by the token's bytes and characters, then on a miss
/// intern_driver with make_specified_string's name of that multibyteness.
/// A multibyte source makes even an ASCII name multibyte; a unibyte one
/// keeps each character as one byte, raw bytes included.
pub(crate) fn intern_read_symbol(
    interp: &mut Interpreter,
    obarray: &Value,
    symbol_name: &str,
    multibyte: bool,
) -> Result<Value, LispError> {
    if multibyte != symbol_name.is_ascii() {
        // An ASCII name from a unibyte source, or a non-ASCII one from a
        // multibyte source: the name made from the text is that string.
        return intern_in_obarray(interp, obarray, symbol_name);
    }
    let read_name = || {
        let bytes = crate::lisp::types::string_data::encode_text(symbol_name, multibyte, &[])?;
        super::strings::make_specified_string(bytes, multibyte)
            .map_err(|error| LispError::Signal(error.to_string()))
    };
    if multibyte {
        // ASCII: the lookup by the text is oblookup's, and the name is
        // made only for a new symbol.
        if let Kind::Record(id) = obarray.kind()
            && interp.is_standard_obarray_id(id.id)
        {
            let mut made = None;
            let symbol =
                crate::lisp::types::SymbolName::intern_initial_with(
                    symbol_name,
                    || match read_name() {
                        Ok(name) => name,
                        Err(error) => {
                            made = Some(error);
                            Value::string(symbol_name)
                        }
                    },
                );
            if let Some(error) = made {
                return Err(error);
            }
            return Ok(Value::Symbol(symbol));
        }
        return intern_in_obarray_with_name(interp, obarray, symbol_name, None, |_| read_name());
    }
    // Raw bytes from a unibyte source: oblookup compares the name's own
    // bytes, which are not the text's.
    let name = read_name()?;
    if let Kind::Record(id) = obarray.kind()
        && interp.is_standard_obarray_id(id.id)
    {
        return Ok(Value::Symbol(
            crate::lisp::types::SymbolName::intern_with_lisp_name(
                symbol_name.to_owned(),
                Some(name),
            ),
        ));
    }
    intern_in_obarray_with_name(interp, obarray, symbol_name, Some(name), |_| Ok(name))
}

/// lread.c:intern_driver is reached only after oblookup misses. In particular,
/// Fintern must not allocate/purecopy a name string when a symbol already exists.
/// KEY, a Lisp string, is what oblookup compares (its SDATA and SCHARS),
/// which for a unibyte name or one with raw bytes are not SYMBOL_NAME's
/// text; without it the text is the key.
pub(crate) fn intern_in_obarray_with_name(
    interp: &mut Interpreter,
    obarray: &Value,
    symbol_name: &str,
    key: Option<Value>,
    make_name: impl FnOnce(&mut Interpreter) -> Result<Value, LispError>,
) -> Result<Value, LispError> {
    let obarray = &coerce_legacy_vector_obarray(interp, obarray)?;
    let Kind::Record(id) = obarray.kind() else {
        return Err(LispError::WrongTypeArgument("obarrayp".into(), *obarray));
    };
    if interp.is_standard_obarray_id(id.id) {
        // oblookup's hit is the bucket's symbol; intern_driver makes one
        // for a miss.
        let found = match key {
            Some(key) => crate::lisp::types::SymbolName::lookup_initial(key).and_then(Result::ok),
            None => interp.standard_obarray_symbol(symbol_name),
        };
        if let Some(symbol) = found {
            return Ok(Value::Symbol(symbol));
        }
        let name = make_name(interp)?;
        return Ok(Value::Symbol(
            crate::lisp::types::SymbolName::intern_with_lisp_name(
                symbol_name.to_owned(),
                Some(name),
            ),
        ));
    }
    let Some(record) = interp.find_record_mut(id) else {
        return Err(LispError::WrongTypeArgument("obarrayp".into(), *obarray));
    };
    let Some(slots) = private_obarray_slots(record) else {
        return Err(LispError::WrongTypeArgument("obarrayp".into(), *obarray));
    };
    // oblookup, then intern_driver for a miss.
    let found = match key {
        Some(key) => obarray::RecordBucketsRef(slots).lookup_name(key),
        None => {
            let (bytes, chars) = obarray::text_key(symbol_name);
            Some(obarray::RecordBucketsRef(slots).lookup(bytes, chars))
        }
    };
    if let Some(Ok(existing)) = found {
        return Ok(Value::Symbol(existing));
    }
    let name = make_name(interp)?;
    let symbol = crate::lisp::types::SymbolName::make_private(
        crate::lisp::types::make_fresh_obarray_symbol_name(symbol_name, id.id),
        name,
    );
    let record = interp.find_record_mut(id).expect("validated obarray");
    let mut buckets = obarray::RecordBuckets(&mut record.slots);
    // intern_sym files it in the bucket of the name oblookup compared.
    let index = obarray::name_bucket(symbol, buckets.size_bits());
    buckets.insert(symbol, index);
    Ok(Value::Symbol(symbol))
}

/// Fintern_soft's oblookup of SYMBOL_NAME, by KEY's bytes and characters
/// when it is given (as `intern_in_obarray_with_name').
pub(crate) fn intern_soft_in_obarray(
    interp: &Interpreter,
    obarray: &Value,
    symbol_name: &str,
    key: Option<Value>,
) -> Result<Value, LispError> {
    if matches!(obarray.kind(), Kind::Record(id) if interp.is_standard_obarray_id(id.id)) {
        let found = match key {
            Some(key) => crate::lisp::types::SymbolName::lookup_initial(key).and_then(Result::ok),
            None => interp.standard_obarray_symbol(symbol_name),
        };
        return Ok(found.map_or(Value::Nil, Value::Symbol));
    }
    if let Kind::Record(id) = obarray.kind()
        && let Some(slots) = interp.find_record(id).and_then(private_obarray_slots)
    {
        // oblookup.
        let found = match key {
            Some(key) => obarray::RecordBucketsRef(slots).lookup_name(key),
            None => {
                let (bytes, chars) = obarray::text_key(symbol_name);
                Some(obarray::RecordBucketsRef(slots).lookup(bytes, chars))
            }
        };
        return Ok(match found {
            Some(Ok(symbol)) => Value::Symbol(symbol),
            _ => Value::Nil,
        });
    }
    // A legacy vector, by its symbols.
    Ok(obarray_symbols(interp, obarray)?
        .into_iter()
        .find(|value| obarray_symbol_matches(value, symbol_name))
        .unwrap_or(Value::Nil))
}

pub(crate) fn unintern_from_obarray(
    interp: &mut Interpreter,
    obarray: &Value,
    target: &Value,
    env: &Env,
) -> Result<bool, LispError> {
    let obarray = &coerce_legacy_vector_obarray(interp, obarray)?;
    let Kind::Record(id) = obarray.kind() else {
        return Err(LispError::WrongTypeArgument("obarrayp".into(), *obarray));
    };
    // Funintern: a symbol is looked up by its own name and only it is
    // removed; a string, after its shorthand, names any symbol.
    let (name, only) = match checked_symbol_identity(interp, target, env) {
        Ok(symbol) => (symbol.lisp_name(), Some(symbol)),
        Err(_) => {
            let text = string_text(target)?;
            let longhand = apply_symbol_shorthands_in_env(interp, &text, env)?;
            if longhand == text {
                (*target, None)
            } else {
                (Value::string(&longhand), None)
            }
        }
    };
    if interp.is_standard_obarray_id(id.id) {
        return Ok(interp.unintern_standard_symbol(name, only));
    }
    let Some(record) = interp.find_record_mut(id) else {
        return Err(LispError::WrongTypeArgument("obarrayp".into(), *obarray));
    };
    if !(record.has_symbol_type(OBARRAY_RECORD_TYPE) && obarray::is_record_slots(&record.slots)) {
        return Err(LispError::WrongTypeArgument("obarrayp".into(), *obarray));
    }
    let mut buckets = obarray::RecordBuckets(&mut record.slots);
    let Some(Ok(symbol)) = buckets.lookup_name(name) else {
        return Ok(false);
    };
    if only.is_some_and(|only| only != symbol) {
        return Ok(false);
    }
    let index = obarray::name_bucket(symbol, buckets.size_bits());
    buckets.remove(symbol, index);
    symbol.cell().mark_uninterned();
    Ok(true)
}

pub(crate) fn values_eq_for_substitution(left: &Value, right: &Value) -> bool {
    match (left.kind(), right.kind()) {
        (Kind::Nil, Kind::Nil) | (Kind::T, Kind::T) => true,
        (Kind::Integer(a), Kind::Integer(b)) => a == b,
        (Kind::BigInteger(a), Kind::BigInteger(b)) => a == b,
        (Kind::Integer(a), Kind::BigInteger(b)) | (Kind::BigInteger(b), Kind::Integer(a)) => {
            BigInt::from(a) == *b
        }
        // Representation equality, like eq/eql (see values_eq_in_env).
        (Kind::Float(a), Kind::Float(b)) => a.to_bits() == b.to_bits(),
        (Kind::Symbol(a), Kind::Symbol(b)) => a == b,
        (Kind::BuiltinFunc(a), Kind::BuiltinFunc(b)) => a == b,
        (Kind::NativeFunction(a), Kind::NativeFunction(b)) => a.ptr_eq(&b),
        (Kind::StringObject(left), Kind::StringObject(right)) => left.ptr_eq(&right),
        (Kind::Cons(left), Kind::Cons(right)) => {
            crate::lisp::types::SharedCons::ptr_eq(&left, &right)
        }
        (Kind::Vector(left), Kind::Vector(right)) => left.ptr_eq(&right),
        (Kind::Closure(left), Kind::Closure(right)) => left.ptr_eq(&right),
        (Kind::Buffer(left), Kind::Buffer(right)) => left.ptr_eq(&right),
        (Kind::Marker(left_id), Kind::Marker(right_id)) => left_id == right_id,
        (Kind::Overlay(left_id), Kind::Overlay(right_id)) => left_id.ptr_eq(&right_id),
        (Kind::CharTable(left_id), Kind::CharTable(right_id)) => left_id == right_id,
        (Kind::Finalizer(left_id), Kind::Finalizer(right_id)) => left_id == right_id,
        (Kind::Record(left), Kind::Record(right)) => left.ptr_eq(&right),
        (Kind::LispRecord(left), Kind::LispRecord(right)) => left.ptr_eq(&right),
        _ => false,
    }
}

pub(crate) fn substitution_visit_key(value: &Value) -> Option<(u8, usize)> {
    match value.kind() {
        Kind::Cons(cell) => Some((0, crate::lisp::types::ConsCell::identity(&cell))),
        Kind::Vector(vector) => Some((4, vector.identity())),
        Kind::StringObject(state) => Some((1, state.identity())),
        Kind::Record(id) => Some((2, id.identity())),
        Kind::LispRecord(record) => Some((4, record.identity())),
        Kind::CharTable(id) => Some((3, id.identity())),
        Kind::SubCharTable(id) => Some((5, id.identity())),
        _ => None,
    }
}

pub(crate) fn substitute_object_recurse(
    interp: &mut Interpreter,
    object: &Value,
    placeholder: &Value,
    subtree: &Value,
    seen: &mut HashSet<(u8, usize)>,
) -> Result<Value, LispError> {
    if values_eq_for_substitution(subtree, placeholder) {
        return Ok(*object);
    }

    // lread.c:substitute_object_recurse cannot traverse Lisp_Subr's C fields.
    // EQ replacement happens first; a different subr signals sequencep.
    if matches!(
        subtree.kind(),
        Kind::BuiltinFunc(_) | Kind::NativeFunction(_)
    ) {
        return Err(wrong_type_argument("sequencep", *subtree));
    }

    let Some(key) = substitution_visit_key(subtree) else {
        return Ok(*subtree);
    };
    if !seen.insert(key) {
        return Ok(*subtree);
    }

    match subtree.kind() {
        Kind::Vector(vector) => {
            let slot_count = vector.len();
            for index in 0..slot_count {
                let current = vector.get(index).expect("in-range vector index");
                let updated =
                    substitute_object_recurse(interp, object, placeholder, &current, seen)?;
                vector.set(index, updated);
            }
            Ok(*subtree)
        }
        Kind::Cons(_) => {
            let Some((car, cdr)) = subtree.cons_values() else {
                return Ok(*subtree);
            };
            let Some((car_slot, cdr_slot)) = subtree.cons_cells() else {
                return Ok(*subtree);
            };
            car_slot.set(substitute_object_recurse(
                interp,
                object,
                placeholder,
                &car,
                seen,
            )?);
            cdr_slot.set(substitute_object_recurse(
                interp,
                object,
                placeholder,
                &cdr,
                seen,
            )?);
            Ok(*subtree)
        }
        Kind::StringObject(state) => {
            // substitute_in_interval: each interval's plist.
            let intervals = state.borrow_mut().props.take();
            if let Some(mut intervals) = intervals {
                let mut failure = None;
                intervals.rewrite_plists(|plist| {
                    match substitute_object_recurse(interp, object, placeholder, &plist, seen) {
                        Ok(value) => value,
                        Err(error) => {
                            failure.get_or_insert(error);
                            plist
                        }
                    }
                });
                state.borrow_mut().props.set(Some(intervals));
                if let Some(error) = failure {
                    return Err(error);
                }
            }
            Ok(*subtree)
        }
        Kind::LispRecord(record) => {
            for index in 0..record.len() {
                let current = record.get(index).expect("in-range record field");
                let updated =
                    substitute_object_recurse(interp, object, placeholder, &current, seen)?;
                record.set(index, updated);
            }
            Ok(*subtree)
        }
        Kind::Record(id) => {
            let slot_count = interp
                .find_record(id)
                .map(|record| record.slots.len())
                .unwrap_or(0);
            for index in 0..slot_count {
                let current = interp
                    .find_record(id)
                    .and_then(|record| record.slots.get(index).cloned())
                    .unwrap_or(Value::Nil);
                let updated =
                    substitute_object_recurse(interp, object, placeholder, &current, seen)?;
                if let Some(record) = interp.find_record_mut(id)
                    && let Some(slot) = record.slots.get_mut(index)
                {
                    *slot = updated;
                }
            }
            Ok(*subtree)
        }
        Kind::CharTable(table) => {
            for (index, value) in table.slots().enumerate() {
                table.set_slot(
                    index,
                    substitute_object_recurse(interp, object, placeholder, &value, seen)?,
                );
            }
            Ok(*subtree)
        }
        Kind::SubCharTable(table) => {
            for (index, value) in table.slots().enumerate() {
                table.set_slot(
                    index,
                    substitute_object_recurse(interp, object, placeholder, &value, seen)?,
                );
            }
            Ok(*subtree)
        }
        _ => Ok(*subtree),
    }
}

pub(crate) fn substitute_object_in_subtree(
    interp: &mut Interpreter,
    object: &Value,
    placeholder: &Value,
    _completed: &Value,
) -> Result<(), LispError> {
    let _ = substitute_object_recurse(interp, object, placeholder, object, &mut HashSet::new())?;
    Ok(())
}

pub(crate) fn completion_ignores_case(interp: &Interpreter, env: &Env) -> bool {
    interp
        .lookup_var_key(cached_symbol!("completion-ignore-case"), env)
        .is_some_and(|value| value.is_truthy())
}

/// minibuf.c's match_regexps for one regexp of `completion-regexp-list':
/// whether PATTERN matches CANDIDATE, folding case with CASE_TABLE's
/// canonicalization when given.
pub(crate) fn completion_regex_matches(
    interp: &mut Interpreter,
    env: &mut Env,
    candidate: &str,
    pattern: &Value,
    case_table: Option<crate::lisp::types::CharTableRef>,
) -> Result<bool, LispError> {
    if !pattern.is_string() {
        return Err(LispError::WrongTypeArgument("stringp".into(), *pattern));
    }
    Ok(
        search::fast_text_match(interp, env, pattern, candidate, &[], true, case_table, true)?
            .is_some(),
    )
}

/// minibuf.c's table type for COLLECTION: a hash table, an obarray (a
/// vector goes through check_obarray), nil or a cons that is not a
/// function as a list, and anything else as a function to call.
#[derive(Clone, Copy, PartialEq, Eq)]
enum CompletionTable {
    Function,
    List,
    Obarray,
    Hash,
}

fn completion_table_type(interp: &Interpreter, collection: &Value, env: &Env) -> CompletionTable {
    if matches!(collection.kind(), Kind::HashTable(_)) {
        CompletionTable::Hash
    } else if is_vector_value(collection) || is_obarray_like_value(interp, collection) {
        CompletionTable::Obarray
    } else if collection.is_nil()
        || (collection.is_cons() && !function_value_p(interp, collection, env))
    {
        CompletionTable::List
    } else {
        CompletionTable::Function
    }
}

/// call3 (collection, string, predicate, ACTION) for a function table.
fn call_completion_table(
    interp: &mut Interpreter,
    string: Value,
    collection: Value,
    predicate: Option<&Value>,
    action: Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    interp.call_function_value(
        collection,
        None,
        &[string, predicate.copied().unwrap_or(Value::Nil), action],
        env,
    )
}

/// minibuf.c's walk over a COLLECTION that is not a function: the
/// elements of a list, the symbols of an obarray, or a hash table's keys
/// with their values.  What the walk holds is rooted across the
/// predicate's calls.
struct CompletionElements {
    kind: CompletionElementsKind,
    /// List: the tail and the tortoise of the cycle check; obarray: its
    /// symbols; hash table: its keys and values, in pairs.
    roots: crate::lisp::alloc::RootedVec<Value>,
    index: usize,
    steps: usize,
    limit: usize,
}

enum CompletionElementsKind {
    List,
    StandardObarray(std::rc::Rc<Vec<crate::lisp::types::SymbolName>>),
    Obarray,
    Hash,
}

/// One possible completion: the element the predicate is given, the name
/// it offers (a symbol's name object), and a hash table's value.
struct CompletionElement {
    elt: Value,
    eltstring: Value,
    hash_value: Option<Value>,
}

/// Fsymbol_name for an element: the symbol's own name object.
fn completion_element_name(value: Value) -> Value {
    match value.kind() {
        Kind::Nil => crate::lisp::types::symbols::builtin_boolean(false).lisp_name(),
        Kind::T => crate::lisp::types::symbols::builtin_boolean(true).lisp_name(),
        Kind::Symbol(symbol) => symbol.lisp_name(),
        _ => value,
    }
}

impl CompletionElements {
    fn new(interp: &Interpreter, collection: &Value) -> Result<Self, LispError> {
        let (kind, roots) = if let Some((_, entries)) = json::hash_table_entries(interp, collection)
        {
            let mut roots = Vec::with_capacity(entries.len() * 2);
            for (key, value) in entries {
                roots.push(key);
                roots.push(value);
            }
            (CompletionElementsKind::Hash, roots)
        } else if matches!(collection.kind(), Kind::Record(id) if interp.is_standard_obarray_id(id.id))
        {
            (
                CompletionElementsKind::StandardObarray(interp.known_symbols_shared()),
                Vec::new(),
            )
        } else if is_vector_value(collection) || is_obarray_like_value(interp, collection) {
            (
                CompletionElementsKind::Obarray,
                obarray_symbols(interp, collection)?,
            )
        } else {
            (CompletionElementsKind::List, vec![*collection, *collection])
        };
        Ok(CompletionElements {
            kind,
            roots: crate::lisp::alloc::RootedVec::from_vec(roots),
            index: 0,
            steps: 0,
            limit: 2,
        })
    }

    fn next(&mut self) -> Result<Option<CompletionElement>, LispError> {
        match &self.kind {
            CompletionElementsKind::List => {
                let Some((elt, tail)) = self.roots[0].cons_values() else {
                    return Ok(None);
                };
                self.roots[0] = tail;
                // minibuf.c walks the list with no cycle check; a circular
                // one is signaled here instead of looping forever.
                if tail.eq_value(self.roots[1]) && tail.is_cons() {
                    return Err(LispError::SignalValue(Value::list([
                        Value::Symbol("circular-list".into()),
                        Value::String("Circular list".into()),
                    ])));
                }
                self.steps += 1;
                if self.steps == self.limit {
                    self.roots[1] = tail;
                    self.steps = 0;
                    self.limit *= 2;
                }
                let eltstring = match elt.cons_values() {
                    Some((key, _)) => key,
                    None => elt,
                };
                Ok(Some(CompletionElement {
                    elt,
                    eltstring: completion_element_name(eltstring),
                    hash_value: None,
                }))
            }
            CompletionElementsKind::StandardObarray(symbols) => {
                let Some(symbol) = symbols.get(self.index) else {
                    return Ok(None);
                };
                self.index += 1;
                let elt = match symbol.as_str() {
                    "nil" => Value::Nil,
                    "t" => Value::T,
                    _ => Value::Symbol(*symbol),
                };
                Ok(Some(CompletionElement {
                    elt,
                    eltstring: completion_element_name(elt),
                    hash_value: None,
                }))
            }
            CompletionElementsKind::Obarray => {
                let Some(&elt) = self.roots.get(self.index) else {
                    return Ok(None);
                };
                self.index += 1;
                Ok(Some(CompletionElement {
                    elt,
                    eltstring: completion_element_name(elt),
                    hash_value: None,
                }))
            }
            CompletionElementsKind::Hash => {
                let Some(&elt) = self.roots.get(self.index) else {
                    return Ok(None);
                };
                let value = self.roots[self.index + 1];
                self.index += 2;
                Ok(Some(CompletionElement {
                    elt,
                    eltstring: completion_element_name(elt),
                    hash_value: Some(value),
                }))
            }
        }
    }
}

/// SCHARS of a string value.
fn string_chars(value: &Value) -> usize {
    match value.kind() {
        Kind::StringObject(string) => string.borrow().len(),
        _ => 0,
    }
}

/// Fcompare_strings (A, 0, A_END, B, 0, B_END, IGNORE_CASE) with both
/// ends within their strings.
fn compare_prefixes(
    interp: &mut Interpreter,
    a: &Value,
    a_end: usize,
    b: &Value,
    b_end: usize,
    ignore_case: bool,
    env: &mut Env,
) -> Result<Value, LispError> {
    compare_string_ranges(interp, a, (0, a_end), b, (0, b_end), ignore_case, env)
}

/// minibuf.c match_regexps: whether STRING matches every regexp of
/// REGEXPS, folding case with the buffer's canonical table when asked.
fn match_regexps(
    interp: &mut Interpreter,
    env: &mut Env,
    string: &Value,
    regexps: Value,
    ignore_case: bool,
) -> Result<bool, LispError> {
    let mut tail = regexps;
    while let Some((regexp, next)) = tail.cons_values() {
        if !regexp.is_string() {
            return Err(LispError::WrongTypeArgument("stringp".into(), regexp));
        }
        let case_table = ignore_case.then(|| interp.current_case_tables().canon);
        if search::fast_string_match(interp, env, &regexp, string, case_table, true)?.is_none() {
            return Ok(false);
        }
        tail = next;
    }
    Ok(true)
}

/// The predicate test of Ftry_completion and Fall_completions: a hash
/// table's predicate also receives the value.
fn completion_predicate_accepts(
    interp: &mut Interpreter,
    predicate: &Value,
    element: &CompletionElement,
    env: &mut Env,
) -> Result<bool, LispError> {
    let predicate = resolve_callable(interp, predicate, env)?;
    let accepted = match element.hash_value {
        Some(value) => invoke_function_value(interp, &predicate, &[element.elt, value], env)?,
        None => invoke_function_value(interp, &predicate, &[element.elt], env)?,
    };
    Ok(accepted.is_truthy())
}

pub(crate) fn try_completion(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    if args.len() < 2 || args.len() > 4 {
        return Err(LispError::WrongNumberOfArgs(
            "try-completion".into(),
            args.len(),
        ));
    }
    // minibuf.c:Ftry_completion: check_obarray on a vector, then CHECK_STRING.
    let string = args[0];
    let elements = match completion_table_type(interp, &args[1], env) {
        CompletionTable::Function => None,
        _ => Some(CompletionElements::new(interp, &args[1])?),
    };
    if !string.is_string() {
        return Err(LispError::WrongTypeArgument("stringp".into(), string));
    }
    let Some(mut elements) = elements else {
        return call_completion_table(interp, string, args[1], args.get(2), Value::Nil, env);
    };
    let ignore_case = completion_ignores_case(interp, env);
    let regexps = interp
        .lookup_var_key(cached_symbol!("completion-regexp-list"), env)
        .unwrap_or(Value::Nil);
    let predicate = args.get(2).copied().filter(|value| !value.is_nil());
    let size = string_chars(&string);
    let mut best = crate::lisp::alloc::RootedVec::from_vec(vec![Value::Nil]);
    let mut bestmatchsize = 0usize;
    let mut matchcount = 0;
    while let Some(element) = elements.next()? {
        let eltstring = element.eltstring;
        if !eltstring.is_string()
            || size > string_chars(&eltstring)
            || !compare_prefixes(interp, &eltstring, size, &string, size, ignore_case, env)?
                .eq_value(Value::T)
        {
            continue;
        }
        // Ignore this element if it fails to match all the regexps.
        if !match_regexps(interp, env, &eltstring, regexps, ignore_case)? {
            continue;
        }
        // Ignore this element if there is a predicate and the predicate
        // doesn't like it.
        if let Some(predicate) = &predicate
            && !completion_predicate_accepts(interp, predicate, &element, env)?
        {
            continue;
        }
        // Update computation of how much all possible completions match.
        let eltsize = string_chars(&eltstring);
        if best[0].is_nil() {
            matchcount = 1;
            best[0] = eltstring;
            bestmatchsize = eltsize;
            continue;
        }
        let compare = bestmatchsize.min(eltsize);
        let bestmatch = best[0];
        let tem = compare_prefixes(
            interp,
            &bestmatch,
            compare,
            &eltstring,
            compare,
            ignore_case,
            env,
        )?;
        let matchsize = match tem.kind() {
            Kind::Integer(position) => position.unsigned_abs() as usize - 1,
            _ => compare,
        };
        if ignore_case {
            // If this is an exact match except for case, use it as the
            // best match rather than one that is not an exact match, to
            // get the case pattern of the actual match.  If there is more
            // than one exact match ignoring case and one of them is exact
            // including case, prefer that one; with no exact match
            // ignoring case, prefer a match that does not change the case
            // of the input.
            let best_size = string_chars(&bestmatch);
            if (matchsize == eltsize && matchsize < best_size)
                || ((matchsize == eltsize) == (matchsize == best_size)
                    && compare_prefixes(interp, &eltstring, size, &string, size, false, env)?
                        .eq_value(Value::T)
                    && !compare_prefixes(interp, &bestmatch, size, &string, size, false, env)?
                        .eq_value(Value::T))
            {
                best[0] = eltstring;
            }
        }
        if bestmatchsize != eltsize
            || bestmatchsize != matchsize
            || (ignore_case
                && !compare_prefixes(interp, &bestmatch, compare, &eltstring, compare, false, env)?
                    .eq_value(Value::T))
        {
            // Don't count the same string multiple times.
            if matchcount <= 1 {
                matchcount += 1;
            }
        }
        bestmatchsize = matchsize;
        if matchsize <= size && !ignore_case && matchcount > 1 {
            // No need to look any further.
            break;
        }
    }
    let bestmatch = best[0];
    if bestmatch.is_nil() {
        return Ok(Value::Nil);
    }
    // If we are ignoring case, and there is no exact match, and no
    // additional text was supplied, don't change the case of what the user
    // typed.
    if ignore_case && bestmatchsize == size && string_chars(&bestmatch) > bestmatchsize {
        return Ok(string);
    }
    // Return t if the supplied string is an exact match (counting case).
    if matchcount == 1 && values_equal(interp, &bestmatch, &string)? {
        return Ok(Value::T);
    }
    substring_value(
        bestmatch,
        Value::Integer(0),
        Value::Integer(bestmatchsize as i64),
        true,
    )
}

pub(crate) fn all_completions(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    if args.len() < 2 || args.len() > 4 {
        return Err(LispError::WrongNumberOfArgs(
            "all-completions".into(),
            args.len(),
        ));
    }
    // minibuf.c:Fall_completions: check_obarray on a vector, then CHECK_STRING.
    let string = args[0];
    let elements = match completion_table_type(interp, &args[1], env) {
        CompletionTable::Function => None,
        _ => Some(CompletionElements::new(interp, &args[1])?),
    };
    if !string.is_string() {
        return Err(LispError::WrongTypeArgument("stringp".into(), string));
    }
    let Some(mut elements) = elements else {
        return call_completion_table(interp, string, args[1], args.get(2), Value::T, env);
    };
    let ignore_case = completion_ignores_case(interp, env);
    let regexps = interp
        .lookup_var_key(cached_symbol!("completion-regexp-list"), env)
        .unwrap_or(Value::Nil);
    let predicate = args.get(2).copied().filter(|value| !value.is_nil());
    let hide_spaces = args.get(3).is_some_and(Value::is_truthy);
    let string_starts_with_space = string_first_byte(&string) == Some(b' ');
    let size = string_chars(&string);
    let mut allmatches = crate::lisp::alloc::RootedVec::new();
    while let Some(element) = elements.next()? {
        let eltstring = element.eltstring;
        if !eltstring.is_string()
            || size > string_chars(&eltstring)
            // If HIDE_SPACES, reject alternatives that start with space
            // unless the input starts with space.
            || (hide_spaces
                && !string_starts_with_space
                && string_first_byte(&eltstring) == Some(b' '))
            || !compare_prefixes(interp, &eltstring, size, &string, size, ignore_case, env)?
                .eq_value(Value::T)
        {
            continue;
        }
        if !match_regexps(interp, env, &eltstring, regexps, ignore_case)? {
            continue;
        }
        if let Some(predicate) = &predicate
            && !completion_predicate_accepts(interp, predicate, &element, env)?
        {
            continue;
        }
        allmatches.push(eltstring);
    }
    Ok(Value::list(allmatches.iter().copied()))
}

/// SREF (STRING, 0): the first byte of a nonempty string.
fn string_first_byte(value: &Value) -> Option<u8> {
    match value.kind() {
        Kind::StringObject(string) => string.borrow().bytes().first().copied(),
        _ => None,
    }
}

pub(crate) fn test_completion(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    if args.len() < 2 || args.len() > 4 {
        return Err(LispError::WrongNumberOfArgs(
            "test-completion".into(),
            args.len(),
        ));
    }
    let string = args[0];
    if !string.is_string() {
        return Err(LispError::WrongTypeArgument("stringp".into(), string));
    }
    // minibuf.c:Ftest_completion.
    let collection = args[1];
    let table = completion_table_type(interp, &collection, env);
    if table == CompletionTable::Function {
        let action = Value::Symbol("lambda".into());
        return call_completion_table(interp, string, collection, args.get(2), action, env);
    }
    let ignore_case = completion_ignores_case(interp, env);
    let mut roots = crate::lisp::alloc::RootedVec::from_vec(vec![Value::Nil, Value::Nil]);
    let hash_table = table == CompletionTable::Hash;
    if let Kind::HashTable(table) = collection.kind() {
        let (_, index) = hash_insert::hash_table_lookup(interp, table, &string, env)?;
        if let Some((key, value)) = index.and_then(|index| table.entry(index)) {
            roots[0] = key;
            roots[1] = value;
        } else {
            let mut found = false;
            for (key, value) in table
                .entries()
                .map(|(_, key, value)| (key, value))
                .collect::<Vec<_>>()
            {
                let strkey = completion_element_name(key);
                if !strkey.is_string() {
                    continue;
                }
                let (string_size, key_size) = (string_chars(&string), string_chars(&strkey));
                if compare_string_ranges(
                    interp,
                    &string,
                    (0, string_size),
                    &strkey,
                    (0, key_size),
                    ignore_case,
                    env,
                )?
                .eq_value(Value::T)
                {
                    roots[0] = key;
                    roots[1] = value;
                    found = true;
                    break;
                }
            }
            if !found {
                return Ok(Value::Nil);
            }
        }
    } else if table == CompletionTable::Obarray {
        // Bypass intern-soft as that loses for nil.
        let name = string_text(&string)?;
        let symbols = obarray_symbols(interp, &collection)?;
        let mut tem = symbols
            .iter()
            .copied()
            .find(|symbol| obarray_symbol_matches(symbol, &name));
        if ignore_case && tem.is_none() {
            let string_size = string_chars(&string);
            for symbol in symbols {
                let symbol_name = completion_element_name(symbol);
                let name_size = string_chars(&symbol_name);
                if compare_string_ranges(
                    interp,
                    &string,
                    (0, string_size),
                    &symbol_name,
                    (0, name_size),
                    true,
                    env,
                )?
                .eq_value(Value::T)
                {
                    tem = Some(symbol);
                    break;
                }
            }
        }
        let Some(tem) = tem else {
            return Ok(Value::Nil);
        };
        roots[0] = tem;
    } else {
        let tem = super::call(
            interp,
            "assoc-string",
            &[
                string,
                collection,
                if ignore_case { Value::T } else { Value::Nil },
            ],
            env,
        )?;
        if tem.is_nil() {
            return Ok(Value::Nil);
        }
        roots[0] = tem;
    }
    // Reject this element if it fails to match all the regexps.
    let regexps = interp
        .lookup_var_key(cached_symbol!("completion-regexp-list"), env)
        .unwrap_or(Value::Nil);
    if !match_regexps(interp, env, &string, regexps, ignore_case)? {
        return Ok(Value::Nil);
    }
    // Finally, check the predicate.
    match args.get(2).filter(|value| !value.is_nil()) {
        Some(predicate) => {
            let predicate = resolve_callable(interp, predicate, env)?;
            if hash_table {
                invoke_function_value(interp, &predicate, &[roots[0], roots[1]], env)
            } else {
                invoke_function_value(interp, &predicate, &[roots[0]], env)
            }
        }
        None => Ok(Value::T),
    }
}

pub(crate) fn internal_complete_buffer(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    if args.len() != 3 {
        return Err(LispError::WrongNumberOfArgs(
            "internal-complete-buffer".into(),
            args.len(),
        ));
    }
    let buffer_alist = Value::list(
        interp
            .buffer_list
            .iter()
            .map(|(id, name)| {
                Value::cons(*name, interp.buffer_value(*id).expect("live buffer object"))
            })
            .collect::<Vec<_>>(),
    );
    match crate::lisp::types::eq_dispatch(args[2]).kind() {
        Kind::Nil => try_completion(interp, &[args[0], buffer_alist, args[1]], env),
        Kind::T => {
            // Fall_completions checks STRING; only this flag reads it here.
            let completions = all_completions(interp, &[args[0], buffer_alist, args[1]], env)?;
            if !string_text(&args[0])?.is_empty() {
                return Ok(completions);
            }
            let all = completions.to_vec()?;
            let visible = all
                .iter()
                .filter(|value| {
                    string_like(value).is_some_and(|string| !string.text.starts_with(' '))
                })
                .cloned()
                .collect::<Vec<_>>();
            if visible.is_empty() && all.len() == interp.buffer_list.len() {
                Ok(completions)
            } else {
                Ok(Value::list(visible))
            }
        }
        Kind::Symbol(flag) if flag.is_named("lambda") => {
            test_completion(interp, &[args[0], buffer_alist, args[1]], env)
        }
        Kind::Symbol(flag) if flag.is_named("metadata") => Ok(Value::list([
            Value::Symbol("metadata".into()),
            Value::cons(
                Value::Symbol("category".into()),
                Value::Symbol("buffer".into()),
            ),
            Value::cons(
                Value::Symbol("cycle-sort-function".into()),
                Value::Symbol("identity".into()),
            ),
        ])),
        _ => Ok(Value::Nil),
    }
}

/// minibuf.c:Fcompleting_read: `completing-read-function''s value called
/// with the eight arguments, those not given nil.
pub(crate) fn completing_read(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    if args.len() < 2 || args.len() > 8 {
        return Err(LispError::WrongNumberOfArgs(
            "completing-read".into(),
            args.len(),
        ));
    }
    let function = super::call(
        interp,
        "symbol-value",
        &[Value::Symbol(
            cached_symbol!("completing-read-function").symbol(),
        )],
        env,
    )?;
    let mut full = [Value::Nil; 8];
    full[..args.len()].copy_from_slice(args);
    let roots = [function];
    interp.with_lisp_stack_roots(&roots.as_slice(), |interp| {
        call_function_value(interp, &function, &full, env)
    })
}

thread_local! {
    /// read_minibuf's INPUT_METHOD: `current-input-method' of the buffer a
    /// read inheriting the input method began in, for the next minibuffer
    /// run to turn on.
    static MINIBUFFER_INPUT_METHOD: std::cell::Cell<Option<Value>> =
        const { std::cell::Cell::new(None) };
}

/// read_minibuf with INHERIT-INPUT-METHOD: `current-input-method' as the
/// current buffer has it (it is buffer-local), remembered before the
/// minibuffer is entered; nothing without INHERIT.
pub(crate) fn inherit_minibuffer_input_method(interp: &Interpreter, env: &Env, inherit: bool) {
    MINIBUFFER_INPUT_METHOD.set(inherit.then(|| {
        interp
            .lookup_var_key(cached_symbol!("current-input-method"), env)
            .unwrap_or(Value::Nil)
    }));
}

pub(crate) fn run_active_minibuffer<T>(
    interp: &mut Interpreter,
    env: &mut Env,
    minibuffer: ActiveMinibuffer,
    body: impl FnOnce(&mut Interpreter, &mut Env) -> Result<T, LispError>,
) -> Result<T, LispError> {
    // The recursive command loop leaves `this-command' and its companions
    // as its last command set them; call-interactively, not read_minibuf,
    // restores the outer command's once its arguments are read.
    let input_method = MINIBUFFER_INPUT_METHOD.take();
    let result = (|| {
        // GNU minibuf.c:read_minibuf runs `minibuffer-setup-hook' with the
        // minibuffer already current (the next statement there is
        // `bset_undo_list (current_buffer, Qnil)').  Hooks therefore see the
        // minibuffer's buffer-local state, including the completion table.
        if interp.has_buffer_id(minibuffer.buffer_id) {
            interp.set_current_buffer_id(minibuffer.buffer_id)?;
        }
        // The input method of the buffer the read began in, when the
        // reader inherits it (a string, and `activate-input-method'
        // defined), is turned on in the minibuffer first.
        if let Some(method) = input_method.filter(|method| method.is_string())
            && super::call(
                interp,
                "fboundp",
                &[Value::Symbol(
                    cached_symbol!("activate-input-method").symbol(),
                )],
                env,
            )?
            .is_truthy()
        {
            // call1 (Qactivate_input_method, input_method): Lisp's own.
            interp.call_function_value(
                Value::Symbol(cached_symbol!("activate-input-method").symbol()),
                Some("activate-input-method"),
                &[method],
                env,
            )?;
        }
        run_hook(
            interp,
            &cached_symbol!("minibuffer-setup-hook").symbol(),
            env,
        )?;

        // A setup hook may temporarily select another buffer (most notably
        // *Completions*).  The command loop itself starts in the minibuffer.
        if interp.has_buffer_id(minibuffer.buffer_id) {
            interp.set_current_buffer_id(minibuffer.buffer_id)?;
            // "Don't allow the user to undo past this point."
            interp.buffer.borrow_mut().set_undo_list(Value::Nil);
        }

        // read_minibuf's recursive_edit_1, which BODY calls.
        body(interp, env)
    })();
    // The unwinding of every exit: `minibuffer-exit-hook' first, then
    // the rest of read_minibuf's unwind records -- none for the exit after
    // a C stack overflow, which leaves GNU's specpdl entries undone, the
    // minibuffer active at its depth.
    if interp.c_stack_overflow_exit {
        return result;
    }
    let exit = run_exit_minibuf_hook(interp, minibuffer.buffer_id, env);
    unwind_active_minibuffer(interp, minibuffer, env)?;
    let value = result?;
    exit.map(|()| value)
}

pub(crate) struct ActiveMinibuffer {
    pub(crate) buffer_id: u64,
    /// The window tree as it stood BEFORE the minibuffer window was
    /// selected, when `read-minibuffer-restore-windows' had read_minibuf
    /// register its restore: a pop-up the setup hook makes
    /// (tmm-add-prompt's completion window) vanishes when the read
    /// finishes, on every exit path, quits included.
    saved_windows: Option<crate::lisp::eval::WindowConfigurationSnapshot>,
    saved_buffer_id: u64,
    saved_selected_window_id: u64,
    previous_minibuffer_selected_window_id: Option<u64>,
    previous_runtime: crate::lisp::eval::MinibufferRuntimeState,
    /// Where this read's entries start in minibuf_save_list.
    save_list_start: usize,
}

/// minibuf_save_list's entries for one read, in the order
/// read_minibuf_unwind restores them: Fthis_command_keys_vector (the keys
/// of the command that read), `help-form', `current-prefix-arg',
/// `minibuffer-history-position', `minibuffer-history-variable' and
/// `overriding-local-map'.  The prompt, its width and the windows are the
/// minibuffer runtime's and the window state's.
const MINIBUF_SAVE_VARIABLES: [&str; 5] = [
    "help-form",
    "current-prefix-arg",
    "minibuffer-history-position",
    "minibuffer-history-variable",
    "overriding-local-map",
];

fn push_minibuf_save_list(interp: &mut Interpreter, env: &Env) -> usize {
    let start = interp.keyboard_input.minibuf_save_list.len();
    // Fthis_command_keys_vector, kept as a list of the same events.
    let keys = Value::list(interp.keyboard_input.command_keys.iter().copied());
    interp.keyboard_input.minibuf_save_list.push(keys);
    for name in MINIBUF_SAVE_VARIABLES {
        let value = interp
            .lookup_var_key(crate::lisp::types::symbols::SymbolKey::new(name), env)
            .unwrap_or(Value::Nil);
        interp.keyboard_input.minibuf_save_list.push(value);
    }
    start
}

/// read_minibuf_unwind's "Restore prompt, etc, from outer minibuffer
/// level": this-command-keys and the saved variables.
fn pop_minibuf_save_list(interp: &mut Interpreter, start: usize, env: &mut Env) {
    let list = &mut interp.keyboard_input.minibuf_save_list;
    let saved = list.split_off(start.min(list.len()));
    let mut saved = saved.into_iter();
    if let Some(keys) = saved.next() {
        interp.keyboard_input.command_keys = keys.to_vec().unwrap_or_default();
    }
    let roots: Vec<Value> = saved.collect();
    interp.with_lisp_stack_roots(&roots.as_slice(), |interp| {
        for (name, value) in MINIBUF_SAVE_VARIABLES.into_iter().zip(roots.iter()) {
            interp.set_variable_key(
                crate::lisp::types::symbols::SymbolKey::new(name),
                *value,
                env,
            );
        }
    });
}

/// minibuf.c:read_minibuf's INITIAL: a string, or (STRING . POSITION)
/// with point at one-indexed POSITION in STRING (at its start for any
/// POSITION below 1).  Returns the string and the `forward-char' from
/// its end that places point; past the end is `end-of-buffer'.
pub(crate) fn minibuffer_initial_input(initial: &Value) -> Result<(Value, i64), LispError> {
    let (string, backup) = match initial.kind() {
        Kind::Cons(_) => (initial.car()?, initial.cdr()?),
        _ if initial.is_nil() => return Ok((Value::Nil, 0)),
        _ => (*initial, Value::Nil),
    };
    let Some(text) = string_like(&string) else {
        return Err(wrong_type_argument("stringp", string));
    };
    if backup.is_nil() {
        return Ok((string, 0));
    }
    let Kind::Integer(position) = backup.kind() else {
        return Err(wrong_type_argument("fixnump", backup));
    };
    // "Convert to distance from end of input."  A position past the end
    // is no error here: only the minibuffer's Fforward_char signals it,
    // which the noninteractive read never reaches.
    let length = text.text.chars().count() as i64;
    let pos = if position < 1 {
        -length
    } else {
        position.saturating_sub(1).saturating_sub(length)
    };
    Ok((string, pos))
}

#[cfg(test)]
pub(crate) fn activate_minibuffer(
    interp: &mut Interpreter,
    prompt: &Value,
    initial_input: &Value,
    local_map: Value,
    env: &mut Env,
) -> Result<ActiveMinibuffer, LispError> {
    activate_minibuffer_with_history(interp, prompt, initial_input, local_map, Value::Nil, env)
}

/// read_minibuf up to its recursive edit, HIST the reader's history spec:
/// a symbol, or (SYMBOL . POSITION).
pub(crate) fn activate_minibuffer_with_history(
    interp: &mut Interpreter,
    prompt: &Value,
    initial_input: &Value,
    local_map: Value,
    history: Value,
    env: &mut Env,
) -> Result<ActiveMinibuffer, LispError> {
    let prompt_string =
        string_like(prompt).ok_or_else(|| wrong_type_argument("stringp", *prompt))?;
    let prompt_length = prompt_string.text.chars().count();
    let (initial_input, initial_backup) = minibuffer_initial_input(initial_input)?;
    let initial_input = &initial_input;
    let initial_string = string_like(initial_input).unwrap_or_else(|| StringLike {
        text: String::new(),
        props: None,
        multibyte: false,
        extended_chars: Vec::new(),
    });
    // The ambient buffer's default directory, or, when that is not a
    // string, FOR_EACH_LIVE_BUFFER's first buffer whose directory is.
    let default_directory = interp
        .lookup_var_key(cached_symbol!("default-directory"), env)
        .filter(Value::is_string)
        .or_else(|| {
            let ids: Vec<u64> = interp.buffer_list.iter().map(|(id, _)| *id).collect();
            ids.into_iter().find_map(|id| {
                interp
                    .buffer_local_value_key(id, cached_symbol!("default-directory"))
                    .filter(Value::is_string)
            })
        });
    // read_minibuf: a minibuffer window showing an active minibuffer (a
    // recursive read) pushes it onto its previous buffers, for
    // minibuffer_unwind to show again when this read ends.
    let minibuffer_window = interp.minibuffer_window_value();
    if let Some(shown) = super::window_buffer_id(interp, &minibuffer_window)
        .and_then(|id| interp.buffer_identity_value(id))
        && this_minibuffer_depth(interp, &shown) != 0
        && interp
            .lookup_function_key(cached_symbol!("push-window-buffer-onto-prev"), env)
            .is_ok()
    {
        call_function_value(
            interp,
            &Value::symbol("push-window-buffer-onto-prev"),
            &[minibuffer_window],
            env,
        )?;
    }
    let saved_windows = interp
        .lookup_var_key(cached_symbol!("read-minibuffer-restore-windows"), env)
        .is_none_or(|restore| restore.is_truthy())
        .then(|| interp.snapshot_window_configuration());
    let depth = interp.minibuffer_depth().saturating_add(1);
    let buffer_id = get_minibuffer(interp, depth);
    let saved_buffer_id = interp.current_buffer_id();
    let saved_selected_window_id = interp.selected_window_id();
    // minibuf_level++ before the frame is chosen: (minibuffer-depth) is the
    // new level from here on, the mode hooks below included.
    let minibuffer_window_id = match minibuffer_window.kind() {
        Kind::Record(window) => window.id,
        _ => saved_selected_window_id,
    };
    let previous_runtime =
        interp.begin_minibuffer_runtime(buffer_id, minibuffer_window_id, prompt_string.text);
    // read_minibuf arranges to restore a number of minibuffer-related
    // variables (minibuf_save_list), then starts changing them: `help-form'
    // is `minibuffer-help-form' for the read.
    let save_list_start = push_minibuf_save_list(interp, env);
    let previous_minibuffer_selected_window_id = interp.minibuffer_selected_window_id();
    let active = ActiveMinibuffer {
        buffer_id,
        saved_windows,
        saved_buffer_id,
        saved_selected_window_id,
        previous_minibuffer_selected_window_id,
        previous_runtime,
        save_list_start,
    };
    // From here on read_minibuf's unwind records cover a nonlocal exit:
    // the variables changed, the minibuffer's mode set, the prompt and
    // initial input inserted (Fforward_char past the input's end signals),
    // clear-message-function.
    let setup = (|| -> Result<(), LispError> {
        let help_form = interp
            .lookup_var_key(
                crate::lisp::types::symbols::SymbolKey::new("minibuffer-help-form"),
                env,
            )
            .unwrap_or(Value::Nil);
        interp.set_variable_key(
            crate::lisp::types::symbols::SymbolKey::new("help-form"),
            help_form,
            env,
        );
        // Vminibuffer_history_position and Vminibuffer_history_variable:
        // HIST's car and cdr (a bare symbol HIST the variable), nil being
        // `minibuffer-history' and 0.
        let (history_variable, history_position) = match history.cons_values() {
            Some((variable, position)) => (variable, position),
            None => (history, Value::Nil),
        };
        let history_variable = if history_variable.is_nil() {
            Value::symbol("minibuffer-history")
        } else {
            history_variable
        };
        let history_position = if history_position.is_nil() {
            Value::Integer(0)
        } else {
            history_position
        };
        interp.set_variable_key(
            cached_symbol!("minibuffer-history-variable"),
            history_variable,
            env,
        );
        interp.set_variable_key(
            cached_symbol!("minibuffer-history-position"),
            history_position,
            env,
        );
        // If this minibuffer is reading a file name, that doesn't mean
        // recursive ones are: `lambda', "t" in this minibuffer, nil in the
        // next.
        if interp
            .lookup_var("minibuffer-completing-file-name", env)
            .is_some_and(|value| value.is_truthy())
        {
            interp.set_variable(
                "minibuffer-completing-file-name",
                Value::symbol("lambda"),
                env,
            );
        }
        // If the history variable is unbound, make it nil.
        if history_variable.is_symbol()
            && super::call(interp, "boundp", &[history_variable], env)?.is_nil()
        {
            super::call(interp, "set", &[history_variable, Value::Nil], env)?;
        }
        // read_minibuf's set_minibuffer_mode: `minibuffer-mode', whose
        // kill-all-local-variables drops what the previous read left local.
        set_minibuffer_mode(interp, buffer_id, depth, env)?;
        if let Some(default_directory) = default_directory {
            interp.set_buffer_local_value_key(
                buffer_id,
                cached_symbol!("default-directory"),
                default_directory,
            );
        }
        interp.set_buffer_local_value_key(
            buffer_id,
            cached_symbol!("buffer-read-only"),
            Value::Nil,
        );
        // "Defeat (setq-default truncate-lines t), since truncated lines do
        // not work correctly in minibuffers."
        interp.set_buffer_local_value_key(buffer_id, cached_symbol!("truncate-lines"), Value::Nil);
        interp.set_current_buffer_id(buffer_id)?;
        // Vminibuf_scroll_window and minibuf_selected_window: the window the
        // read began in, unless a recursive read began in the minibuffer
        // window.
        if depth == 1 || minibuffer_window_id != saved_selected_window_id {
            interp.replace_minibuffer_selected_window_id(Some(saved_selected_window_id));
        }
        // "Empty out the minibuffers of all frames, except those frames where
        // there is an active minibuffer": the level-0 one goes inactive.
        let empty = get_minibuffer(interp, 0);
        set_minibuffer_mode(interp, empty, 0, env)?;
        // "Display this minibuffer in the proper window" (set_window_buffer,
        // its markers at the buffer's point before the erase) and
        // Fselect_window it: the read happens there, never in the entry
        // window.
        if let Kind::Record(minibuffer_window_id) = minibuffer_window.kind() {
            interp.show_buffer_in_window(minibuffer_window_id.id, buffer_id, false);
            super::call(interp, "select-window", &[minibuffer_window], env)?;
        }
        // read_minibuf erases the buffer and inserts the prompt with
        // `inhibit-read-only' and `inhibit-modification-hooks' bound: the
        // ordinary editing primitives, recording undo, then the field and
        // stickiness controls that keep typed text from inheriting the prompt's
        // properties (front-sticky, rear-nonsticky), then
        // `minibuffer-prompt-properties' (the read-only guard; its face is
        // added after the prompt's own faces).
        with_minibuffer_edits_inhibited(interp, env, |interp, env| {
            super::call(interp, "erase-buffer", &[], env)?;
            super::call(interp, "insert", &[*prompt], env)?;
            let prompt_end = Value::Integer(interp.buffer.borrow().point() as i64);
            if prompt_length == 0 {
                return Ok(());
            }
            let stamp = |interp: &mut Interpreter, env: &mut Env, name: Value, value: Value| {
                super::call(
                    interp,
                    "put-text-property",
                    &[Value::Integer(1), prompt_end, name, value],
                    env,
                )
                .map(drop)
            };
            for name in ["front-sticky", "rear-nonsticky", "field"] {
                stamp(interp, env, Value::symbol(name), Value::T)?;
            }
            let properties = interp
                .lookup_var_key(cached_symbol!("minibuffer-prompt-properties"), env)
                .unwrap_or(Value::Nil);
            let mut list = properties;
            while let Some((name, rest)) = list.cons_values() {
                let Some((value, rest)) = rest.cons_values() else {
                    break;
                };
                if crate::lisp::types::eq(name, Value::symbol("face")) {
                    super::call(
                        interp,
                        "add-face-text-property",
                        &[Value::Integer(1), prompt_end, value, Value::T],
                        env,
                    )?;
                } else {
                    stamp(interp, env, name, value)?;
                }
                list = rest;
            }
            Ok(())
        })?;
        // The initial input goes in after the bindings are gone, an ordinary
        // insertion.
        if !initial_string.text.is_empty() {
            if matches!(initial_input.kind(), Kind::StringObject(_)) {
                super::call(interp, "insert", &[*initial_input], env)?;
            } else {
                let initial_start = interp.buffer.borrow().point();
                let props = match &initial_string.props {
                    Some(intervals) => crate::buffer::InsertedProperties::Intervals {
                        source: intervals,
                        inherit: false,
                    },
                    None => crate::buffer::InsertedProperties::PLAIN,
                };
                let vars = interp.insertion_property_variables();
                interp.adjust_suspend_auto_hscroll(
                    initial_start,
                    initial_start + initial_string.text.chars().count(),
                );
                interp
                    .buffer
                    .borrow_mut()
                    .insert_with(&initial_string.text, props, false, &vars);
                interp
                    .buffer
                    .borrow_mut()
                    .set_inserted_extended_chars(initial_start, &initial_string.extended_chars);
            }
        }
        // Fforward_char (POS) from the end of the initial input: a position
        // past it leaves point at ZV and signals `end-of-buffer'.
        let past_end = initial_backup > 0;
        {
            let mut buffer = interp.buffer.borrow_mut();
            let position = buffer.point_max();
            if !past_end {
                buffer.goto_char(position.saturating_sub(initial_backup.unsigned_abs() as usize));
            }
        }

        if past_end {
            return Err(LispError::SignalValue(Value::list([Value::symbol(
                "end-of-buffer",
            )])));
        }
        // GNU minibuf.c:read_minibuf has already selected/activated the
        // minibuffer when clear_message runs, but has not installed its map.
        super::dispatch::clear_message(interp, env, true, true)
    })();
    if let Err(error) = setup {
        if interp.c_stack_overflow_exit {
            return Err(error);
        }
        // Run the exit hook before restoring state, and restore even if
        // that hook itself exits nonlocally.
        let exit_result = run_exit_minibuf_hook(interp, buffer_id, env);
        unwind_active_minibuffer(interp, active, env)?;
        exit_result?;
        return Err(error);
    }
    if let Some(mut buffer) = interp.get_buffer_by_id_mut(buffer_id) {
        buffer.set_keymap(local_map);
    }
    Ok(active)
}

/// minibuf.c:read_minibuf's guard: without `enable-recursive-minibuffers'
/// a read while a minibuffer is active is a user error when the minibuffer
/// window is selected, and from any other window cancels the active read
/// (its recursive edit signals the thrown string as an error).
pub(crate) fn check_recursive_minibuffer(interp: &Interpreter, env: &Env) -> Result<(), LispError> {
    if interp.minibuffer_depth() == 0
        || interp
            .lookup_var_key(cached_symbol!("enable-recursive-minibuffers"), env)
            .is_some_and(|value| value.is_truthy())
    {
        return Ok(());
    }
    let message = Value::string("Command attempted to use minibuffer while in minibuffer");
    let in_minibuffer_window = matches!(
        interp.minibuffer_window_value().kind(),
        Kind::Record(window) if window.id == interp.selected_window_id()
    );
    if in_minibuffer_window {
        Err(LispError::SignalValue(Value::list([
            Value::symbol("user-error"),
            message,
        ])))
    } else {
        Err(LispError::Throw(Value::symbol("exit"), message))
    }
}

/// minibuf.c:minibuffer_unwind: the minibuffer window shows the next less
/// nested minibuffer, the first of its previous buffers when that is live
/// (popped, with its start and point), or else the null minibuffer.
fn minibuffer_unwind(interp: &mut Interpreter, env: &mut Env) -> Result<(), LispError> {
    let window = interp.minibuffer_window_value();
    let Kind::Record(window_id) = window.kind() else {
        return Ok(());
    };
    let window_id = window_id.id;
    // WINDOW_LIVE_P: a live window shows a buffer.
    if super::window_buffer_id(interp, &window).is_none() {
        return Ok(());
    }
    let previous = interp
        .find_record(window_id)
        .and_then(|record| record.slots.get(super::WINDOW_PREV_BUFFERS_SLOT).copied())
        .unwrap_or(Value::Nil);
    if let Some(cell) = previous.cons_ref() {
        let entry = cell.car.get();
        if let Kind::Buffer(buffer) = entry.car()?.kind()
            && interp
                .buffer_object(buffer.id)
                .is_some_and(|live| live.ptr_eq(&buffer))
        {
            if let Some(record) = interp.find_record_mut(window_id) {
                record.slots[super::WINDOW_PREV_BUFFERS_SLOT] = cell.cdr.get();
            }
            interp.show_buffer_in_window(window_id, buffer.id, false);
            let rest = entry.cdr()?;
            super::call(interp, "set-window-start", &[window, rest.car()?], env)?;
            super::call(
                interp,
                "set-window-point",
                &[window, rest.cdr()?.car()?],
                env,
            )?;
            return Ok(());
        }
    }
    // set_window_buffer (window, nth_minibuffer (0), 0, 0).
    if let Kind::Buffer(null) = nth_minibuffer(interp, 0).kind()
        && let Some(null) = interp.live_buffer_id(null)
    {
        interp.show_buffer_in_window(window_id, null, false);
    }
    Ok(())
}

/// minibuf.c:this_minibuffer_depth: BUFFER's depth among the active
/// minibuffers (Vminibuffer_list's entries 1 to minibuf_level), or 0 when
/// it is none of them.
pub(crate) fn this_minibuffer_depth(interp: &Interpreter, buffer: &Value) -> usize {
    (1..=interp.minibuffer_depth())
        .find(|&depth| nth_minibuffer(interp, depth).eq_value(*buffer))
        .unwrap_or(0)
}

/// minibuf.c:nth_minibuffer: Vminibuffer_list's DEPTHth entry, nil past
/// its end.
pub(crate) fn nth_minibuffer(interp: &Interpreter, depth: usize) -> Value {
    interp
        .minibuffer_list
        .get(depth)
        .copied()
        .unwrap_or(Value::Nil)
}

/// Fmemq (BUFFER, Vminibuffer_list).
pub(crate) fn minibuffer_list_has(interp: &Interpreter, buffer: &Value) -> bool {
    interp
        .minibuffer_list
        .iter()
        .any(|entry| entry.eq_value(*buffer))
}

/// minibuf.c:live_minibuffer_p: BUFFER is one of the active minibuffers,
/// never *Minibuf-0*.
pub(crate) fn live_minibuffer_p(interp: &Interpreter, buffer: &Value) -> bool {
    !nth_minibuffer(interp, 0).eq_value(*buffer) && this_minibuffer_depth(interp, buffer) != 0
}

/// minibuf.c:Fminibuffer_innermost_command_loop_p: BUFFER is an active
/// minibuffer read at the current command loop level.
pub(crate) fn minibuffer_innermost_command_loop_p(interp: &Interpreter, buffer: &Value) -> bool {
    let depth = this_minibuffer_depth(interp, buffer);
    depth != 0
        && interp.minibuffer_command_loop_level(depth) == Some(interp.command_loop_recursion_depth)
}

/// minibuf.c:get_minibuffer: Vminibuffer_list's DEPTHth buffer, the list
/// grown to hold it; a live one has both overlay lists emptied and is
/// reset_buffer's, else Fget_buffer_create of " *Minibuf-DEPTH*" takes
/// the place, its undo enabled ("Although the buffer's name starts with a
/// space, undo should be enabled in it").
pub(crate) fn get_minibuffer(interp: &mut Interpreter, depth: usize) -> u64 {
    if interp.minibuffer_list.len() <= depth {
        interp.minibuffer_list.resize(depth + 1, Value::Nil);
    }
    if let Kind::Buffer(buffer) = interp.minibuffer_list[depth].kind()
        && let Some(id) = interp.live_buffer_id(buffer)
    {
        if let Some(mut buffer) = interp.get_buffer_by_id_mut(id) {
            buffer.overlays.clear();
        }
        interp.reset_buffer(id);
        return id;
    }
    let name = format!(" *Minibuf-{depth}*");
    let id = interp
        .find_buffer_id(&name)
        .unwrap_or_else(|| interp.create_buffer(&name).0);
    // Do this before set_minibuffer_mode.
    interp.minibuffer_list[depth] = interp.buffer_value(id).unwrap_or(Value::Nil);
    if let Some(mut buffer) = interp.get_buffer_by_id_mut(id) {
        buffer.enable_undo();
    }
    id
}

/// minibuf.c:set_minibuffer_mode: in BUFFER, `minibuffer-mode' for an
/// active DEPTH, `minibuffer-inactive-mode' for level 0 (where
/// minibuffer.el is not loaded, kill-all-local-variables), the current
/// buffer kept.  A runtime without minibuffer.el, which GNU never is at an
/// active level, drops the previous read's locals instead.
fn set_minibuffer_mode(
    interp: &mut Interpreter,
    buffer_id: u64,
    depth: usize,
    env: &mut Env,
) -> Result<(), LispError> {
    let mode = if depth > 0 {
        "minibuffer-mode"
    } else {
        "minibuffer-inactive-mode"
    };
    let defined = interp.lookup_function(mode, env).is_ok();
    if !defined && depth > 0 {
        interp.clear_buffer_local_state(buffer_id);
        return Ok(());
    }
    let saved = interp.current_buffer_id();
    interp.set_current_buffer_id(buffer_id)?;
    let result = if defined {
        interp
            .call_function_value(Value::symbol(mode), Some(mode), &[], env)
            .map(drop)
    } else {
        super::call(interp, "kill-all-local-variables", &[], env).map(drop)
    };
    if interp.has_buffer_id(saved) {
        interp.set_current_buffer_id(saved)?;
    }
    result
}

/// emacs.c's set_initial_minibuffer_mode, before the top level: the
/// level-0 minibuffer in its inactive mode.
pub(crate) fn set_initial_minibuffer_mode(interp: &mut Interpreter) -> Result<(), LispError> {
    let buffer_id = get_minibuffer(interp, 0);
    set_minibuffer_mode(interp, buffer_id, 0, &mut Env::new())
}

/// BODY with `inhibit-read-only' and `inhibit-modification-hooks' bound to
/// t, as read_minibuf and read_minibuf_unwind edit the minibuffer.
fn with_minibuffer_edits_inhibited<T>(
    interp: &mut Interpreter,
    env: &mut Env,
    body: impl FnOnce(&mut Interpreter, &mut Env) -> Result<T, LispError>,
) -> Result<T, LispError> {
    let read_only = interp.bind_special_dynamic("inhibit-read-only", Value::T, env)?;
    let hooks = match interp.bind_special_dynamic("inhibit-modification-hooks", Value::T, env) {
        Ok(restore) => restore,
        Err(error) => {
            interp.restore_special_dynamic(read_only, env)?;
            return Err(error);
        }
    };
    let result = body(interp, env);
    let restored = interp
        .restore_special_dynamic(hooks, env)
        .and(interp.restore_special_dynamic(read_only, env));
    let value = result?;
    restored.map(|()| value)
}

/// minibuf.c:run_exit_minibuf_hook: `minibuffer-exit-hook' in the
/// minibuffer while it is live, the current buffer kept.
fn run_exit_minibuf_hook(
    interp: &mut Interpreter,
    buffer_id: u64,
    env: &mut Env,
) -> Result<(), LispError> {
    let saved = interp.current_buffer_id();
    if interp.has_buffer_id(buffer_id) {
        interp.set_current_buffer_id(buffer_id)?;
    }
    let result = super::safe_run_hooks(
        interp,
        &cached_symbol!("minibuffer-exit-hook").symbol(),
        env,
    );
    if interp.has_buffer_id(saved) {
        interp.set_current_buffer_id(saved)?;
    }
    result
}

/// read_minibuf_unwind's erasure of the expired minibuffer, which keeps
/// `deactivate-mark' as it was ("Prevent error in erase-buffer").
fn erase_expired_minibuffer(interp: &mut Interpreter, env: &mut Env) -> Result<(), LispError> {
    with_minibuffer_edits_inhibited(interp, env, |interp, env| {
        let old_deactivate_mark = interp
            .lookup_var_key(cached_symbol!("deactivate-mark"), env)
            .unwrap_or(Value::Nil);
        let erased = super::call(interp, "erase-buffer", &[], env).map(drop);
        interp.set_c_variable("deactivate-mark", old_deactivate_mark);
        erased
    })
}

/// read_minibuf's unwind records after `minibuffer-exit-hook', in their
/// order: read_minibuf_unwind, the window-configuration restore with
/// DONT-SET-MINIWINDOW (the minibuffer window keeps its buffer),
/// minibuffer_unwind, and restore_buffer.
pub(crate) fn unwind_active_minibuffer(
    interp: &mut Interpreter,
    mut state: ActiveMinibuffer,
    env: &mut Env,
) -> Result<(), LispError> {
    let saved_windows = state.saved_windows.take();
    let saved_buffer_id = state.saved_buffer_id;
    read_minibuf_unwind(interp, state, env);
    if let Some(saved_windows) = saved_windows {
        let _ = interp.restore_window_configuration_with(saved_windows, true);
    }
    minibuffer_unwind(interp, env)?;
    if interp.has_buffer_id(saved_buffer_id) {
        let _ = interp.set_current_buffer_id(saved_buffer_id);
    }
    Ok(())
}

fn read_minibuf_unwind(interp: &mut Interpreter, state: ActiveMinibuffer, env: &mut Env) {
    // In the expired minibuffer: the outer level's depth and prompt
    // return, the buffer is erased, and it goes back to
    // `minibuffer-inactive-mode' so no binding made for the read outlives it.
    let expired = interp.has_buffer_id(state.buffer_id)
        && interp.set_current_buffer_id(state.buffer_id).is_ok();
    interp.restore_minibuffer_runtime(state.previous_runtime);
    // "Restore prompt, etc, from outer minibuffer level".
    pop_minibuf_save_list(interp, state.save_list_start, env);
    if expired {
        let _ = erase_expired_minibuffer(interp, env);
        if interp
            .lookup_function_key(cached_symbol!("minibuffer-inactive-mode"), env)
            .is_ok()
        {
            let _ = interp.call_function_value(
                Value::symbol("minibuffer-inactive-mode"),
                Some("minibuffer-inactive-mode"),
                &[],
                env,
            );
        }
    }
    interp.replace_minibuffer_selected_window_id(state.previous_minibuffer_selected_window_id);
    // "Switch the current windows away from the expired minibuffer
    // window": unless the minibuffer window has an outer read's buffer to
    // go back to, the calling window is Fselect_window'ed.
    let minibuffer_window = interp.minibuffer_window_value();
    let outer_reads = match minibuffer_window.kind() {
        Kind::Record(window) => interp
            .find_record(window.id)
            .and_then(|record| record.slots.get(super::WINDOW_PREV_BUFFERS_SLOT))
            .is_some_and(|previous| !previous.is_nil()),
        _ => false,
    };
    let calling_window = interp.record_value(state.saved_selected_window_id);
    if !outer_reads && super::window_buffer_id(interp, &calling_window).is_some() {
        let _ = super::call(interp, "select-window", &[calling_window], env);
    }
}

pub(crate) fn interactive_form_items(func: &Value) -> Option<Vec<Value>> {
    if let Kind::BuiltinFunc(name) = func.kind() {
        if let Some(form) = generated_builtin_arities::generated_builtin_interactive_form(&name) {
            let parsed = crate::lisp::reader::Reader::new(form)
                .read_all()
                .ok()?
                .into_iter()
                .next()?;
            return parsed.to_vec().ok();
        }
        // The GNU-generated interactive-form table is the only native
        // source; a miss means the builtin is not a command.
        return None;
    }
    // A raw `(lambda ARGS . BODY)' LIST also has an interactive form (GNU
    // interactive_form handles unevaluated lambda expressions; advice.el's
    // ad-interactive-form probes stored advice bodies this way).
    if let Ok(items) = func.to_vec()
        && matches!(items.first().map(|v| v.kind()), Some(Kind::Symbol(head)) if head.is_named("lambda"))
    {
        return items.get(2..).and_then(interactive_form_in_body);
    }
    let Kind::Closure(lambda) = func.kind() else {
        return None;
    };
    lambda
        .interactive_spec()
        .map(|spec| vec![Value::Symbol("interactive".into()), spec])
}

/// Return GNU's `(interactive SPEC)' metadata for every callable
/// representation.  Genuine byte-code closures store SPEC in closure slot
/// five rather than as a body form; keep that representation detail behind
/// the same query used by `commandp', `interactive-form', and dispatch.
pub(crate) fn callable_interactive_form_items(
    interp: &Interpreter,
    func: &Value,
) -> Option<Vec<Value>> {
    if let Kind::Record(id) = func.kind()
        && let Some(record) = interp.find_record(id)
        && record.kind == crate::lisp::eval::RecordKind::ModuleFunction
    {
        return record
            .slots
            .get(1)?
            .to_vec()
            .ok()
            .filter(|items| !items.is_empty());
    }
    if let Kind::NativeFunction(function) = func.kind() {
        let spec = function.interactive();
        return (!spec.is_nil()).then(|| spec.to_vec().ok()).flatten();
    }
    // callint.c's cons-lambda branch: a spec without MODES entries is
    // answered verbatim (`(interactive)' stays bare, `(interactive "p")'
    // unchanged); only a spec carrying modes is trimmed to
    // `(interactive DESCRIPTOR)'.
    interactive_form_items(func).map(|items| {
        if items.len() <= 2 {
            items
        } else {
            vec![
                Value::symbol("interactive"),
                items.get(1).cloned().unwrap_or(Value::Nil),
            ]
        }
    })
}

fn interactive_form_in_body(body: &[Value]) -> Option<Vec<Value>> {
    for form in body.iter() {
        if matches!(form.kind(), Kind::StringObject(_)) {
            continue;
        }
        // Internal evaluator closure markers precede the interactive form
        // in lowered bodies.
        if matches!(form.kind(), Kind::Symbol(marker) if marker.as_str().starts_with(":closure-")) {
            continue;
        }
        if is_declare_form(form) {
            continue;
        }
        let Ok(items) = form.to_vec() else {
            break;
        };
        if matches!(items.first().map(|v| v.kind()), Some(Kind::Symbol(name)) if name.is_named("interactive"))
        {
            return Some(items);
        }
        break;
    }
    None
}
