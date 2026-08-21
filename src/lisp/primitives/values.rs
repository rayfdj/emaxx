use super::*;

pub fn buffer_undo_list_value(buffer: &crate::buffer::Buffer) -> Value {
    buffer.undo_list_value()
}

pub(crate) fn values_equal(interp: &Interpreter, left: &Value, right: &Value) -> bool {
    // Scalar fast paths: identical outcome to the recursive walk below, but
    // without paying for a fresh seen-set and the aggregate-type probes.
    match (left, right) {
        (Value::Integer(a), Value::Integer(b)) => return a == b,
        (Value::Symbol(a), Value::Symbol(b)) => return a == b,
        (Value::Nil, Value::Nil) | (Value::T, Value::T) => return true,
        (Value::Nil | Value::T, Value::Integer(_)) | (Value::Integer(_), Value::Nil | Value::T) => {
            return false;
        }
        _ => {}
    }
    values_equal_recursive(interp, left, right, &mut HashSet::new())
}

pub(crate) fn keymap_records_equal(
    interp: &Interpreter,
    left_id: u64,
    right_id: u64,
    seen: &mut HashSet<(usize, usize)>,
) -> bool {
    let pair = (left_id as usize, right_id as usize);
    if !seen.insert(pair) {
        return true;
    }

    let left_value = Value::Record(left_id);
    let right_value = Value::Record(right_id);
    let Ok(Some(left_items)) = keymap_list_items(interp, &left_value) else {
        return false;
    };
    let Ok(Some(right_items)) = keymap_list_items(interp, &right_value) else {
        return false;
    };
    if left_items.len() != right_items.len()
        || !left_items
            .iter()
            .zip(right_items.iter())
            .all(|(left, right)| values_equal_recursive(interp, left, right, seen))
    {
        return false;
    }

    let left_parent = interp
        .find_record(left_id)
        .and_then(|record| record.slots.get(KEYMAP_PARENT_SLOT).cloned())
        .unwrap_or(Value::Nil);
    let right_parent = interp
        .find_record(right_id)
        .and_then(|record| record.slots.get(KEYMAP_PARENT_SLOT).cloned())
        .unwrap_or(Value::Nil);
    values_equal_recursive(interp, &left_parent, &right_parent, seen)
}

fn char_tables_equal(
    interp: &Interpreter,
    left_id: u64,
    right_id: u64,
    seen: &mut HashSet<(usize, usize)>,
) -> bool {
    if left_id == right_id {
        return true;
    }
    // Keep char-table recursion keys disjoint from the small numeric record
    // IDs used elsewhere in this equality walk.
    let pair = (
        (left_id as usize) ^ usize::MAX,
        (right_id as usize) ^ usize::MAX,
    );
    if !seen.insert(pair) {
        return true;
    }
    let (Some(left), Some(right)) = (
        interp.find_char_table(left_id),
        interp.find_char_table(right_id),
    ) else {
        return false;
    };
    left.subtype == right.subtype
        && values_equal_recursive(interp, &left.default, &right.default, seen)
        && left.extra_slots.len() == right.extra_slots.len()
        && left
            .extra_slots
            .iter()
            .zip(&right.extra_slots)
            .all(|(left, right)| values_equal_recursive(interp, left, right, seen))
        && left.entries.len() == right.entries.len()
        && left
            .entries
            .iter()
            .zip(&right.entries)
            .all(|(left_entry, right_entry)| {
                left_entry.start == right_entry.start
                    && left_entry.end == right_entry.end
                    && values_equal_recursive(interp, &left_entry.value, &right_entry.value, seen)
            })
        && left.category_docs == right.category_docs
        && match (left.parent, right.parent) {
            (None, None) => true,
            (Some(left), Some(right)) => char_tables_equal(interp, left, right, seen),
            _ => false,
        }
}

pub(crate) fn keymap_record_equals_list(
    interp: &Interpreter,
    keymap_id: u64,
    list: &Value,
    seen: &mut HashSet<(usize, usize)>,
) -> bool {
    let keymap_value = Value::Record(keymap_id);
    let Ok(Some(items)) = keymap_list_items(interp, &keymap_value) else {
        return false;
    };
    let Ok(list_items) = list.to_vec() else {
        return false;
    };
    items.len() == list_items.len()
        && items
            .iter()
            .zip(list_items.iter())
            .all(|(left, right)| values_equal_recursive(interp, left, right, seen))
}

pub(crate) fn record_equals_record_literal_form(
    interp: &Interpreter,
    record_id: u64,
    form: &Value,
    seen: &mut HashSet<(usize, usize)>,
) -> bool {
    let Some(record) = interp.find_record(record_id) else {
        return false;
    };
    if record.kind != crate::lisp::eval::RecordKind::Record {
        return false;
    }
    let Some(items) = record_literal_items(form) else {
        return false;
    };
    if let Some((car, _)) = form.cons_cells() {
        let pair = (record_id as usize, car.cell_id());
        if !seen.insert(pair) {
            return true;
        }
    }

    let expected_fields = std::iter::once(record.type_tag.clone())
        .chain(record.slots.iter().cloned())
        .collect::<Vec<_>>();
    let actual_fields = &items[1..];

    expected_fields.len() == actual_fields.len()
        && expected_fields
            .iter()
            .zip(actual_fields.iter())
            .all(|(left, right)| {
                let right_value = record_literal_slot_data(right);
                values_equal_recursive(interp, left, &right_value, seen)
            })
}

pub(crate) fn values_equal_recursive(
    interp: &Interpreter,
    left: &Value,
    right: &Value,
    seen: &mut HashSet<(usize, usize)>,
) -> bool {
    if let (Some(left_string), Some(right_string)) = (string_like(left), string_like(right)) {
        return left_string.text == right_string.text
            && left_string.extended_chars == right_string.extended_chars;
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
        let (Some((left_car, _)), Some((right_car, _))) = (left.cons_cells(), right.cons_cells())
        else {
            return false;
        };
        if !seen.insert((left_car.cell_id(), right_car.cell_id())) {
            return true;
        }
        let (Ok(left_items), Ok(right_items)) = (vector_items(left), vector_items(right)) else {
            return false;
        };
        return left_items.len() == right_items.len()
            && left_items
                .iter()
                .zip(right_items.iter())
                .all(|(left, right)| values_equal_recursive(interp, left, right, seen));
    }
    match (left, right) {
        (Value::Nil, Value::Nil) | (Value::T, Value::T) => true,
        (Value::Integer(a), Value::Integer(b)) => a == b,
        (Value::BigInteger(a), Value::BigInteger(b)) => a == b,
        (Value::Integer(a), Value::BigInteger(b)) | (Value::BigInteger(b), Value::Integer(a)) => {
            BigInt::from(*a) == **b
        }
        (Value::Float(a), Value::Float(b)) => a == b || (a.is_nan() && b.is_nan()),
        (Value::String(a), Value::String(b)) => a == b,
        (Value::StringObject(a), Value::StringObject(b)) => {
            let a = a.borrow();
            let b = b.borrow();
            a.text == b.text && a.extended_chars == b.extended_chars
        }
        (Value::String(a), Value::StringObject(b)) => {
            let b = b.borrow();
            b.extended_chars.is_empty() && a.as_str() == b.text
        }
        (Value::StringObject(a), Value::String(b)) => {
            let a = a.borrow();
            a.extended_chars.is_empty() && a.text == b.as_str()
        }
        (Value::Symbol(a), Value::Symbol(b)) => a == b,
        (Value::BuiltinFunc(a), Value::BuiltinFunc(b)) => a == b,
        (Value::Buffer(a), Value::Buffer(b)) => a.id == b.id,
        (Value::Marker(a), Value::Marker(b)) => markers_equal(interp, *a, *b),
        (Value::Overlay(a), Value::Overlay(b)) => overlays_equal(interp, *a, *b),
        (Value::CharTable(left_id), Value::CharTable(right_id)) => {
            char_tables_equal(interp, *left_id, *right_id, seen)
        }
        (Value::Frame(left_id), Value::Frame(right_id)) => left_id == right_id,
        (Value::Terminal(left_id), Value::Terminal(right_id)) => left_id == right_id,
        (Value::Record(left_id), Value::Record(right_id))
            if interp
                .find_record(*left_id)
                .is_some_and(|record| record.kind == crate::lisp::eval::RecordKind::Keymap)
                && interp
                    .find_record(*right_id)
                    .is_some_and(|record| record.kind == crate::lisp::eval::RecordKind::Keymap) =>
        {
            keymap_records_equal(interp, *left_id, *right_id, seen)
        }
        (Value::Record(left_id), Value::Cons(_))
            if interp
                .find_record(*left_id)
                .is_some_and(|record| record.kind == crate::lisp::eval::RecordKind::Keymap) =>
        {
            keymap_record_equals_list(interp, *left_id, right, seen)
        }
        (Value::Cons(_), Value::Record(right_id))
            if interp
                .find_record(*right_id)
                .is_some_and(|record| record.kind == crate::lisp::eval::RecordKind::Keymap) =>
        {
            keymap_record_equals_list(interp, *right_id, left, seen)
        }
        (Value::Record(left_id), Value::Record(right_id)) => {
            if left_id == right_id {
                return true;
            }
            let (Some(left_record), Some(right_record)) =
                (interp.find_record(*left_id), interp.find_record(*right_id))
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
                    | crate::lisp::eval::RecordKind::HashTable
                    | crate::lisp::eval::RecordKind::Obarray
                    | crate::lisp::eval::RecordKind::Window
                    | crate::lisp::eval::RecordKind::WindowConfiguration
                    | crate::lisp::eval::RecordKind::Thread
                    | crate::lisp::eval::RecordKind::Mutex
                    | crate::lisp::eval::RecordKind::ConditionVariable
                    | crate::lisp::eval::RecordKind::NativeCompUnit
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
            let pair = (*left_id as usize, *right_id as usize);
            if !seen.insert(pair) {
                return true;
            }
            values_equal_recursive(interp, &left_record.type_tag, &right_record.type_tag, seen)
                && left_record.slots.len() == right_record.slots.len()
                && left_record
                    .slots
                    .iter()
                    .zip(right_record.slots.iter())
                    .all(|(left, right)| values_equal_recursive(interp, left, right, seen))
        }
        (Value::Record(left_id), _) if record_literal_items(right).is_some() => {
            record_equals_record_literal_form(interp, *left_id, right, seen)
        }
        (_, Value::Record(right_id)) if record_literal_items(left).is_some() => {
            record_equals_record_literal_form(interp, *right_id, left, seen)
        }
        (Value::Cons(_), Value::Cons(_)) => {
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
            values_equal_recursive(interp, &a_car, &b_car, seen)
                && values_equal_recursive(interp, &a_cdr, &b_cdr, seen)
        }
        (Value::Lambda(left), Value::Lambda(right)) => {
            let left_ptr = Rc::as_ptr(left) as usize;
            let right_ptr = Rc::as_ptr(right) as usize;
            if left_ptr == right_ptr || !seen.insert((left_ptr, right_ptr)) {
                return true;
            }
            let left_slots = interp.interpreted_closure_slots(left);
            let right_slots = interp.interpreted_closure_slots(right);
            if left_slots.len() != right_slots.len() {
                return false;
            }
            left_slots
                .iter()
                .zip(right_slots.iter())
                .all(|(left, right)| values_equal_recursive(interp, left, right, seen))
        }
        _ => left == right,
    }
}

pub(crate) fn values_eql(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Nil, Value::Nil) | (Value::T, Value::T) => true,
        (Value::Integer(a), Value::Integer(b)) => a == b,
        (Value::BigInteger(a), Value::BigInteger(b)) => a == b,
        (Value::Float(a), Value::Float(b)) => a == b,
        (Value::Symbol(a), Value::Symbol(b)) => a == b,
        (Value::BuiltinFunc(a), Value::BuiltinFunc(b)) => a == b,
        (Value::String(left), Value::String(right)) => left.ptr_eq(right),
        (Value::StringObject(left), Value::StringObject(right)) => Rc::ptr_eq(left, right),
        (Value::Cons(left), Value::Cons(right)) => Rc::ptr_eq(left, right),
        (Value::Lambda(left), Value::Lambda(right)) => Rc::ptr_eq(left, right),
        (Value::Buffer(left), Value::Buffer(right)) => left.id == right.id,
        (Value::Marker(left_id), Value::Marker(right_id))
        | (Value::Overlay(left_id), Value::Overlay(right_id))
        | (Value::CharTable(left_id), Value::CharTable(right_id))
        | (Value::Frame(left_id), Value::Frame(right_id))
        | (Value::Terminal(left_id), Value::Terminal(right_id))
        | (Value::Record(left_id), Value::Record(right_id))
        | (Value::Finalizer(left_id), Value::Finalizer(right_id)) => left_id == right_id,
        _ => false,
    }
}

pub(crate) fn values_eq_in_env(
    interp: &Interpreter,
    left: &Value,
    right: &Value,
    env: &Env,
) -> bool {
    // Only records can carry symbol-with-pos payloads; every other pair
    // (the overwhelmingly common case) must not pay the outlined probes.
    if (matches!(left, Value::Record(_)) || matches!(right, Value::Record(_)))
        && let Some(equal) = symbol_with_pos_eq_in_env(interp, left, right, env)
    {
        return equal;
    }

    match (left, right) {
        (Value::Nil, Value::Nil) | (Value::T, Value::T) => true,
        (Value::Integer(a), Value::Integer(b)) => a == b,
        (Value::BigInteger(a), Value::BigInteger(b)) => a == b,
        (Value::Float(a), Value::Float(b)) => a == b,
        (Value::Symbol(a), Value::Symbol(b)) => a == b,
        (Value::BuiltinFunc(a), Value::BuiltinFunc(b)) => a == b,
        (Value::String(left), Value::String(right)) => left.ptr_eq(right),
        (Value::StringObject(left), Value::StringObject(right)) => Rc::ptr_eq(left, right),
        (Value::String(_), Value::StringObject(_)) | (Value::StringObject(_), Value::String(_)) => {
            false
        }
        (Value::Cons(left), Value::Cons(right)) => Rc::ptr_eq(left, right),
        (Value::Lambda(left), Value::Lambda(right)) => Rc::ptr_eq(left, right),
        (Value::Buffer(left), Value::Buffer(right)) => left.id == right.id,
        (Value::Marker(left_id), Value::Marker(right_id))
        | (Value::Overlay(left_id), Value::Overlay(right_id))
        | (Value::CharTable(left_id), Value::CharTable(right_id))
        | (Value::Frame(left_id), Value::Frame(right_id))
        | (Value::Terminal(left_id), Value::Terminal(right_id))
        | (Value::Record(left_id), Value::Record(right_id))
        | (Value::Finalizer(left_id), Value::Finalizer(right_id)) => left_id == right_id,
        _ => false,
    }
}

pub(crate) fn plist_type_error(plist: &Value) -> LispError {
    LispError::SignalValue(Value::list([
        Value::Symbol("wrong-type-argument".into()),
        Value::Symbol("plistp".into()),
        plist.clone(),
    ]))
}

pub(crate) fn safe_list_length(list: &Value) -> i64 {
    let mut len = 0i64;
    let mut current = list.clone();
    let mut seen = crate::lisp::types::CycleGuard::new();
    loop {
        match current {
            Value::Cons(cons_cell) => {
                let cdr = &cons_cell.cdr;
                let cell_id = crate::lisp::types::ConsCell::identity(&cons_cell);
                if seen.step(cell_id) {
                    return len;
                }
                len += 1;
                current = cdr.borrow().clone();
            }
            Value::Nil => return len,
            _ => return len,
        }
    }
}

pub(crate) fn nthcdr_value(count: &Value, list: &Value) -> Result<Value, LispError> {
    let mut remaining = match count {
        Value::Integer(n) => BigInt::from(*n),
        Value::BigInteger(n) => n.clone().into(),
        _ => return Err(LispError::TypeError("integer".into(), count.type_name())),
    };

    if remaining <= BigInt::zero() {
        return Ok(list.clone());
    }

    let mut current = list.clone();
    let mut visited = HashMap::new();
    let mut steps = 0usize;

    loop {
        if remaining.is_zero() {
            return Ok(current);
        }

        match current.clone() {
            Value::Nil => return Ok(Value::Nil),
            Value::Cons(cons_cell) => {
                let cdr = &cons_cell.cdr;
                let cell_id = crate::lisp::types::ConsCell::identity(&cons_cell);
                if let Some(&cycle_start) = visited.get(&cell_id) {
                    let cycle_len = steps.saturating_sub(cycle_start);
                    if cycle_len > 0 {
                        remaining %= BigInt::from(cycle_len);
                        if remaining.is_zero() {
                            return Ok(current);
                        }
                    }
                } else {
                    visited.insert(cell_id, steps);
                }

                remaining -= 1;
                steps += 1;
                current = cdr.borrow().clone();
            }
            other => return Err(wrong_type_argument("listp", other)),
        }
    }
}

pub(crate) fn sequence_length_value(interp: &Interpreter, value: &Value) -> Result<i64, LispError> {
    if let Some(items) = keymap_list_items(interp, value)? {
        return Ok(items.len() as i64);
    }
    if let Some(items) = record_literal_items(value) {
        return Ok(items.len().saturating_sub(1) as i64);
    }
    match value {
        item if string_like(item).is_some() => Ok(string_text(item)?.chars().count() as i64),
        Value::Nil => Ok(0),
        Value::Cons(_) if is_vector_value(value) => Ok(vector_items(value)?.len() as i64),
        Value::CharTable(_) => Ok(0x40_0000),
        item if is_bool_vector_value(interp, item) => {
            Ok(bool_vector_values(interp, item)?.len() as i64)
        }
        Value::Lambda(lambda) => Ok(lambda.public_len() as i64),
        Value::Cons(_) => Ok(value.to_vec()?.len() as i64),
        Value::Record(id) => {
            let record = interp
                .find_record(*id)
                .ok_or_else(|| LispError::TypeError("record".into(), format!("record<{id}>")))?;
            match record.kind {
                // GNU records carry their type tag in public slot zero;
                // Emaxx stores that tag separately from `slots'.
                crate::lisp::eval::RecordKind::Record => Ok((record.slots.len() + 1) as i64),
                // GNU Lisp_Closure slots already start at CLOSURE_ARGLIST and
                // have no public type-tag slot (lisp.h, enum Lisp_Closure).
                crate::lisp::eval::RecordKind::Closure => Ok(record.slots.len() as i64),
                // Other RecordKind variants are host storage for distinct GNU
                // pseudovectors.  Flength accepts none of them here; bool
                // vectors and keymaps were projected through their GNU public
                // sequence representations above.
                _ => Err(LispError::TypeError("sequence".into(), value.type_name())),
            }
        }
        _ => Err(LispError::TypeError("sequence".into(), value.type_name())),
    }
}

fn text_property_plists_equal_including_properties(
    left: &[(String, Value)],
    right: &[(String, Value)],
    seen: &mut HashSet<(usize, usize)>,
) -> bool {
    left.len() == right.len()
        && left.iter().all(|(key, left_value)| {
            right.iter().any(|(right_key, right_value)| {
                right_key == key
                    && values_equal_including_properties_recursive(left_value, right_value, seen)
            })
        })
}

pub(crate) fn values_equal_including_properties(left: &Value, right: &Value) -> bool {
    values_equal_including_properties_recursive(left, right, &mut HashSet::new())
}

pub(crate) fn values_equal_including_properties_recursive(
    left: &Value,
    right: &Value,
    seen: &mut HashSet<(usize, usize)>,
) -> bool {
    if let (Some(left_string), Some(right_string)) = (string_like(left), string_like(right)) {
        // GNU's compare_string_intervals walks POSITIONS, so interval
        // segmentation is not significant, and plists within a span
        // compare as sets (intervals_equal in intervals.c).
        if left_string.text != right_string.text
            || left_string.extended_chars != right_string.extended_chars
        {
            return false;
        }
        let len = left_string.text.chars().count();
        let collect_props = |string: &StringLike, pos: usize| {
            let mut out: Vec<(String, Value)> = Vec::new();
            for span in &string.props {
                if span.start <= pos && pos < span.end {
                    for (key, value) in &span.props {
                        if !out.iter().any(|(existing, _)| existing == key) {
                            out.push((key.clone(), value.clone()));
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
            if !text_property_plists_equal_including_properties(
                &collect_props(&left_string, pos),
                &collect_props(&right_string, pos),
                seen,
            ) {
                return false;
            }
        }
        return true;
    }
    if let (Ok(left_items), Ok(right_items)) = (vector_items(left), vector_items(right))
        && matches!(left, Value::Cons(_))
        && matches!(right, Value::Cons(_))
    {
        return left_items.len() == right_items.len()
            && left_items
                .iter()
                .zip(right_items.iter())
                .all(|(left, right)| {
                    values_equal_including_properties_recursive(left, right, seen)
                });
    }
    match (left, right) {
        (Value::Nil, Value::Nil) | (Value::T, Value::T) => true,
        (Value::Integer(a), Value::Integer(b)) => a == b,
        (Value::BigInteger(a), Value::BigInteger(b)) => a == b,
        (Value::Integer(a), Value::BigInteger(b)) | (Value::BigInteger(b), Value::Integer(a)) => {
            &BigInt::from(*a) == b
        }
        (Value::Float(a), Value::Float(b)) => a == b || (a.is_nan() && b.is_nan()),
        (Value::Symbol(a), Value::Symbol(b)) => a == b,
        (Value::Cons(_), Value::Cons(_)) => {
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
            values_equal_including_properties_recursive(&a_car, &b_car, seen)
                && values_equal_including_properties_recursive(&a_cdr, &b_cdr, seen)
        }
        _ => left == right,
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
        left.clone(),
        right.clone(),
    ]))
}

pub(crate) fn circular_signal(value: &Value) -> LispError {
    LispError::SignalValue(Value::list([
        Value::Symbol("circular".into()),
        value.clone(),
    ]))
}

pub(crate) fn is_number_value(value: &Value) -> bool {
    matches!(
        value,
        Value::Integer(_) | Value::BigInteger(_) | Value::Float(_)
    )
}

pub(crate) fn plain_symbol_name(value: &Value) -> Option<&str> {
    match value {
        Value::Nil => Some("nil"),
        Value::T => Some("t"),
        Value::Symbol(symbol) => Some(symbol),
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
    env: &Env,
) -> Result<Option<ValueOrder>, LispError> {
    let symbols_with_pos_enabled = interp
        .lookup_var("symbols-with-pos-enabled", env)
        .is_some_and(|value| value.is_truthy());
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

pub(crate) fn compare_buffer_ids(interp: &Interpreter, left_id: u64, right_id: u64) -> ValueOrder {
    match (
        interp.has_buffer_id(left_id),
        interp.has_buffer_id(right_id),
    ) {
        (true, true) => order_from_ordering(left_id.cmp(&right_id)),
        (false, true) => ValueOrder::Less,
        (true, false) => ValueOrder::Greater,
        (false, false) => ValueOrder::Unordered,
    }
}

pub(crate) fn compare_record_values(
    interp: &Interpreter,
    left: &Value,
    right: &Value,
    env: &Env,
    seen_lists: &mut HashSet<(usize, usize)>,
) -> Result<Option<ValueOrder>, LispError> {
    match (left, right) {
        (Value::Record(_), _) | (_, Value::Record(_))
            if record_type_name(interp, left) == Some("symbol-with-pos")
                || record_type_name(interp, right) == Some("symbol-with-pos") =>
        {
            return compare_symbol_values(interp, left, right, env);
        }
        (Value::Record(_), _) | (_, Value::Record(_)) => {}
        _ => return Ok(None),
    }

    let (Value::Record(left_id), Value::Record(right_id)) = (left, right) else {
        return Err(type_mismatch_signal(left, right));
    };
    let Some(left_record) = interp.find_record(*left_id) else {
        return Ok(Some(order_from_ordering(left_id.cmp(right_id))));
    };
    let Some(right_record) = interp.find_record(*right_id) else {
        return Ok(Some(order_from_ordering(left_id.cmp(right_id))));
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
                interp.process_name(*left_id),
                interp.process_name(*right_id),
            ) {
                (Some(left), Some(right)) => compare_plain_symbol_names(&left, &right),
                _ => ValueOrder::Unordered,
            },
        )),
        crate::lisp::eval::RecordKind::Record | crate::lisp::eval::RecordKind::Keymap => {
            match value_ordering(
                interp,
                &left_record.type_tag,
                &right_record.type_tag,
                env,
                seen_lists,
            )? {
                ValueOrder::Less => return Ok(Some(ValueOrder::Less)),
                ValueOrder::Greater => return Ok(Some(ValueOrder::Greater)),
                ValueOrder::Equal | ValueOrder::Unordered => {}
            }
            Ok(Some(compare_sequence_values(
                interp,
                &left_record.slots,
                &right_record.slots,
                env,
                seen_lists,
            )?))
        }
        crate::lisp::eval::RecordKind::Closure
        | crate::lisp::eval::RecordKind::Font
        | crate::lisp::eval::RecordKind::SymbolWithPos
        | crate::lisp::eval::RecordKind::HashTable
        | crate::lisp::eval::RecordKind::Obarray
        | crate::lisp::eval::RecordKind::Window
        | crate::lisp::eval::RecordKind::WindowConfiguration
        | crate::lisp::eval::RecordKind::Thread
        | crate::lisp::eval::RecordKind::Mutex
        | crate::lisp::eval::RecordKind::ConditionVariable
        | crate::lisp::eval::RecordKind::NativeCompUnit
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
            return Ok(order_from_option(numeric_ordering(interp, left, right)?));
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

    if matches!(left, Value::CharTable(_)) || matches!(right, Value::CharTable(_)) {
        return if matches!((left, right), (Value::CharTable(_), Value::CharTable(_))) {
            Ok(ValueOrder::Unordered)
        } else {
            Err(type_mismatch_signal(left, right))
        };
    }

    if matches!(left, Value::Buffer(_)) || matches!(right, Value::Buffer(_)) {
        return match (left, right) {
            (Value::Buffer(left), Value::Buffer(right)) => {
                Ok(compare_buffer_ids(interp, left.id, right.id))
            }
            _ => Err(type_mismatch_signal(left, right)),
        };
    }

    if matches!(left, Value::Marker(_)) || matches!(right, Value::Marker(_)) {
        return match (left, right) {
            (Value::Marker(left_id), Value::Marker(right_id)) => {
                let Some(left_buffer) = interp.marker_buffer_id(*left_id) else {
                    return Ok(ValueOrder::Unordered);
                };
                let Some(right_buffer) = interp.marker_buffer_id(*right_id) else {
                    return Ok(ValueOrder::Unordered);
                };
                let buffer_order = compare_buffer_ids(interp, left_buffer, right_buffer);
                if buffer_order != ValueOrder::Equal {
                    return Ok(buffer_order);
                }
                let Some(left_pos) = interp.marker_position(*left_id) else {
                    return Ok(ValueOrder::Unordered);
                };
                let Some(right_pos) = interp.marker_position(*right_id) else {
                    return Ok(ValueOrder::Unordered);
                };
                Ok(order_from_ordering(left_pos.cmp(&right_pos)))
            }
            _ => Err(type_mismatch_signal(left, right)),
        };
    }

    if matches!(left, Value::Cons(_)) || matches!(right, Value::Cons(_)) {
        return match (left, right) {
            (Value::Nil, Value::Nil) => Ok(ValueOrder::Equal),
            (Value::Nil, Value::Cons(_)) => Ok(ValueOrder::Less),
            (Value::Cons(_), Value::Nil) => Ok(ValueOrder::Greater),
            (Value::Cons(left_cell), Value::Cons(right_cell)) => {
                let key = (
                    crate::lisp::types::ConsCell::identity(left_cell),
                    crate::lisp::types::ConsCell::identity(right_cell),
                );
                if !seen_lists.insert(key) {
                    return Err(circular_signal(left));
                }
                let result = (|| {
                    let left_head = left_cell.car.borrow().clone();
                    let left_tail = left_cell.cdr.borrow().clone();
                    let right_head = right_cell.car.borrow().clone();
                    let right_tail = right_cell.cdr.borrow().clone();
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

pub(crate) fn proper_list_length(value: &Value) -> Option<usize> {
    if matches!(value, Value::Nil) {
        return Some(0);
    }
    if !matches!(value, Value::Cons(_)) || is_vector_value(value) {
        return None;
    }
    match value.to_vec() {
        Ok(items) => Some(items.len()),
        Err(LispError::TypeError(expected, _)) if expected == "list" => None,
        Err(LispError::SignalValue(signal)) if circular_list_signal_p(&signal) => None,
        Err(_) => None,
    }
}

pub(crate) fn circular_list_signal_p(value: &Value) -> bool {
    value
        .to_vec()
        .ok()
        .and_then(|items| items.first().cloned())
        .and_then(|head| head.as_symbol().ok().map(str::to_string))
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

    match sequence {
        Value::Nil | Value::Cons(_) => Ok(Value::list(
            sequence
                .to_vec()?
                .into_iter()
                .filter(|item| !values_equal(interp, item, elt))
                .collect::<Vec<_>>(),
        )),
        _ => Err(LispError::TypeError(
            "sequence".into(),
            sequence.type_name(),
        )),
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
                tail_cell.set_cdr(value.clone())?;
                return Ok(head.unwrap_or_else(|| value.clone()));
            }
            return Ok(value.clone());
        }

        if value.is_nil() {
            continue;
        }

        let last_cell = last_nconc_cell(value)?;
        if let Some(tail_cell) = &tail {
            tail_cell.set_cdr(value.clone())?;
        } else {
            head = Some(value.clone());
        }
        tail = Some(last_cell);
    }

    Ok(head.unwrap_or(Value::Nil))
}

pub(crate) fn copy_alist_value(value: &Value) -> Result<Value, LispError> {
    if string_like(value).is_some() || is_vector_value(value) {
        return Err(LispError::TypeError("list".into(), value.type_name()));
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
    let after_method = &rest[method_end + 1..];
    let host_end = after_method.find(':')?;
    let authority = &after_method[..host_end];
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
    let mut current = value.clone();
    let mut seen = crate::lisp::types::CycleGuard::new();
    loop {
        let Some((car, cdr)) = (current.clone()).cons_cells() else {
            return Err(LispError::TypeError("consp".into(), current.type_name()));
        };
        let cell_id = car.cell_id();
        if seen.step(cell_id) {
            return Err(LispError::SignalValue(Value::list([
                Value::symbol("circular-list"),
                Value::string("Circular list"),
            ])));
        }
        match cdr.borrow().clone() {
            Value::Cons(_) => current = cdr.borrow().clone(),
            _ => return Ok(current),
        }
    }
}

pub(crate) fn sxhash_value(interp: &Interpreter, value: &Value, mode: HashMode) -> i64 {
    let mut state = 0xcbf2_9ce4_8422_2325u64;
    hash_value_recursive(interp, &mut state, value, mode);
    (state & 0x7fff_ffff_ffff_ffff) as i64
}

/// Return the structural hash used to index a GNU `equal' hash table.
///
/// Some runtime objects can compare equal to a different representation
/// (keymap records and their public list projection, for example), and cyclic
/// cons graphs need bounded traversal.  Keep those values in a collision
/// bucket.  Ordinary source forms are acyclic cons trees of scalar values, so
/// they retain the O(1)-bucket behavior of GNU's native hash tables.
pub(crate) fn equal_hash_table_key_hash(interp: &Interpreter, value: &Value) -> Option<i64> {
    fn indexable(
        value: &Value,
        visiting: &mut HashSet<usize>,
        visited: &mut HashSet<usize>,
    ) -> bool {
        match value {
            Value::Record(_)
            | Value::Buffer(_)
            | Value::Marker(_)
            | Value::Overlay(_)
            | Value::CharTable(_)
            | Value::Lambda(_)
            | Value::ReaderForm(_) => false,
            Value::Cons(cons_cell) => {
                let car = &cons_cell.car;
                let cdr = &cons_cell.cdr;
                let identity = crate::lisp::types::ConsCell::identity(cons_cell);
                if visited.contains(&identity) {
                    return true;
                }
                if !visiting.insert(identity) {
                    return false;
                }
                let car_value = car.borrow();
                if matches!(
                    &*car_value,
                    Value::Symbol(symbol) if symbol == "keymap"
                ) {
                    visiting.remove(&identity);
                    return false;
                }
                let result = indexable(&car_value, visiting, visited)
                    && indexable(&cdr.borrow(), visiting, visited);
                visiting.remove(&identity);
                if result {
                    visited.insert(identity);
                }
                result
            }
            _ => true,
        }
    }

    indexable(value, &mut HashSet::new(), &mut HashSet::new())
        .then(|| sxhash_value(interp, value, HashMode::Equal))
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
        return equal_hash_table_key_hash(interp, value);
    }
    let mut state = 0xcbf2_9ce4_8422_2325u64;
    if matches!(value, Value::Record(_))
        && let Some((symbol, _)) = symbol_with_pos_parts(interp, value)
    {
        hash_value_eq(&mut state, &symbol);
        return Some(state as i64);
    }
    match value {
        Value::Cons(cons_cell) => {
            hash_mix(&mut state, 16);
            hash_mix(
                &mut state,
                crate::lisp::types::ConsCell::identity(cons_cell) as u64,
            );
        }
        Value::Float(number) => {
            hash_mix(&mut state, 15);
            let normalized = if *number == 0.0 { 0.0f64 } else { *number };
            hash_mix(&mut state, normalized.to_bits());
        }
        Value::BigInteger(number) => {
            hash_mix(&mut state, 14);
            hash_str(&mut state, &number.to_string());
        }
        other => match test {
            RuntimeHashTest::Eql => hash_value_eql(&mut state, other),
            _ => hash_value_eq(&mut state, other),
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
) {
    hash_mix(state, props.len() as u64);
    for span in props {
        hash_mix(state, span.start as u64);
        hash_mix(state, span.end as u64);
        hash_mix(state, span.props.len() as u64);
        for (key, value) in &span.props {
            hash_str(state, key);
            hash_value_equal_at(interp, state, value, true, depth + 1);
        }
    }
}

pub(crate) fn hash_value_recursive(
    interp: &Interpreter,
    state: &mut u64,
    value: &Value,
    mode: HashMode,
) {
    match mode {
        HashMode::Eq => hash_value_eq(state, value),
        HashMode::Eql => hash_value_eql(state, value),
        HashMode::Equal | HashMode::EqualIncludingProperties => hash_value_equal(
            interp,
            state,
            value,
            mode == HashMode::EqualIncludingProperties,
        ),
    }
}

pub(crate) fn hash_value_eq(state: &mut u64, value: &Value) {
    match value {
        Value::Nil => hash_mix(state, 0),
        Value::T => hash_mix(state, 1),
        Value::Integer(number) => {
            hash_mix(state, 2);
            hash_mix(state, *number as u64);
        }
        Value::Symbol(symbol) => {
            hash_mix(state, 3);
            hash_str(state, symbol);
        }
        Value::StringObject(shared) => {
            hash_mix(state, 4);
            hash_mix(state, Rc::as_ptr(shared) as usize as u64);
        }
        Value::String(text) => {
            hash_mix(state, 5);
            hash_mix(state, text.as_ptr() as usize as u64);
            hash_mix(state, text.len() as u64);
        }
        Value::BuiltinFunc(name) => {
            hash_mix(state, 6);
            hash_str(state, name);
        }
        Value::Lambda(lambda_value) => {
            let _ = &lambda_value.params;
            let _ = &lambda_value.body;
            let env = &lambda_value.env;
            hash_mix(state, 7);
            hash_mix(state, Rc::as_ptr(env) as usize as u64);
        }
        Value::Buffer(buffer_value) => {
            let id = buffer_value.id;
            let _ = &buffer_value.name;
            hash_mix(state, 8);
            hash_mix(state, id);
        }
        Value::Marker(id) => {
            hash_mix(state, 9);
            hash_mix(state, *id);
        }
        Value::Overlay(id) => {
            hash_mix(state, 10);
            hash_mix(state, *id);
        }
        Value::CharTable(id) => {
            hash_mix(state, 11);
            hash_mix(state, *id);
        }
        Value::Frame(id) => {
            hash_mix(state, 18);
            hash_mix(state, *id);
        }
        Value::Terminal(id) => {
            hash_mix(state, 19);
            hash_mix(state, *id);
        }
        Value::Record(id) => {
            hash_mix(state, 12);
            hash_mix(state, *id);
        }
        Value::Finalizer(id) => {
            hash_mix(state, 13);
            hash_mix(state, *id);
        }
        Value::ReaderForm(form) => {
            hash_mix(state, 20);
            hash_mix(state, Rc::as_ptr(form) as usize as u64);
        }
        Value::Unbound => {
            hash_mix(state, 17);
        }
        Value::BigInteger(number) => {
            hash_mix(state, 14);
            hash_str(state, &number.to_string());
        }
        Value::Float(number) => {
            hash_mix(state, 15);
            hash_mix(state, number.to_bits());
        }
        Value::Cons(_) => {
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
    match value {
        Value::Float(number) => {
            hash_mix(state, 21);
            hash_mix(state, number.to_bits());
        }
        Value::BigInteger(number) => {
            hash_mix(state, 22);
            hash_str(state, &number.to_string());
        }
        other => hash_value_eq(state, other),
    }
}

pub(crate) fn hash_value_equal(
    interp: &Interpreter,
    state: &mut u64,
    value: &Value,
    include_properties: bool,
) {
    hash_value_equal_at(interp, state, value, include_properties, 0);
}

/// `sxhash_obj' (fns.c:5505) with GNU's depth bound: past `SXHASH_MAX_DEPTH'
/// every object hashes as 0 regardless of its contents.
pub(crate) fn hash_value_equal_at(
    interp: &Interpreter,
    state: &mut u64,
    value: &Value,
    include_properties: bool,
    depth: u32,
) {
    if depth > SXHASH_MAX_DEPTH {
        hash_mix(state, 0);
        return;
    }
    match value {
        Value::Nil => hash_mix(state, 30),
        Value::T => hash_mix(state, 31),
        Value::Integer(number) => {
            hash_mix(state, 32);
            hash_str(state, &number.to_string());
        }
        Value::BigInteger(number) => {
            hash_mix(state, 32);
            hash_str(state, &number.to_string());
        }
        Value::Float(number) => {
            hash_mix(state, 34);
            let bits = if *number == 0.0 {
                0.0f64.to_bits()
            } else if number.is_nan() {
                f64::NAN.to_bits()
            } else {
                number.to_bits()
            };
            hash_mix(state, bits);
        }
        Value::String(text) => {
            hash_mix(state, 35);
            hash_str(state, text);
        }
        Value::StringObject(shared) => {
            hash_mix(state, 35);
            let shared = shared.borrow();
            hash_str(state, &shared.text);
            for (index, code) in &shared.extended_chars {
                hash_mix(state, *index as u64);
                hash_mix(state, u64::from(*code));
            }
            if include_properties {
                hash_props(interp, state, &shared.props, depth);
            }
        }
        Value::Symbol(symbol) => {
            hash_mix(state, 37);
            hash_str(state, symbol);
        }
        Value::Cons(_) => {
            hash_mix(state, 38);
            // `sxhash_list' (fns.c:5420): walk at most `SXHASH_MAX_LEN'
            // elements, and only while the structure is shallower than
            // `SXHASH_MAX_DEPTH'; whatever tail remains is folded in one
            // level deeper, which is what terminates a circular list.
            let mut tail = value.clone();
            if depth < SXHASH_MAX_DEPTH {
                for _ in 0..SXHASH_MAX_LEN {
                    let Some((car, cdr)) = tail.cons_values() else {
                        break;
                    };
                    hash_value_equal_at(interp, state, &car, include_properties, depth + 1);
                    tail = cdr;
                }
            }
            if !tail.is_nil() {
                hash_value_equal_at(interp, state, &tail, include_properties, depth + 1);
            }
        }
        Value::BuiltinFunc(name) => {
            hash_mix(state, 39);
            hash_str(state, name);
        }
        Value::Lambda(lambda_value) => {
            hash_mix(state, 40);
            // `sxhash_vector' (fns.c:5447) bounds a closure the same way.
            for slot in interp
                .interpreted_closure_slots(lambda_value)
                .into_iter()
                .take(SXHASH_MAX_LEN)
            {
                hash_value_equal_at(interp, state, &slot, include_properties, depth + 1);
            }
        }
        Value::Buffer(buffer_value) => {
            let id = buffer_value.id;
            let name = &buffer_value.name;
            hash_mix(state, 41);
            hash_mix(state, id);
            hash_str(state, name);
        }
        Value::Marker(id) => {
            hash_marker_equal(interp, state, *id);
        }
        Value::Overlay(id) => {
            hash_mix(state, 43);
            hash_mix(state, *id);
        }
        Value::CharTable(id) => {
            hash_char_table_equal(interp, state, *id, include_properties, depth);
        }
        Value::Frame(id) => {
            hash_mix(state, 48);
            hash_mix(state, *id);
        }
        Value::Terminal(id) => {
            hash_mix(state, 49);
            hash_mix(state, *id);
        }
        Value::Record(id) => {
            hash_record_equal(interp, state, *id, include_properties, depth);
        }
        Value::Finalizer(id) => {
            hash_mix(state, 46);
            hash_mix(state, *id);
        }
        Value::ReaderForm(form) => {
            hash_mix(state, 50);
            hash_mix(state, Rc::as_ptr(form) as usize as u64);
        }
        Value::Unbound => {
            hash_mix(state, 47);
        }
    }
}

pub(crate) fn hash_marker_equal(interp: &Interpreter, state: &mut u64, id: u64) {
    hash_mix(state, 42);
    match (interp.marker_buffer_id(id), interp.marker_position(id)) {
        (Some(buffer_id), Some(position)) => {
            hash_mix(state, buffer_id);
            hash_mix(state, position as u64);
        }
        _ => hash_mix(state, id),
    }
}

pub(crate) fn hash_char_table_equal(
    interp: &Interpreter,
    state: &mut u64,
    id: u64,
    include_properties: bool,
    depth: u32,
) {
    hash_mix(state, 44);
    let Some(table) = interp.find_char_table(id) else {
        hash_mix(state, id);
        return;
    };

    match &table.subtype {
        Some(subtype) => {
            hash_mix(state, 1);
            hash_str(state, subtype);
        }
        None => hash_mix(state, 0),
    }
    hash_mix(state, table.parent.unwrap_or(0));
    hash_value_equal_at(interp, state, &table.default, include_properties, depth + 1);
    hash_mix(state, table.extra_slots.len() as u64);
    for slot in table.extra_slots.iter().take(SXHASH_MAX_LEN) {
        hash_value_equal_at(interp, state, slot, include_properties, depth + 1);
    }
    hash_mix(state, table.entries.len() as u64);
    for entry in table.entries.iter().take(SXHASH_MAX_LEN) {
        hash_mix(state, entry.start as u64);
        hash_mix(state, entry.end as u64);
        hash_value_equal_at(interp, state, &entry.value, include_properties, depth + 1);
    }
    hash_mix(state, table.category_docs.len() as u64);
    for (code, doc) in &table.category_docs {
        hash_mix(state, *code as u64);
        hash_str(state, doc);
    }
}

pub(crate) fn hash_record_equal(
    interp: &Interpreter,
    state: &mut u64,
    id: u64,
    include_properties: bool,
    depth: u32,
) {
    hash_mix(state, 45);
    let Some(record) = interp.find_record(id) else {
        hash_mix(state, id);
        return;
    };

    match record.kind {
        crate::lisp::eval::RecordKind::BoolVector => {
            hash_str(state, "bool-vector");
            if let Ok(bits) = bool_vector_bits(interp, &Value::Record(id)) {
                hash_mix(state, bits.len() as u64);
                for bit in bits {
                    hash_mix(state, u64::from(bit));
                }
            } else {
                hash_mix(state, id);
            }
        }
        crate::lisp::eval::RecordKind::Process
        | crate::lisp::eval::RecordKind::HashTable
        | crate::lisp::eval::RecordKind::Obarray
        | crate::lisp::eval::RecordKind::Window
        | crate::lisp::eval::RecordKind::WindowConfiguration
        | crate::lisp::eval::RecordKind::Thread
        | crate::lisp::eval::RecordKind::Mutex
        | crate::lisp::eval::RecordKind::ConditionVariable
        | crate::lisp::eval::RecordKind::NativeCompUnit
        | crate::lisp::eval::RecordKind::SymbolWithPos
        | crate::lisp::eval::RecordKind::TreeSitterParser
        | crate::lisp::eval::RecordKind::TreeSitterNode
        | crate::lisp::eval::RecordKind::TreeSitterCompiledQuery
        | crate::lisp::eval::RecordKind::Sqlite => {
            hash_value_equal_at(interp, state, &record.type_tag, include_properties, depth + 1);
            hash_mix(state, id);
        }
        crate::lisp::eval::RecordKind::Record
        | crate::lisp::eval::RecordKind::Closure
        | crate::lisp::eval::RecordKind::Font
        | crate::lisp::eval::RecordKind::Keymap => {
            hash_value_equal_at(interp, state, &record.type_tag, include_properties, depth + 1);
            hash_mix(state, record.slots.len() as u64);
            // fns.c:5447 `sxhash_vector' hashes a record's leading slots only.
            for slot in record.slots.iter().take(SXHASH_MAX_LEN) {
                hash_value_equal_at(interp, state, slot, include_properties, depth + 1);
            }
        }
    }
}

pub(crate) fn markers_equal(interp: &Interpreter, left_id: u64, right_id: u64) -> bool {
    let Some(left) = interp.find_marker(left_id) else {
        return left_id == right_id;
    };
    let Some(right) = interp.find_marker(right_id) else {
        return false;
    };
    left.buffer_id == right.buffer_id && left.position == right.position
}

pub(crate) fn overlays_equal(interp: &Interpreter, left_id: u64, right_id: u64) -> bool {
    let Some(left) = interp.find_overlay(left_id) else {
        return left_id == right_id;
    };
    let Some(right) = interp.find_overlay(right_id) else {
        return false;
    };
    left.beg == right.beg
        && left.end == right.end
        && left.buffer_id == right.buffer_id
        && left.plist.len() == right.plist.len()
        && left.plist.iter().zip(&right.plist).all(
            |((left_key, left_value), (right_key, right_value))| {
                left_key == right_key && values_equal(interp, left_value, right_value)
            },
        )
}

pub(crate) fn resolve_callable(
    interp: &Interpreter,
    value: &Value,
    env: &Env,
) -> Result<Value, LispError> {
    match value {
        Value::Symbol(name) => interp.lookup_function(name, env),
        _ => Ok(value.clone()),
    }
}

pub(crate) fn is_lambda_expression(value: &Value) -> bool {
    value.to_vec().ok().is_some_and(|items| {
        matches!(items.first(), Some(Value::Symbol(name)) if name == "lambda")
            && items.get(1).is_some()
    })
}

pub(crate) fn callable_value_p(interp: &Interpreter, value: &Value) -> bool {
    matches!(value, Value::BuiltinFunc(_) | Value::Lambda(_))
        || is_lambda_expression(value)
        || matches!(
            value,
            Value::Record(id)
                if interp
                    .find_record(*id)
                    .is_some_and(|record| record.kind == crate::lisp::eval::RecordKind::Closure)
        )
}

pub(crate) fn value_matches_with_test(
    interp: &mut Interpreter,
    left: &Value,
    right: &Value,
    testfn: Option<&Value>,
    env: &mut Env,
) -> Result<bool, LispError> {
    match testfn.filter(|value| !value.is_nil()) {
        None => Ok(values_eq_in_env(interp, left, right, env)),
        Some(Value::Symbol(name)) | Some(Value::BuiltinFunc(name)) => match name.as_str() {
            "eq" => Ok(values_eq_in_env(interp, left, right, env)),
            "eql" => Ok(values_eql(left, right)),
            "equal" => Ok(values_equal(interp, left, right)),
            _ => {
                let func = resolve_callable(interp, testfn.expect("checked Some"), env)?;
                Ok(
                    invoke_function_value(interp, &func, &[left.clone(), right.clone()], env)?
                        .is_truthy(),
                )
            }
        },
        Some(other) => {
            let func = resolve_callable(interp, other, env)?;
            Ok(
                invoke_function_value(interp, &func, &[left.clone(), right.clone()], env)?
                    .is_truthy(),
            )
        }
    }
}

pub(crate) fn invoke_function_value(
    interp: &mut Interpreter,
    func: &Value,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    interp.call_function_value(func.clone(), None, args, env)
}

pub(crate) fn callable_name(original: &Value, resolved: &Value) -> Option<String> {
    match original {
        Value::Symbol(name) => Some(name.to_string()),
        _ => match resolved {
            Value::BuiltinFunc(name) => Some(name.to_string()),
            _ => None,
        },
    }
}

pub(crate) const KEYMAP_RECORD_TYPE: &str = "keymap";
pub(crate) const KEYMAP_PARENT_SLOT: usize = 1;
pub(crate) const KEYMAP_BINDINGS_SLOT: usize = 2;
pub(crate) const KEYMAP_CHAR_TABLE_SLOT: usize = 3;
pub(crate) const KEYMAP_PUBLIC_VIEW_SLOT: usize = 4;

pub(crate) fn make_runtime_keymap(interp: &mut Interpreter, name: Option<&str>) -> Value {
    let keymap = interp.create_pseudovector(
        crate::lisp::eval::RecordKind::Keymap,
        KEYMAP_RECORD_TYPE,
        vec![
            name.map(Value::string).unwrap_or(Value::Nil),
            Value::Nil,
            Value::Nil,
        ],
    );
    if let Value::Record(id) = keymap {
        refresh_runtime_keymap_public_view(interp, id)
            .expect("new runtime keymap has a valid public view");
    }
    keymap
}

pub(crate) fn make_runtime_full_keymap(interp: &mut Interpreter, name: Option<&str>) -> Value {
    let keymap = make_runtime_keymap(interp, name);
    let Value::Record(id) = keymap.clone() else {
        return keymap;
    };
    let char_table = interp.make_char_table(None, Value::Nil);
    if let Some(record) = interp.find_record_mut(id) {
        if record.slots.len() <= KEYMAP_CHAR_TABLE_SLOT {
            record.slots.resize(KEYMAP_CHAR_TABLE_SLOT + 1, Value::Nil);
        }
        record.slots[KEYMAP_CHAR_TABLE_SLOT] = char_table;
    }
    refresh_runtime_keymap_public_view(interp, id)
        .expect("new full keymap has a valid public view");
    keymap
}

pub(crate) fn runtime_keymap_public_view(interp: &Interpreter, keymap: &Value) -> Option<Value> {
    let id = keymap_record_id(interp, keymap)?;
    interp
        .find_record(id)
        .and_then(|record| record.slots.get(KEYMAP_PUBLIC_VIEW_SLOT))
        .cloned()
}

pub(crate) fn public_keymap_value(interp: &Interpreter, value: &Value) -> Value {
    fn project(interp: &Interpreter, value: &Value, seen: &mut HashSet<usize>) -> Value {
        if let Some(view) = runtime_keymap_public_view(interp, value) {
            return view;
        }
        let Some((car, cdr)) = value.cons_cells() else {
            return value.clone();
        };
        let id = car.cell_id();
        if !seen.insert(id) {
            return value.clone();
        }
        let original_car = car.borrow().clone();
        let original_cdr = cdr.borrow().clone();
        let projected_car = project(interp, &original_car, seen);
        let projected_cdr = project(interp, &original_cdr, seen);
        seen.remove(&id);
        if values_eql(&projected_car, &original_car) && values_eql(&projected_cdr, &original_cdr) {
            value.clone()
        } else {
            Value::cons(projected_car, projected_cdr)
        }
    }

    project(interp, value, &mut HashSet::new())
}

pub(crate) fn refresh_runtime_keymap_public_view(
    interp: &mut Interpreter,
    keymap_id: u64,
) -> Result<(), LispError> {
    let (name, parent, char_table, bindings, existing) = {
        let Some(record) = interp.find_record(keymap_id) else {
            return Ok(());
        };
        (
            record.slots.first().cloned().unwrap_or(Value::Nil),
            record
                .slots
                .get(KEYMAP_PARENT_SLOT)
                .cloned()
                .unwrap_or(Value::Nil),
            keymap_char_table(record),
            keymap_bindings(record)?,
            record.slots.get(KEYMAP_PUBLIC_VIEW_SLOT).cloned(),
        )
    };

    let mut items = Vec::new();
    let has_char_table = char_table.is_some();
    if let Some(char_table) = char_table {
        items.push(char_table);
    }
    let has_name = !name.is_nil();
    if has_name {
        items.push(name);
    }
    items.extend(
        bindings
            .iter()
            .filter(|binding| !binding.after_prompt)
            .map(|binding| {
                Value::cons(
                    keymap_entry_key_value(&binding_key_parts(binding), &binding.key),
                    public_keymap_value(interp, &binding.value),
                )
            }),
    );
    if has_name {
        // Pre-prompt bindings precede the prompt in GNU's public list.
        let prompt = items.remove(has_char_table as usize);
        let index = has_char_table as usize
            + bindings
                .iter()
                .filter(|binding| !binding.after_prompt)
                .count();
        items.insert(index, prompt);
    }
    items.extend(
        bindings
            .iter()
            .filter(|binding| binding.after_prompt)
            .map(|binding| {
                Value::cons(
                    keymap_entry_key_value(&binding_key_parts(binding), &binding.key),
                    public_keymap_value(interp, &binding.value),
                )
            }),
    );
    if !parent.is_nil() {
        items.push(public_keymap_value(interp, &parent));
    }

    let view = if let Some(existing @ Value::Cons(_)) = existing {
        existing.set_car(Value::Symbol("keymap".into()))?;
        existing.set_cdr(Value::list(items))?;
        existing
    } else {
        Value::cons(Value::Symbol("keymap".into()), Value::list(items))
    };
    let Some(record) = interp.find_record_mut(keymap_id) else {
        return Ok(());
    };
    record.slots.resize(KEYMAP_PUBLIC_VIEW_SLOT + 1, Value::Nil);
    record.slots[KEYMAP_PUBLIC_VIEW_SLOT] = view.clone();
    interp.register_keymap_public_cons_owners(keymap_id, &view);
    Ok(())
}

pub(crate) fn is_keymap_placeholder(value: &Value) -> bool {
    value.to_vec().ok().is_some_and(
        |items| matches!(items.first(), Some(Value::Symbol(symbol)) if symbol == "keymap"),
    )
}

pub(crate) fn keymap_record_id(interp: &Interpreter, value: &Value) -> Option<u64> {
    let Value::Record(id) = value else {
        return None;
    };
    interp
        .find_record(*id)
        .filter(|record| record.kind == crate::lisp::eval::RecordKind::Keymap)
        .map(|_| *id)
}

pub(crate) fn is_keymap_value(interp: &Interpreter, value: &Value) -> bool {
    is_keymap_placeholder(value) || keymap_record_id(interp, value).is_some()
}

pub(crate) fn keymap_char_table(record: &crate::lisp::eval::RecordState) -> Option<Value> {
    record
        .slots
        .get(KEYMAP_CHAR_TABLE_SLOT)
        .cloned()
        .filter(|value| !value.is_nil())
}

/// Return the character table carried by either Emaxx's identity-bearing
/// keymap record or GNU's public `(keymap CHAR-TABLE ...)' list shape.
///
/// Keymap-producing Lisp commonly canonicalizes a runtime map before handing
/// it to another walker.  Read-side operations must not lose the full-map
/// portion merely because that boundary projected the record as a Lisp list.
pub(crate) fn keymap_char_table_value(interp: &Interpreter, keymap: &Value) -> Option<Value> {
    if let Some(id) = keymap_record_id(interp, keymap) {
        return interp.find_record(id).and_then(keymap_char_table);
    }
    if !is_keymap_placeholder(keymap) {
        return None;
    }
    keymap
        .to_vec()
        .ok()?
        .into_iter()
        .skip(1)
        .find(|item| matches!(item, Value::CharTable(_)))
}

pub(crate) fn keymap_bindings(
    record: &crate::lisp::eval::RecordState,
) -> Result<Vec<RuntimeKeymapBinding>, LispError> {
    let bindings = record
        .slots
        .get(KEYMAP_BINDINGS_SLOT)
        .cloned()
        .unwrap_or(Value::Nil);
    let mut result = Vec::new();
    for entry in bindings.to_vec()? {
        if let Ok(items) = entry.to_vec()
            && items.len() >= 2
            && let Ok(key) = string_text(&items[0])
        {
            result.push(RuntimeKeymapBinding {
                key,
                parts: items.get(3).and_then(|parts| {
                    if parts.is_nil() {
                        return None;
                    }
                    parts.to_vec().ok().and_then(|items| {
                        let parts = items
                            .into_iter()
                            .filter_map(|item| string_text(&item).ok())
                            .collect::<Vec<_>>();
                        (!parts.is_empty()).then_some(parts)
                    })
                }),
                value: items[1].clone(),
                after_prompt: items.get(2).is_some_and(Value::is_truthy),
            });
            continue;
        }

        let key = string_text(&entry.car()?)?;
        result.push(RuntimeKeymapBinding {
            key,
            parts: None,
            value: entry.cdr()?,
            after_prompt: false,
        });
    }
    Ok(result)
}

/// Return the bindings directly stored in either Emaxx's identity-bearing
/// runtime keymap or GNU's public Lisp `(keymap ...)' representation.  Lisp
/// libraries are allowed to construct and pass the latter directly, so all
/// One keymap record's materialized lookup state: the ordered binding
/// projection every walker shares, plus a command-remapping index probed
/// once per keystroke by `command-remapping'.  Cached per record and
/// dropped by `find_record_mut' (see `keymap_bindings_cache').
#[derive(Clone)]
pub(crate) struct CachedKeymapIndex {
    pub(crate) bindings: std::rc::Rc<Vec<RuntimeKeymapBinding>>,
    /// The `<remap>' prefix keymap, when this map carries one.  Only the
    /// LINK is cached here: the inner map is its own record with its own
    /// independently invalidated binding cache, so later `define-key
    /// [remap ...]' calls (which mutate the inner record, not this one)
    /// stay visible.
    pub(crate) remap_map: Option<Value>,
}

/// read-only keymap walkers must share this projection rather than silently
/// treating non-record maps as empty.
pub(crate) fn keymap_direct_bindings(
    interp: &Interpreter,
    keymap: &Value,
) -> Result<std::rc::Rc<Vec<RuntimeKeymapBinding>>, LispError> {
    if let Some(id) = keymap_record_id(interp, keymap) {
        // Key lookup walks every active map on every keystroke; the
        // materialized, ordered projection is cached per record and dropped
        // by `find_record_mut' whenever anything rewrites the record
        // (`define-key' included), the byte-code program cache's contract.
        let index = (id as usize).saturating_sub(1);
        if let Some(Some(cached)) = interp.keymap_bindings_cache.borrow().get(index) {
            return Ok(std::rc::Rc::clone(&cached.bindings));
        }
        let Some(record) = interp.find_record(id) else {
            return Ok(std::rc::Rc::new(Vec::new()));
        };
        // Character bindings now live in the leading char-table (lookup
        // consults it before this sparse projection), so no enumeration
        // order fixup is needed here anymore.
        let mut bindings = keymap_bindings(record)?;
        // Materialize each entry's parsed key parts while building the
        // cached projection: lookups compare parts on every scan, and
        // re-deriving them per probe was a measurable slice of every
        // keystroke's `key-binding'.
        for binding in &mut bindings {
            if binding.parts.is_none() {
                binding.parts = Some(approximate_key_parts(&binding.key));
            }
        }
        let bindings = std::rc::Rc::new(bindings);
        // `[remap CMD]' bindings live behind a `<remap>' prefix map;
        // capture the link so `command-remapping' probes the inner map
        // directly instead of a string lookup per map per keystroke.
        let remap_map = bindings
            .iter()
            .find(|binding| {
                matches!(binding.parts.as_deref(),
                    Some([part]) if canonical_key_part(part) == "remap")
            })
            .map(|binding| binding.value.clone());
        let entry = CachedKeymapIndex {
            bindings: std::rc::Rc::clone(&bindings),
            remap_map,
        };
        let mut cache = interp.keymap_bindings_cache.borrow_mut();
        if cache.len() <= index {
            cache.resize(index + 1, None);
        }
        cache[index] = Some(entry);
        return Ok(bindings);
    }

    let Ok(items) = keymap.to_vec() else {
        return Ok(std::rc::Rc::new(Vec::new()));
    };
    if !matches!(items.first(), Some(Value::Symbol(symbol)) if symbol == "keymap") {
        return Ok(std::rc::Rc::new(Vec::new()));
    }

    let mut bindings = Vec::new();
    for entry in items.into_iter().skip(1) {
        if is_keymap_value(interp, &entry) {
            // A bare keymap element is an inherited parent, not a binding.
            continue;
        }
        if let Some(binding) = runtime_keymap_binding_from_public_entry(&entry, true)? {
            bindings.push(binding);
        }
    }
    Ok(std::rc::Rc::new(bindings))
}

fn runtime_keymap_binding_from_public_entry(
    entry: &Value,
    after_prompt: bool,
) -> Result<Option<RuntimeKeymapBinding>, LispError> {
    let Some((event, definition)) = entry.cons_values() else {
        return Ok(None);
    };
    let sequence = Value::list([Value::Symbol("vector-literal".into()), event]);
    let Ok(parts) = key_sequence_keymap_parts(&sequence) else {
        return Ok(None);
    };
    if parts.is_empty() {
        return Ok(None);
    }
    Ok(Some(RuntimeKeymapBinding {
        key: key_sequence_binding_text(&sequence)?,
        parts: Some(parts),
        value: definition,
        after_prompt,
    }))
}

/// Replace the public cdr of an identity-bearing runtime keymap.
///
/// GNU keymaps are cons lists, and dumped Lisp legitimately mutates a map's
/// tail with `setcdr'.  Emaxx stores keymaps in records so their identity is
/// stable across Rust-owned lookup tables; this is the single mutation door
/// that translates the public `(keymap ...)' tail back into that record.
pub(crate) fn replace_runtime_keymap_tail(
    interp: &mut Interpreter,
    keymap: &Value,
    tail: &Value,
) -> Result<bool, LispError> {
    let Some(id) = keymap_record_id(interp, keymap) else {
        return Ok(false);
    };
    if let Some(view) = runtime_keymap_public_view(interp, keymap) {
        view.set_cdr(tail.clone())?;
    }
    sync_runtime_keymap_from_public_view(interp, id)?;
    Ok(true)
}

pub(crate) fn sync_runtime_keymap_from_public_view(
    interp: &mut Interpreter,
    keymap_id: u64,
) -> Result<(), LispError> {
    let Some(view) = interp
        .find_record(keymap_id)
        .and_then(|record| record.slots.get(KEYMAP_PUBLIC_VIEW_SLOT))
        .cloned()
    else {
        return Ok(());
    };
    let tail = view.cdr()?;
    let items = tail
        .to_vec()
        .map_err(|_| wrong_type_argument("listp", tail.clone()))?;

    let mut name = Value::Nil;
    let mut parent = Value::Nil;
    let mut char_table = Value::Nil;
    let mut before_prompt = Vec::new();
    let mut after_prompt = Vec::new();
    let mut saw_prompt = false;

    for item in items {
        if matches!(item, Value::CharTable(_)) {
            char_table = item;
            continue;
        }
        if is_keymap_value(interp, &item) {
            parent = item;
            continue;
        }
        if !saw_prompt && string_like(&item).is_some() {
            name = item;
            saw_prompt = true;
            continue;
        }
        if let Some(binding) = runtime_keymap_binding_from_public_entry(&item, saw_prompt)? {
            if saw_prompt {
                after_prompt.push(binding);
            } else {
                before_prompt.push(binding);
            }
        }
    }

    before_prompt.extend(after_prompt);

    let Some(record) = interp.find_record_mut(keymap_id) else {
        return Ok(());
    };
    record.slots.resize(KEYMAP_PUBLIC_VIEW_SLOT + 1, Value::Nil);
    record.slots[0] = name;
    record.slots[KEYMAP_PARENT_SLOT] = parent;
    record.slots[KEYMAP_BINDINGS_SLOT] = keymap_bindings_value(before_prompt);
    record.slots[KEYMAP_CHAR_TABLE_SLOT] = char_table;
    record.slots[KEYMAP_PUBLIC_VIEW_SLOT] = view.clone();
    interp.register_keymap_public_cons_owners(keymap_id, &view);
    Ok(())
}

pub(crate) fn keymap_parent_values(interp: &Interpreter, keymap: &Value) -> Vec<Value> {
    if let Some(id) = keymap_record_id(interp, keymap) {
        return interp
            .find_record(id)
            .and_then(|record| record.slots.get(KEYMAP_PARENT_SLOT))
            .filter(|parent| parent.is_truthy())
            .cloned()
            .into_iter()
            .collect();
    }
    keymap
        .to_vec()
        .unwrap_or_default()
        .into_iter()
        .skip(1)
        .filter(|item| is_keymap_value(interp, item))
        .collect()
}

pub(crate) fn keymap_value_identity(interp: &Interpreter, keymap: &Value) -> Option<(bool, usize)> {
    if let Some(id) = keymap_record_id(interp, keymap) {
        return Some((true, id as usize));
    }
    match keymap {
        Value::Cons(cell) if is_keymap_placeholder(keymap) => {
            Some((false, crate::lisp::types::ConsCell::identity(cell)))
        }
        _ => None,
    }
}

pub(crate) fn keymap_bindings_value(bindings: Vec<RuntimeKeymapBinding>) -> Value {
    Value::list(bindings.into_iter().map(|binding| {
        Value::list([
            Value::String(binding.key.into()),
            binding.value,
            if binding.after_prompt {
                Value::T
            } else {
                Value::Nil
            },
            binding
                .parts
                .map(|parts| {
                    Value::list(parts.into_iter().map(|value| Value::String(value.into())))
                })
                .unwrap_or(Value::Nil),
        ])
    }))
}


pub(crate) fn keymap_define_character_range(
    interp: &mut Interpreter,
    keymap: &Value,
    start: i64,
    end: i64,
    binding: Value,
) -> Result<(), LispError> {
    let (start, end) = (
        u32::try_from(start)
            .map_err(|_| LispError::Signal("Invalid keymap character range".into()))?,
        u32::try_from(end)
            .map_err(|_| LispError::Signal("Invalid keymap character range".into()))?,
    );
    if let Some(Value::CharTable(table_id)) = keymap_char_table_value(interp, keymap) {
        // A nil binding in a full GNU keymap is represented by t inside the
        // char-table so it remains explicitly unbound instead of falling
        // through to another sparse element or parent.
        let stored = if binding.is_nil() { Value::T } else { binding };
        interp.char_table_set_range(table_id, start, end, stored)?;
        // Remove sparse character entries covered by the newly-written
        // range.  GNU's `store_in_keymap' updates the leading char-table and
        // returns before scanning the sparse tail, so those entries become
        // unreachable through the public lookup order.
        if let Some(id) = keymap_record_id(interp, keymap)
            && let Some(record) = interp.find_record_mut(id)
        {
            let mut bindings = keymap_bindings(record)?;
            bindings.retain(|entry| {
                !matches!(
                    keymap_entry_key_value(&binding_key_parts(entry), &entry.key),
                    Value::Integer(code) if (i64::from(start)..=i64::from(end)).contains(&code)
                )
            });
            record.slots[KEYMAP_BINDINGS_SLOT] = keymap_bindings_value(bindings);
            refresh_runtime_keymap_public_view(interp, id)?;
        }
        return Ok(());
    }

    // GNU inserts a keymap char-table when a character range is defined on a
    // sparse map.  Keep the same public shape instead of failing locally.
    let Some(id) = keymap_record_id(interp, keymap) else {
        return Err(LispError::Signal(
            "attempt to define a key in a non-keymap".into(),
        ));
    };
    let table = interp.make_char_table(Some("keymap".into()), Value::Nil);
    let Value::CharTable(table_id) = table else {
        unreachable!("make-char-table returns a character table")
    };
    let stored = if binding.is_nil() { Value::T } else { binding };
    interp.char_table_set_range(table_id, start, end, stored)?;
    if let Some(record) = interp.find_record_mut(id) {
        record.slots.resize(KEYMAP_PUBLIC_VIEW_SLOT + 1, Value::Nil);
        record.slots[KEYMAP_CHAR_TABLE_SLOT] = table;
    }
    refresh_runtime_keymap_public_view(interp, id)
}

pub(crate) fn keymap_define_binding_with_placement(
    interp: &mut Interpreter,
    keymap: &Value,
    key: &str,
    key_parts: Option<Vec<String>>,
    binding: Value,
    after_prompt: bool,
) -> Result<(), LispError> {
    if let Some(parts) = key_parts.as_ref().filter(|parts| parts.len() > 1) {
        let head = &parts[..1];
        // GNU's define-key descends through the map being modified.  An
        // inherited command at HEAD is therefore shadowed by a new local
        // prefix instead of incorrectly making this sequence invalid.
        let existing = keymap_lookup_direct_binding_exact_parts(interp, keymap, head)?;
        let existing = keymap_get_keyelt(interp, &existing, false, &mut Vec::new())?;
        let prefix = if existing.is_nil() {
            let prefix = make_runtime_keymap(interp, None);
            keymap_define_binding_with_placement(
                interp,
                keymap,
                &head.join(" "),
                Some(head.to_vec()),
                prefix.clone(),
                after_prompt,
            )?;
            prefix
        } else if is_keymap_value(interp, &existing) {
            existing
        } else if let Value::Symbol(symbol) = &existing
            && let Ok(function) = interp.lookup_function(symbol, &Vec::new())
            && is_keymap_value(interp, &function)
        {
            function
        } else {
            return Err(LispError::Signal(
                "Key sequence starts with non-prefix key".into(),
            ));
        };
        return keymap_define_binding_with_placement(
            interp,
            &prefix,
            &parts[1..].join(" "),
            Some(parts[1..].to_vec()),
            binding,
            after_prompt,
        );
    }

    // Mirror ordinary character definitions into a full keymap's leading
    // char-table.  The sparse entry remains as Emaxx's identity-bearing
    // prefix/navigation index; lookup honors the char-table first, and a
    // later GNU range write removes covered sparse entries below.  This keeps
    // `(cadr MAP)' truthful without making dumped-state replay depend on a
    // second, implicit prefix index.
    if let Some(parts) = key_parts.as_ref()
        && let [part] = parts.as_slice()
        && let Some(Value::CharTable(table_id)) = keymap_char_table_value(interp, keymap)
    {
        let event = keymap_entry_key_value(std::slice::from_ref(part), key);
        if let Value::Integer(code) = event
            && (0..=0x3f_ffff).contains(&code)
        {
            interp.char_table_set(table_id, code as u32, binding.clone())?;
        }
    }

    let Some(id) = keymap_record_id(interp, keymap) else {
        return Ok(());
    };
    let Some(record) = interp.find_record_mut(id) else {
        return Ok(());
    };
    let mut bindings = keymap_bindings(record)?;
    let existing = bindings.iter().position(|existing| existing.key == key);
    let (insert_at, after_prompt) = if let Some(index) = existing {
        let placement = bindings[index].after_prompt;
        bindings.remove(index);
        (index.min(bindings.len()), placement)
    } else if after_prompt {
        (
            bindings
                .iter()
                .position(|binding| binding.after_prompt)
                .unwrap_or(bindings.len()),
            true,
        )
    } else {
        (0, false)
    };
    let binding = RuntimeKeymapBinding {
        key: key.to_string(),
        parts: key_parts,
        value: binding,
        after_prompt,
    };
    bindings.insert(insert_at, binding);
    if record.slots.len() <= KEYMAP_BINDINGS_SLOT {
        record.slots.resize(KEYMAP_BINDINGS_SLOT + 1, Value::Nil);
    }
    record.slots[KEYMAP_BINDINGS_SLOT] = keymap_bindings_value(bindings);
    refresh_runtime_keymap_public_view(interp, id)?;
    Ok(())
}

pub(crate) fn keymap_remove_binding(
    interp: &mut Interpreter,
    keymap: &Value,
    key: &str,
) -> Result<(), LispError> {
    let parts = approximate_key_parts(key);
    if let [part] = parts.as_slice()
        && let Some(Value::CharTable(table_id)) = keymap_char_table_value(interp, keymap)
    {
        let event = keymap_entry_key_value(std::slice::from_ref(part), key);
        if let Value::Integer(code) = event
            && let Ok(code) = u32::try_from(code)
        {
            interp.char_table_set(table_id, code, Value::Nil)?;
        }
    }
    // Prefer the canonical key string stored by the corresponding define
    // operation.  Re-parsing structured event names such as `mouse-5' can
    // resemble a textual multi-event sequence; only descend when no exact
    // entry exists in this map.
    if let Some(id) = keymap_record_id(interp, keymap)
        && let Some(record) = interp.find_record_mut(id)
    {
        let mut bindings = keymap_bindings(record)?;
        let original_len = bindings.len();
        bindings.retain(|existing| existing.key != key);
        if bindings.len() != original_len {
            if record.slots.len() <= KEYMAP_BINDINGS_SLOT {
                record.slots.resize(KEYMAP_BINDINGS_SLOT + 1, Value::Nil);
            }
            record.slots[KEYMAP_BINDINGS_SLOT] = keymap_bindings_value(bindings);
            refresh_runtime_keymap_public_view(interp, id)?;
            return Ok(());
        }
    }

    if parts.len() > 1 {
        let prefix = keymap_lookup_direct_binding_exact_parts(interp, keymap, &parts[..1])?;
        let prefix = keymap_get_keyelt(interp, &prefix, false, &mut Vec::new())?;
        if is_keymap_value(interp, &prefix) {
            return keymap_remove_binding(interp, &prefix, &parts[1..].join(" "));
        }
        return Ok(());
    }
    let Some(id) = keymap_record_id(interp, keymap) else {
        return Ok(());
    };
    let Some(record) = interp.find_record_mut(id) else {
        return Ok(());
    };
    let mut bindings = keymap_bindings(record)?;
    bindings.retain(|existing| {
        existing.key != key && !key_parts_match(&binding_key_parts(existing), &parts)
    });
    if record.slots.len() <= KEYMAP_BINDINGS_SLOT {
        record.slots.resize(KEYMAP_BINDINGS_SLOT + 1, Value::Nil);
    }
    record.slots[KEYMAP_BINDINGS_SLOT] = keymap_bindings_value(bindings);
    refresh_runtime_keymap_public_view(interp, id)?;
    Ok(())
}

pub(crate) fn approximate_key_parts(key: &str) -> Vec<String> {
    textual_key_sequence_keymap_parts(&Value::String(key.to_string().into()))
        .unwrap_or_else(|_| key.split_whitespace().map(str::to_string).collect())
}

pub(crate) fn binding_key_parts(binding: &RuntimeKeymapBinding) -> Vec<String> {
    binding
        .parts
        .clone()
        .unwrap_or_else(|| approximate_key_parts(&binding.key))
}

/// `key_parts_match' against a binding without cloning its parts vector;
/// the exact-lookup scan runs this per entry per keystroke.
fn binding_matches_key_parts(binding: &RuntimeKeymapBinding, requested: &[String]) -> bool {
    match &binding.parts {
        Some(parts) => key_parts_match(parts, requested),
        None => key_parts_match(&approximate_key_parts(&binding.key), requested),
    }
}

pub(crate) fn canonical_key_part(part: &str) -> String {
    let part = part
        .strip_prefix('<')
        .and_then(|part| part.strip_suffix('>'))
        .filter(|part| !part.is_empty())
        .unwrap_or(part);
    part.replace("\\ ", "-").replace(' ', "-").to_lowercase()
}

pub(crate) fn key_parts_match(binding_parts: &[String], requested_parts: &[String]) -> bool {
    if binding_parts.len() != requested_parts.len() {
        return false;
    }

    let mut in_menu_path = false;
    for (binding, requested) in binding_parts.iter().zip(requested_parts) {
        if binding == requested {
            if canonical_key_part(binding) == "menu-bar" {
                in_menu_path = true;
            }
            continue;
        }

        let binding_canonical = canonical_key_part(binding);
        let requested_canonical = canonical_key_part(requested);
        let symbolic_like = binding.starts_with('<')
            || binding.ends_with('>')
            || requested.starts_with('<')
            || requested.ends_with('>');
        if binding_canonical == "menu-bar" && requested_canonical == "menu-bar" {
            in_menu_path = true;
            continue;
        }

        if (symbolic_like || in_menu_path) && binding_canonical == requested_canonical {
            continue;
        }

        if binding_canonical != requested_canonical {
            return false;
        }

        // Plain letter keys are distinct events per case ("x" vs "X", "M-x"
        // vs "M-X"); only control keys fold case (C-X and C-x are both code
        // 24), and symbolic <...> names compare case-insensitively above.
        if key_part_case_significant(binding)
            && key_part_case_significant(requested)
            && binding.rsplit('-').next() != requested.rsplit('-').next()
        {
            return false;
        }
    }

    true
}

fn key_part_case_significant(part: &str) -> bool {
    if part.starts_with('<') || part.ends_with('>') {
        return false;
    }
    let mut segments: Vec<&str> = part.split('-').collect();
    let Some(last) = segments.pop() else {
        return false;
    };
    if segments.contains(&"C") {
        return false;
    }
    let mut chars = last.chars();
    matches!((chars.next(), chars.next()), (Some(ch), None) if ch.is_ascii_alphabetic())
}

pub(crate) enum KeyLookupResult {
    Missing,
    Value(Value),
    PrefixLen(usize),
}

pub(crate) fn keymap_lookup_binding_exact_parts(
    interp: &Interpreter,
    keymap: &Value,
    key_parts: &[String],
) -> Result<Value, LispError> {
    keymap_lookup_binding_exact_parts_with_default(interp, keymap, key_parts, false)
}

pub(crate) fn keymap_lookup_binding_exact_parts_with_default(
    interp: &Interpreter,
    keymap: &Value,
    key_parts: &[String],
    accept_default: bool,
) -> Result<Value, LispError> {
    keymap_lookup_binding_exact_parts_bounded(interp, keymap, key_parts, accept_default, 32)
}

fn keymap_lookup_direct_binding_exact_parts(
    interp: &Interpreter,
    keymap: &Value,
    key_parts: &[String],
) -> Result<Value, LispError> {
    // The character table appears first in GNU full keymaps and therefore
    // wins over any legacy sparse entry for the same character.
    if let [part] = key_parts
        && let Some(Value::CharTable(table_id)) = keymap_char_table_value(interp, keymap)
    {
        let event = keymap_entry_key_value(std::slice::from_ref(part), part);
        if let Value::Integer(code) = event
            && let Ok(code) = u32::try_from(code)
            && let Some(value) = interp.char_table_get(table_id, code)
            && !value.is_nil()
        {
            return Ok(if value == Value::T { Value::Nil } else { value });
        }
    }
    let bindings = keymap_direct_bindings(interp, keymap)?;
    for binding in bindings.iter() {
        if binding_matches_key_parts(binding, key_parts) {
            return Ok(binding.value.clone());
        }
    }
    Ok(Value::Nil)
}

fn keymap_binding_map(interp: &Interpreter, binding: &Value) -> Option<Value> {
    if is_keymap_value(interp, binding) {
        return Some(binding.clone());
    }
    let Value::Symbol(name) = binding else {
        return None;
    };
    interp
        .lookup_function(name, &Vec::new())
        .ok()
        .filter(|function| is_keymap_value(interp, function))
}

fn keymap_lookup_binding_exact_parts_bounded(
    interp: &Interpreter,
    keymap: &Value,
    key_parts: &[String],
    accept_default: bool,
    depth: usize,
) -> Result<Value, LispError> {
    let Some(depth) = depth.checked_sub(1) else {
        return Ok(Value::Nil);
    };
    if let [part] = key_parts
        && let Some(Value::CharTable(table_id)) = keymap_char_table_value(interp, keymap)
    {
        let event = keymap_entry_key_value(std::slice::from_ref(part), part);
        if let Value::Integer(code) = event
            && let Ok(code) = u32::try_from(code)
            && let Some(value) = interp.char_table_get(table_id, code)
            && !value.is_nil()
        {
            return Ok(if value == Value::T { Value::Nil } else { value });
        }
    }
    let bindings = keymap_direct_bindings(interp, keymap)?;
    for binding in bindings.iter() {
        if binding_matches_key_parts(binding, key_parts) {
            return Ok(binding.value.clone());
        }
    }
    // A shorter binding to a prefix keymap resolves the remaining events in
    // that keymap ("C-x X" -> map, then "w" inside it).
    for binding in bindings.iter() {
        let binding_parts = binding_key_parts(binding);
        if !binding_parts.is_empty()
            && binding_parts.len() < key_parts.len()
            && key_parts_match(&binding_parts, &key_parts[..binding_parts.len()])
            && let Some(prefix_map) = keymap_binding_map(interp, &binding.value)
        {
            let nested = keymap_lookup_binding_exact_parts_bounded(
                interp,
                &prefix_map,
                &key_parts[binding_parts.len()..],
                accept_default,
                depth,
            )?;
            if !nested.is_nil() {
                return Ok(nested);
            }
        }
    }
    if accept_default && key_parts.len() == 1 && key_parts != ["<t>".to_string()] {
        for binding in bindings.iter() {
            if binding_key_parts(binding) == ["<t>".to_string()] {
                return Ok(binding.value.clone());
            }
        }
    }
    for parent in keymap_parent_values(interp, keymap) {
        let value = keymap_lookup_binding_exact_parts_bounded(
            interp,
            &parent,
            key_parts,
            accept_default,
            depth,
        )?;
        if !value.is_nil() {
            return Ok(value);
        }
    }
    Ok(Value::Nil)
}

pub(crate) fn keymap_lookup_binding(
    interp: &Interpreter,
    keymap: &Value,
    key: &str,
) -> Result<Value, LispError> {
    keymap_lookup_binding_exact_parts(interp, keymap, &approximate_key_parts(key))
}

pub(crate) fn keymap_lookup_sequence_single_map(
    interp: &mut Interpreter,
    keymap: &Value,
    key_parts: &[String],
    accept_default: bool,
    env: &mut Env,
) -> Result<KeyLookupResult, LispError> {
    if key_parts.is_empty() {
        return Ok(KeyLookupResult::Missing);
    }

    let binding =
        keymap_lookup_binding_exact_parts_with_default(interp, keymap, key_parts, accept_default)?;
    if !binding.is_nil() {
        return Ok(KeyLookupResult::Value(keymap_get_keyelt(
            interp, &binding, true, env,
        )?));
    }

    for prefix_len in (1..key_parts.len()).rev() {
        let binding = keymap_lookup_binding_exact_parts_with_default(
            interp,
            keymap,
            &key_parts[..prefix_len],
            accept_default,
        )?;
        if binding.is_nil() {
            continue;
        }
        let resolved = keymap_get_keyelt(interp, &binding, true, env)?;
        if let Some(prefix_map) = keymap_reference_map(interp, &resolved, env) {
            match keymap_lookup_sequence_single_map(
                interp,
                &prefix_map,
                &key_parts[prefix_len..],
                accept_default,
                env,
            )? {
                KeyLookupResult::Missing => {}
                KeyLookupResult::Value(value) => return Ok(KeyLookupResult::Value(value)),
                KeyLookupResult::PrefixLen(len) => {
                    return Ok(KeyLookupResult::PrefixLen(prefix_len + len));
                }
            }
        } else {
            return Ok(KeyLookupResult::PrefixLen(prefix_len));
        }
    }

    Ok(KeyLookupResult::Missing)
}

pub(crate) fn keymap_lookup_sequence_value(
    interp: &mut Interpreter,
    keymap_or_maps: &Value,
    key_parts: &[String],
    env: &mut Env,
) -> Result<Value, LispError> {
    keymap_lookup_sequence_value_with_default(interp, keymap_or_maps, key_parts, false, env)
}

pub(crate) fn keymap_lookup_sequence_value_with_default(
    interp: &mut Interpreter,
    keymap_or_maps: &Value,
    key_parts: &[String],
    accept_default: bool,
    env: &mut Env,
) -> Result<Value, LispError> {
    // GNU returns the map (or stack of maps) itself for an empty sequence.
    // Help uses that identity operation to propagate root-level shadow maps.
    if key_parts.is_empty() {
        return Ok(keymap_or_maps.clone());
    }

    if is_keymap_value(interp, keymap_or_maps) {
        return Ok(
            match keymap_lookup_sequence_single_map(
                interp,
                keymap_or_maps,
                key_parts,
                accept_default,
                env,
            )? {
                KeyLookupResult::Missing => Value::Nil,
                KeyLookupResult::Value(value) => value,
                KeyLookupResult::PrefixLen(len) => Value::Integer(len as i64),
            },
        );
    }

    // GNU's keymap walkers call `lookup-key' on the current list TAIL while
    // iterating a canonical sparse map.  Such a tail is intentionally not a
    // `keymapp', but lookup still treats its `(EVENT . DEFINITION)' entries
    // as a partial map.  Preserve that contract before interpreting an
    // ordinary list as a list of complete keymaps.
    if let Ok(entries) = keymap_or_maps.to_vec()
        && entries.iter().any(|entry| entry.cons_values().is_some())
        && !entries.iter().any(|entry| is_keymap_value(interp, entry))
    {
        for entry in &entries {
            let Some((event, definition)) = entry.cons_values() else {
                continue;
            };
            let event_sequence =
                Value::list([Value::Symbol("vector-literal".into()), event.clone()]);
            let Ok(event_parts) = key_sequence_keymap_parts(&event_sequence) else {
                continue;
            };
            if event_parts.is_empty()
                || event_parts.len() > key_parts.len()
                || !key_parts_match(&event_parts, &key_parts[..event_parts.len()])
            {
                continue;
            }
            let resolved = keymap_get_keyelt(interp, &definition, true, env)?;
            if event_parts.len() == key_parts.len() {
                return Ok(resolved);
            }
            if let Some(prefix_map) = keymap_reference_map(interp, &resolved, env) {
                return keymap_lookup_sequence_value_with_default(
                    interp,
                    &prefix_map,
                    &key_parts[event_parts.len()..],
                    accept_default,
                    env,
                );
            }
            return Ok(Value::Integer(event_parts.len() as i64));
        }
        return Ok(Value::Nil);
    }

    let mut prefix_match = None;
    for keymap in keymap_or_maps.to_vec()? {
        if !is_keymap_value(interp, &keymap) {
            continue;
        }
        match keymap_lookup_sequence_single_map(interp, &keymap, key_parts, accept_default, env)? {
            KeyLookupResult::Missing => {}
            KeyLookupResult::Value(value) => return Ok(value),
            KeyLookupResult::PrefixLen(len) => prefix_match = Some(len),
        }
    }

    Ok(prefix_match
        .map(|len| Value::Integer(len as i64))
        .unwrap_or(Value::Nil))
}

pub(crate) fn keymap_get_keyelt(
    interp: &mut Interpreter,
    object: &Value,
    autoload: bool,
    env: &mut Env,
) -> Result<Value, LispError> {
    let mut current = object.clone();
    loop {
        let Value::Cons(_) = current else {
            return Ok(current);
        };

        let car = current.car()?;
        if matches!(&car, Value::Symbol(symbol) if symbol == "menu-item") {
            let Ok(items) = current.to_vec() else {
                return Ok(current);
            };
            let Some(mut definition) = items.get(2).cloned() else {
                return Ok(current);
            };
            if autoload {
                let mut index = 3usize;
                while index + 1 < items.len() {
                    if matches!(&items[index], Value::Symbol(symbol) if symbol == ":filter") {
                        let filter = unwrap_function_quote(&items[index + 1]);
                        definition = call_function_value(
                            interp,
                            &filter,
                            std::slice::from_ref(&definition),
                            env,
                        )?;
                        break;
                    }
                    index += 2;
                }
            }
            current = definition;
            continue;
        }

        if string_like(&car).is_some() {
            current = current.cdr()?;
            continue;
        }

        return Ok(current);
    }
}

/// Resolve the indirection used by prefix commands.  GNU keymaps may bind an
/// event either to a map directly or to a symbol whose function definition is
/// that map (for example `mode-specific-command-prefix').  All recursive
/// readers need the same rule; menu-item unwrapping remains the caller's job
/// so it can choose whether filters should run.
pub(crate) fn keymap_reference_map(
    interp: &Interpreter,
    value: &Value,
    env: &Env,
) -> Option<Value> {
    if is_keymap_value(interp, value) {
        return Some(value.clone());
    }
    let Value::Symbol(symbol) = value else {
        return None;
    };
    interp
        .lookup_function(symbol, env)
        .ok()
        .filter(|function| is_keymap_value(interp, function))
}

pub(crate) fn unwrap_function_quote(value: &Value) -> Value {
    value
        .to_vec()
        .ok()
        .and_then(|items| match items.as_slice() {
            [Value::Symbol(symbol), inner] if symbol == "function" => Some(inner.clone()),
            _ => None,
        })
        .unwrap_or_else(|| value.clone())
}

pub(crate) fn keymap_binding_display_name(value: &Value) -> String {
    match value {
        Value::Nil => "undefined".into(),
        Value::Symbol(name) | Value::BuiltinFunc(name) => name.to_string(),
        Value::Record(_) => "Prefix Command".into(),
        Value::Cons(_) => value
            .to_vec()
            .ok()
            .and_then(|items| match items.as_slice() {
                [Value::Symbol(symbol), inner] if symbol == "function" || symbol == "quote" => {
                    Some(keymap_binding_display_name(inner))
                }
                _ => None,
            })
            .unwrap_or_else(|| value.to_string()),
        _ => value.to_string(),
    }
}

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

    let prefix = args
        .get(1)
        .filter(|value| !value.is_nil())
        .map(key_sequence_binding_text)
        .transpose()?;
    let mut output = String::from("key             binding\n---             -------\n");
    let mut seen = HashSet::new();

    let mut visited = HashSet::new();
    for map in current_active_maps(interp, env, None)? {
        collect_described_keymap_bindings(
            interp,
            &map,
            &[],
            prefix.as_deref(),
            &mut visited,
            &mut seen,
            &mut output,
            env,
        )?;
    }

    interp.insert_current_buffer(&output);
    Ok(Value::Nil)
}

#[allow(clippy::too_many_arguments)]
fn collect_described_keymap_bindings(
    interp: &mut Interpreter,
    map: &Value,
    prefix_parts: &[String],
    requested_prefix: Option<&str>,
    visited: &mut HashSet<((bool, usize), String)>,
    seen: &mut HashSet<String>,
    output: &mut String,
    env: &mut Env,
) -> Result<(), LispError> {
    let Some(identity) = keymap_value_identity(interp, map) else {
        return Ok(());
    };
    if !visited.insert((identity, prefix_parts.join(" "))) {
        return Ok(());
    }
    let bindings = keymap_direct_bindings(interp, map)?;
    for binding in bindings.iter() {
        let mut parts = prefix_parts.to_vec();
        parts.extend(binding_key_parts(binding));
        let key = parts.join(" ");
        let resolved = keymap_get_keyelt(interp, &binding.value, true, env)?;
        if let Some(nested) = keymap_reference_map(interp, &resolved, env) {
            collect_described_keymap_bindings(
                interp,
                &nested,
                &parts,
                requested_prefix,
                visited,
                seen,
                output,
                env,
            )?;
            continue;
        }
        if resolved.is_nil()
            || !requested_prefix
                .is_none_or(|prefix| key == prefix || key.starts_with(&format!("{prefix} ")))
            || !seen.insert(key.clone())
        {
            continue;
        }
        output.push_str(&format!(
            "{key:<16} {}\n",
            keymap_binding_display_name(&resolved)
        ));
    }
    Ok(())
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
    let Value::Integer(code) = event else {
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

pub(crate) fn key_parts_to_sequence_value(parts: &[String]) -> Value {
    let mut items = vec![Value::Symbol("vector-literal".into())];
    for part in parts {
        let (_, _, saw_prefix) = parse_kbd_prefixes(part);
        if !part.starts_with('<')
            && !part.ends_with('>')
            && !saw_prefix
            && named_kbd_key_code(part).is_none()
            && part.chars().count() > 1
        {
            items.push(Value::Symbol(part.clone().into()));
            continue;
        }
        items.extend(
            parse_kbd_token(part)
                .into_iter()
                .map(reader_key_event_value),
        );
    }
    Value::list(items)
}

fn where_is_binding_rank(parts: &[String]) -> (usize, bool) {
    let events = key_parts_to_sequence_value(parts)
        .to_vec()
        .unwrap_or_default();
    let event_count = events.len().saturating_sub(1);
    let starts_with_function_key = !matches!(events.get(1), Some(Value::Integer(_)));
    (event_count, starts_with_function_key)
}

pub(crate) fn accessible_keymaps(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    need_arg_range("accessible-keymaps", args, 1, 2)?;

    let mut queue: Vec<(Vec<String>, Value)> = Vec::new();
    if let Some(prefix) = args.get(1).filter(|value| !value.is_nil()) {
        let prefix_parts = key_sequence_keymap_parts(prefix)?;
        let target = keymap_lookup_sequence_value(interp, &args[0], &prefix_parts, env)?;
        if !is_keymap_value(interp, &target) {
            return Ok(Value::Nil);
        }
        queue.push((prefix_parts, target));
    } else if is_keymap_value(interp, &args[0]) {
        queue.push((Vec::new(), args[0].clone()));
    } else {
        return Ok(Value::Nil);
    }

    let mut seen_maps = HashSet::new();
    let mut index = 0usize;
    while index < queue.len() {
        let (prefix, map) = queue[index].clone();
        index += 1;
        let Some(identity) = keymap_value_identity(interp, &map) else {
            continue;
        };
        if !seen_maps.insert(identity) {
            continue;
        }
        for binding in keymap_direct_bindings(interp, &map)?.iter() {
            let resolved = keymap_get_keyelt(interp, &binding.value, false, env)?;
            let Some(prefix_map) = keymap_reference_map(interp, &resolved, env) else {
                continue;
            };
            let mut sequence = prefix.clone();
            sequence.extend(binding_key_parts(binding));
            queue.push((sequence, prefix_map));
        }
    }

    Ok(Value::list(queue.into_iter().map(|(parts, map)| {
        Value::cons(key_parts_to_sequence_value(&parts), map)
    })))
}

pub(crate) fn help_describe_vector(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    need_args("help--describe-vector", args, 7)?;
    let saved_buffer_id = interp.current_buffer_id();
    let Value::CharTable(table_id) = args[0] else {
        return Ok(Value::Nil);
    };
    let prefix = if args[1].is_nil() {
        Vec::new()
    } else {
        key_sequence_binding_parts(&args[1])?
    };
    let partial = args[3].is_truthy();
    let shadow = &args[4];
    let entire_map = &args[5];
    let mention_shadow = args[6].is_truthy();
    let entries = interp
        .char_table_effective_ranges(table_id)
        .unwrap_or_default();
    let mut ranges = Vec::<(u32, u32, Value, Value)>::new();

    for entry in entries {
        for code in entry.start..=entry.end {
            let definition = keymap_get_keyelt(interp, &entry.value, true, env)?;
            if definition.is_nil()
                || partial
                    && matches!(&definition, Value::Symbol(symbol)
                        if interp.get_symbol_property(symbol, "suppress-keymap")
                            .is_some_and(|value| value.is_truthy()))
            {
                continue;
            }
            let event_parts = vec![describe_key_code(i64::from(code))];
            if !entire_map.is_nil() {
                let effective =
                    keymap_lookup_sequence_value(interp, entire_map, &event_parts, env)?;
                if !values_eq_in_env(interp, &effective, &definition, env) {
                    continue;
                }
            }
            let mut shadowed_by = if shadow.is_nil() {
                Value::Nil
            } else {
                keymap_lookup_sequence_value(interp, shadow, &event_parts, env)?
            };
            // A binding does not shadow itself.  This is also the partition
            // key for ranges: adjacent characters may only coalesce when the
            // same definition shadows all of them.
            let shadowed =
                !shadowed_by.is_nil() && !values_eq_in_env(interp, &shadowed_by, &definition, env);
            if !shadowed {
                shadowed_by = Value::Nil;
            }
            if shadowed && !mention_shadow {
                continue;
            }

            if let Some((_, end, previous, previous_shadowed_by)) = ranges.last_mut()
                && end.saturating_add(1) == code
                && values_eq_in_env(interp, previous, &definition, env)
                && values_eq_in_env(interp, previous_shadowed_by, &shadowed_by, env)
            {
                *end = code;
            } else {
                ranges.push((code, code, definition, shadowed_by));
            }
        }
    }

    let mut first = true;
    interp.switch_to_buffer_id(saved_buffer_id)?;
    let output_buffer = Value::buffer(interp.current_buffer_id(), interp.buffer.name.clone());
    let restore = interp.bind_special_variable("standard-output", output_buffer, env)?;
    let mut result = (|| -> Result<Value, LispError> {
        for (start, end, definition, shadowed_by) in ranges {
            if first {
                interp.insert_current_buffer("\n");
                first = false;
            }
            let describe = |code| {
                let mut parts = prefix.clone();
                parts.push(describe_key_code(i64::from(code)));
                key_sequence_binding_text(&key_parts_to_sequence_value(&parts))
            };
            interp.insert_current_buffer(&describe(start)?);
            if start != end {
                interp.insert_current_buffer(" .. ");
                interp.insert_current_buffer(&describe(end)?);
            }
            call_function_value(interp, &args[2], &[definition], env)?;
            if !shadowed_by.is_nil() {
                let point = interp.buffer.point();
                if interp.buffer.char_before() == Some('\n') {
                    let _ = interp.delete_region_current_buffer(point - 1, point);
                }
                if let Value::Symbol(command) = shadowed_by {
                    interp
                        .insert_current_buffer(&format!("  (currently shadowed by `{command}')\n"));
                } else {
                    interp.insert_current_buffer("  (currently shadowed)\n");
                }
            }
        }
        Ok(Value::Nil)
    })();
    if let Err(error) = interp.restore_special_binding(restore, env)
        && result.is_ok()
    {
        result = Err(error);
    }
    result
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
            match mode {
                Value::Symbol(name) => Some(name),
                _ => None,
            }
        })
        .collect::<HashSet<_>>();

    let mut alists = Vec::new();
    if let Some(emulation_alists) = interp.lookup_var("emulation-mode-map-alists", env) {
        for element in emulation_alists.to_vec().unwrap_or_default() {
            alists.push(match element {
                Value::Symbol(variable) => interp.lookup_var(&variable, env).unwrap_or(Value::Nil),
                other => other,
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
            let Value::Symbol(mode_name) = mode else {
                continue;
            };
            if index == ordinary_index && overridden_modes.contains(&mode_name) {
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
    let buffer_keymap = |pos: usize| {
        buffer_property_at_with_category(interp, &interp.buffer, pos, "keymap")
            .filter(|value| is_keymap_value(interp, value))
    };

    let string_keymap = |value: &Value| {
        string_property_at_with_category(interp, value, 0, "keymap")
            .filter(|value| is_keymap_value(interp, value))
    };

    match posn {
        None => buffer_keymap(interp.buffer.point()),
        Some(value) if value.is_nil() => buffer_keymap(interp.buffer.point()),
        Some(value) if string_like(value).is_some() => {
            string_keymap(value).or_else(|| buffer_keymap(interp.buffer.point()))
        }
        Some(value) => {
            let items = value.to_vec().ok()?;
            let area = items.get(1)?;
            let string = items.get(4).and_then(|value| value.car().ok());
            if let Some(string) = string.as_ref().and_then(string_keymap) {
                return Some(string);
            }
            if !area.is_nil() {
                return None;
            }
            match items.get(5) {
                Some(Value::Integer(pos)) if *pos > 0 => buffer_keymap(*pos as usize),
                _ => buffer_keymap(interp.buffer.point()),
            }
        }
    }
}

pub(crate) fn current_active_maps(
    interp: &Interpreter,
    env: &Env,
    posn: Option<&Value>,
) -> Result<Vec<Value>, LispError> {
    let mut maps = Vec::new();
    if let Some(map) = interp.lookup_var("overriding-terminal-local-map", env)
        && is_keymap_value(interp, &map)
    {
        maps.push(map);
    }
    if let Some(map) = interp.lookup_var("overriding-local-map", env)
        && is_keymap_value(interp, &map)
    {
        maps.push(map);
    }
    if let Some(map) = keymap_at_active_position(interp, posn) {
        maps.push(map);
    }
    maps.extend(active_minor_mode_maps(interp, env)?);
    if let Some(map) = interp.lookup_var("current-local-map", env)
        && is_keymap_value(interp, &map)
    {
        maps.push(map);
    }
    let global_map = interp.current_global_map_value();
    if is_keymap_value(interp, &global_map) {
        maps.push(global_map);
    }
    Ok(maps)
}

pub(crate) fn where_is_internal_maps(
    interp: &Interpreter,
    arg: Option<&Value>,
    env: &Env,
) -> Result<Vec<Value>, LispError> {
    let Some(arg) = arg else {
        return current_active_maps(interp, env, None);
    };
    if arg.is_nil() {
        return current_active_maps(interp, env, None);
    }
    if is_keymap_value(interp, arg) {
        // A single keymap means that map followed by the global map.  A
        // one-element LIST of keymaps is the GNU spelling for searching only
        // that map; help.el deliberately uses both forms for its fallback.
        let mut maps = vec![arg.clone()];
        let global_map = interp.current_global_map_value();
        if is_keymap_value(interp, &global_map)
            && keymap_value_identity(interp, &global_map) != keymap_value_identity(interp, arg)
        {
            maps.push(global_map);
        }
        return Ok(maps);
    }
    let mut maps = Vec::new();
    for item in arg.to_vec()? {
        if is_keymap_value(interp, &item) {
            maps.push(item);
        }
    }
    Ok(maps)
}

pub(crate) fn where_is_internal(
    interp: &mut Interpreter,
    command: &str,
    keymaps: &[Value],
    first_only: bool,
    env: &mut Env,
) -> Result<Vec<Value>, LispError> {
    let maps_value = Value::list(keymaps.iter().cloned());
    let remapped_command = command_remapping(
        interp,
        &Value::Symbol(command.into()),
        Some(&maps_value),
        env,
    )
    .ok()
    .and_then(|value| command_name_for_remapping(&value));
    let target_command = remapped_command.as_deref().unwrap_or(command);

    let target_keymap_id = interp
        .lookup_function(target_command, env)
        .ok()
        .and_then(|function| keymap_record_id(interp, &function));
    let mut matches = Vec::<Vec<String>>::new();
    let mut collector = WhereIsCollector {
        target_command,
        target_keymap_id,
        env,
        visited: HashSet::new(),
        seen: HashSet::new(),
        matches: &mut matches,
    };
    for keymap in keymaps {
        collect_where_is_matches(interp, keymap, &[], &mut collector)?;
    }

    let active_maps = Value::list(keymaps.iter().cloned());
    matches.retain(|parts| {
        let Ok(value) = keymap_lookup_sequence_value(interp, &active_maps, parts, env) else {
            return false;
        };
        command_name_for_remapping(&value).as_deref() == Some(target_command)
            || (target_keymap_id.is_some() && keymap_record_id(interp, &value) == target_keymap_id)
    });

    let mut advertised_preferred = false;
    if remapped_command.is_none()
        && let Some(advertised) = interp
            .get_symbol_property(command, ":advertised-binding")
            .or_else(|| interp.get_symbol_property(command, "advertised-binding"))
        && let Ok(advertised_parts) = key_sequence_keymap_parts(&advertised)
        && let Some(index) = matches.iter().position(|parts| parts == &advertised_parts)
    {
        let preferred = matches.remove(index);
        matches.insert(0, preferred);
        advertised_preferred = true;
    }

    if first_only && !advertised_preferred {
        matches.sort_by_key(|parts| where_is_binding_rank(parts));
    }

    let preferred_modifier = preferred_modifier_name(interp, env);
    if first_only
        && !advertised_preferred
        && let Some(preferred) = preferred_modifier.as_deref()
        && let Some(index) = matches
            .iter()
            .position(|parts| parts_use_preferred_modifier(parts, preferred))
    {
        let preferred = matches.remove(index);
        matches.insert(0, preferred);
    }

    Ok(matches
        .into_iter()
        .map(|parts| {
            key_parts_to_sequence_value(&maybe_prefer_modifier_notation(interp, &parts, env))
        })
        .collect())
}

pub(crate) fn key_part_is_non_key_event(interp: &Interpreter, part: &str) -> bool {
    interp
        .get_symbol_property(
            part.trim_start_matches('<').trim_end_matches('>'),
            "non-key-event",
        )
        .is_some_and(|value| value.is_truthy())
}

pub(crate) fn key_parts_are_remap(parts: &[String]) -> bool {
    parts
        .first()
        .is_some_and(|part| canonical_key_part(part) == "remap")
}

pub(crate) fn maybe_prefer_modifier_notation(
    interp: &Interpreter,
    parts: &[String],
    env: &Env,
) -> Vec<String> {
    let Some(preferred) = preferred_modifier_name(interp, env) else {
        return parts.to_vec();
    };

    if !matches!(preferred.as_str(), "alt" | "meta") || parts.len() < 2 {
        return parts.to_vec();
    }
    if canonical_key_part(&parts[0]) != "esc" {
        return parts.to_vec();
    }

    let mut preferred_parts = Vec::with_capacity(parts.len() - 1);
    preferred_parts.push(format!("M-{}", parts[1]));
    preferred_parts.extend(parts.iter().skip(2).cloned());
    preferred_parts
}

fn preferred_modifier_name(interp: &Interpreter, env: &Env) -> Option<String> {
    match interp.lookup_var("where-is-preferred-modifier", env)? {
        Value::Symbol(symbol) => Some(symbol.to_string()),
        Value::String(text) => Some(text.to_string()),
        Value::StringObject(state) => Some(state.borrow().text.clone()),
        _ => None,
    }
}

fn parts_use_preferred_modifier(parts: &[String], preferred: &str) -> bool {
    let prefix = match preferred {
        "alt" => "A-",
        "meta" => "M-",
        "control" | "ctrl" => "C-",
        "hyper" => "H-",
        "shift" => "S-",
        "super" => "s-",
        _ => return false,
    };
    parts.iter().any(|part| part.starts_with(prefix))
        || matches!(preferred, "alt" | "meta")
            && parts
                .first()
                .is_some_and(|part| canonical_key_part(part) == "esc")
}

pub(crate) struct WhereIsCollector<'a> {
    target_command: &'a str,
    target_keymap_id: Option<u64>,
    env: &'a mut Env,
    visited: HashSet<((bool, usize), String)>,
    seen: HashSet<String>,
    matches: &'a mut Vec<Vec<String>>,
}

pub(crate) fn collect_where_is_matches(
    interp: &mut Interpreter,
    keymap: &Value,
    prefix: &[String],
    collector: &mut WhereIsCollector<'_>,
) -> Result<(), LispError> {
    let Some(identity) = keymap_value_identity(interp, keymap) else {
        return Ok(());
    };
    let prefix_key = prefix.join(" ");
    if !collector.visited.insert((identity, prefix_key)) {
        return Ok(());
    }
    for binding in keymap_direct_bindings(interp, keymap)?.iter() {
        let parts = binding_key_parts(binding);
        if parts
            .iter()
            .any(|part| key_part_is_non_key_event(interp, part))
        {
            continue;
        }
        let full_parts = prefix
            .iter()
            .cloned()
            .chain(parts.iter().cloned())
            .collect::<Vec<_>>();
        let resolved = keymap_get_keyelt(interp, &binding.value, true, collector.env)?;
        // A prefix command matches through symbol function indirection: the
        // binding may be the keymap that is the command's definition.
        let matches_target = command_name_for_remapping(&resolved).as_deref()
            == Some(collector.target_command)
            || (collector.target_keymap_id.is_some()
                && keymap_record_id(interp, &resolved) == collector.target_keymap_id);
        if !key_parts_are_remap(&full_parts) && matches_target {
            let key = full_parts.join(" ");
            if collector.seen.insert(key) {
                collector.matches.push(full_parts.clone());
            }
        }

        let nested = keymap_get_keyelt(interp, &binding.value, false, collector.env)?;
        if let Some(prefix_map) = keymap_reference_map(interp, &nested, collector.env) {
            collect_where_is_matches(interp, &prefix_map, &full_parts, collector)?;
        }
    }
    for parent in keymap_parent_values(interp, keymap) {
        collect_where_is_matches(interp, &parent, prefix, collector)?;
    }
    Ok(())
}


pub(crate) fn remap_key_binding_text(command: &str) -> String {
    format!("<remap> <{command}>")
}

pub(crate) fn command_name_for_remapping(value: &Value) -> Option<String> {
    match value {
        Value::Symbol(name) | Value::BuiltinFunc(name) => Some(name.to_string()),
        Value::Cons(_) => value
            .to_vec()
            .ok()
            .and_then(|items| match items.as_slice() {
                [Value::Symbol(symbol), inner] if symbol == "function" || symbol == "quote" => {
                    command_name_for_remapping(inner)
                }
                _ => None,
            }),
        _ => None,
    }
}

pub(crate) fn command_remapping(
    interp: &Interpreter,
    command: &Value,
    keymaps: Option<&Value>,
    env: &Env,
) -> Result<Value, LispError> {
    let Some(command_name) = command_name_for_remapping(command) else {
        return Ok(Value::Nil);
    };
    let maps = match keymaps {
        Some(keymaps) => where_is_internal_maps(interp, Some(keymaps), env)?,
        None => current_active_maps(interp, env, None)?,
    };
    command_remapping_in_maps(interp, &command_name, &maps)
}

/// The remap probe against already-assembled maps: record-backed keymaps
/// answer from their cached remap index, everything else takes the string
/// lookup path.  `key-binding' calls this once per keystroke.
pub(crate) fn command_remapping_in_maps(
    interp: &Interpreter,
    command_name: &str,
    maps: &[Value],
) -> Result<Value, LispError> {
    let mut remap_key = None;
    for map in maps {
        match remap_probe(interp, map, command_name, 32)? {
            RemapProbe::Found(binding) => return Ok(binding),
            RemapProbe::Absent => continue,
            RemapProbe::NeedsFullLookup => {
                let remap_key = remap_key
                    .get_or_insert_with(|| remap_key_binding_text(command_name))
                    .as_str();
                let binding = keymap_lookup_binding(interp, map, remap_key)?;
                if !binding.is_nil() {
                    return Ok(binding);
                }
            }
        }
    }
    Ok(Value::Nil)
}

enum RemapProbe {
    Found(Value),
    /// The map and its whole parent chain were index-covered and hold no
    /// remap entry — the common case for every self-inserting key.
    Absent,
    /// A non-record link in the chain: only the string lookup understands
    /// list keymaps, so this map needs the slow path.
    NeedsFullLookup,
}

/// Probe MAP's cached remap index, following record parents.  DEPTH bounds
/// cyclic parent chains the way the bounded string lookup does.
fn remap_probe(
    interp: &Interpreter,
    map: &Value,
    command_name: &str,
    depth: usize,
) -> Result<RemapProbe, LispError> {
    if depth == 0 {
        return Ok(RemapProbe::Absent);
    }
    let Some(id) = keymap_record_id(interp, map) else {
        return Ok(RemapProbe::NeedsFullLookup);
    };
    keymap_direct_bindings(interp, map)?;
    let index = (id as usize).saturating_sub(1);
    let cached = interp
        .keymap_bindings_cache
        .borrow()
        .get(index)
        .cloned()
        .flatten();
    let Some(cached) = cached else {
        return Ok(RemapProbe::NeedsFullLookup);
    };
    if let Some(remap_map) = &cached.remap_map {
        // The inner map's entries are one-part command names; it has few
        // entries and its own cache slot, so a scan stays cheap and
        // current.
        for binding in keymap_direct_bindings(interp, remap_map)?.iter() {
            let matches = match binding.parts.as_deref() {
                Some([part]) => canonical_key_part(part) == command_name,
                _ => false,
            };
            if matches && !binding.value.is_nil() {
                return Ok(RemapProbe::Found(binding.value.clone()));
            }
        }
    }
    let parent = interp
        .find_record(id)
        .and_then(|record| record.slots.get(KEYMAP_PARENT_SLOT).cloned());
    match parent {
        None | Some(Value::Nil) => Ok(RemapProbe::Absent),
        Some(parent) => remap_probe(interp, &parent, command_name, depth - 1),
    }
}

// The keymaps consulted for command dispatch, in GNU order: the overriding
// maps suppress minor-mode and local maps entirely; otherwise
// minor-mode-overriding-map-alist entries replace the matching
// minor-mode-map-alist entries, followed by the buffer's local map.
pub(crate) fn active_command_keymaps(
    interp: &Interpreter,
    env: &Env,
) -> Result<Vec<Value>, LispError> {
    let mut maps = Vec::new();
    if let Some(map) = interp
        .lookup_var("overriding-terminal-local-map", env)
        .filter(Value::is_truthy)
    {
        // `internal-push-keymap' marks transient compositions this way so an
        // unbound key resumes the ordinary local-map search.
        let add_active_maps = map.to_vec().ok().is_some_and(|items| {
            items
                .iter()
                .any(|item| matches!(item, Value::Symbol(name) if name == "add-keymap-witness"))
        });
        if is_keymap_value(interp, &map) {
            maps.push(map);
        }
        if !add_active_maps {
            return Ok(maps);
        }
    }
    if let Some(map) = interp
        .lookup_var("overriding-local-map", env)
        .filter(Value::is_truthy)
    {
        if is_keymap_value(interp, &map) {
            maps.push(map);
        }
        return Ok(maps);
    }
    maps.extend(
        active_minor_mode_bindings(interp, env)?
            .into_iter()
            .map(|(_, map)| map),
    );
    if let Some(map) = interp
        .lookup_var("current-local-map", env)
        .filter(Value::is_truthy)
        && is_keymap_value(interp, &map)
    {
        maps.push(map);
    }
    Ok(maps)
}

pub(crate) fn key_binding(
    interp: &Interpreter,
    key: &str,
    accept_default: bool,
    no_remap: bool,
    env: &Env,
) -> Result<Value, LispError> {
    key_binding_with_parts(
        interp,
        &approximate_key_parts(key),
        accept_default,
        no_remap,
        env,
    )
}

/// keymap.c:Fkey_binding over an already-decoded event-part sequence; the
/// value-aware decoding (`key_sequence_keymap_parts') preserves symbol
/// events like `left' that a textual round-trip loses.
pub(crate) fn key_binding_with_parts(
    interp: &Interpreter,
    key_parts: &[String],
    accept_default: bool,
    no_remap: bool,
    env: &Env,
) -> Result<Value, LispError> {
    let key_parts = key_parts.to_vec();
    let maps = active_command_keymaps(interp, env)?;
    let mut raw_binding = Value::Nil;
    for map in &maps {
        let binding = keymap_lookup_binding_exact_parts_with_default(
            interp,
            map,
            &key_parts,
            accept_default,
        )?;
        if !binding.is_nil() {
            raw_binding = binding;
            break;
        }
    }

    let global_map = interp.current_global_map_value();
    if raw_binding.is_nil() && is_keymap_value(interp, &global_map) {
        let binding = keymap_lookup_binding_exact_parts_with_default(
            interp,
            &global_map,
            &key_parts,
            accept_default,
        )?;
        if !binding.is_nil() {
            raw_binding = binding;
        }
    }

    if no_remap || raw_binding.is_nil() {
        return Ok(raw_binding);
    }

    let remapped = match command_name_for_remapping(&raw_binding) {
        // Reuse the maps this lookup already assembled (plus the global
        // map, which GNU's remap pass also consults).
        Some(command_name) => {
            let mut remap_maps = maps;
            if is_keymap_value(interp, &global_map) {
                remap_maps.push(global_map);
            }
            command_remapping_in_maps(interp, &command_name, &remap_maps)?
        }
        None => Value::Nil,
    };
    Ok(if remapped.is_nil() {
        raw_binding
    } else {
        remapped
    })
}

fn keymap_has_prefix(
    interp: &Interpreter,
    keymap: &Value,
    requested_parts: &[String],
) -> Result<bool, LispError> {
    let Some(id) = keymap_record_id(interp, keymap) else {
        return Ok(false);
    };
    let Some(record) = interp.find_record(id) else {
        return Ok(false);
    };
    for binding in keymap_bindings(record)?.into_iter() {
        let binding_parts = binding_key_parts(&binding);
        if binding_parts.len() > requested_parts.len()
            && key_parts_match(&binding_parts[..requested_parts.len()], requested_parts)
        {
            return Ok(true);
        }
    }
    match record.slots.get(KEYMAP_PARENT_SLOT) {
        Some(Value::Nil) | None => Ok(false),
        Some(parent) => keymap_has_prefix(interp, parent, requested_parts),
    }
}

// Whether KEY is a proper prefix of a longer binding in the active keymaps,
// so the command loop should keep reading events instead of dispatching.
pub(crate) fn key_sequence_is_prefix(
    interp: &Interpreter,
    key: &str,
    env: &Env,
) -> Result<bool, LispError> {
    // These prefix maps are present in GNU's standard global map even when
    // none of their descendants are represented in Emaxx's compact default
    // binding table.  In particular, C-c remains a prefix after a mode
    // removes its last C-c binding, so the command loop reports the complete
    // unbound sequence rather than declaring C-c itself undefined.
    if matches!(key, "C-c" | "C-x" | "C-x 4" | "C-x 5" | "ESC") {
        return Ok(true);
    }
    // Autoloaded prefix commands have a non-nil binding whose function cell
    // is `(autoload ... keymap)'.  GNU treats that as a keymap before loading
    // the owner; once loaded, the same symbol resolves directly to the map.
    let binding = key_binding(interp, key, false, true, env)?;
    if let Value::Symbol(name) = &binding
        && let Ok(function) = interp.lookup_function(name, env)
        && (is_keymap_value(interp, &function)
            || autoload_parts(&function).is_some_and(
                |(_, _, kind)| matches!(kind, Value::Symbol(kind) if kind == "keymap"),
            ))
    {
        return Ok(true);
    }
    let requested = approximate_key_parts(key);
    if requested.is_empty() {
        return Ok(false);
    }
    for map in active_command_keymaps(interp, env)? {
        if keymap_has_prefix(interp, &map, &requested)? {
            return Ok(true);
        }
    }
    let global_map = interp.current_global_map_value();
    if is_keymap_value(interp, &global_map) && keymap_has_prefix(interp, &global_map, &requested)? {
        return Ok(true);
    }
    Ok(false)
}

/// GNU `doc.c:Ftext_quoting_style' collapses the Lisp variable to one of
/// three effective styles.  Emaxx currently has no terminal display-table
/// override, so nil follows GNU's display-capable default and means `curve'.
pub(crate) fn effective_text_quoting_style(interp: &Interpreter, env: &Env) -> &'static str {
    match interp.lookup_var("text-quoting-style", env) {
        Some(Value::Symbol(style)) if style == "grave" => "grave",
        Some(Value::Symbol(style)) if style == "straight" => "straight",
        Some(Value::Symbol(style)) if style == "curve" => "curve",
        _ => "curve",
    }
}
