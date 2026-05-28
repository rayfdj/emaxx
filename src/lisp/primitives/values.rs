use super::*;

pub fn buffer_undo_list_value(buffer: &crate::buffer::Buffer) -> Value {
    let mut entries = buffer
        .undo_entries()
        .iter()
        .rev()
        .map(|entry| match entry {
            crate::buffer::UndoEntry::Insert { pos, len } => {
                Value::cons(Value::Integer(*pos as i64), Value::Integer(*len as i64))
            }
            crate::buffer::UndoEntry::Delete { pos, text, .. } => {
                Value::cons(Value::String(text.clone()), Value::Integer(*pos as i64))
            }
            crate::buffer::UndoEntry::Combined { display, .. }
            | crate::buffer::UndoEntry::Opaque(display) => display.clone(),
            crate::buffer::UndoEntry::Boundary => Value::Nil,
        })
        .collect::<Vec<_>>();
    entries.extend(buffer.undo_meta_entries().iter().rev().cloned());
    if buffer.file.is_some()
        && buffer.undo_entries().iter().any(|entry| {
            matches!(
                entry,
                crate::buffer::UndoEntry::Insert { .. } | crate::buffer::UndoEntry::Delete { .. }
            )
        })
    {
        entries.push(Value::list([
            Value::T,
            Value::Integer(0),
            Value::Integer(0),
            Value::Integer(0),
            Value::Integer(0),
        ]));
    }
    Value::list(entries)
}

pub(crate) fn values_equal(interp: &Interpreter, left: &Value, right: &Value) -> bool {
    values_equal_recursive(interp, left, right, &mut HashSet::new())
}

pub(crate) fn values_equal_checked(
    interp: &Interpreter,
    left: &Value,
    right: &Value,
) -> Result<bool, LispError> {
    ensure_acyclic_cons_graph(left)?;
    ensure_acyclic_cons_graph(right)?;
    Ok(values_equal(interp, left, right))
}

fn ensure_acyclic_cons_graph(value: &Value) -> Result<(), LispError> {
    fn visit(
        value: &Value,
        visiting: &mut HashSet<usize>,
        visited: &mut HashSet<usize>,
    ) -> Result<(), LispError> {
        let Value::Cons(car_cell, cdr_cell) = value else {
            return Ok(());
        };
        let ptr = Rc::as_ptr(car_cell) as usize;
        if visited.contains(&ptr) {
            return Ok(());
        }
        if !visiting.insert(ptr) {
            return Err(LispError::SignalValue(Value::list([
                Value::Symbol("circular-list".into()),
                Value::String("Circular list".into()),
            ])));
        }

        visit(&car_cell.borrow(), visiting, visited)?;
        visit(&cdr_cell.borrow(), visiting, visited)?;

        visiting.remove(&ptr);
        visited.insert(ptr);
        Ok(())
    }

    visit(value, &mut HashSet::new(), &mut HashSet::new())
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
    let Some(items) = record_literal_items(form) else {
        return false;
    };
    if let Some((car, _)) = form.cons_cells() {
        let pair = (record_id as usize, Rc::as_ptr(&car) as usize);
        if !seen.insert(pair) {
            return true;
        }
    }

    let expected_fields = if record.type_name == "literal-record" {
        record.slots.clone()
    } else {
        std::iter::once(Value::Symbol(record.type_name.clone()))
            .chain(record.slots.iter().cloned())
            .collect()
    };
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
        return left_string.text == right_string.text;
    }
    if is_bool_vector_value(interp, left) && is_bool_vector_value(interp, right) {
        return bool_vector_values(interp, left).ok() == bool_vector_values(interp, right).ok();
    }
    if let (Ok(left_items), Ok(right_items)) = (vector_items(left), vector_items(right))
        && matches!(left, Value::Cons(_, _))
        && matches!(right, Value::Cons(_, _))
    {
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
            &BigInt::from(*a) == b
        }
        (Value::Float(a), Value::Float(b)) => a == b || (a.is_nan() && b.is_nan()),
        (Value::String(a), Value::String(b)) => a == b,
        (Value::Symbol(a), Value::Symbol(b)) => a == b,
        (Value::BuiltinFunc(a), Value::BuiltinFunc(b)) => a == b,
        (Value::Buffer(a, _), Value::Buffer(b, _)) => a == b,
        (Value::Marker(a), Value::Marker(b)) => markers_equal(interp, *a, *b),
        (Value::Overlay(a), Value::Overlay(b)) => overlays_equal(interp, *a, *b),
        (Value::Record(left_id), Value::Record(right_id))
            if interp
                .find_record(*left_id)
                .is_some_and(|record| record.type_name == KEYMAP_RECORD_TYPE)
                && interp
                    .find_record(*right_id)
                    .is_some_and(|record| record.type_name == KEYMAP_RECORD_TYPE) =>
        {
            keymap_records_equal(interp, *left_id, *right_id, seen)
        }
        (Value::Record(left_id), Value::Cons(_, _))
            if interp
                .find_record(*left_id)
                .is_some_and(|record| record.type_name == KEYMAP_RECORD_TYPE) =>
        {
            keymap_record_equals_list(interp, *left_id, right, seen)
        }
        (Value::Cons(_, _), Value::Record(right_id))
            if interp
                .find_record(*right_id)
                .is_some_and(|record| record.type_name == KEYMAP_RECORD_TYPE) =>
        {
            keymap_record_equals_list(interp, *right_id, left, seen)
        }
        (Value::Record(left_id), _) if record_literal_items(right).is_some() => {
            record_equals_record_literal_form(interp, *left_id, right, seen)
        }
        (_, Value::Record(right_id)) if record_literal_items(left).is_some() => {
            record_equals_record_literal_form(interp, *right_id, left, seen)
        }
        (Value::Cons(_, _), Value::Cons(_, _)) => {
            let Some((left_car, _)) = left.cons_cells() else {
                return false;
            };
            let Some((right_car, _)) = right.cons_cells() else {
                return false;
            };
            let pair = (
                Rc::as_ptr(&left_car) as usize,
                Rc::as_ptr(&right_car) as usize,
            );
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
        _ => left == right,
    }
}

pub(crate) fn values_eql(left: &Value, right: &Value) -> bool {
    left == right
}

pub(crate) fn values_eq_in_env(
    interp: &Interpreter,
    left: &Value,
    right: &Value,
    env: &Env,
) -> bool {
    if let Some(equal) = symbol_with_pos_eq_in_env(interp, left, right, env) {
        return equal;
    }

    match (left, right) {
        (Value::Nil, Value::Nil) | (Value::T, Value::T) => true,
        (Value::Integer(a), Value::Integer(b)) => a == b,
        (Value::BigInteger(a), Value::BigInteger(b)) => a == b,
        (Value::Float(a), Value::Float(b)) => a == b,
        (Value::Symbol(a), Value::Symbol(b)) => a == b,
        (Value::BuiltinFunc(a), Value::BuiltinFunc(b)) => a == b,
        (Value::StringObject(left), Value::StringObject(right)) => Rc::ptr_eq(left, right),
        (Value::String(_), Value::String(_))
        | (Value::String(_), Value::StringObject(_))
        | (Value::StringObject(_), Value::String(_)) => false,
        (Value::Cons(left_car, left_cdr), Value::Cons(right_car, right_cdr)) => {
            Rc::ptr_eq(left_car, right_car) && Rc::ptr_eq(left_cdr, right_cdr)
        }
        (
            Value::Lambda(left_params, left_body, left_env),
            Value::Lambda(right_params, right_body, right_env),
        ) => {
            left_params == right_params
                && left_body == right_body
                && Rc::ptr_eq(left_env, right_env)
        }
        (Value::Buffer(left_id, _), Value::Buffer(right_id, _))
        | (Value::Marker(left_id), Value::Marker(right_id))
        | (Value::Overlay(left_id), Value::Overlay(right_id))
        | (Value::CharTable(left_id), Value::CharTable(right_id))
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
    let mut seen = HashSet::new();
    loop {
        match current {
            Value::Cons(car, cdr) => {
                let cell_id = Rc::as_ptr(&car) as usize;
                if !seen.insert(cell_id) {
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
        Value::BigInteger(n) => n.clone(),
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
            Value::Cons(car, cdr) => {
                let cell_id = Rc::as_ptr(&car) as usize;
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
            other => return other.cdr(),
        }
    }
}

pub(crate) fn sequence_length_value(interp: &Interpreter, value: &Value) -> Result<i64, LispError> {
    match value {
        item if string_like(item).is_some() => Ok(string_text(item)?.chars().count() as i64),
        Value::Nil => Ok(0),
        Value::Cons(_, _) if is_vector_value(value) => Ok(vector_items(value)?.len() as i64),
        Value::CharTable(_) => Ok(0x40_0000),
        item if is_bool_vector_value(interp, item) => {
            Ok(bool_vector_values(interp, item)?.len() as i64)
        }
        Value::Cons(_, _) => Ok(value.to_vec()?.len() as i64),
        Value::Record(id) => {
            let record = interp
                .find_record(*id)
                .ok_or_else(|| LispError::TypeError("record".into(), format!("record<{id}>")))?;
            Ok((record.slots.len() + 1) as i64)
        }
        _ => Err(LispError::TypeError("sequence".into(), value.type_name())),
    }
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
        return left_string.text == right_string.text && left_string.props == right_string.props;
    }
    if let (Ok(left_items), Ok(right_items)) = (vector_items(left), vector_items(right))
        && matches!(left, Value::Cons(_, _))
        && matches!(right, Value::Cons(_, _))
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
        (Value::Cons(_, _), Value::Cons(_, _)) => {
            let Some((left_car, _)) = left.cons_cells() else {
                return false;
            };
            let Some((right_car, _)) = right.cons_cells() else {
                return false;
            };
            let pair = (
                Rc::as_ptr(&left_car) as usize,
                Rc::as_ptr(&right_car) as usize,
            );
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RecordCompareKind {
    Generic,
    BoolVector,
    Process,
    HashTable,
    Obarray,
}

pub(crate) fn record_compare_kind(type_name: &str) -> RecordCompareKind {
    match type_name {
        "bool-vector" => RecordCompareKind::BoolVector,
        "process" => RecordCompareKind::Process,
        "hash-table" => RecordCompareKind::HashTable,
        "obarray" => RecordCompareKind::Obarray,
        _ => RecordCompareKind::Generic,
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

    let left_kind = record_compare_kind(&left_record.type_name);
    let right_kind = record_compare_kind(&right_record.type_name);
    if left_kind != right_kind {
        return Err(type_mismatch_signal(left, right));
    }

    match left_kind {
        RecordCompareKind::BoolVector => {
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
        RecordCompareKind::Process => Ok(Some(order_from_ordering(left_id.cmp(right_id)))),
        RecordCompareKind::HashTable | RecordCompareKind::Obarray => {
            Ok(Some(ValueOrder::Unordered))
        }
        RecordCompareKind::Generic => {
            let type_order =
                compare_plain_symbol_names(&left_record.type_name, &right_record.type_name);
            if type_order != ValueOrder::Equal {
                return Ok(Some(type_order));
            }
            Ok(Some(compare_sequence_values(
                interp,
                &left_record.slots,
                &right_record.slots,
                env,
                seen_lists,
            )?))
        }
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

    if matches!(left, Value::Buffer(_, _)) || matches!(right, Value::Buffer(_, _)) {
        return match (left, right) {
            (Value::Buffer(left_id, _), Value::Buffer(right_id, _)) => {
                Ok(compare_buffer_ids(interp, *left_id, *right_id))
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

    if matches!(left, Value::Cons(_, _)) || matches!(right, Value::Cons(_, _)) {
        return match (left, right) {
            (Value::Nil, Value::Nil) => Ok(ValueOrder::Equal),
            (Value::Nil, Value::Cons(_, _)) => Ok(ValueOrder::Less),
            (Value::Cons(_, _), Value::Nil) => Ok(ValueOrder::Greater),
            (Value::Cons(left_car, _), Value::Cons(right_car, _)) => {
                let key = (
                    Rc::as_ptr(left_car) as usize,
                    Rc::as_ptr(right_car) as usize,
                );
                if !seen_lists.insert(key) {
                    return Err(circular_signal(left));
                }
                let result = (|| {
                    let Some((left_head, left_tail)) = left.cons_values() else {
                        return Ok(ValueOrder::Unordered);
                    };
                    let Some((right_head, right_tail)) = right.cons_values() else {
                        return Ok(ValueOrder::Unordered);
                    };
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
            left_string.text.cmp(&right_string.text),
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
    if !matches!(value, Value::Cons(_, _)) || is_vector_value(value) {
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
        let mut result = vec![Value::symbol("vector")];
        result.extend(filtered);
        return Ok(Value::list(result));
    }

    match sequence {
        Value::Nil | Value::Cons(_, _) => Ok(Value::list(
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

pub(crate) fn rassq_delete_all(key: &Value, alist: &Value) -> Result<Value, LispError> {
    let filtered = alist
        .to_vec()?
        .into_iter()
        .filter(|entry| match entry {
            Value::Cons(_, _) => entry.cdr().is_ok_and(|value| value != *key),
            _ => true,
        })
        .collect::<Vec<_>>();
    Ok(Value::list(filtered))
}

pub(crate) fn assq_delete_all(key: &Value, alist: &Value) -> Result<Value, LispError> {
    let filtered = alist
        .to_vec()?
        .into_iter()
        .filter(|entry| match entry {
            Value::Cons(_, _) => entry.car().is_ok_and(|value| value != *key),
            _ => true,
        })
        .collect::<Vec<_>>();
    Ok(Value::list(filtered))
}

pub(crate) fn assoc_delete_all(
    interp: &Interpreter,
    key: &Value,
    alist: &Value,
) -> Result<Value, LispError> {
    let filtered = alist
        .to_vec()?
        .into_iter()
        .filter(|entry| match entry {
            Value::Cons(_, _) => entry
                .car()
                .is_ok_and(|value| !values_equal(interp, &value, key)),
            _ => true,
        })
        .collect::<Vec<_>>();
    Ok(Value::list(filtered))
}

pub(crate) fn format_prompt(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    if args.len() < 2 {
        return Err(LispError::WrongNumberOfArgs(
            "format-prompt".into(),
            args.len(),
        ));
    }

    let prompt = if args.len() == 2 {
        call(interp, "substitute-command-keys", &[args[0].clone()], env)?
    } else {
        let mut format_args = Vec::with_capacity(args.len() - 1);
        format_args.push(call(
            interp,
            "substitute-command-keys",
            &[args[0].clone()],
            env,
        )?);
        format_args.extend_from_slice(&args[2..]);
        call(interp, "format", &format_args, env)?
    };

    let default = match &args[1] {
        Value::Nil => None,
        Value::Cons(_, _) => args[1].car().ok(),
        other => Some(other.clone()),
    }
    .filter(|value| {
        string_like(value)
            .map(|string| !string.text.is_empty())
            .unwrap_or(true)
    });

    let mut result = string_text(&prompt)?.to_string();
    if let Some(default) = default {
        let default_format = interp
            .lookup_var("minibuffer-default-prompt-format", env)
            .and_then(|value| string_like(&value).map(|string| string.text))
            .unwrap_or_else(|| " (default %s)".into());
        let default_string = match string_like(&default) {
            Some(string) => string.text,
            None => default.to_string(),
        };
        let format_args = [Value::String(default_format), Value::String(default_string)];
        result.push_str(&string_text(&call(interp, "format", &format_args, env)?)?);
    }
    result.push_str(": ");
    Ok(Value::String(result))
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

pub(crate) fn copy_tree_value(
    interp: &mut Interpreter,
    value: &Value,
    vectors_and_records: bool,
) -> Result<Value, LispError> {
    if is_vector_value(value) {
        if !vectors_and_records {
            return Ok(value.clone());
        }
        let items = value.to_vec()?;
        let mut copied = Vec::with_capacity(items.len());
        if let Some(tag) = items.first() {
            copied.push(tag.clone());
        }
        for item in items.into_iter().skip(1) {
            copied.push(copy_tree_value(interp, &item, true)?);
        }
        return Ok(Value::list(copied));
    }

    match value {
        Value::Cons(_, _) => {
            let Some((car, cdr)) = value.cons_values() else {
                return Ok(value.clone());
            };
            Ok(Value::cons(
                copy_tree_value(interp, &car, vectors_and_records)?,
                copy_tree_value(interp, &cdr, vectors_and_records)?,
            ))
        }
        Value::Record(id) if vectors_and_records => {
            let record = interp
                .find_record(*id)
                .cloned()
                .ok_or_else(|| LispError::TypeError("record".into(), format!("record<{id}>")))?;
            let mut slots = Vec::with_capacity(record.slots.len());
            for slot in &record.slots {
                slots.push(copy_tree_value(interp, slot, true)?);
            }
            Ok(interp.create_record(&record.type_name, slots))
        }
        _ => Ok(value.clone()),
    }
}

pub(crate) fn flatten_tree_value(value: &Value, leaves: &mut Vec<Value>) {
    match value {
        Value::Nil => {}
        Value::Cons(car, cdr) => {
            flatten_tree_value(&car.borrow(), leaves);
            flatten_tree_value(&cdr.borrow(), leaves);
        }
        leaf => leaves.push(leaf.clone()),
    }
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
    pub(crate) user: Option<String>,
    pub(crate) host: String,
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
    let (user, host) = match authority.rsplit_once('@') {
        Some((user, host)) if !host.is_empty() => (Some(user.to_string()), host.to_string()),
        _ if authority.is_empty() && method == "mock" => (None, String::new()),
        _ if authority.is_empty() => return None,
        _ => (None, authority.to_string()),
    };
    Some(RemoteFileNameParts {
        prefix: path[..1 + method_end + 1 + host_end + 1].to_string(),
        method,
        user,
        host,
        localname,
    })
}

#[derive(Default)]
pub(crate) struct ClDeleteIfOptions {
    start: usize,
    end: Option<usize>,
    from_end: bool,
}

pub(crate) fn collect_list_cells(value: &Value) -> Result<(Vec<Value>, Value), LispError> {
    let mut cells = Vec::new();
    let mut current = value.clone();
    let mut seen = HashSet::new();
    loop {
        match current.clone() {
            Value::Nil => return Ok((cells, Value::Nil)),
            Value::Cons(car, cdr) => {
                let cell_id = Rc::as_ptr(&car) as usize;
                if !seen.insert(cell_id) {
                    return Err(LispError::SignalValue(Value::list([
                        Value::symbol("circular-list"),
                        Value::string("Circular list"),
                    ])));
                }
                cells.push(current.clone());
                current = cdr.borrow().clone();
            }
            other => {
                return Err(LispError::TypeError("list".into(), other.type_name()));
            }
        }
    }
}

pub(crate) fn parse_cl_delete_if_options(args: &[Value]) -> Result<ClDeleteIfOptions, LispError> {
    let mut options = ClDeleteIfOptions::default();
    let mut index = 2usize;
    while index < args.len() {
        let Some(keyword) = args[index].as_symbol().ok() else {
            return Err(LispError::Signal("Unsupported cl-delete-if syntax".into()));
        };
        let Some(value) = args.get(index + 1) else {
            return Err(LispError::Signal("Unsupported cl-delete-if syntax".into()));
        };
        match keyword {
            ":start" => {
                options.start = value.as_integer()?.max(0) as usize;
            }
            ":end" => {
                options.end = Some(value.as_integer()?.max(0) as usize);
            }
            ":from-end" => {
                options.from_end = value.is_truthy();
            }
            _ => return Err(LispError::Signal("Unsupported cl-delete-if syntax".into())),
        }
        index += 2;
    }
    Ok(options)
}

pub(crate) fn cl_delete_if_values(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    if args.len() < 2 {
        return Err(LispError::WrongNumberOfArgs(
            "cl-delete-if".into(),
            args.len(),
        ));
    }

    let options = parse_cl_delete_if_options(args)?;
    let _ = options.from_end;
    let (cells, tail) = collect_list_cells(&args[1])?;
    let len = cells.len();
    let start = options.start.min(len);
    let end = options.end.unwrap_or(len).min(len);
    let mut keep = vec![true; len];
    let predicate = resolve_callable(interp, &args[0], env)?;
    let predicate_name = args[0].as_symbol().ok();

    for index in start..end {
        let item = cells[index].car()?;
        if interp
            .call_function_value(
                predicate.clone(),
                predicate_name,
                std::slice::from_ref(&item),
                env,
            )?
            .is_truthy()
        {
            keep[index] = false;
        }
    }
    let kept_cells = cells
        .iter()
        .zip(keep.iter())
        .filter_map(|(cell, keep_cell)| keep_cell.then_some(cell.clone()))
        .collect::<Vec<_>>();

    if kept_cells.is_empty() {
        return Ok(tail);
    }

    for window in kept_cells.windows(2) {
        window[0].set_cdr(window[1].clone())?;
    }
    if let Some(last) = kept_cells.last() {
        last.set_cdr(tail)?;
    }
    Ok(kept_cells[0].clone())
}

pub(crate) fn last_nconc_cell(value: &Value) -> Result<Value, LispError> {
    let mut current = value.clone();
    let mut seen = HashSet::new();
    loop {
        let Value::Cons(car, cdr) = current.clone() else {
            return Err(LispError::TypeError("consp".into(), current.type_name()));
        };
        let cell_id = Rc::as_ptr(&car) as usize;
        if !seen.insert(cell_id) {
            return Err(LispError::SignalValue(Value::list([
                Value::symbol("circular-list"),
                Value::string("Circular list"),
            ])));
        }
        match cdr.borrow().clone() {
            Value::Cons(_, _) => current = cdr.borrow().clone(),
            _ => return Ok(current),
        }
    }
}

pub(crate) fn sxhash_value(interp: &Interpreter, value: &Value, mode: HashMode) -> i64 {
    let mut state = 0xcbf2_9ce4_8422_2325u64;
    hash_value_recursive(interp, &mut state, value, mode);
    (state & 0x7fff_ffff_ffff_ffff) as i64
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

pub(crate) fn hash_props(interp: &Interpreter, state: &mut u64, props: &[StringPropertySpan]) {
    hash_mix(state, props.len() as u64);
    for span in props {
        hash_mix(state, span.start as u64);
        hash_mix(state, span.end as u64);
        hash_mix(state, span.props.len() as u64);
        for (key, value) in &span.props {
            hash_str(state, key);
            hash_value_recursive(interp, state, value, HashMode::EqualIncludingProperties);
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
        Value::Lambda(_, _, env) => {
            hash_mix(state, 7);
            hash_mix(state, Rc::as_ptr(env) as usize as u64);
        }
        Value::Buffer(id, _) => {
            hash_mix(state, 8);
            hash_mix(state, *id);
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
        Value::Record(id) => {
            hash_mix(state, 12);
            hash_mix(state, *id);
        }
        Value::Finalizer(id) => {
            hash_mix(state, 13);
            hash_mix(state, *id);
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
        Value::Cons(_, _) => {
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
    match value {
        Value::Nil => hash_mix(state, 30),
        Value::T => hash_mix(state, 31),
        Value::Integer(number) => {
            hash_mix(state, 32);
            hash_mix(state, *number as u64);
        }
        Value::BigInteger(number) => {
            hash_mix(state, 33);
            hash_str(state, &number.to_string());
        }
        Value::Float(number) => {
            hash_mix(state, 34);
            hash_mix(state, number.to_bits());
        }
        Value::String(text) => {
            hash_mix(state, 35);
            hash_str(state, text);
        }
        Value::StringObject(shared) => {
            hash_mix(state, 36);
            let shared = shared.borrow();
            hash_str(state, &shared.text);
            if include_properties {
                hash_props(interp, state, &shared.props);
            }
        }
        Value::Symbol(symbol) => {
            hash_mix(state, 37);
            hash_str(state, symbol);
        }
        Value::Cons(_, _) => {
            let Some((car, cdr)) = value.cons_values() else {
                return;
            };
            hash_mix(state, 38);
            hash_value_equal(interp, state, &car, include_properties);
            hash_value_equal(interp, state, &cdr, include_properties);
        }
        Value::BuiltinFunc(name) => {
            hash_mix(state, 39);
            hash_str(state, name);
        }
        Value::Lambda(params, body, _) => {
            hash_mix(state, 40);
            for param in params {
                hash_str(state, param);
            }
            for form in body {
                hash_value_equal(interp, state, form, include_properties);
            }
        }
        Value::Buffer(id, name) => {
            hash_mix(state, 41);
            hash_mix(state, *id);
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
            hash_char_table_equal(interp, state, *id, include_properties);
        }
        Value::Record(id) => {
            hash_record_equal(interp, state, *id, include_properties);
        }
        Value::Finalizer(id) => {
            hash_mix(state, 46);
            hash_mix(state, *id);
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
    hash_value_equal(interp, state, &table.default, include_properties);
    hash_mix(state, table.extra_slots.len() as u64);
    for slot in &table.extra_slots {
        hash_value_equal(interp, state, slot, include_properties);
    }
    hash_mix(state, table.entries.len() as u64);
    for entry in &table.entries {
        hash_mix(state, entry.start as u64);
        hash_mix(state, entry.end as u64);
        hash_value_equal(interp, state, &entry.value, include_properties);
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
) {
    hash_mix(state, 45);
    let Some(record) = interp.find_record(id) else {
        hash_mix(state, id);
        return;
    };

    match record_compare_kind(&record.type_name) {
        RecordCompareKind::BoolVector => {
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
        RecordCompareKind::Process | RecordCompareKind::HashTable | RecordCompareKind::Obarray => {
            hash_str(state, &record.type_name);
            hash_mix(state, id);
        }
        RecordCompareKind::Generic => {
            hash_str(state, &record.type_name);
            hash_mix(state, record.slots.len() as u64);
            for slot in &record.slots {
                hash_value_equal(interp, state, slot, include_properties);
            }
        }
    }
}

pub(crate) fn custom_current_group_file(interp: &Interpreter) -> Option<String> {
    interp.current_load_file().map(str::to_string)
}

pub(crate) fn custom_group_assoc_cdr(list: &Value, key: &str) -> Option<Value> {
    let entries = list.to_vec().ok()?;
    for entry in entries {
        if let Some((car, cdr)) = entry.cons_values()
            && string_text(&car).ok().as_deref() == Some(key)
        {
            return Some(cdr);
        }
    }
    None
}

pub(crate) fn custom_current_group(interp: &Interpreter) -> Option<Value> {
    let file = custom_current_group_file(interp)?;
    let alist = interp
        .symbol_value_cell("custom-current-group-alist")
        .ok()?;
    custom_group_assoc_cdr(&alist, &file)
}

pub(crate) fn custom_set_current_group(interp: &mut Interpreter, group: &str) {
    let Some(file) = custom_current_group_file(interp) else {
        return;
    };
    let entry = Value::cons(Value::String(file.clone()), Value::Symbol(group.into()));
    let existing = interp
        .symbol_value_cell("custom-current-group-alist")
        .unwrap_or(Value::Nil);
    let mut entries = existing.to_vec().unwrap_or_default();
    if let Some(index) = entries.iter().position(|value| match value {
        Value::Cons(_, _) => {
            value
                .cons_values()
                .and_then(|(car, _)| string_text(&car).ok())
                .as_deref()
                == Some(file.as_str())
        }
        _ => false,
    }) {
        entries[index] = entry;
    } else {
        entries.insert(0, entry);
    }
    interp.set_global_binding("custom-current-group-alist", Value::list(entries));
}

pub(crate) fn custom_add_to_group(
    interp: &mut Interpreter,
    group: &str,
    option: Value,
    widget: Value,
) {
    let entry = Value::list([option, widget]);
    let members = interp
        .get_symbol_property(group, "custom-group")
        .unwrap_or(Value::Nil);
    let existing = members.to_vec().unwrap_or_default();
    if existing
        .iter()
        .any(|value| values_equal(interp, value, &entry))
    {
        return;
    }
    let updated = if members.is_nil() {
        Value::list([entry])
    } else {
        nconc_values(&[members, Value::list([entry.clone()])])
            .unwrap_or_else(|_| Value::list([entry]))
    };
    interp.put_symbol_property(group, "custom-group", updated);
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

pub(crate) fn literal_form(value: &Value) -> Value {
    match value {
        Value::Cons(_, _) | Value::Symbol(_) => {
            Value::list([Value::Symbol("quote".into()), value.clone()])
        }
        other => other.clone(),
    }
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

pub(crate) fn seq_uniq(
    interp: &mut Interpreter,
    sequence: &Value,
    testfn: Option<&Value>,
    env: &mut Env,
) -> Result<Value, LispError> {
    let mut unique = Vec::new();
    let default_test = Value::Symbol("equal".into());
    let testfn = testfn.or(Some(&default_test));
    for item in sequence.to_vec()? {
        let mut seen = false;
        for existing in &unique {
            if value_matches_with_test(interp, &item, existing, testfn, env)? {
                seen = true;
                break;
            }
        }
        if !seen {
            unique.push(item);
        }
    }
    Ok(Value::list(unique))
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
        Value::Symbol(name) => Some(name.clone()),
        _ => match resolved {
            Value::BuiltinFunc(name) => Some(name.clone()),
            _ => None,
        },
    }
}

pub(crate) fn keymap_placeholder(name: Option<&str>) -> Value {
    let mut items = vec![Value::Symbol("keymap".into())];
    if let Some(name) = name {
        items.push(Value::String(name.into()));
    }
    Value::list(items)
}

pub(crate) const KEYMAP_RECORD_TYPE: &str = "keymap";
pub(crate) const KEYMAP_PARENT_SLOT: usize = 1;
pub(crate) const KEYMAP_BINDINGS_SLOT: usize = 2;
pub(crate) const KEYMAP_CHAR_TABLE_SLOT: usize = 3;

pub(crate) fn make_runtime_keymap(interp: &mut Interpreter, name: Option<&str>) -> Value {
    interp.create_record(
        KEYMAP_RECORD_TYPE,
        vec![
            name.map(Value::string).unwrap_or(Value::Nil),
            Value::Nil,
            Value::Nil,
        ],
    )
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
    keymap
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
        .filter(|record| record.type_name == KEYMAP_RECORD_TYPE)
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

pub(crate) fn keymap_bindings_value(bindings: Vec<RuntimeKeymapBinding>) -> Value {
    Value::list(bindings.into_iter().map(|binding| {
        Value::list([
            Value::String(binding.key),
            binding.value,
            if binding.after_prompt {
                Value::T
            } else {
                Value::Nil
            },
            binding
                .parts
                .map(|parts| Value::list(parts.into_iter().map(Value::String)))
                .unwrap_or(Value::Nil),
        ])
    }))
}

pub(crate) fn keymap_define_binding(
    interp: &mut Interpreter,
    keymap: &Value,
    key: &str,
    binding: Value,
) -> Result<(), LispError> {
    keymap_define_binding_with_placement(interp, keymap, key, None, binding, true)
}

pub(crate) fn keymap_define_binding_with_placement(
    interp: &mut Interpreter,
    keymap: &Value,
    key: &str,
    key_parts: Option<Vec<String>>,
    binding: Value,
    after_prompt: bool,
) -> Result<(), LispError> {
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
        (bindings.len(), true)
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
    Ok(())
}

pub(crate) fn keymap_define_binding_after(
    interp: &mut Interpreter,
    keymap: &Value,
    key: &str,
    key_parts: Option<Vec<String>>,
    binding: Value,
    after_parts: Option<&[String]>,
) -> Result<(), LispError> {
    let Some(id) = keymap_record_id(interp, keymap) else {
        return Ok(());
    };
    let Some(record) = interp.find_record_mut(id) else {
        return Ok(());
    };
    let mut bindings = keymap_bindings(record)?;
    if let Some(index) = bindings.iter().position(|existing| existing.key == key) {
        bindings.remove(index);
    }

    let insert_at = after_parts
        .and_then(|after| {
            bindings
                .iter()
                .position(|existing| key_parts_match(&binding_key_parts(existing), after))
                .map(|index| index + 1)
        })
        .or_else(|| {
            bindings
                .iter()
                .rposition(|existing| existing.after_prompt)
                .map(|index| index + 1)
        })
        .unwrap_or(bindings.len());

    bindings.insert(
        insert_at,
        RuntimeKeymapBinding {
            key: key.to_string(),
            parts: key_parts,
            value: binding,
            after_prompt: true,
        },
    );
    if record.slots.len() <= KEYMAP_BINDINGS_SLOT {
        record.slots.resize(KEYMAP_BINDINGS_SLOT + 1, Value::Nil);
    }
    record.slots[KEYMAP_BINDINGS_SLOT] = keymap_bindings_value(bindings);
    Ok(())
}

pub(crate) fn keymap_remove_binding(
    interp: &mut Interpreter,
    keymap: &Value,
    key: &str,
) -> Result<(), LispError> {
    let Some(id) = keymap_record_id(interp, keymap) else {
        return Ok(());
    };
    let Some(record) = interp.find_record_mut(id) else {
        return Ok(());
    };
    let mut bindings = keymap_bindings(record)?;
    bindings.retain(|existing| existing.key != key);
    if record.slots.len() <= KEYMAP_BINDINGS_SLOT {
        record.slots.resize(KEYMAP_BINDINGS_SLOT + 1, Value::Nil);
    }
    record.slots[KEYMAP_BINDINGS_SLOT] = keymap_bindings_value(bindings);
    Ok(())
}

pub(crate) fn approximate_key_parts(key: &str) -> Vec<String> {
    key_sequence_binding_parts(&Value::String(key.to_string()))
        .unwrap_or_else(|_| key.split_whitespace().map(str::to_string).collect())
}

pub(crate) fn binding_key_parts(binding: &RuntimeKeymapBinding) -> Vec<String> {
    binding
        .parts
        .clone()
        .unwrap_or_else(|| approximate_key_parts(&binding.key))
}

pub(crate) fn canonical_key_part(part: &str) -> String {
    part.trim_start_matches('<')
        .trim_end_matches('>')
        .replace("\\ ", "-")
        .replace(' ', "-")
        .to_lowercase()
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
    }

    true
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
    let Some(id) = keymap_record_id(interp, keymap) else {
        return Ok(Value::Nil);
    };
    let Some(record) = interp.find_record(id) else {
        return Ok(Value::Nil);
    };
    for binding in keymap_bindings(record)?.into_iter() {
        if key_parts_match(&binding_key_parts(&binding), key_parts) {
            return Ok(binding.value);
        }
    }
    if key_parts.len() == 1 {
        for binding in keymap_bindings(record)?.into_iter() {
            if binding_key_parts(&binding) == ["t".to_string()] {
                return Ok(binding.value);
            }
        }
    }
    match record.slots.get(KEYMAP_PARENT_SLOT) {
        Some(Value::Nil) | None => Ok(Value::Nil),
        Some(parent) => keymap_lookup_binding_exact_parts(interp, parent, key_parts),
    }
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
    env: &mut Env,
) -> Result<KeyLookupResult, LispError> {
    if key_parts.is_empty() {
        return Ok(KeyLookupResult::Missing);
    }

    let binding = keymap_lookup_binding_exact_parts(interp, keymap, key_parts)?;
    if !binding.is_nil() {
        return Ok(KeyLookupResult::Value(keymap_get_keyelt(
            interp, &binding, true, env,
        )?));
    }

    for prefix_len in (1..key_parts.len()).rev() {
        let binding = keymap_lookup_binding_exact_parts(interp, keymap, &key_parts[..prefix_len])?;
        if binding.is_nil() {
            continue;
        }
        let resolved = keymap_get_keyelt(interp, &binding, true, env)?;
        if is_keymap_value(interp, &resolved) {
            match keymap_lookup_sequence_single_map(
                interp,
                &resolved,
                &key_parts[prefix_len..],
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
    if is_keymap_value(interp, keymap_or_maps) {
        return Ok(
            match keymap_lookup_sequence_single_map(interp, keymap_or_maps, key_parts, env)? {
                KeyLookupResult::Missing => Value::Nil,
                KeyLookupResult::Value(value) => value,
                KeyLookupResult::PrefixLen(len) => Value::Integer(len as i64),
            },
        );
    }

    let mut prefix_match = None;
    for keymap in keymap_or_maps.to_vec()? {
        if !is_keymap_value(interp, &keymap) {
            continue;
        }
        match keymap_lookup_sequence_single_map(interp, &keymap, key_parts, env)? {
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
        let Value::Cons(_, _) = current else {
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
        Value::Symbol(name) | Value::BuiltinFunc(name) => name.clone(),
        Value::Record(_) => "Prefix Command".into(),
        Value::Cons(_, _) => value
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
    let saved_buffer_id = interp.current_buffer_id();
    let mut output = String::from("key             binding\n---             -------\n");
    let mut seen = HashSet::new();

    for map in current_active_maps(interp, env, None)? {
        let Some(id) = keymap_record_id(interp, &map) else {
            continue;
        };
        let Some(record) = interp.find_record(id) else {
            continue;
        };
        for binding in keymap_bindings(record)? {
            if !prefix.as_deref().is_none_or(|prefix| {
                binding.key == prefix || binding.key.starts_with(&format!("{prefix} "))
            }) {
                continue;
            }
            if !seen.insert(binding.key.clone()) {
                continue;
            }
            let resolved = keymap_get_keyelt(interp, &binding.value, true, env)?;
            if resolved.is_nil() {
                continue;
            }
            output.push_str(&format!(
                "{:<16} {}\n",
                binding.key,
                keymap_binding_display_name(&resolved)
            ));
        }
    }

    interp.switch_to_buffer_id(saved_buffer_id)?;
    interp.insert_current_buffer(&output);
    Ok(Value::Nil)
}

pub(crate) fn reader_control_char(base: i64) -> Option<i64> {
    let ch = char::from_u32(base as u32)?;
    match ch {
        '@' | '`' | ' ' => Some(0),
        '?' => Some(0x7f),
        _ if ch.is_ascii() => Some(i64::from((ch.to_ascii_lowercase() as u8) & 0x1f)),
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
            items.push(Value::Symbol(part.clone()));
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

pub(crate) fn accessible_keymaps(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    need_arg_range("accessible-keymaps", args, 1, 2)?;

    let mut queue: Vec<(Vec<String>, Value)> = Vec::new();
    if let Some(prefix) = args.get(1).filter(|value| !value.is_nil()) {
        let prefix_parts = key_sequence_binding_parts(prefix)?;
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
        let (_, map) = &queue[index];
        index += 1;
        let Some(id) = keymap_record_id(interp, map) else {
            continue;
        };
        if !seen_maps.insert(id) {
            continue;
        }
        let Some(record) = interp.find_record(id) else {
            continue;
        };
        for binding in keymap_bindings(record)? {
            let resolved = keymap_get_keyelt(interp, &binding.value, false, env)?;
            if !is_keymap_value(interp, &resolved) {
                continue;
            }
            let mut sequence = queue[index - 1].0.clone();
            sequence.extend(binding_key_parts(&binding));
            queue.push((sequence, resolved));
        }
    }

    Ok(Value::list(queue.into_iter().map(|(parts, map)| {
        Value::cons(key_parts_to_sequence_value(&parts), map)
    })))
}

pub(crate) fn single_char_binding_key(parts: &[String]) -> Option<char> {
    let [part] = parts else {
        return None;
    };
    let mut chars = part.chars();
    let ch = chars.next()?;
    (chars.next().is_none()).then_some(ch)
}

pub(crate) fn help_describe_vector(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    need_args("help--describe-vector", args, 7)?;

    let mention_shadow = args[6].is_truthy();
    let shadow = args[4].clone();
    let entire_map = args[5].clone();
    let Some(id) = keymap_record_id(interp, &entire_map) else {
        return Ok(Value::Nil);
    };
    let Some(record) = interp.find_record(id) else {
        return Ok(Value::Nil);
    };

    let mut bindings = keymap_bindings(record)?
        .into_iter()
        .filter_map(|binding| {
            let parts = binding_key_parts(&binding);
            single_char_binding_key(&parts).map(|ch| (ch, binding.value))
        })
        .collect::<Vec<_>>();
    bindings.sort_by_key(|(ch, _)| *ch as u32);

    let mut lines = Vec::new();
    let mut range_start = None::<char>;
    let mut range_end = None::<char>;
    let mut range_command = String::new();

    let flush_range =
        |lines: &mut Vec<String>, start: Option<char>, end: Option<char>, command: &str| {
            if let (Some(start), Some(end)) = (start, end) {
                let key = if start == end {
                    start.to_string()
                } else {
                    format!("{start} .. {end}")
                };
                lines.push(format!("{key}{command}"));
            }
        };

    for (key, binding) in bindings {
        let resolved = keymap_get_keyelt(interp, &binding, true, env)?;
        let command = keymap_binding_display_name(&resolved);
        let shadow_binding =
            keymap_lookup_binding_exact_parts(interp, &shadow, &[key.to_string()])?;
        let shadow_text = if mention_shadow && !shadow_binding.is_nil() {
            let shadow_resolved = keymap_get_keyelt(interp, &shadow_binding, true, env)?;
            let shadow_name = keymap_binding_display_name(&shadow_resolved);
            (shadow_name != command).then_some(shadow_name)
        } else {
            None
        };

        if let Some(shadow_name) = shadow_text {
            flush_range(
                &mut lines,
                range_start.take(),
                range_end.take(),
                &range_command,
            );
            lines.push(format!(
                "{key}{command}  (currently shadowed by `{shadow_name}')"
            ));
            range_command.clear();
            continue;
        }

        match (range_start, range_end) {
            (Some(start), Some(end))
                if command == range_command && (end as u32).saturating_add(1) == key as u32 =>
            {
                range_start = Some(start);
                range_end = Some(key);
            }
            _ => {
                flush_range(
                    &mut lines,
                    range_start.take(),
                    range_end.take(),
                    &range_command,
                );
                range_start = Some(key);
                range_end = Some(key);
                range_command = command;
            }
        }
    }
    flush_range(&mut lines, range_start, range_end, &range_command);

    if !lines.is_empty() {
        interp.insert_current_buffer(&format!("\n{}\n", lines.join("\n")));
    }
    Ok(Value::Nil)
}

pub(crate) fn active_minor_mode_maps(
    interp: &Interpreter,
    env: &Env,
) -> Result<Vec<Value>, LispError> {
    let Some(alist) = interp.lookup_var("minor-mode-map-alist", env) else {
        return Ok(Vec::new());
    };
    let mut maps = Vec::new();
    for entry in alist.to_vec()? {
        let Value::Cons(mode, map) = entry else {
            continue;
        };
        let mode_value = mode.borrow().clone();
        let map_value = map.borrow().clone();
        let Value::Symbol(mode_name) = mode_value else {
            continue;
        };
        if interp
            .lookup_var(&mode_name, env)
            .is_some_and(|value| value.is_truthy())
            && is_keymap_value(interp, &map_value)
        {
            maps.push(map_value);
        }
    }
    Ok(maps)
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
    if let Some(map) = interp.lookup_var("global-map", env)
        && is_keymap_value(interp, &map)
    {
        maps.push(map);
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
        return Ok(vec![arg.clone()]);
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

    let mut matches = Vec::<Vec<String>>::new();
    let mut collector = WhereIsCollector {
        target_command,
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
        keymap_lookup_sequence_value(interp, &active_maps, parts, env)
            .ok()
            .and_then(|value| command_name_for_remapping(&value))
            .as_deref()
            == Some(target_command)
    });

    if remapped_command.is_none()
        && let Some(advertised) = interp
            .get_symbol_property(command, ":advertised-binding")
            .or_else(|| interp.get_symbol_property(command, "advertised-binding"))
        && let Ok(advertised_parts) = key_sequence_binding_parts(&advertised)
        && let Some(index) = matches.iter().position(|parts| parts == &advertised_parts)
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
    let Some(preferred) = interp.lookup_var("where-is-preferred-modifier", env) else {
        return parts.to_vec();
    };
    let Some(preferred) = (match preferred {
        Value::Symbol(symbol) => Some(symbol),
        Value::String(text) => Some(text),
        Value::StringObject(state) => Some(state.borrow().text.clone()),
        _ => None,
    }) else {
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

pub(crate) struct WhereIsCollector<'a> {
    target_command: &'a str,
    env: &'a mut Env,
    visited: HashSet<(u64, String)>,
    seen: HashSet<String>,
    matches: &'a mut Vec<Vec<String>>,
}

pub(crate) fn collect_where_is_matches(
    interp: &mut Interpreter,
    keymap: &Value,
    prefix: &[String],
    collector: &mut WhereIsCollector<'_>,
) -> Result<(), LispError> {
    let Some(id) = keymap_record_id(interp, keymap) else {
        return Ok(());
    };
    let prefix_key = prefix.join(" ");
    if !collector.visited.insert((id, prefix_key)) {
        return Ok(());
    }
    let Some(record) = interp.find_record(id) else {
        return Ok(());
    };
    for binding in keymap_bindings(record)? {
        let parts = binding_key_parts(&binding);
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
        if !key_parts_are_remap(&full_parts)
            && command_name_for_remapping(&resolved).as_deref() == Some(collector.target_command)
        {
            let key = full_parts.join(" ");
            if collector.seen.insert(key) {
                collector.matches.push(full_parts.clone());
            }
        }

        let nested = keymap_get_keyelt(interp, &binding.value, false, collector.env)?;
        if is_keymap_value(interp, &nested) {
            collect_where_is_matches(interp, &nested, &full_parts, collector)?;
        }
    }
    Ok(())
}

pub(crate) fn default_global_binding_for_key(key: &str) -> Option<&'static str> {
    match key {
        "C-s" => Some("isearch-forward"),
        "M-a" => Some("backward-sentence"),
        "C-SPC" => Some("set-mark-command"),
        "M-}" => Some("forward-paragraph"),
        "C-x n n" => Some("narrow-to-region"),
        "M-/" => Some("dabbrev-expand"),
        "C-M-/" => Some("dabbrev-completion"),
        "C-x 4 d" => Some("dired-other-window"),
        "C-x 5 d" => Some("dired-other-frame"),
        "C-x 5 C-o" => Some("display-buffer-other-frame"),
        key if key.chars().count() == 1 => Some("self-insert-command"),
        _ => None,
    }
}

pub(crate) fn remap_key_binding_text(command: &str) -> String {
    format!("<remap> <{command}>")
}

pub(crate) fn command_name_for_remapping(value: &Value) -> Option<String> {
    match value {
        Value::Symbol(name) | Value::BuiltinFunc(name) => Some(name.clone()),
        Value::Cons(_, _) => value
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
    let remap_key = remap_key_binding_text(&command_name);
    let maps = match keymaps {
        Some(keymaps) => where_is_internal_maps(interp, Some(keymaps), env)?,
        None => current_active_maps(interp, env, None)?,
    };
    for map in maps {
        let binding = keymap_lookup_binding(interp, &map, &remap_key)?;
        if !binding.is_nil() {
            return Ok(binding);
        }
    }
    Ok(Value::Nil)
}

pub(crate) fn key_binding(interp: &Interpreter, key: &str, env: &Env) -> Result<Value, LispError> {
    let maps = active_minor_mode_maps(interp, env)?;
    for map in &maps {
        let binding = keymap_lookup_binding(interp, map, key)?;
        if !binding.is_nil() {
            return Ok(binding);
        }
    }

    if let Some(global_map) = interp.lookup_var("global-map", env)
        && is_keymap_value(interp, &global_map)
    {
        let binding = keymap_lookup_binding(interp, &global_map, key)?;
        if !binding.is_nil() {
            return Ok(binding);
        }
    }

    let Some(command) = default_global_binding_for_key(key) else {
        return Ok(Value::Nil);
    };
    let remap_key = remap_key_binding_text(command);
    for map in &maps {
        let binding = keymap_lookup_binding(interp, map, &remap_key)?;
        if !binding.is_nil() {
            return Ok(binding);
        }
    }

    Ok(Value::Symbol(command.into()))
}

pub(crate) fn keymap_binding_matches_command(binding: &Value, command: &str) -> bool {
    match binding {
        Value::Symbol(name) | Value::BuiltinFunc(name) => name == command,
        Value::Cons(_, _) => binding
            .to_vec()
            .ok()
            .is_some_and(|items| match items.as_slice() {
                [Value::Symbol(symbol), inner] if symbol == "function" || symbol == "quote" => {
                    keymap_binding_matches_command(inner, command)
                }
                [Value::Symbol(symbol), _, inner, ..] if symbol == "menu-item" => {
                    keymap_binding_matches_command(inner, command)
                }
                _ => false,
            }),
        _ => false,
    }
}

pub(crate) fn keymap_binding_text_for_command(
    interp: &Interpreter,
    keymap: &Value,
    command: &str,
) -> Option<String> {
    let id = keymap_record_id(interp, keymap)?;
    let record = interp.find_record(id)?;
    for binding in keymap_bindings(record).ok()?.into_iter().rev() {
        if keymap_binding_matches_command(&binding.value, command) {
            return Some(binding.key);
        }
    }
    match record.slots.get(KEYMAP_PARENT_SLOT) {
        Some(Value::Nil) | None => None,
        Some(parent) => keymap_binding_text_for_command(interp, parent, command),
    }
}

pub(crate) fn substitute_command_keys(interp: &Interpreter, text: &str, env: &Env) -> String {
    let chars: Vec<char> = text.chars().collect();
    let mut output = String::new();
    let mut current_map = interp.lookup_var("global-map", env);
    let mut index = 0usize;

    while index < chars.len() {
        if chars[index] == '\\' && index + 1 < chars.len() {
            match chars[index + 1] {
                '<' => {
                    if let Some(end) = chars[index + 2..]
                        .iter()
                        .position(|ch| *ch == '>')
                        .map(|offset| index + 2 + offset)
                    {
                        let map_name: String = chars[index + 2..end].iter().collect();
                        current_map = interp.lookup_var(&map_name, env);
                        index = end + 1;
                        continue;
                    }
                }
                '[' => {
                    if let Some(end) = chars[index + 2..]
                        .iter()
                        .position(|ch| *ch == ']')
                        .map(|offset| index + 2 + offset)
                    {
                        let command: String = chars[index + 2..end].iter().collect();
                        let command = command.trim();
                        let replacement = current_map
                            .as_ref()
                            .and_then(|keymap| {
                                keymap_binding_text_for_command(interp, keymap, command)
                            })
                            .unwrap_or_else(|| format!("M-x {command}"));
                        output.push_str(&replacement);
                        index = end + 1;
                        continue;
                    }
                }
                _ => {}
            }
        }

        output.push(chars[index]);
        index += 1;
    }

    output
}
