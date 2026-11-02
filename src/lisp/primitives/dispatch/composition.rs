use super::*;
use crate::lisp::types::LispErrorKind;
use crate::lisp::types::{Kind, PropertyName};

fn vector_value(items: impl IntoIterator<Item = Value>) -> Value {
    Value::vector(items)
}

fn args_out_of_range(values: impl IntoIterator<Item = Value>) -> LispError {
    LispError::SignalValue(Value::list(
        std::iter::once(Value::symbol("args-out-of-range")).chain(values),
    ))
}

fn current_buffer_value(interp: &Interpreter) -> Value {
    Value::Buffer(interp.buffer)
}

fn checked_buffer_region(
    interp: &Interpreter,
    start: &Value,
    end: &Value,
) -> Result<(usize, usize), LispError> {
    validate_region(interp, start, end)
}

fn checked_string_region(
    string: &Value,
    start: &Value,
    end: &Value,
) -> Result<(usize, usize), LispError> {
    let text = string_like(string).ok_or_else(|| wrong_type_argument("stringp", *string))?;
    let len = text.text.chars().count() as i64;
    let normalize = |value: &Value| -> Result<i64, LispError> {
        let raw = value.as_integer()?;
        Ok(if raw < 0 { len + raw } else { raw })
    };
    let from = normalize(start)?;
    let to = normalize(end)?;
    if from < 0 || to < 0 || from > to || to > len {
        return Err(args_out_of_range([*string, *start, *end]));
    }
    Ok((from as usize, to as usize))
}

fn composition_property(length: usize, components: Value, modification: Value) -> Value {
    Value::cons(
        Value::cons(Value::Integer(length as i64), components),
        modification,
    )
}

fn compose_region(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    let (start, end) = checked_buffer_region(interp, &args[0], &args[1])?;
    let components = args.get(2).cloned().unwrap_or(Value::Nil);
    if !matches!(
        components.kind(),
        Kind::Nil | Kind::Integer(_) | Kind::Cons(_) | Kind::StringObject(_)
    ) && !is_vector_value(&components)
    {
        return Err(wrong_type_argument("vectorp", components));
    }
    let modification = args.get(3).cloned().unwrap_or(Value::Nil);
    let property = composition_property(end - start, components, modification);
    // composite.c:compose_text puts the property as Fput_text_property does,
    // through the change protocol.
    let properties = Value::list([Value::symbol("composition"), property]);
    let buffer_id = interp.current_buffer_id();
    interp.with_lisp_stack_roots(&properties, |interp| {
        modify_buffer_text_properties(
            interp,
            buffer_id,
            start,
            end,
            &|buffer| buffer.would_add_text_properties(start, end, properties),
            &|buffer| {
                let _ = buffer.add_text_properties_range(
                    start,
                    end,
                    properties,
                    crate::intervals::PropertySetType::Replace,
                    true,
                );
            },
            env,
        )
    })?;
    Ok(Value::Nil)
}

fn compose_string(args: &[Value]) -> Result<Value, LispError> {
    if string_like(&args[0]).is_none() {
        return Err(wrong_type_argument("stringp", args[0]));
    }
    let (start, end) = checked_string_region(&args[0], &args[1], &args[2])?;
    let components = args.get(3).cloned().unwrap_or(Value::Nil);
    let modification = args.get(4).cloned().unwrap_or(Value::Nil);
    let property = composition_property(end - start, components, modification);
    // composite.c:compose_text: Fput_text_property of `composition'.
    add_string_properties(
        &args[0],
        start,
        end,
        Value::list([Value::symbol("composition"), property]),
    )?;
    Ok(args[0])
}

/// composite.h's CHECK_* masks: which compositions around a change
/// update_compositions makes valid.
pub(crate) const CHECK_HEAD: u8 = 1;
pub(crate) const CHECK_TAIL: u8 = 2;
pub(crate) const CHECK_INSIDE: u8 = 4;
pub(crate) const CHECK_BORDER: u8 = CHECK_HEAD | CHECK_TAIL;
pub(crate) const CHECK_ALL: u8 = CHECK_BORDER | CHECK_INSIDE;

fn composition_symbol() -> Value {
    cached_symbol!("composition").with(|symbol| Value::Symbol(*symbol))
}

/// intervals.c:get_property_and_range of `composition' at POS of OBJECT
/// (nil the current buffer): the range and the property.
fn property_and_range(
    interp: &Interpreter,
    pos: usize,
    object: &Value,
) -> Result<Option<(usize, usize, Value)>, LispError> {
    use crate::lisp::primitives::textprop::{SearchObject, get_property_and_range};
    let target = if object.is_nil() {
        SearchObject::Buffer(interp.current_buffer_id())
    } else {
        SearchObject::String(*object)
    };
    Ok(
        get_property_and_range(interp, pos, PropertyName::new(composition_symbol()), target)?
            .map(|(prop, start, end)| (start, end, prop)),
    )
}

/// composite.c find_composition: the static composition at POS of OBJECT
/// (nil the current buffer), else with LIMIT the nearest one from POS
/// toward it, its range and its unchecked property.
fn find_composition(
    interp: &Interpreter,
    pos: usize,
    limit: Option<usize>,
    object: &Value,
) -> Result<Option<(usize, usize, Value)>, LispError> {
    if let Some(found) = property_and_range(interp, pos, object)? {
        return Ok(Some(found));
    }
    let Some(limit) = limit.filter(|limit| *limit != pos) else {
        return Ok(None);
    };
    let args = [
        Value::Integer(pos as i64),
        composition_symbol(),
        *object,
        Value::Integer(limit as i64),
    ];
    let pos = if limit > pos {
        let next = crate::lisp::primitives::textprop::next_single_property_change(interp, &args)?
            .as_integer()?;
        if next == limit as i64 {
            return Ok(None);
        }
        next as usize
    } else {
        if let Some(found) = property_and_range(interp, pos - 1, object)? {
            return Ok(Some(found));
        }
        let previous =
            crate::lisp::primitives::textprop::previous_single_property_change(interp, &args)?
                .as_integer()?;
        if previous == limit as i64 {
            return Ok(None);
        }
        previous as usize - 1
    };
    property_and_range(interp, pos, object)
}

/// composite.h composition_valid_p: PROP is a `composition' property
/// of Form-A ((LENGTH . COMPONENTS) . MODIFICATION-FUNC) or Form-B
/// (ID LENGTH COMPONENTS-VEC . MODIFICATION-FUNC) whose LENGTH is END -
/// START.  GNU's bound admits the ID one past the last composition.
fn composition_valid_p(interp: &Interpreter, start: usize, end: usize, prop: Value) -> bool {
    let Some((head, tail)) = prop.cons_values() else {
        return false;
    };
    let length = match head.kind() {
        Kind::Integer(id) => {
            if id < 0 || id as u64 > interp.composition_states.len() as u64 {
                return false;
            }
            match tail.cons_values() {
                Some((length, _)) => length,
                None => return false,
            }
        }
        Kind::Cons(cell) => {
            let components = cell.cdr.get();
            if !(matches!(
                components.kind(),
                Kind::Nil | Kind::StringObject(_) | Kind::Integer(_) | Kind::Cons(_)
            ) || is_vector_value(&components))
            {
                return false;
            }
            cell.car.get()
        }
        _ => return false,
    };
    matches!(length.kind(), Kind::Integer(length) if length == (end - start) as i64)
}

/// COMPOSITION_MODIFICATION_FUNC of PROP.
fn composition_modification_func(prop: Value) -> Value {
    let Some((head, tail)) = prop.cons_values() else {
        return Value::Nil;
    };
    if matches!(head.kind(), Kind::Integer(_)) {
        tail.cons_values()
            .and_then(|(_, rest)| rest.cons_values())
            .map_or(Value::Nil, |(_, function)| function)
    } else {
        tail
    }
}

/// COMPOSITION_COMPONENTS of a registered PROP.
fn registered_components(prop: Value) -> Value {
    prop.cons_values()
        .and_then(|(_, tail)| tail.cons_values())
        .and_then(|(_, rest)| rest.cons_values())
        .map_or(Value::Nil, |(components, _)| components)
}

/// The character codes of NCHARS characters of OBJECT (nil the current
/// buffer) from CHARPOS, as fetch_char_advance and
/// fetch_string_char_advance read them.
fn composition_codes(
    interp: &Interpreter,
    object: &Value,
    charpos: usize,
    nchars: usize,
) -> Vec<u32> {
    if object.is_nil() {
        return crate::lisp::primitives::buffer_text_codes(interp, charpos, charpos + nchars);
    }
    let Kind::StringObject(state) = object.kind() else {
        return Vec::new();
    };
    let state = state.borrow();
    let Some(mut offset) = state.byte_offset(charpos) else {
        return Vec::new();
    };
    (0..nchars)
        .map_while(|_| state.character_advance(&mut offset).map(|code| code as u32))
        .collect()
}

/// composite.c's width of a key element: XFIXNUM of it into an int, TAB
/// one column, else CHARACTER_WIDTH.
fn component_width(interp: &Interpreter, env: &Env, component: &Value) -> i64 {
    let code = match component.kind() {
        Kind::Integer(code) => code as i32,
        _ => 0,
    };
    if code == '\t' as i32 {
        return 1;
    }
    // A negative code is a control character to CHARACTER_WIDTH.
    let code = u32::try_from(code).unwrap_or(0);
    crate::lisp::primitives::character_width(interp, code, env) as i64
}

/// get_composition_id's width of a rule-based composition of KEY over
/// its GLYPH_LEN glyphs; the loop bound is GNU's GLYPH_LEN, not the
/// key's length.
fn rule_composition_width(interp: &Interpreter, env: &Env, key: &[Value], glyph_len: usize) -> i64 {
    let mut leftmost = 0.0_f64;
    let mut rightmost =
        key.first()
            .map_or(0, |component| component_width(interp, env, component)) as f64;
    let mut index = 1;
    while index < glyph_len {
        let rule = match key[index].kind() {
            Kind::Integer(rule) => rule as i32,
            _ => 0,
        };
        let this_width = component_width(interp, env, &key[index + 1]) as f64;
        // COMPOSITION_DECODE_REFS.
        let rule = rule & 0xFF;
        let mut global_reference = rule / 12;
        if global_reference > 12 {
            global_reference = 11;
        }
        let new_reference = rule % 12;
        let this_left = leftmost + f64::from(global_reference % 3) * (rightmost - leftmost) / 2.0
            - f64::from(new_reference % 3) * this_width / 2.0;
        if this_left < leftmost {
            leftmost = this_left;
        }
        if this_left + this_width > rightmost {
            rightmost = this_left + this_width;
        }
        index += 2;
    }
    let span = rightmost - leftmost;
    let width = span as i64;
    if (width as f64) < span {
        width + 1
    } else {
        width
    }
}

/// composition_hash_table's hash_lookup of KEY (its `equal' HASH): the
/// registered composition's id.
fn lookup_composition(
    interp: &mut Interpreter,
    hash: u32,
    key: &Value,
) -> Result<Option<usize>, LispError> {
    use crate::lisp::eval::RuntimeHashTest;
    while interp.composition_index.indexed < interp.composition_states.len() {
        let id = interp.composition_index.indexed;
        let stored = interp.composition_states[id].components;
        let stored_hash = standard_hash_code(interp, RuntimeHashTest::Equal, &stored);
        interp
            .composition_index
            .by_hash
            .entry(stored_hash)
            .or_default()
            .push(id);
        interp.composition_index.indexed += 1;
    }
    let candidates = interp
        .composition_index
        .by_hash
        .get(&hash)
        .cloned()
        .unwrap_or_default();
    for id in candidates {
        let stored = interp.composition_states[id].components;
        if stored.word() == key.word() || values_equal(interp, &stored, key)? {
            return Ok(Some(id));
        }
    }
    Ok(None)
}

/// composite.c get_composition_id: the id of the composition PROP makes
/// of NCHARS characters of OBJECT (nil the current buffer) from CHARPOS,
/// registering a new valid one; PROP turns from Form-A into Form-B with
/// the key the table holds.  None for an invalid composition.
fn get_composition_id(
    interp: &mut Interpreter,
    env: &Env,
    charpos: usize,
    nchars: usize,
    prop: Value,
    object: &Value,
) -> Result<Option<usize>, LispError> {
    use crate::lisp::eval::RuntimeHashTest;
    if nchars == 0 {
        return Ok(None);
    }
    let Some((id, tail)) = prop.cons_values() else {
        return Ok(None);
    };
    if let Kind::Integer(id) = id.kind() {
        return Ok(usize::try_from(id)
            .ok()
            .filter(|id| *id < interp.composition_states.len()));
    }
    let Some((length, components)) = id.cons_values() else {
        return Ok(None);
    };
    if !matches!(length.kind(), Kind::Integer(length) if length == nchars as i64) {
        return Ok(None);
    }
    let key = match components.kind() {
        Kind::Integer(_) => Value::vector([components]),
        // Fvconcat.
        Kind::StringObject(_) | Kind::Cons(_) => Value::vector(sequence_values(&components)?),
        Kind::Nil => Value::vector(
            composition_codes(interp, object, charpos, nchars)
                .into_iter()
                .map(|code| Value::Integer(i64::from(code))),
        ),
        _ if is_vector_value(&components) => components,
        _ => return Ok(None),
    };
    let hash = standard_hash_code(interp, RuntimeHashTest::Equal, &key);
    if let Some(found) = lookup_composition(interp, hash, &key)? {
        let stored = interp.composition_states[found].components;
        prop.set_car(Value::Integer(found as i64))?;
        prop.set_cdr(Value::cons(
            Value::Integer(nchars as i64),
            Value::cons(stored, tail),
        ))?;
        return Ok(Some(found));
    }
    let key_items = vector_items(&key)?;
    let glyph_string =
        is_vector_value(&components) && key_items.len() >= 2 && is_vector_value(&key_items[0]);
    if glyph_string {
        if !key_items[1..].iter().all(is_vector_value) {
            return Ok(None);
        }
    } else if (is_vector_value(&components) || matches!(components.kind(), Kind::Cons(_)))
        && (key_items.len() % 2 == 0
            || !key_items
                .iter()
                .all(|item| matches!(item.kind(), Kind::Integer(_))))
    {
        return Ok(None);
    }
    let id = interp.composition_states.len();
    prop.set_car(Value::Integer(id as i64))?;
    prop.set_cdr(Value::cons(
        Value::Integer(nchars as i64),
        Value::cons(key, tail),
    ))?;
    // COMPOSITION_RELATIVE for nil components, COMPOSITION_WITH_ALTCHARS
    // for a character or string, else COMPOSITION_WITH_RULE_ALTCHARS.
    let rule_based = !matches!(
        components.kind(),
        Kind::Nil | Kind::Integer(_) | Kind::StringObject(_)
    );
    let width = if rule_based {
        rule_composition_width(interp, env, &key_items, key_items.len().div_ceil(2))
    } else {
        key_items
            .iter()
            .map(|component| component_width(interp, env, component))
            .fold(0, i64::max)
    };
    interp
        .composition_states
        .push(crate::lisp::eval::CompositionState {
            components: key,
            relative: !rule_based,
            width,
        });
    Ok(Some(id))
}

/// A string's static compositions, for character.c lisp_string_width's
/// find_composition (POS, -1, ...) and get_composition_id.
pub(crate) struct StringCompositions;

impl StringCompositions {
    pub(crate) fn of(_interp: &Interpreter, string: &Value) -> Result<Self, LispError> {
        if string_like(string).is_none() {
            return Err(wrong_type_argument("stringp", *string));
        }
        Ok(Self)
    }

    /// The end and width of the composition STRING's `composition'
    /// property makes of its characters from POSITION to the end of the
    /// property's range, when the property is valid for them.
    pub(crate) fn at(
        &self,
        interp: &mut Interpreter,
        env: &Env,
        string: &Value,
        position: usize,
    ) -> Result<Option<(usize, usize)>, LispError> {
        let Some((_, end, prop)) = find_composition(interp, position, None, string)? else {
            return Ok(None);
        };
        let Some(id) = get_composition_id(interp, env, position, end - position, prop, string)?
        else {
            return Ok(None);
        };
        Ok(Some((
            end,
            interp.composition_states[id].width.max(0) as usize,
        )))
    }
}

/// composite.c run_composition_function: the modification function of
/// PROP, a symbol (Ffboundp), called on FROM..TO widened over the
/// invalid compositions next to it.
fn run_composition_function(
    interp: &mut Interpreter,
    from: usize,
    to: usize,
    prop: Value,
    env: &mut Env,
) -> Result<(), LispError> {
    let function = composition_modification_func(prop);
    let nil = Value::Nil;
    let (begv, zv) = {
        let buffer = interp.buffer.borrow();
        (buffer.point_min(), buffer.point_max())
    };
    let mut from = from;
    let mut to = to;
    if from > begv
        && let Some((start, end, prop)) = find_composition(interp, from - 1, None, &nil)?
        && !composition_valid_p(interp, start, end, prop)
    {
        from = start;
    }
    if to < zv
        && let Some((start, end, prop)) = find_composition(interp, to, None, &nil)?
        && !composition_valid_p(interp, start, end, prop)
    {
        to = end;
    }
    let symbol = checked_symbol_identity(interp, &function, env)?;
    if interp
        .raw_function_binding_symbol(&symbol, env)
        .is_some_and(|binding| !binding.is_nil())
    {
        interp.call_function_value(
            function,
            None,
            &[Value::Integer(from as i64), Value::Integer(to as i64)],
            env,
        )?;
    }
    Ok(())
}

/// update_compositions' Fput_text_property of a top-level copy of PROP
/// over FROM..TO, so that two compositions made adjacent stay apart.
fn put_composition_copy(
    interp: &mut Interpreter,
    from: usize,
    to: usize,
    prop: Value,
    env: &mut Env,
) -> Result<(), LispError> {
    let Some((head, tail)) = prop.cons_values() else {
        return Ok(());
    };
    let properties = Value::list([composition_symbol(), Value::cons(head, tail)]);
    crate::lisp::primitives::textprop::add_text_properties(
        interp,
        &Value::Integer(from as i64),
        &Value::Integer(to as i64),
        properties,
        Value::Nil,
        env,
    )?;
    Ok(())
}

/// update_compositions' Fremove_list_of_text_properties of
/// `auto-composed' over FROM..TO with `inhibit-read-only',
/// `inhibit-modification-hooks' and `inhibit-point-motion-hooks' t.  It
/// is not called where it would remove nothing and the range is valid.
fn remove_auto_composed(
    interp: &mut Interpreter,
    from: usize,
    to: usize,
    env: &mut Env,
) -> Result<(), LispError> {
    let auto_composed = cached_symbol!("auto-composed").with(|symbol| Value::Symbol(*symbol));
    {
        let buffer = interp.buffer.borrow();
        let in_range = buffer.point_min() <= from && to <= buffer.point_max();
        let present = buffer.intervals().is_some_and(|intervals| {
            let intervals = crate::buffer::PropertyIntervals::new(&intervals);
            let mut interval = Some(intervals.find(from));
            while let Some(current) = interval {
                if current.start >= to {
                    break;
                }
                let mut plist = current.plist;
                while let Some((name, rest)) = plist.cons_values() {
                    if name.word() == auto_composed.word() {
                        return true;
                    }
                    plist = rest.cons_values().map_or(Value::Nil, |(_, next)| next);
                }
                interval = intervals.next(&current);
            }
            false
        });
        if in_range && !present {
            return Ok(());
        }
    }
    let bindings = [
        cached_symbol!("inhibit-read-only"),
        cached_symbol!("inhibit-modification-hooks"),
        cached_symbol!("inhibit-point-motion-hooks"),
    ];
    let mut restores = Vec::with_capacity(bindings.len());
    let mut result = Ok(());
    for key in bindings {
        match interp.bind_special_dynamic(key.name(), Value::T, env) {
            Ok(restore) => restores.push(restore),
            Err(error) => {
                result = Err(error);
                break;
            }
        }
    }
    if result.is_ok() {
        result = crate::lisp::primitives::textprop::remove_text_properties(
            interp,
            &Value::Integer(from as i64),
            &Value::Integer(to as i64),
            Value::list([auto_composed]),
            Value::Nil,
            true,
            env,
        )
        .map(|_| ());
    }
    while let Some(restore) = restores.pop() {
        let restored = interp.restore_special_dynamic(restore, env);
        if result.is_ok() {
            result = restored;
        }
    }
    result
}

/// composite.c update_compositions: after a change of FROM..TO of the
/// current buffer (FROM = TO for a deletion), the compositions at its
/// head and tail (and with CHECK_INSIDE within it) of CHECK_MASK made
/// valid by their modification functions, a composition the change made
/// adjacent to another of the same property given a copy of it, and
/// `auto-composed' removed from the range they cover.
pub(crate) fn update_compositions(
    interp: &mut Interpreter,
    from: usize,
    to: usize,
    check_mask: u8,
    env: &mut Env,
) -> Result<(), LispError> {
    if crate::lisp::primitives::modification_hooks_inhibited(interp) {
        return Ok(());
    }
    let bounds = |interp: &Interpreter| {
        let buffer = interp.buffer.borrow();
        (buffer.point_min(), buffer.point_max())
    };
    let (begv, zv) = bounds(interp);
    if !(begv <= from && from <= to && to <= zv) {
        return Ok(());
    }
    // Without intervals there is no composition and nothing to remove.
    if !interp.buffer.borrow().has_interval_tree() {
        return Ok(());
    }
    let nil = Value::Nil;
    let mut from = from;
    let (mut min_pos, mut max_pos) = (from, to);
    if check_mask & CHECK_HEAD != 0 {
        let before = if from > bounds(interp).0 {
            find_composition(interp, from - 1, None, &nil)?
                .filter(|(start, end, prop)| composition_valid_p(interp, *start, *end, *prop))
        } else {
            None
        };
        if let Some((start, end, prop)) = before {
            min_pos = start;
            if end > to {
                max_pos = end;
            }
            if from < end {
                put_composition_copy(interp, from, end, prop, env)?;
            }
            run_composition_function(interp, start, end, prop, env)?;
            from = end;
        } else if from < bounds(interp).1
            && let Some((start, end, prop)) = find_composition(interp, from, None, &nil)?
        {
            // find_composition's END is FROM, valid or not.
            from = end;
            if composition_valid_p(interp, start, end, prop) {
                if from > to {
                    max_pos = from;
                }
                run_composition_function(interp, start, from, prop, env)?;
            }
        }
    }
    if check_mask & CHECK_INSIDE != 0 {
        while from + 1 < to {
            let Some((start, end, prop)) = find_composition(interp, from, Some(to), &nil)? else {
                break;
            };
            from = end;
            if !composition_valid_p(interp, start, from, prop) || from + 1 >= to {
                break;
            }
            run_composition_function(interp, start, from, prop, env)?;
        }
    }
    if check_mask & CHECK_TAIL != 0 {
        let at_tail = if from < to {
            find_composition(interp, to - 1, None, &nil)?
                .filter(|(start, end, prop)| composition_valid_p(interp, *start, *end, *prop))
        } else {
            None
        };
        if let Some((start, end, prop)) = at_tail {
            if to < end {
                put_composition_copy(interp, start, to, prop, env)?;
                max_pos = end;
            }
            run_composition_function(interp, start, end, prop, env)?;
        } else if to < bounds(interp).1
            && let Some((start, end, prop)) = find_composition(interp, to, None, &nil)?
            && composition_valid_p(interp, start, end, prop)
        {
            run_composition_function(interp, start, end, prop, env)?;
            max_pos = end;
        }
    }
    if min_pos < max_pos {
        remove_auto_composed(interp, min_pos, max_pos, env)?;
    }
    Ok(())
}

fn composition_chars(
    interp: &Interpreter,
    string: &Value,
    start: usize,
    end: usize,
) -> Result<Vec<char>, LispError> {
    let text = if string.is_nil() {
        interp
            .buffer
            .borrow()
            .buffer_substring(start, end)
            .map_err(|error| LispError::Signal(error.to_string()))?
    } else {
        string_like(string)
            .ok_or_else(|| wrong_type_argument("stringp", *string))?
            .text
            .chars()
            .skip(start)
            .take(end - start)
            .collect()
    };
    Ok(text.chars().collect())
}

fn char_table_id_from_var(
    interp: &Interpreter,
    env: &Env,
    name: &str,
) -> Option<crate::lisp::types::CharTableRef> {
    match (interp.lookup_var(name, env)?).kind() {
        Kind::CharTable(id) => Some(id),
        _ => None,
    }
}

/// composite.c `char_composable_p': anything from space upward composes
/// unless its general category is Z* or C*, with ZWNJ/ZWJ, the TR51 tag
/// range, and Zs carved back in.  `unicode-category-table' stores the raw
/// fixnum encoding of `enum unicode_category' (character.h); every
/// composable category sorts at or before Zs, index 23.
fn char_composable_p(interp: &Interpreter, env: &Env, c: char) -> bool {
    const ZERO_WIDTH_NON_JOINER: u32 = 0x200C;
    const ZERO_WIDTH_JOINER: u32 = 0x200D;
    const TAG_SPACE: u32 = 0xE0020;
    const CANCEL_TAG: u32 = 0xE007F;
    const UNICODE_CATEGORY_ZS: i64 = 23;
    let c = c as u32;
    c >= ' ' as u32
        && (c == ZERO_WIDTH_NON_JOINER
            || c == ZERO_WIDTH_JOINER
            || (TAG_SPACE..=CANCEL_TAG).contains(&c)
            || matches!(
                char_table_id_from_var(interp, env, "unicode-category-table")
                    .and_then(|id| interp.char_table_get(id, c)).map(|v| v.kind()),
                Some(Kind::Integer(category)) if category <= UNICODE_CATEGORY_ZS
            ))
}

/// composite.c `inhibit_auto_composition': `auto-composition-mode' nil
/// turns automatic composition off entirely; a string value turns it off
/// only on the terminal type it names.
fn inhibit_auto_composition(interp: &Interpreter, env: &Env) -> bool {
    let Some(mode) = interp.lookup_var("auto-composition-mode", env) else {
        return true;
    };
    if mode.is_nil() {
        return true;
    }
    if let Some(mode) = string_like(&mode) {
        return interp
            .tty_terminal_type()
            .is_some_and(|terminal_type| *terminal_type == mode.text);
    }
    false
}

fn target_char_at(interp: &Interpreter, string: &Value, pos: usize) -> Option<char> {
    if string.is_nil() {
        interp.buffer.borrow().char_at(pos)
    } else {
        string_like(string)?.text.chars().nth(pos)
    }
}

/// `composition-function-table' holds, per character, a list of
/// [PATTERN LOOKBACK FUNC] rules.  The C walks only a proper cons chain
/// (`for (...; CONSP (val); val = XCDR (val))'); a bare vector is ignored.
fn composition_rules_for_char(interp: &Interpreter, env: &Env, c: char) -> Vec<Value> {
    let Some(table_id) = char_table_id_from_var(interp, env, "composition-function-table") else {
        return Vec::new();
    };
    let Some(value) = interp.char_table_get(table_id, c as u32) else {
        return Vec::new();
    };
    let mut rules = Vec::new();
    let mut tail = value;
    while let Some((head, rest)) = tail.cons_values() {
        rules.push(head);
        tail = rest;
    }
    rules
}

/// LGSTRING_CHAR_LEN: the number of characters a glyph-string covers is
/// the length of its header vector minus the leading font slot.
fn lgstring_char_len(gstring: &Value) -> Option<usize> {
    let items = vector_items(gstring).ok()?;
    let header = items.first()?;
    Some(vector_items(header).ok()?.len().saturating_sub(1))
}

/// composite.c `autocmp_chars': try to compose the characters at CHARPOS
/// according to RULE ([PATTERN LOOKBACK FUNC]), bounded by LIMIT.  Match
/// data is saved and restored around the attempt
/// (`record_unwind_save_match_data'), and point is restored after calling
/// out to Lisp on the buffer path (`restore_point_unwind').
fn autocmp_chars(
    interp: &mut Interpreter,
    env: &mut Env,
    rule_items: &[Value],
    charpos: usize,
    limit: usize,
    string: &Value,
) -> Result<Value, LispError> {
    let saved_match_data = interp.last_match_data.clone();
    let saved_match_buffer = interp.last_thing_searched;
    let saved_point = string.is_nil().then(|| interp.buffer.borrow().point());
    let result = autocmp_chars_inner(interp, env, rule_items, charpos, limit, string);
    if let Some(point) = saved_point {
        let clamped = point.clamp(
            interp.buffer.borrow().point_min(),
            interp.buffer.borrow().point_max(),
        );
        interp.buffer.borrow_mut().goto_char(clamped);
    }
    interp.last_match_data = saved_match_data;
    interp.last_thing_searched = saved_match_buffer;
    result
}

fn autocmp_chars_inner(
    interp: &mut Interpreter,
    env: &mut Env,
    rule_items: &[Value],
    charpos: usize,
    limit: usize,
    string: &Value,
) -> Result<Value, LispError> {
    let pattern_value = &rule_items[0];
    let len = if pattern_value.is_nil() {
        1
    } else {
        if !pattern_value.is_string() {
            // Non-string, non-nil PATTERN: the rule matches nothing.
            return Ok(Value::Nil);
        }
        match search::fast_looking_at(interp, env, pattern_value, charpos, limit, string)? {
            Some(len) if len > 0 => len,
            _ => return Ok(Value::Nil),
        }
    };
    let to = charpos + len;
    // On a character terminal the font object handed down the shaping
    // pipeline is the frame itself (autocmp_chars: font_object =
    // win->frame; the window-system font_range branch is compiled out).
    let font_object = interp.selected_frame_value();
    // GNU fetches the glyph-string here to check for a cached shaped ID.
    // Emaxx never registers shaped glyph-strings in a cache, so the ID is
    // always nil and control continues to `auto-composition-function'
    // exactly as GNU does on a cache miss; only the fetch's own validation
    // (unibyte text, invalid region) remains observable.
    get_glyph_string(
        interp,
        env,
        &[
            Value::Integer(charpos as i64),
            Value::Integer(to as i64),
            font_object,
            *string,
        ],
    )?;
    let function = interp
        .lookup_var("auto-composition-function", env)
        .unwrap_or(Value::Nil);
    let call = interp.call_function_value(
        function,
        None,
        &[
            rule_items[2],
            Value::Integer(charpos as i64),
            Value::Integer(to as i64),
            font_object,
            *string,
            Value::Nil,
        ],
        env,
    );
    match call.map_err(LispError::into_kind) {
        Ok(value) => Ok(value),
        Err(error @ LispErrorKind::Throw(..)) => Err(LispError::from(error)),
        // safe_calln: a signaled condition absorbs into a failed
        // composition attempt.
        Err(_) => Ok(Value::Nil),
    }
}

struct AutoCompositionMatch {
    start: usize,
    end: usize,
    gstring: Value,
}

/// composite.c `find_automatic_composition' on the BACKLIM = -1 path used
/// by `find-composition-internal': search for an automatic composition at
/// or near POS, walking back across composable characters to a safe start
/// and matching `composition-function-table' rules forward.
fn find_automatic_composition(
    interp: &mut Interpreter,
    env: &mut Env,
    pos: usize,
    limit: Option<usize>,
    string: &Value,
) -> Result<Option<AutoCompositionMatch>, LispError> {
    const MAX_AUTO_COMPOSITION_LOOKBACK: usize = 3;
    // The C first locates a window showing the current buffer; with none
    // (an undisplayed temp buffer -- even when the target is a string)
    // there is no automatic composition.
    let window = super::display::call(interp, "get-buffer-window", &[Value::Nil, Value::Nil], env)?;
    if window.is_nil() {
        return Ok(None);
    }

    let (head, tail) = if string.is_nil() {
        // BACKLIM is -1 here, so the backward search stops at the first
        // newline before POS: a newline can never be composed.  (GNU's
        // long-line-optimizations narrowing is not modeled.)
        let min = interp.buffer.borrow().point_min();
        let before = interp
            .buffer
            .borrow()
            .buffer_substring(min, pos)
            .map_err(|error| LispError::Signal(error.to_string()))?;
        let head = before
            .rfind('\n')
            .map(|byte| min + before[..byte].chars().count() + 1)
            .unwrap_or(min);
        (head, interp.buffer.borrow().point_max())
    } else {
        (
            0,
            string_like(string)
                .map(|text| text.text.chars().count())
                .unwrap_or(0),
        )
    };
    // A negative LIMIT means: find a composition covering the character
    // after POS.
    let limit_value = limit.unwrap_or(pos);
    let mut fore_check_limit = if limit_value <= pos {
        tail.min(pos + 1 + MAX_AUTO_COMPOSITION_LOOKBACK)
    } else {
        tail.min(limit_value + MAX_AUTO_COMPOSITION_LOOKBACK)
    };

    let mut cur = pos;
    loop {
        let composable =
            target_char_at(interp, string, cur).is_some_and(|c| char_composable_p(interp, env, c));
        if !composable && limit_value <= pos {
            // Case (1): rewind to the previous composable character.
            loop {
                if cur <= limit_value {
                    return Ok(None);
                }
                cur -= 1;
                if target_char_at(interp, string, cur)
                    .is_some_and(|c| char_composable_p(interp, env, c))
                {
                    break;
                }
            }
            fore_check_limit = cur + 1;
        }
        if composable || limit_value <= pos {
            // Rewind to a position where forward search is safe: just
            // after the nearest non-composable character (or HEAD).
            while head < cur {
                let prev = cur;
                cur -= 1;
                if !target_char_at(interp, string, cur)
                    .is_some_and(|c| char_composable_p(interp, env, c))
                {
                    cur = prev;
                    break;
                }
            }
        }

        // search_forward:
        let mut last: Option<AutoCompositionMatch> = None;
        let scan_start = cur;
        while cur < fore_check_limit {
            let Some(c) = target_char_at(interp, string, cur) else {
                break;
            };
            let mut advanced = false;
            for rule in composition_rules_for_char(interp, env, c) {
                let Ok(items) = vector_items(&rule) else {
                    continue;
                };
                if items.len() != 3 {
                    continue;
                }
                let Kind::Integer(lookback) = items[1].kind() else {
                    continue;
                };
                if lookback < 0 || lookback as usize > cur {
                    continue;
                }
                let check_pos = cur - lookback as usize;
                if check_pos < head {
                    continue;
                }
                if limit_value <= pos {
                    if pos < check_pos {
                        continue;
                    }
                } else if limit_value <= check_pos {
                    continue;
                }
                let gstring = autocmp_chars(interp, env, &items, check_pos, tail, string)?;
                // The C stores every attempt into *gstring: a later failed
                // attempt clears an earlier non-target success.
                let char_len = if gstring.is_nil() {
                    None
                } else {
                    lgstring_char_len(&gstring).filter(|len| *len > 0)
                };
                let Some(char_len) = char_len else {
                    last = None;
                    continue;
                };
                let start = check_pos;
                let end = check_pos + char_len;
                let found = AutoCompositionMatch {
                    start,
                    end,
                    gstring,
                };
                let is_target = if pos < limit_value {
                    pos < end
                } else {
                    start <= pos && pos < end
                };
                if is_target {
                    return Ok(Some(found));
                }
                last = Some(found);
                cur = end;
                advanced = true;
                break;
            }
            if !advanced {
                cur += 1;
            }
        }

        if pos < limit_value {
            // Cases (2) and (4): a single forward pass decides.
            return Ok(None);
        }
        if last.is_some() {
            // A composition was found past POS; the caller checks whether
            // it actually covers POS.
            return Ok(last);
        }
        if scan_start == head {
            return Ok(None);
        }
        cur = scan_start - 1;
    }
}

fn terminal_font(interp: &Interpreter, value: &Value) -> Result<Value, LispError> {
    if value.is_nil()
        || matches!(value.kind(), Kind::Terminal(terminal) if terminal.borrow().live)
        || matches!(value.kind(), Kind::Frame(id) if interp.frame_is_live(id))
    {
        Ok(Value::symbol(&interp.effective_terminal_coding_system()))
    } else {
        Err(wrong_type_argument("terminal-live-p", *value))
    }
}

/// fill_gstring_body reads the fontless glyph width from
/// `char-width-table' (XFIXNAT (CHAR_TABLE_REF (Vchar_width_table, c))),
/// not from any host notion of display width.
fn char_width_table_width(interp: &Interpreter, env: &Env, character: char) -> i64 {
    char_table_id_from_var(interp, env, "char-width-table")
        .and_then(|id| interp.char_table_get(id, character as u32))
        .and_then(|value| value.integer_value())
        .unwrap_or(0)
        .max(0)
}

fn glyph_string(
    interp: &Interpreter,
    env: &Env,
    chars: &[char],
    font: Value,
    compose_cluster: bool,
) -> Value {
    let header = vector_value(
        std::iter::once(font).chain(
            chars
                .iter()
                .map(|character| Value::Integer(*character as i64)),
        ),
    );
    let cluster_end = chars.len().saturating_sub(1) as i64;
    let glyphs = chars.iter().enumerate().map(|(index, character)| {
        let width = char_width_table_width(interp, env, *character);
        let from = if compose_cluster { 0 } else { index as i64 };
        let to = if compose_cluster {
            cluster_end
        } else {
            index as i64
        };
        vector_value([
            Value::Integer(from),
            Value::Integer(to),
            Value::Integer(*character as i64),
            Value::Integer(*character as i64),
            Value::Integer(width),
            Value::Integer(0),
            Value::Integer(width),
            Value::Integer(1),
            Value::Integer(0),
            Value::Nil,
        ])
    });
    let mut body = vec![header, Value::Nil];
    body.extend(glyphs);
    body.resize(body.len().max(10), Value::Nil);
    vector_value(body)
}

fn find_composition_internal(
    interp: &mut Interpreter,
    env: &mut Env,
    args: &[Value],
) -> Result<Value, LispError> {
    let string = &args[2];
    // composite.c: fix_position of POS and of LIMIT, then STRING checked
    // and POS ranged.
    let fixed_pos = crate::lisp::primitives::textprop::fixnum_coerce_marker(interp, &args[0])?;
    let raw_limit = if args[1].is_nil() {
        None
    } else {
        Some(crate::lisp::primitives::textprop::fixnum_coerce_marker(
            interp, &args[1],
        )?)
    };
    let (minimum, maximum) = if string.is_nil() {
        (
            interp.buffer.borrow().point_min(),
            interp.buffer.borrow().point_max(),
        )
    } else {
        let string = string_like(string).ok_or_else(|| wrong_type_argument("stringp", *string))?;
        (0, string.text.chars().count())
    };
    if !(minimum as i64..=maximum as i64).contains(&fixed_pos) {
        let object = if string.is_nil() {
            current_buffer_value(interp)
        } else {
            *string
        };
        return Err(args_out_of_range([object, args[0]]));
    }
    let position = fixed_pos as usize;
    // composite.c clips LIMIT to the current buffer's ZV, a string's
    // search included; a negative one is no limit.
    let zv = interp.buffer.borrow().point_max() as i64;
    let limit = raw_limit.and_then(|raw| {
        let clipped = raw.min(zv);
        (clipped >= 0).then(|| clipped.clamp(minimum as i64, maximum as i64) as usize)
    });
    let string = *string;
    let Some((start, end, prop)) = find_composition(interp, position, limit, &string)? else {
        // No static composition: the automatic one, for a multibyte
        // target with auto-composition not inhibited.  The shaping runs
        // through the GNU Lisp owners (`auto-compose-chars',
        // `compose-gstring-for-terminal') over the ported C substrate.
        let multibyte = if string.is_nil() {
            interp.buffer.borrow().is_multibyte()
        } else {
            string_like(&string).is_some_and(|text| text.multibyte)
        };
        if multibyte
            && !inhibit_auto_composition(interp, env)
            && let Some(found) = find_automatic_composition(interp, env, position, limit, &string)?
        {
            return Ok(Value::list([
                Value::Integer(found.start as i64),
                Value::Integer(found.end as i64),
                found.gstring,
            ]));
        }
        return Ok(Value::Nil);
    };
    if !(start <= position && position < end) {
        // A static composition found through the LIMIT search that does
        // not cover POS: an automatic one closer to POS wins (this
        // find_automatic_composition call carries no multibyte or
        // inhibition guard).
        if let Some(found) = find_automatic_composition(interp, env, position, limit, &string)?
            && (if found.end <= position {
                found.end > end
            } else {
                found.start < start
            })
        {
            return Ok(Value::list([
                Value::Integer(found.start as i64),
                Value::Integer(found.end as i64),
                found.gstring,
            ]));
        }
    }
    let range = [Value::Integer(start as i64), Value::Integer(end as i64)];
    if !composition_valid_p(interp, start, end, prop) {
        return Ok(Value::list(range.into_iter().chain([Value::Nil])));
    }
    if !args[3].is_truthy() {
        return Ok(Value::list(range.into_iter().chain([Value::T])));
    }
    let id = match prop.cons_values().map(|(head, _)| head.kind()) {
        Some(Kind::Integer(id)) => usize::try_from(id).ok(),
        _ => get_composition_id(interp, env, start, end - start, prop, &string)?,
    };
    let Some(state) = id.and_then(|id| interp.composition_states.get(id)).cloned() else {
        return Ok(Value::list(range));
    };
    let components =
        crate::lisp::primitives::copy_sequence_value(interp, &registered_components(prop))?;
    Ok(Value::list(range.into_iter().chain([
        components,
        if state.relative { Value::T } else { Value::Nil },
        composition_modification_func(prop),
        Value::Integer(state.width),
    ])))
}

fn get_glyph_string(interp: &Interpreter, env: &Env, args: &[Value]) -> Result<Value, LispError> {
    let font = terminal_font(interp, &args[2])?;
    let chars = if args[3].is_nil() {
        // composite.c: a unibyte buffer cannot be shaped, checked before
        // validate_region.
        if !interp.buffer.borrow().is_multibyte() {
            return Err(LispError::Signal("Attempt to shape unibyte text".into()));
        }
        let (start, end) = checked_buffer_region(interp, &args[0], &args[1])?;
        composition_chars(interp, &Value::Nil, start, end)?
    } else {
        let (start, end) = checked_string_region(&args[3], &args[0], &args[1])?;
        composition_chars(interp, &args[3], start, end)?
    };
    if chars.is_empty() {
        return Err(LispError::Signal(
            "Attempt to shape zero-length text".into(),
        ));
    }
    Ok(glyph_string(interp, env, &chars, font, false))
}

fn sort_rules(rules: &Value) -> Result<Value, LispError> {
    let original = rules;
    let mut rules = original
        .to_vec()
        .map_err(|_| wrong_type_argument("listp", *original))?;
    if rules.len() <= 1 {
        return Ok(*original);
    }
    let mut keyed = Vec::with_capacity(rules.len());
    for rule in rules.drain(..) {
        let valid = vector_items(&rule).ok().filter(|items| {
            items.len() == 3 && matches!(items.get(1).map(|v| v.kind()), Some(Kind::Integer(value)) if value >= 0)
        });
        let Some(items) = valid else {
            return Err(LispError::Signal(
                "Invalid composition rule in RULES argument".into(),
            ));
        };
        keyed.push((items[1].as_integer().unwrap_or(0), rule));
    }
    keyed.sort_by_key(|(lookback, _)| std::cmp::Reverse(*lookback));
    Ok(Value::list(keyed.into_iter().map(|(_, rule)| rule)))
}

define_dispatch!(
    pub(super) fn call(
        interp: &mut Interpreter,
        name: &str,
        args: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        match name {
            "clear-composition-cache" => {
                need_args(name, args, 0)?;
                Ok(Value::Nil)
            }
            "compose-region-internal" => {
                need_arg_range(name, args, 2, 4)?;
                compose_region(interp, args, env)
            }
            "compose-string-internal" => {
                need_arg_range(name, args, 3, 5)?;
                compose_string(args)
            }
            "composition-get-gstring" => {
                need_args(name, args, 4)?;
                get_glyph_string(interp, env, args)
            }
            "composition-sort-rules" => {
                need_args(name, args, 1)?;
                sort_rules(&args[0])
            }
            "find-composition-internal" => {
                need_args(name, args, 4)?;
                find_composition_internal(interp, env, args)
            }
        }
    }
);
