use super::*;
use crate::lisp::types::CharTableRef;
use crate::lisp::types::Kind;

/// fns.c:plist_put, shared by Fput and Fplist_put's default EQ path.
/// Existing pairs are mutated in place; only a new property allocates conses.
pub(super) fn plist_put_eq(
    interp: &mut Interpreter,
    plist: Value,
    property: Value,
    value: Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    let mut tail = plist;
    let mut previous_value_cell = Value::Nil;
    let mut tortoise = tail;
    let mut maximum = 2_isize;
    let mut remaining = 0_isize;
    let mut quit_count = 2_u16;
    while let Kind::Cons(cell) = tail.kind() {
        let rest = cell.cdr.get();
        let Kind::Cons(value_cell) = rest.kind() else {
            break;
        };
        if values_eq_in_env(interp, &cell.car.get(), &property, env) {
            rest.set_car(value)?;
            return Ok(plist);
        }
        previous_value_cell = rest;
        tail = value_cell.cdr.get();
        // lisp.h:FOR_EACH_TAIL advances two cells per plist pair. Match its
        // quit checks and the exact tail supplied to circular_list.
        quit_count = quit_count.wrapping_sub(1);
        let compare = if quit_count != 0 {
            true
        } else {
            interp.maybe_quit(env)?;
            remaining = remaining.wrapping_sub(1);
            remaining > 0
        };
        if compare {
            if tail.word() == tortoise.word() {
                return Err(LispError::SignalValue(Value::list([
                    Value::symbol("circular-list"),
                    tail,
                ])));
            }
        } else {
            maximum = maximum.wrapping_shl(1);
            quit_count = maximum as u16;
            remaining = maximum >> u16::BITS;
            tortoise = tail;
        }
    }
    if !tail.is_nil() {
        return Err(plist_type_error(&plist));
    }
    let new_pair = Value::cons(property, Value::cons(value, Value::Nil));
    if previous_value_cell.is_nil() {
        Ok(new_pair)
    } else {
        previous_value_cell.set_cdr(new_pair)?;
        Ok(plist)
    }
}

/// fns.c's plist test: EQ without PREDICATE (plist_get, plist_put and
/// plist_member), else `call2 (predicate, XCAR (tail), prop)'.
fn plist_property_matches(
    interp: &mut Interpreter,
    env: &mut Env,
    property: Value,
    prop: Value,
    predicate: Value,
) -> Result<bool, LispError> {
    if predicate.is_nil() {
        return Ok(values_eq_in_env(interp, &property, &prop, env));
    }
    Ok(call_function_value(interp, &predicate, &[property, prop], env)?.is_truthy())
}

/// fns.c:Fplist_get and plist_get: FOR_EACH_TAIL_SAFE two cells a step;
/// a pair without a value, a non-list or a cycle ends it with nil.
fn plist_get_value(
    interp: &mut Interpreter,
    env: &mut Env,
    plist: Value,
    prop: Value,
    predicate: Option<Value>,
) -> Result<Value, LispError> {
    let predicate = predicate.unwrap_or(Value::Nil);
    let mut tail = plist;
    let mut walk = crate::lisp::types::TailWalk::new(tail);
    while let Some(cell) = tail.cons_ref() {
        let Some(rest) = cell.cdr.get().cons_ref() else {
            break;
        };
        if plist_property_matches(interp, env, cell.car.get(), prop, predicate)? {
            return Ok(rest.car.get());
        }
        tail = rest.cdr.get();
        if walk.cycled_no_quit(tail) {
            break;
        }
    }
    Ok(Value::Nil)
}

/// fns.c:Fplist_put with PREDICATE: FOR_EACH_TAIL two cells a step,
/// storing into the value cell of the first matching property, else
/// CHECK_TYPE (NILP (tail), Qplistp, plist) and a new pair at the end.
fn plist_put_with_predicate(
    interp: &mut Interpreter,
    env: &mut Env,
    plist: Value,
    prop: Value,
    val: Value,
    predicate: Value,
) -> Result<Value, LispError> {
    let mut prev = Value::Nil;
    let mut tail = plist;
    let mut walk = crate::lisp::types::TailWalk::new(tail);
    while let Some(cell) = tail.cons_ref() {
        let rest = cell.cdr.get();
        let Some(value_cell) = rest.cons_ref() else {
            break;
        };
        if plist_property_matches(interp, env, cell.car.get(), prop, predicate)? {
            rest.set_car(val)?;
            return Ok(plist);
        }
        prev = tail;
        tail = value_cell.cdr.get();
        if walk.cycled(tail, || interp.maybe_quit(env))? {
            return Err(crate::lisp::types::circular_list(tail));
        }
    }
    if !tail.is_nil() {
        return Err(plist_type_error(&plist));
    }
    let rest = if prev.is_nil() {
        plist
    } else {
        prev.cdr()?.cdr()?
    };
    let new_cell = Value::cons(prop, Value::cons(val, rest));
    if prev.is_nil() {
        return Ok(new_cell);
    }
    prev.cdr()?.set_cdr(new_cell)?;
    Ok(plist)
}

/// fns.c:Fplist_member and plist_member: FOR_EACH_TAIL two cells a step
/// to the tail whose property matches; CHECK_TYPE (NILP (tail), Qplistp,
/// plist) when none does.
pub(super) fn plist_member_value(
    interp: &mut Interpreter,
    env: &mut Env,
    plist: Value,
    prop: Value,
    predicate: Option<Value>,
) -> Result<Value, LispError> {
    let predicate = predicate.unwrap_or(Value::Nil);
    let mut tail = plist;
    let mut walk = crate::lisp::types::TailWalk::new(tail);
    while let Some(cell) = tail.cons_ref() {
        if plist_property_matches(interp, env, cell.car.get(), prop, predicate)? {
            return Ok(tail);
        }
        tail = cell.cdr.get();
        let Some(value_cell) = tail.cons_ref() else {
            break;
        };
        tail = value_cell.cdr.get();
        if walk.cycled(tail, || interp.maybe_quit(env))? {
            return Err(crate::lisp::types::circular_list(tail));
        }
    }
    if !tail.is_nil() {
        return Err(plist_type_error(&plist));
    }
    Ok(Value::Nil)
}

pub(super) fn fixnum_index_arg(value: &Value) -> Result<i64, LispError> {
    match value.kind() {
        Kind::Integer(index) => Ok(index),
        other => Err(wrong_type_argument("fixnump", other.value())),
    }
}

fn args_out_of_range(sequence: &Value, index: &Value) -> LispError {
    LispError::SignalValue(Value::list([
        Value::Symbol("args-out-of-range".into()),
        *sequence,
        *index,
    ]))
}

#[derive(Clone, Copy)]
enum DeleteListComparison {
    Eq,
    Equal,
}

fn delete_from_list(
    interp: &mut Interpreter,
    elt: &Value,
    list: &Value,
    env: &Env,
    comparison: DeleteListComparison,
) -> Result<Value, LispError> {
    // fns.c:Fdelq and Fdelete's list case: FOR_EACH_TAIL, unlinking each
    // match, then CHECK_LIST_END against the list as it now begins.
    let mut head = *list;
    let mut previous: Option<Value> = None;
    let mut tail = *list;
    let mut walk = crate::lisp::types::TailWalk::new(tail);
    while let Some(cell) = tail.cons_ref() {
        let next = cell.cdr.get();
        let matches = match comparison {
            DeleteListComparison::Eq => values_eq_in_env(interp, &cell.car.get(), elt, env),
            DeleteListComparison::Equal => values_equal(interp, elt, &cell.car.get())?,
        };
        if matches {
            if let Some(previous) = &previous {
                previous.set_cdr(next)?;
            } else {
                head = next;
            }
        } else {
            previous = Some(tail);
        }
        tail = next;
        if walk.cycled_no_quit(tail) {
            return Err(crate::lisp::types::circular_list(tail));
        }
    }
    if !tail.is_nil() {
        return Err(wrong_type_argument("listp", head));
    }
    Ok(head)
}

fn current_category_table_id(interp: &mut Interpreter) -> CharTableRef {
    interp
        .buffer_local_value(interp.current_buffer_id(), "category-table")
        .and_then(|value| match value.kind() {
            Kind::CharTable(id) => Some(id),
            _ => None,
        })
        .unwrap_or_else(|| interp.ensure_standard_category_table())
}

fn category_table_arg(
    interp: &mut Interpreter,
    value: Option<&Value>,
    default_to_standard: bool,
) -> Result<CharTableRef, LispError> {
    let id = match value.map(|v| v.kind()) {
        Some(Kind::CharTable(id)) => id,
        Some(Kind::Nil) | None if default_to_standard => interp.ensure_standard_category_table(),
        Some(Kind::Nil) | None => current_category_table_id(interp),
        Some(other) => {
            return Err(LispError::TypeError(
                "category-table".into(),
                other.value().type_name(),
            ));
        }
    };
    if !id.has_purpose("category-table") {
        return Err(LispError::TypeError(
            "category-table".into(),
            "char-table".into(),
        ));
    }
    Ok(id)
}

/// data.c's `enum bool_vector_op'.
#[derive(Clone, Copy, PartialEq, Eq)]
enum BoolVectorOp {
    ExclusiveOr,
    Union,
    Intersection,
    SetDifference,
    Subsetp,
}

/// data.c:wrong_length_argument.
fn wrong_length_argument(values: &[Value]) -> LispError {
    let mut list = vec![Value::symbol("wrong-length-argument")];
    list.extend(values.iter().map(|value| match value.kind() {
        Kind::BoolVector(vector) => Value::Integer(vector.len() as i64),
        _ => *value,
    }));
    LispError::SignalValue(Value::list(list))
}

/// data.c:bool_vector_binop_driver: OP of A and B word by word, into a new
/// vector, or into DEST, which is then returned only if it changed; the
/// subset test is a predicate.
fn bool_vector_binop_driver(
    a: &Value,
    b: &Value,
    dest: Value,
    op: BoolVectorOp,
) -> Result<Value, LispError> {
    let (av, bv) = (bool_vector_arg(a)?, bool_vector_arg(b)?);
    let nr_bits = av.len();
    if bv.len() != nr_bits {
        return Err(wrong_length_argument(&if dest.is_nil() {
            vec![*a, *b]
        } else {
            vec![*a, *b, dest]
        }));
    }
    let combine = |x: usize, y: usize| match op {
        BoolVectorOp::ExclusiveOr => x ^ y,
        BoolVectorOp::Union => x | y,
        BoolVectorOp::Intersection => x & y,
        BoolVectorOp::SetDifference | BoolVectorOp::Subsetp => x & !y,
    };
    let (adata, bdata) = (av.words(), bv.words());
    let dest = if dest.is_nil() {
        Value::BoolVector(crate::lisp::types::BoolVectorRef::new(nr_bits))
    } else {
        let destv = bool_vector_arg(&dest)?;
        if destv.len() != nr_bits {
            return Err(wrong_length_argument(&[*a, *b, dest]));
        }
        if op == BoolVectorOp::Subsetp {
            return Ok(
                if adata
                    .iter()
                    .zip(bdata)
                    .all(|(x, y)| x.get() & !y.get() == 0)
                {
                    Value::T
                } else {
                    Value::Nil
                },
            );
        }
        let unchanged = destv
            .words()
            .iter()
            .zip(adata.iter().zip(bdata))
            .all(|(d, (x, y))| d.get() == combine(x.get(), y.get()));
        if unchanged {
            return Ok(Value::Nil);
        }
        dest
    };
    let destv = bool_vector_arg(&dest)?;
    for (d, (x, y)) in destv.words().iter().zip(adata.iter().zip(bdata)) {
        d.set(combine(x.get(), y.get()));
    }
    Ok(dest)
}

/// data.c:Fbool_vector_subsetp: the driver with B as the destination.
pub(super) fn bool_vector_subsetp(a: &Value, b: &Value) -> Result<Value, LispError> {
    bool_vector_binop_driver(a, b, *b, BoolVectorOp::Subsetp)
}

/// data.c:Fbool_vector_not: ~A, into a new vector or into B.
fn bool_vector_not(a: &Value, b: Value) -> Result<Value, LispError> {
    let av = bool_vector_arg(a)?;
    let nr_bits = av.len();
    let b = if b.is_nil() {
        Value::BoolVector(crate::lisp::types::BoolVectorRef::new(nr_bits))
    } else {
        if bool_vector_arg(&b)?.len() != nr_bits {
            return Err(wrong_length_argument(&[*a, b]));
        }
        b
    };
    let bv = bool_vector_arg(&b)?;
    use crate::lisp::alloc::vectors::bool_vectors::BITS_PER_BITS_WORD;
    for (index, (to, from)) in bv.words().iter().zip(av.words()).enumerate() {
        let mut word = !from.get();
        if index == nr_bits / BITS_PER_BITS_WORD {
            // bool_vector_spare_mask: the bits past the end stay clear.
            word &= (1 << (nr_bits % BITS_PER_BITS_WORD)) - 1;
        }
        to.set(word);
    }
    Ok(b)
}

/// data.c:Fbool_vector_count_consecutive: how many bits from I on equal
/// B, I at most the length.
fn bool_vector_count_consecutive(a: &Value, b: &Value, i: &Value) -> Result<Value, LispError> {
    let vector = bool_vector_arg(a)?;
    let start = match i.kind() {
        Kind::Integer(index) if index >= 0 => index as usize,
        _ => return Err(LispError::WrongTypeArgument("wholenump".into(), *i)),
    };
    let nr_bits = vector.len();
    if start > nr_bits {
        return Err(LispError::SignalValue(Value::list([
            Value::symbol("args-out-of-range"),
            *a,
            *i,
        ])));
    }
    let target = b.is_truthy();
    let count = (start..nr_bits)
        .take_while(|index| vector.get(*index) == Some(target))
        .count();
    Ok(Value::Integer(count as i64))
}

fn category_arg(value: &Value) -> Result<u32, LispError> {
    match value.kind() {
        Kind::Integer(code) if (32..=126).contains(&code) => Ok(code as u32),
        _ => Err(LispError::WrongTypeArgument("categoryp".into(), *value)),
    }
}

fn category_set_bits(value: &Value) -> Result<Vec<bool>, LispError> {
    bool_vector_bits(value)
        .ok()
        .filter(|bits| bits.len() == 128)
        .ok_or_else(|| LispError::WrongTypeArgument("categorysetp".into(), *value))
}

fn table_character(value: &Value) -> Result<u32, LispError> {
    match value.kind() {
        Kind::Integer(code) if (0..=0x3f_ffff).contains(&code) => Ok(code as u32),
        _ => Err(LispError::WrongTypeArgument("characterp".into(), *value)),
    }
}

fn category_character_range(value: &Value) -> Result<(u32, u32), LispError> {
    if matches!(value.kind(), Kind::Integer(_)) {
        let character = table_character(value)?;
        return Ok((character, character));
    }
    let Kind::Cons(cell) = value.kind() else {
        return Err(LispError::WrongTypeArgument("consp".into(), *value));
    };
    Ok((
        table_character(&cell.car.get())?,
        table_character(&cell.cdr.get())?,
    ))
}

/// Boundaries at which the effective value of a char table can change.
///
/// Splitting a category update at every current boundary preserves earlier
/// per-range values without walking every Unicode scalar in a large GNU
/// category range. Only radix nodes intersecting the update are visited.
fn char_table_change_boundaries(table_id: CharTableRef, start: u32, end: u32) -> Vec<u32> {
    let mut boundaries = vec![start];
    let mut next_table = Some(table_id);
    while let Some(id) = next_table {
        let table = id;
        table.append_change_boundaries(start, end, &mut boundaries);
        next_table = table.parent();
    }
    boundaries.sort_unstable();
    boundaries.dedup();
    boundaries
}

// chartab.c:optimize_sub_char_table. Root each active node and the first
// value across user predicates, including predicates that detach that node.
fn optimize_sub_char_table(
    interp: &mut Interpreter,
    table: crate::lisp::types::SubCharTableRef,
    test: Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    interp.with_lisp_stack_roots(&Value::SubCharTable(table), |interp| {
        let mut first = table.slot(0);
        if let Kind::SubCharTable(child) = first.kind() {
            first = optimize_sub_char_table(interp, child, test, env)?;
            table.set_slot(0, first);
        }
        interp.with_lisp_stack_roots(&first, |interp| {
            let mut optimizable = !matches!(first.kind(), Kind::SubCharTable(_));
            for index in 1..table.slots().len() {
                let mut value = table.slot(index);
                if let Kind::SubCharTable(child) = value.kind() {
                    value = optimize_sub_char_table(interp, child, test, env)?;
                    table.set_slot(index, value);
                }
                if optimizable {
                    optimizable = if test.is_nil() {
                        values_equal(interp, &value, &first)?
                    } else if test.eq_value(Value::symbol("eq")) {
                        value.eq_value(first)
                    } else {
                        call_function_value(interp, &test, &[value, first], env)?.is_truthy()
                    };
                }
            }
            Ok(if optimizable {
                first
            } else {
                Value::SubCharTable(table)
            })
        })
    })
}

fn optimize_char_table(
    interp: &mut Interpreter,
    table: CharTableRef,
    test: Value,
    env: &mut Env,
) -> Result<(), LispError> {
    interp.with_lisp_stack_roots(&Value::CharTable(table), |interp| {
        for index in 4..68 {
            if let Kind::SubCharTable(child) = table.slot(index).kind() {
                table.set_slot(index, optimize_sub_char_table(interp, child, test, env)?);
            }
        }
        table.refresh_ascii();
        Ok(())
    })
}

// chartab.c:map_char_table reuses one range cons and reads each node as the
// walk reaches it. A snapshot of effective ranges loses callback mutations.
type CharTableCallback<'a> = dyn FnMut(&mut Interpreter, Value, Value, CharTableRef, bool, &mut Env) -> Result<(), LispError>
    + 'a;

fn map_char_table_emit(
    interp: &mut Interpreter,
    function: &mut CharTableCallback<'_>,
    range: Value,
    value: Value,
    top: CharTableRef,
    decode: bool,
    env: &mut Env,
) -> Result<(), LispError> {
    let key = if range.car()?.eq_value(range.cdr()?) {
        range.car()?
    } else {
        range
    };
    function(interp, key, value, top, decode, env)
}

fn map_char_table_node(
    interp: &mut Interpreter,
    function: &mut CharTableCallback<'_>,
    node: Value,
    mut value: Value,
    range: Value,
    top: CharTableRef,
    env: &mut Env,
) -> Result<Value, LispError> {
    interp.with_lisp_stack_roots(&vec![node, Value::CharTable(top), range, value], |interp| {
        let (depth, minimum, mut maximum) = match node.kind() {
            Kind::CharTable(_) => (0, 0, 0x3fffff),
            Kind::SubCharTable(table) => {
                let depth = table.depth();
                let minimum = i64::from(table.min_char());
                (depth, minimum, minimum + [65536, 4096, 128][depth - 1] - 1)
            }
            _ => unreachable!("character table node"),
        };
        let width = [65536, 4096, 128, 1][depth];
        let mut from = range.car()?.as_integer()?;
        let to = range.cdr()?.as_integer()?;
        maximum = maximum.min(to);
        let mut index = if from <= minimum {
            0
        } else {
            (from - minimum) / width
        };
        let mut character = minimum + index * width;
        let uniprop = top.is_uniprop();
        let decode = uniprop && top.extra(1) == Some(Value::Integer(0));
        while character <= maximum {
            let mut this = match node.kind() {
                Kind::CharTable(table) => table.slot(4 + index as usize),
                Kind::SubCharTable(table) => table.child(index as usize, uniprop),
                _ => unreachable!(),
            };
            let next = character + width;
            if matches!(this.kind(), Kind::SubCharTable(_)) {
                if to >= next {
                    range.set_cdr(Value::Integer(next - 1))?;
                }
                value = map_char_table_node(interp, function, this, value, range, top, env)?;
            } else {
                if this.is_nil() {
                    this = top.default();
                }
                if !value.eq_value(this) {
                    interp.with_lisp_stack_roots(&this, |interp| -> Result<(), LispError> {
                        let mut different = true;
                        if value.is_nil()
                            && let Some(parent) = top.parent()
                        {
                            // The GNU temporary parent=nil operation cannot call
                            // Lisp. Resolve the same local value without a store.
                            value = parent.explicit_get(from as u32);
                            if value.is_nil() {
                                value = parent.default();
                            }
                            range.set_cdr(Value::Integer(character - 1))?;
                            value = map_char_table_node(
                                interp,
                                function,
                                Value::CharTable(parent),
                                value,
                                range,
                                parent,
                                env,
                            )?;
                            if value.eq_value(this) {
                                different = false;
                            }
                        }
                        if !value.is_nil() && different {
                            range.set_cdr(Value::Integer(character - 1))?;
                            map_char_table_emit(interp, function, range, value, top, decode, env)?;
                        }
                        Ok(())
                    })?;
                    value = this;
                    from = character;
                    range.set_car(Value::Integer(character))?;
                }
            }
            range.set_cdr(Value::Integer(to))?;
            index += 1;
            character = next;
        }
        Ok(value)
    })
}

pub(in crate::lisp::primitives) fn map_char_table_with(
    interp: &mut Interpreter,
    function: &mut CharTableCallback<'_>,
    mut table: CharTableRef,
    env: &mut Env,
) -> Result<Value, LispError> {
    let range = Value::cons(Value::Integer(0), Value::Integer(0x3fffff));
    let decode = table.is_uniprop() && table.extra(1) == Some(Value::Integer(0));
    interp.with_lisp_stack_roots(&vec![range, Value::CharTable(table)], |interp| {
        let value = table.explicit_get(0);
        let mut value = map_char_table_node(
            interp,
            function,
            Value::CharTable(table),
            value,
            range,
            table,
            env,
        )?;
        while value.is_nil()
            && let Some(parent) = table.parent()
        {
            let from = range.car()?.as_integer()? as u32;
            value = parent.explicit_get(from);
            if value.is_nil() {
                value = parent.default();
            }
            value = map_char_table_node(
                interp,
                function,
                Value::CharTable(parent),
                value,
                range,
                parent,
                env,
            )?;
            table = parent;
        }
        if !value.is_nil() {
            map_char_table_emit(interp, function, range, value, table, decode, env)?;
        }
        Ok(Value::Nil)
    })
}

fn map_char_table(
    interp: &mut Interpreter,
    function: Value,
    table: CharTableRef,
    env: &mut Env,
) -> Result<Value, LispError> {
    interp.with_lisp_stack_roots(&function, |interp| {
        map_char_table_with(
            interp,
            &mut |interp, key, mut value, top, decode, env| {
                // chartab.c decodes Unicode properties for a Lisp callback,
                // but leaves the stored value intact for a C callback.
                if decode && let Some(Kind::Vector(values)) = top.extra(4).map(|value| value.kind())
                {
                    let index = value.as_integer()?;
                    if let Ok(index) = usize::try_from(index)
                        && let Some(decoded) = values.get(index)
                    {
                        value = decoded;
                    }
                }
                call_function_value(interp, &function, &[key, value], env)?;
                Ok(())
            },
            table,
            env,
        )
    })
}

define_dispatch!(
    pub(super) fn call(
        interp: &mut Interpreter,
        name: &str,
        args: &[Value],
        env: &mut crate::lisp::types::Env,
    ) -> Result<Value, LispError> {
        match name {
            // ── Plist operations ──
            "plist-get" => {
                need_arg_range(name, args, 2, 3)?;
                plist_get_value(interp, env, args[0], args[1], args.get(2).copied())
            }

            "plist-put" => {
                need_arg_range(name, args, 3, 4)?;
                let predicate = args.get(3).copied().unwrap_or(Value::Nil);
                if predicate.is_nil() {
                    return plist_put_eq(interp, args[0], args[1], args[2], env);
                }
                plist_put_with_predicate(interp, env, args[0], args[1], args[2], predicate)
            }

            "plist-member" => {
                need_arg_range(name, args, 2, 3)?;
                plist_member_value(interp, env, args[0], args[1], args.get(2).copied())
            }

            // ── Sort ──
            "sort" => {
                if args.is_empty() {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                // fns.c:Fsort.  Only the old-style call (sort SEQ PREDICATE)
                // sorts in place unless asked; the keyword form sorts a copy.
                let mut lessp = None;
                let mut key = None;
                let mut in_place = false;
                let mut reverse = false;
                if args.len() == 2 {
                    lessp = Some(args[1]);
                    in_place = true;
                } else if args.len().is_multiple_of(2) {
                    return Err(LispError::SignalValue(Value::list([
                        Value::symbol("error"),
                        Value::string("Invalid argument list"),
                    ])));
                } else {
                    for &[keyword, value] in args[1..].as_chunks::<2>().0 {
                        match keyword.kind() {
                            Kind::Symbol(keyword) if keyword == ":key" => key = Some(value),
                            Kind::Symbol(keyword) if keyword == ":lessp" => lessp = Some(value),
                            Kind::Symbol(keyword) if keyword == ":in-place" => {
                                in_place = value.is_truthy();
                            }
                            Kind::Symbol(keyword) if keyword == ":reverse" => {
                                reverse = value.is_truthy();
                            }
                            // eval.c:signal_error splices a proper list.
                            _ => {
                                let data = match keyword.kind() {
                                    Kind::Nil | Kind::Cons(_) => keyword.to_vec().ok(),
                                    _ => None,
                                }
                                .unwrap_or_else(|| vec![keyword]);
                                return Err(LispError::SignalValue(Value::list(
                                    [
                                        Value::symbol("error"),
                                        Value::string("Invalid keyword argument"),
                                    ]
                                    .into_iter()
                                    .chain(data),
                                )));
                            }
                        }
                    }
                }
                let (kind, items) = sort_sequence_kind_and_items(&args[0])?;
                let sorted = match sort_sequence_items(
                    interp,
                    items,
                    key.as_ref(),
                    lessp.as_ref(),
                    reverse,
                    env,
                ) {
                    Ok(sorted) => sorted,
                    Err(failure) => {
                        // sort.c permutes a vector's own slots: a signal
                        // leaves one sorted in place as far as the sort got.
                        if in_place
                            && matches!(kind, SortSequenceKind::Vector)
                            && let Some(items) = &failure.items
                        {
                            write_sorted_sequence(&args[0], &kind, items)?;
                        }
                        return Err(failure.error);
                    }
                };
                if in_place {
                    write_sorted_sequence(&args[0], &kind, &sorted)?;
                    Ok(args[0])
                } else {
                    Ok(build_sorted_sequence(&kind, sorted))
                }
            }
            "random" => {
                if args.is_empty() || args[0].is_nil() {
                    Ok(Value::Integer(random_fixnum()))
                } else {
                    match args[0].kind() {
                        Kind::T => {
                            set_random_seed(nondeterministic_random_seed());
                            Ok(Value::Integer(random_fixnum()))
                        }
                        Kind::StringObject(_) => {
                            let seed = string_like(&args[0])
                                .expect("string variants should be string-like")
                                .text;
                            set_random_seed(random_seed_from_bytes(seed.as_bytes()));
                            Ok(Value::Integer(random_fixnum()))
                        }
                        Kind::Integer(_) | Kind::BigInteger(_) => {
                            let limit = integer_like_bigint(interp, &args[0])?;
                            if limit <= BigInt::zero() {
                                Err(LispError::SignalValue(Value::list([
                                    Value::Symbol("args-out-of-range".into()),
                                    args[0],
                                ])))
                            } else {
                                Ok(normalize_bigint_value(random_bigint_below(&limit)))
                            }
                        }
                        // Frandom treats other objects like an omitted limit.
                        _ => Ok(Value::Integer(random_fixnum())),
                    }
                }
            }

            "vector" => Ok(Value::vector(args.iter().copied())),
            "bool-vector-count-population" => {
                need_args(name, args, 1)?;
                let vector = bool_vector_arg(&args[0])?;
                Ok(Value::Integer(
                    vector
                        .words()
                        .iter()
                        .map(|word| word.get().count_ones() as i64)
                        .sum(),
                ))
            }
            "bool-vector-count-consecutive" => {
                need_args(name, args, 3)?;
                bool_vector_count_consecutive(&args[0], &args[1], &args[2])
            }
            "bool-vector-intersection"
            | "bool-vector-union"
            | "bool-vector-exclusive-or"
            | "bool-vector-set-difference" => {
                need_arg_range(name, args, 2, 3)?;
                let op = match name {
                    "bool-vector-intersection" => BoolVectorOp::Intersection,
                    "bool-vector-union" => BoolVectorOp::Union,
                    "bool-vector-exclusive-or" => BoolVectorOp::ExclusiveOr,
                    _ => BoolVectorOp::SetDifference,
                };
                let dest = args.get(2).copied().unwrap_or(Value::Nil);
                bool_vector_binop_driver(&args[0], &args[1], dest, op)
            }
            "bool-vector-not" => {
                need_arg_range(name, args, 1, 2)?;
                bool_vector_not(&args[0], args.get(1).copied().unwrap_or(Value::Nil))
            }

            "aref" => {
                need_args(name, args, 2)?;
                let raw_idx = fixnum_index_arg(&args[1])?;
                if let Kind::StringObject(state) = args[0].kind() {
                    // data.c:Faref: SREF of a unibyte string, STRING_CHAR at
                    // string_char_to_byte of a multibyte one.
                    return usize::try_from(raw_idx)
                        .ok()
                        .and_then(|idx| state.borrow().character_at(idx))
                        .map(Value::Integer)
                        .ok_or_else(|| args_out_of_range(&args[0], &args[1]));
                }
                let literal = record_literal_items(&args[0]);
                // data.c:Faref exposes records and closures, but not the
                // other pseudovectors kept in our internal Record storage.
                if literal.is_none()
                    && !args[0].is_string()
                    && !is_vector_value(&args[0])
                    && !matches!(
                        args[0].kind(),
                        Kind::Closure(_)
                            | Kind::CharTable(_)
                            | Kind::LispRecord(_)
                            | Kind::BoolVector(_)
                    )
                {
                    return Err(LispError::WrongTypeArgument("arrayp".into(), args[0]));
                }
                if matches!(args[0].kind(), Kind::CharTable(_))
                    && !(0..=0x3f_ffff).contains(&raw_idx)
                {
                    return Err(LispError::WrongTypeArgument("characterp".into(), args[1]));
                }
                if raw_idx < 0 {
                    return Err(args_out_of_range(&args[0], &args[1]));
                }
                let idx = raw_idx as usize;
                // Support both list-vectors and strings
                if let Some(items) = literal {
                    return record_literal_aref(&args[0], &items, idx, &args[1]);
                }
                match args[0].kind() {
                    Kind::Closure(lambda) => lambda
                        .get(idx)
                        .ok_or_else(|| args_out_of_range(&args[0], &args[1])),
                    Kind::CharTable(id) => {
                        let key = raw_idx as u32;
                        Ok(id.get(key))
                    }
                    Kind::LispRecord(record) => record
                        .get(idx)
                        .ok_or_else(|| args_out_of_range(&args[0], &args[1])),
                    Kind::BoolVector(vector) => vector
                        .get(idx)
                        .map(|bit| if bit { Value::T } else { Value::Nil })
                        .ok_or_else(|| args_out_of_range(&args[0], &args[1])),
                    _ => {
                        if is_vector_value(&args[0]) {
                            vector_slot_value(&args[0], idx)
                        } else {
                            Err(LispError::WrongTypeArgument("arrayp".into(), args[0]))
                        }
                    }
                }
            }

            "aset" => {
                need_args(name, args, 3)?;
                let raw_idx = fixnum_index_arg(&args[1])?;
                if let Kind::StringObject(_) = args[0].kind() {
                    // data.c:Faset's string case: CHECK_IMPURE, the index,
                    // then CHECK_CHARACTER of NEWELT.
                    aset_string_value(&args[0], raw_idx, &args[2])
                        .map_err(|error| string_storage_error(interp, env, error))?;
                    return Ok(args[2]);
                }
                // data.c:Faset permits actual records as well as arrays;
                // native functions and other opaque pseudovectors are neither.
                if !args[0].is_string()
                    && !is_vector_value(&args[0])
                    && !matches!(
                        args[0].kind(),
                        Kind::CharTable(_) | Kind::LispRecord(_) | Kind::BoolVector(_)
                    )
                {
                    return Err(LispError::WrongTypeArgument("arrayp".into(), args[0]));
                }
                if matches!(args[0].kind(), Kind::CharTable(_))
                    && !(0..=0x3f_ffff).contains(&raw_idx)
                {
                    return Err(LispError::WrongTypeArgument("characterp".into(), args[1]));
                }
                if raw_idx < 0 {
                    return Err(args_out_of_range(&args[0], &args[1]));
                }
                let idx = raw_idx as usize;
                match args[0].kind() {
                    value if is_vector_value(&value.value()) => {
                        aset_vector_value(&value.value(), idx, args[2])
                            .map_err(|_| args_out_of_range(&args[0], &args[1]))?;
                        Ok(args[2])
                    }
                    Kind::CharTable(id) => {
                        let key = raw_idx as u32;
                        interp.char_table_set(id, key, args[2])?;
                        Ok(args[2])
                    }
                    Kind::BoolVector(vector) => {
                        if idx >= vector.len() {
                            return Err(args_out_of_range(&args[0], &args[1]));
                        }
                        vector.set(idx, args[2].is_truthy());
                        Ok(args[2])
                    }
                    Kind::LispRecord(record) => {
                        if !record.set(idx, args[2]) {
                            return Err(args_out_of_range(&args[0], &args[1]));
                        }
                        Ok(args[2])
                    }
                    _ => Err(LispError::WrongTypeArgument("arrayp".into(), args[0])),
                }
            }

            "nreverse" => {
                need_args(name, args, 1)?;
                nreverse_sequence_value(interp, env, &args[0])
            }

            "copy-sequence" => {
                need_args(name, args, 1)?;
                copy_sequence_value(interp, &args[0])
            }
            "fillarray" => {
                need_args(name, args, 2)?;
                match args[0].kind() {
                    value if is_vector_value(&value.value()) => {
                        let len = vector_items(&value.value())?.len();
                        for index in 0..len {
                            aset_vector_value(&value.value(), index, args[1])?;
                        }
                        Ok(args[0])
                    }
                    Kind::StringObject(state) => {
                        // fns.c validates ITEM before the empty fast path and
                        // before CHECK_IMPURE on a nonempty string.
                        crate::lisp::types::string_data::character_code(args[1])?;
                        if state.borrow().len() != 0 {
                            state.check_impure()?;
                            state.borrow_mut().fill(args[1])?;
                        }
                        Ok(args[0])
                    }
                    // alloc.c:bool_vector_fill.
                    Kind::BoolVector(vector) => {
                        vector.fill(args[1].is_truthy());
                        Ok(args[0])
                    }
                    Kind::CharTable(id) => {
                        // fns.c:Ffillarray writes contents/default; it leaves the ASCII field alone.
                        for index in 4..68 {
                            id.set_slot(index, args[1]);
                        }
                        id.set_default(args[1]);
                        Ok(args[0])
                    }
                    other => Err(LispError::WrongTypeArgument("arrayp".into(), other.value())),
                }
            }
            "load-average" => {
                if args.len() > 1 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                let average = sysinfo::System::load_average();
                let values = [average.one, average.five, average.fifteen];
                if args.first().is_some_and(Value::is_truthy) {
                    Ok(Value::list(values.map(Value::float)))
                } else {
                    // GNU multiplies the host values by 100 and truncates the
                    // resulting positive doubles to integers.
                    Ok(Value::list(
                        values.map(|value| Value::Integer((value * 100.0) as i64)),
                    ))
                }
            }
            "locale-info" => {
                need_args(name, args, 1)?;
                let item = args[0].as_symbol()?;
                // fns.c:Flocale_info -- real langinfo(3) answers.  This was
                // a stub returning nil for every ITEM, which sent GNU's own
                // set-locale-environment down the no-codeset branch at boot
                // (mule-cmds.el reads `codeset' to pick the coding systems).
                // GNU decodes the strings with locale-coding-system; the
                // names this build consults are ASCII in the harness
                // locales, so a lossy UTF-8 read is byte-identical there.
                #[cfg(unix)]
                {
                    fn langinfo_string(item: libc::nl_item) -> Option<String> {
                        let pointer = unsafe { libc::nl_langinfo(item) };
                        if pointer.is_null() {
                            return None;
                        }
                        let text = unsafe { std::ffi::CStr::from_ptr(pointer) };
                        Some(text.to_string_lossy().into_owned())
                    }
                    fn langinfo_vector(items: &[libc::nl_item]) -> Value {
                        Value::vector(items.iter().map(|item| {
                            langinfo_string(*item)
                                .map(|name| Value::String(name.into()))
                                .unwrap_or(Value::Nil)
                        }))
                    }
                    match item {
                        "codeset" => {
                            return Ok(langinfo_string(libc::CODESET)
                                .filter(|name| !name.is_empty())
                                .map(|name| Value::String(name.into()))
                                .unwrap_or(Value::Nil));
                        }
                        "days" => {
                            return Ok(langinfo_vector(&[
                                libc::DAY_1,
                                libc::DAY_2,
                                libc::DAY_3,
                                libc::DAY_4,
                                libc::DAY_5,
                                libc::DAY_6,
                                libc::DAY_7,
                            ]));
                        }
                        "months" => {
                            return Ok(langinfo_vector(&[
                                libc::MON_1,
                                libc::MON_2,
                                libc::MON_3,
                                libc::MON_4,
                                libc::MON_5,
                                libc::MON_6,
                                libc::MON_7,
                                libc::MON_8,
                                libc::MON_9,
                                libc::MON_10,
                                libc::MON_11,
                                libc::MON_12,
                            ]));
                        }
                        #[cfg(target_os = "linux")]
                        "paper" => {
                            // glibc returns the millimeter value IN the
                            // pointer, exactly as fns.c casts it.  The
                            // nl_item codes are _NL_ITEM(LC_PAPER = 7,
                            // index): height 0, width 1.  Darwin has no
                            // LC_PAPER; GNU there answers nil, and so does
                            // the cfg fall-through below.
                            let width = unsafe { libc::nl_langinfo((7 << 16) | 1) } as isize;
                            let height = unsafe { libc::nl_langinfo(7 << 16) } as isize;
                            return Ok(Value::list([
                                Value::Integer(width as i64),
                                Value::Integer(height as i64),
                            ]));
                        }
                        _ => {}
                    }
                }
                Ok(Value::Nil)
            }
            "clear-string" => {
                need_args(name, args, 1)?;
                match args[0].kind() {
                    Kind::StringObject(state) => {
                        let needs_clear = {
                            let contents = state.borrow();
                            contents.storage_bytes() != 0 || contents.is_multibyte()
                        };
                        if needs_clear {
                            state.check_impure()?;
                            state.borrow_mut().clear();
                        }
                        Ok(Value::Nil)
                    }
                    other => Err(LispError::WrongTypeArgument(
                        "stringp".into(),
                        other.value(),
                    )),
                }
            }

            "propertize" => {
                if args.is_empty() || args.len().is_multiple_of(2) {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                let len = match args[0].kind() {
                    Kind::StringObject(state) => state.borrow().len(),
                    _ => return Err(LispError::WrongTypeArgument("stringp".into(), args[0])),
                };
                // editfns.c:Fpropertize starts with Fcopy_sequence, sharing
                // property values while copying the string's real bytes.
                let value = shared_string_copy(&args[0])?;
                let props = args[1..]
                    .chunks(2)
                    .map(|pair| (crate::lisp::types::PropertyName::new(pair[0]), pair[1]))
                    .collect::<Vec<_>>();
                modify_shared_string_properties(&value, 0, len, |mut current| {
                    for (name, value) in &props {
                        if let Some((_, existing)) = current.iter_mut().find(|(key, _)| key == name)
                        {
                            *existing = *value;
                        } else {
                            current.push((*name, *value));
                        }
                    }
                    current
                })?;
                Ok(value)
            }

            "make-char-table" => {
                need_args(name, args, 1)?;
                interp.make_char_table_for_purpose(
                    args[0],
                    args.get(1).copied().unwrap_or(Value::Nil),
                )
            }

            "char-table-p" => {
                need_args(name, args, 1)?;
                Ok(if matches!(args[0].kind(), Kind::CharTable(_)) {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "case-table-p" => {
                need_args(name, args, 1)?;
                let valid = matches!(
                    args[0].kind(),
                    Kind::CharTable(id) if crate::lisp::primitives::casetab::case_table_p(id)
                );
                Ok(if valid { Value::T } else { Value::Nil })
            }
            "syntax-table-p" => {
                need_args(name, args, 1)?;
                let valid = matches!(
                    args[0].kind(),
                    Kind::CharTable(id) if id.has_purpose("syntax-table")
                );
                Ok(if valid { Value::T } else { Value::Nil })
            }

            "char-table-subtype" => {
                need_args(name, args, 1)?;
                let Kind::CharTable(id) = args[0].kind() else {
                    return Err(LispError::WrongTypeArgument("char-table-p".into(), args[0]));
                };
                Ok(id.purpose())
            }

            "char-table-parent" => {
                need_args(name, args, 1)?;
                let Kind::CharTable(id) = args[0].kind() else {
                    return Err(LispError::WrongTypeArgument("char-table-p".into(), args[0]));
                };
                Ok(id.parent().map_or(Value::Nil, Value::CharTable))
            }

            "set-char-table-parent" => {
                need_args(name, args, 2)?;
                let Kind::CharTable(id) = args[0].kind() else {
                    return Err(LispError::WrongTypeArgument("char-table-p".into(), args[0]));
                };
                let parent = match args[1].kind() {
                    Kind::Nil => None,
                    Kind::CharTable(parent_id) => Some(parent_id),
                    other => {
                        return Err(LispError::WrongTypeArgument(
                            "char-table-p".into(),
                            other.value(),
                        ));
                    }
                };
                interp.set_char_table_parent(id, parent)?;
                Ok(args[1])
            }

            "char-table-extra-slot" => {
                need_args(name, args, 2)?;
                let Kind::CharTable(id) = args[0].kind() else {
                    return Err(LispError::WrongTypeArgument("char-table-p".into(), args[0]));
                };
                let slot = fixnum_index_arg(&args[1])?;
                usize::try_from(slot)
                    .ok()
                    .and_then(|slot| id.extra(slot))
                    .ok_or_else(|| args_out_of_range(&args[0], &args[1]))
            }

            "set-char-table-extra-slot" => {
                need_args(name, args, 3)?;
                let Kind::CharTable(id) = args[0].kind() else {
                    return Err(LispError::WrongTypeArgument("char-table-p".into(), args[0]));
                };
                let slot = fixnum_index_arg(&args[1])?;
                if !usize::try_from(slot).is_ok_and(|slot| id.set_extra(slot, args[2])) {
                    return Err(args_out_of_range(&args[0], &args[1]));
                }
                Ok(args[2])
            }

            "char-table-range" => {
                need_args(name, args, 2)?;
                let Kind::CharTable(id) = args[0].kind() else {
                    return Err(LispError::WrongTypeArgument("char-table-p".into(), args[0]));
                };
                match char_table_range_spec(&args[1], name)? {
                    None => Ok(id.default()),
                    Some((start, end)) if matches!(args[1].kind(), Kind::Cons(_)) => {
                        Ok(id.range_value(start, end))
                    }
                    Some((start, _)) => Ok(id.get(start)),
                }
            }

            "set-char-table-range" => {
                need_args(name, args, 3)?;
                let Kind::CharTable(id) = args[0].kind() else {
                    return Err(LispError::WrongTypeArgument("char-table-p".into(), args[0]));
                };
                if args[1].eq_value(Value::T) {
                    id.set_all(args[2]);
                } else {
                    let range = char_table_range_spec(&args[1], name)?;
                    match range {
                        None => id.set_default(args[2]),
                        Some((start, end)) => id.set_range(start, end, args[2]),
                    }
                }
                Ok(args[2])
            }
            "optimize-char-table" => {
                need_arg_range(name, args, 1, 2)?;
                let Kind::CharTable(table) = args[0].kind() else {
                    return Err(LispError::WrongTypeArgument("char-table-p".into(), args[0]));
                };
                let test = args.get(1).copied().unwrap_or(Value::Nil);
                optimize_char_table(interp, table, test, env)?;
                Ok(Value::Nil)
            }

            "map-char-table" => {
                need_args(name, args, 2)?;
                let Kind::CharTable(id) = args[1].kind() else {
                    return Err(LispError::WrongTypeArgument("char-table-p".into(), args[1]));
                };
                map_char_table(interp, args[0], id, env)
            }

            "current-case-table" => Ok(Value::CharTable(interp.current_case_table_id())),

            "standard-case-table" => Ok(Value::CharTable(interp.standard_case_table_id())),

            "set-case-table" => {
                need_args(name, args, 1)?;
                crate::lisp::primitives::casetab::set_case_table(interp, args[0], false)
            }

            "set-standard-case-table" => {
                need_args(name, args, 1)?;
                crate::lisp::primitives::casetab::set_case_table(interp, args[0], true)
            }

            "copy-syntax-table" => {
                if args.len() > 1 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                let source = match args.first().map(|v| v.kind()) {
                    Some(Kind::CharTable(id)) => id,
                    Some(Kind::Nil) | None => interp.standard_syntax_table_id(),
                    Some(other) => {
                        return Err(LispError::WrongTypeArgument(
                            "char-table-p".into(),
                            other.value(),
                        ));
                    }
                };
                if !source.has_purpose("syntax-table") {
                    return Err(LispError::TypeError(
                        "syntax-table".into(),
                        "char-table".into(),
                    ));
                }
                interp.copy_syntax_table(source)
            }

            "syntax-table" => {
                need_args(name, args, 0)?;
                Ok(Value::CharTable(interp.current_syntax_table_id()))
            }

            "standard-syntax-table" => Ok(Value::CharTable(interp.standard_syntax_table_id())),

            "set-syntax-table" => {
                need_args(name, args, 1)?;
                let Kind::CharTable(id) = args[0].kind() else {
                    return Err(LispError::WrongTypeArgument("char-table-p".into(), args[0]));
                };
                interp.set_current_syntax_table(id);
                Ok(args[0])
            }

            "modify-syntax-entry" => {
                if args.len() < 2 || args.len() > 3 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                let (start, end) = if matches!(args[0].kind(), Kind::Cons(_)) {
                    category_character_range(&args[0])?
                } else {
                    let code = table_character(&args[0])?;
                    (code, code)
                };
                let syntax = string_text(&args[1])?;
                let entry = syntax::parse_syntax_spec(&syntax).ok_or_else(|| {
                    let letter = syntax.chars().next().unwrap_or('\0');
                    LispError::Signal(format!("Invalid syntax description letter: {letter}"))
                })?;
                let table_id = match args.get(2).map(|v| v.kind()) {
                    Some(Kind::CharTable(id)) => id,
                    // syntax.c:Fmodify_syntax_entry uses the current table
                    // for nil, including native calls' padded optional slot.
                    Some(Kind::Nil) | None => interp.current_syntax_table_id(),
                    Some(other) => {
                        return Err(LispError::WrongTypeArgument(
                            "char-table-p".into(),
                            other.value(),
                        ));
                    }
                };
                interp.char_table_set_range(
                    table_id,
                    start,
                    end,
                    syntax::syntax_entry_value(interp, entry),
                )?;
                // "We clear the regexp cache, since character classes can
                // now have different values from those in the compiled
                // regexps."
                crate::lisp::primitives::search::clear_regexp_cache();
                Ok(Value::Nil)
            }

            "setcar" => direct_setcar(interp, args, env),
            "setcdr" => direct_setcdr(interp, args, env),
            "make-category-table" => {
                let table = CharTableRef::new(Value::symbol("category-table"), Value::Nil, 2);
                table.set_default(make_bool_vector_value([false; 128]));
                for index in 4..68 {
                    table.set_slot(index, make_bool_vector_value([false; 128]));
                }
                table.set_extra(0, Value::vector(std::iter::repeat_n(Value::Nil, 95)));
                Ok(Value::CharTable(table))
            }

            "category-table-p" => {
                need_args(name, args, 1)?;
                Ok(match args[0].kind() {
                    Kind::CharTable(id) if id.has_purpose("category-table") => Value::T,
                    _ => Value::Nil,
                })
            }

            "standard-category-table" => {
                Ok(Value::CharTable(interp.ensure_standard_category_table()))
            }

            "category-table" => Ok(Value::CharTable(current_category_table_id(interp))),

            "set-category-table" => {
                need_args(name, args, 1)?;
                let table = Value::CharTable(category_table_arg(interp, args.first(), false)?);
                interp.set_buffer_local_value(interp.current_buffer_id(), "category-table", table);
                Ok(table)
            }

            "define-category" => {
                need_arg_range(name, args, 2, 3)?;
                let category = category_arg(&args[0])?;
                string_text(&args[1])?;
                let table = category_table_arg(interp, args.get(2), false)?;
                interp.define_category(table, category, args[1])?;
                Ok(Value::Nil)
            }

            "category-docstring" => {
                need_arg_range(name, args, 1, 2)?;
                let category = category_arg(&args[0])?;
                let table = category_table_arg(interp, args.get(1), false)?;
                Ok(interp.category_docstring(table, category))
            }

            "get-unused-category" => {
                need_arg_range(name, args, 0, 1)?;
                let table = category_table_arg(interp, args.first(), false)?;
                Ok((b' '..=b'~')
                    .find(|category| {
                        interp
                            .category_docstring(table, u32::from(*category))
                            .is_nil()
                    })
                    .map(|category| Value::Integer(i64::from(category)))
                    .unwrap_or(Value::Nil))
            }

            "make-category-set" => {
                need_args(name, args, 1)?;
                let string = string_like(&args[0])
                    .ok_or_else(|| LispError::WrongTypeArgument("stringp".into(), args[0]))?;
                if string.multibyte {
                    return Err(LispError::Signal(
                        "Multibyte string in `make-category-set'".into(),
                    ));
                }
                let mut bits = [false; 128];
                for ch in string.text.chars() {
                    bits[category_arg(&Value::Integer(ch as i64))? as usize] = true;
                }
                Ok(make_bool_vector_value(bits))
            }

            "category-set-mnemonics" => {
                need_args(name, args, 1)?;
                let bits = category_set_bits(&args[0])?;
                let text: String = (32u8..127)
                    .filter(|i| bits[*i as usize])
                    .map(char::from)
                    .collect();
                Ok(Value::String(text.into()))
            }

            "modify-category-entry" => {
                need_arg_range(name, args, 2, 4)?;
                let (start, end) = category_character_range(&args[0])?;
                let category = category_arg(&args[1])?;
                let table = category_table_arg(interp, args.get(2), false)?;
                if interp.category_docstring(table, category).is_nil() {
                    return Err(LispError::Signal(format!(
                        "Undefined category: {}",
                        char::from(category as u8)
                    )));
                }
                let set = !args.get(3).is_some_and(Value::is_truthy);
                if start > end {
                    return Ok(Value::Nil);
                }
                let boundaries = char_table_change_boundaries(table, start, end);
                for (index, segment_start) in boundaries.iter().copied().enumerate() {
                    let segment_end = boundaries.get(index + 1).map_or(end, |next| next - 1);
                    let mut bits = category_set_bits(&table.get(segment_start))?;
                    if bits[category as usize] == set {
                        continue;
                    }
                    bits[category as usize] = set;
                    let mut value = make_bool_vector_value(bits);
                    let mut hash = table.extra(1).unwrap_or(Value::Nil);
                    if hash.is_nil() {
                        hash = json::make_hash_table(interp, "equal", Vec::new());
                        table.set_extra(1, hash);
                    }
                    let Kind::HashTable(id) = hash.kind() else {
                        return Err(LispError::WrongTypeArgument("hash-table-p".into(), hash));
                    };
                    if let Some(key) = interp.equal_hash_lookup_key(id, &value, env)? {
                        value = key;
                    } else {
                        interp.equal_hash_put(id, value, Value::Nil, env)?;
                    }
                    table.set_range(segment_start, segment_end, value);
                }
                Ok(Value::Nil)
            }

            "char-category-set" => {
                need_args(name, args, 1)?;
                let character = table_character(&args[0])?;
                Ok(current_category_table_id(interp).get(character))
            }

            "copy-category-table" => {
                need_arg_range(name, args, 0, 1)?;
                let copy = category_table_arg(interp, args.first(), true)?.copy();
                copy.set_default(copy_sequence_value(interp, &copy.default())?);
                copy.set_extra(
                    0,
                    copy_sequence_value(interp, &copy.extra(0).unwrap_or(Value::Nil))?,
                );
                for entry in copy.effective_ranges() {
                    if !entry.value.is_nil() {
                        copy.set_range(
                            entry.start,
                            entry.end,
                            copy_sequence_value(interp, &entry.value)?,
                        );
                    }
                }
                Ok(Value::CharTable(copy))
            }

            "translate-region-internal" => {
                need_args(name, args, 3)?;
                let (from, to) = validate_region(interp, &args[0], &args[1])?;
                // editfns.c: a char-table whose purpose is
                // `translation-table', or a string.
                let table = match args[2].kind() {
                    Kind::CharTable(id) if id.has_purpose("translation-table") => {
                        TranslationTable::Chars(id)
                    }
                    Kind::CharTable(_) => {
                        return Err(LispError::Signal("Not a translation table".into()));
                    }
                    Kind::StringObject(_) => TranslationTable::String(args[2]),
                    _ => return Err(LispError::WrongTypeArgument("stringp".into(), args[2])),
                };
                translate_region_internal(interp, from, to, table, env)
            }

            "undo-boundary" => interp.undo_boundary(env),

            "take" | "ntake" => {
                need_args(name, args, 2)?;
                let n = args[0].as_integer()?;
                if n <= 0 {
                    return Ok(Value::Nil);
                }
                let n = n as usize;
                let list = args[1];
                if name == "take" {
                    let mut current = list;
                    let mut items = Vec::new();
                    let mut remaining = n;
                    while remaining > 0 {
                        match current.kind() {
                            Kind::Nil => break,
                            Kind::Cons(cons_cell) => {
                                let car = &cons_cell.car;
                                let cdr = &cons_cell.cdr;
                                items.push(car.get());
                                current = cdr.get();
                                remaining -= 1;
                            }
                            value => {
                                return Err(LispError::WrongTypeArgument(
                                    "listp".into(),
                                    value.value(),
                                ));
                            }
                        }
                    }
                    Ok(Value::list(items))
                } else {
                    let head = list;
                    let mut current = head;
                    let mut remaining = n;
                    while remaining > 1 {
                        match current.kind() {
                            Kind::Nil => return Ok(Value::Nil),
                            Kind::Cons(cons_cell) => {
                                let _ = &cons_cell.car;
                                let cdr = &cons_cell.cdr;
                                let next = cdr.get();
                                match next.kind() {
                                    Kind::Cons(_) => {
                                        current = next;
                                        remaining -= 1;
                                    }
                                    Kind::Nil => return Ok(head),
                                    value => {
                                        return Err(LispError::WrongTypeArgument(
                                            "listp".into(),
                                            value.value(),
                                        ));
                                    }
                                }
                            }
                            value => {
                                return Err(LispError::WrongTypeArgument(
                                    "listp".into(),
                                    value.value(),
                                ));
                            }
                        }
                    }
                    match current.kind() {
                        Kind::Nil => Ok(Value::Nil),
                        Kind::Cons(cons_cell) => {
                            let _ = &cons_cell.car;
                            let cdr = &cons_cell.cdr;
                            cdr.set(Value::Nil);
                            Ok(head)
                        }
                        value => Err(LispError::WrongTypeArgument("listp".into(), value.value())),
                    }
                }
            }

            "delq" => {
                need_args(name, args, 2)?;
                delete_from_list(interp, &args[0], &args[1], env, DeleteListComparison::Eq)
            }

            "delete" => {
                need_args(name, args, 2)?;
                let elt = &args[0];
                if matches!(args[1].kind(), Kind::StringObject(_)) || is_vector_value(&args[1]) {
                    return remove_equal(interp, elt, &args[1]);
                }

                if !args[1].is_nil() && !args[1].is_cons() {
                    return Err(wrong_type_argument("sequencep", args[1]));
                }
                // GNU Fdelete reuses every retained cons cell.  This is
                // observable through eq and is what subr.el's destructive
                // delete-dups relies on; rebuilding a filtered list silently
                // changes both APIs' contracts.
                delete_from_list(interp, elt, &args[1], env, DeleteListComparison::Equal)
            }

            "make-list" => {
                need_args(name, args, 2)?;
                let n = args[0].as_integer()?;
                let val = args[1];
                let items: Vec<Value> = (0..n).map(|_| val).collect();
                Ok(Value::list(items))
            }
        }
    }
);

/// The `setcar' primitive, callable directly (a subr's function pointer).
pub(super) fn direct_setcar(
    _interp: &mut Interpreter,
    args: &[Value],
    _env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    need_args("setcar", args, 2)?;
    args[0].set_car(args[1])?;
    Ok(args[1])
}

/// The `setcdr' primitive, callable directly (a subr's function pointer).
pub(super) fn direct_setcdr(
    _interp: &mut Interpreter,
    args: &[Value],
    _env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    need_args("setcdr", args, 2)?;
    args[0].set_cdr(args[1])?;
    Ok(args[1])
}
