//! casetab.c: installing case tables.  `set-case-table' and
//! `set-standard-case-table' complete a table's missing companions (the
//! upcase table, the canonicalization, the equivalences) from its
//! downcase mapping and store all four in the buffer's slots or the
//! standard ones; buffer.c's reset_buffer_local_variables gives a buffer
//! the standard four, completing an invalidated standard table first.

use super::*;
use crate::lisp::eval::coding::CaseTables;
use crate::lisp::types::{CharTableRef, Kind};

/// A character a table maps to, as FIXNATP sees it.
fn natnum(value: Value) -> Option<u32> {
    match value.kind() {
        Kind::Integer(n) => u32::try_from(n).ok(),
        _ => None,
    }
}

/// The FROM..=TO a map_char_table key covers.
fn key_range(key: Value) -> Result<(u32, u32), LispError> {
    match key.kind() {
        Kind::Cons(_) => Ok((
            key.car()?.as_integer()? as u32,
            key.cdr()?.as_integer()? as u32,
        )),
        _ => {
            let c = key.as_integer()? as u32;
            Ok((c, c))
        }
    }
}

/// Faref of a char-table: CHECK_CHARACTER of the index, then the entry.
fn aref(table: CharTableRef, index: Value) -> Result<Value, LispError> {
    match index.kind() {
        Kind::Integer(c) if (0..=0x3fffff).contains(&c) => Ok(table.get(c as u32)),
        _ => Err(LispError::WrongTypeArgument("characterp".into(), index)),
    }
}

fn char_table(value: Option<Value>) -> Option<CharTableRef> {
    match value?.kind() {
        Kind::CharTable(table) => Some(table),
        _ => None,
    }
}

/// Fcase_table_p.
pub(crate) fn case_table_p(table: CharTableRef) -> bool {
    if !table.has_purpose("case-table") {
        return false;
    }
    let slot = |n| table.extra(n).unwrap_or(Value::Nil);
    let (up, canon, eqv) = (slot(0), slot(1), slot(2));
    matches!(up.kind(), Kind::Nil | Kind::CharTable(_))
        && ((canon.is_nil() && eqv.is_nil())
            || (matches!(canon.kind(), Kind::CharTable(_))
                && matches!(eqv.kind(), Kind::Nil | Kind::CharTable(_))))
}

fn make_case_table(interp: &mut Interpreter) -> CharTableRef {
    let Kind::CharTable(table) = interp
        .make_char_table(Some("case-table".into()), Value::Nil)
        .kind()
    else {
        unreachable!("make_char_table returns a char-table");
    };
    table
}

/// map_char_table over TABLE with C callback F (KEY, VALUE).
fn map_runs(
    interp: &mut Interpreter,
    table: CharTableRef,
    f: &mut dyn FnMut(&mut Interpreter, Value, Value) -> Result<(), LispError>,
) -> Result<(), LispError> {
    let mut env = Env::new();
    dispatch::map_char_table_with(
        interp,
        &mut |interp, key, value, _, _, _| f(interp, key, value),
        table,
        &mut env,
    )?;
    Ok(())
}

/// set_identity: each character TABLE maps to a character maps to itself
/// in TARGET.
fn set_identity(
    interp: &mut Interpreter,
    table: CharTableRef,
    target: CharTableRef,
) -> Result<(), LispError> {
    map_runs(interp, table, &mut |_, key, value| {
        if natnum(value).is_some() {
            let (from, to) = key_range(key)?;
            for c in from..=to {
                target.set(c, Value::Integer(i64::from(c)));
            }
        }
        Ok(())
    })
}

/// shuffle: TARGET's entries for each character C TABLE maps to ELT and
/// for ELT are swapped, so that TARGET permutes each class cyclically.
fn shuffle(
    interp: &mut Interpreter,
    table: CharTableRef,
    target: CharTableRef,
) -> Result<(), LispError> {
    map_runs(interp, table, &mut |_, key, value| {
        let Some(elt) = natnum(value) else {
            return Ok(());
        };
        let (from, to) = key_range(key)?;
        for c in from..=to {
            let tem = aref(target, value)?;
            target.set(elt, Value::Integer(i64::from(c)));
            target.set(c, tem);
        }
        Ok(())
    })
}

/// casetab.c:set_case_table.
pub(crate) fn set_case_table(
    interp: &mut Interpreter,
    table: Value,
    standard: bool,
) -> Result<Value, LispError> {
    let down = match table.kind() {
        Kind::CharTable(down) if case_table_p(down) => down,
        _ => return Err(LispError::WrongTypeArgument("case-table-p".into(), table)),
    };
    let up = match char_table(down.extra(0)) {
        Some(up) => up,
        None => {
            let up = make_case_table(interp);
            interp.with_lisp_stack_roots(&Value::CharTable(up), |interp| {
                set_identity(interp, down, up)?;
                shuffle(interp, down, up)
            })?;
            down.set_extra(0, Value::CharTable(up));
            up
        }
    };
    let canon = match char_table(down.extra(1)) {
        Some(canon) => canon,
        None => {
            let canon = make_case_table(interp);
            down.set_extra(1, Value::CharTable(canon));
            // set_canon: each character's canonical equivalent is
            // DOWN[UP[DOWN[C]]].
            map_runs(interp, down, &mut |_, key, value| {
                if natnum(value).is_some() {
                    let mapped = aref(down, aref(up, value)?)?;
                    let (from, to) = key_range(key)?;
                    canon.set_range(from, to, mapped);
                }
                Ok(())
            })?;
            canon
        }
    };
    let eqv = match char_table(down.extra(2)) {
        Some(eqv) => eqv,
        None => {
            let eqv = make_case_table(interp);
            interp.with_lisp_stack_roots(&Value::CharTable(eqv), |interp| {
                set_identity(interp, canon, eqv)?;
                shuffle(interp, canon, eqv)
            })?;
            down.set_extra(2, Value::CharTable(eqv));
            eqv
        }
    };
    // "This is so set_image_of_range_1 in regex-emacs.c can find the EQV
    // table."
    canon.set_extra(2, Value::CharTable(eqv));
    let tables = CaseTables {
        down,
        up,
        canon,
        eqv,
    };
    if standard {
        interp.set_standard_case_tables(tables);
    } else {
        interp.set_current_case_tables(tables);
    }
    Ok(table)
}

/// buffer.c:reset_buffer_local_variables's case tables: the standard
/// table's own four, after completing it if it has been invalidated.
pub(crate) fn reset_buffer_case_tables(interp: &mut Interpreter, buffer_id: u64) {
    let down = interp.ensure_standard_case_table();
    let valid = |down: CharTableRef| (0..3).all(|n| char_table(down.extra(n)).is_some());
    if !valid(down) {
        // A standard table is a case table (set-standard-case-table
        // checked it), so this completes it.
        let _ = set_case_table(interp, Value::CharTable(down), true);
    }
    let down = interp.ensure_standard_case_table();
    let (Some(up), Some(canon), Some(eqv)) = (
        char_table(down.extra(0)),
        char_table(down.extra(1)),
        char_table(down.extra(2)),
    ) else {
        return;
    };
    interp.set_buffer_case_tables(
        buffer_id,
        CaseTables {
            down,
            up,
            canon,
            eqv,
        },
    );
}

/// casefiddle.c:prepare_casing_context: a downcase table whose
/// canonicalization was cleared is installed again first.
pub(crate) fn prepare_case_tables(interp: &mut Interpreter) -> Result<(), LispError> {
    let down = interp.current_case_table_id();
    if down.extra(1).is_none_or(|canon| canon.is_nil()) {
        set_case_table(interp, Value::CharTable(down), false)?;
    }
    Ok(())
}
