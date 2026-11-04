//! ccl.c: the code conversion language.  `ccl_driver' runs a compiled
//! program over a source and a destination of ints, suspending where its
//! source or destination runs out; Vccl_program_table holds the
//! registered programs; the primitives below and the coding systems of
//! type `ccl' set programs up from it.

use super::*;
use crate::lisp::types::Kind;
use std::cell::{Cell, RefCell};
use std::rc::Rc;

// The header of compiled code.
const CCL_HEADER_BUF_MAG: usize = 0;
const CCL_HEADER_EOF: usize = 1;
const CCL_HEADER_MAIN: i32 = 2;

const CCL_CODE_MAX: i32 = (1 << (28 - 1)) - 1;
const CCL_CODE_MIN: i32 = -1 - CCL_CODE_MAX;

// CCL commands.
const CCL_SET_REGISTER: i32 = 0x00;
const CCL_SET_SHORT_CONST: i32 = 0x01;
const CCL_SET_CONST: i32 = 0x02;
const CCL_SET_ARRAY: i32 = 0x03;
const CCL_JUMP: i32 = 0x04;
const CCL_JUMP_COND: i32 = 0x05;
const CCL_WRITE_REGISTER_JUMP: i32 = 0x06;
const CCL_WRITE_REGISTER_READ_JUMP: i32 = 0x07;
const CCL_WRITE_CONST_JUMP: i32 = 0x08;
const CCL_WRITE_CONST_READ_JUMP: i32 = 0x09;
const CCL_WRITE_STRING_JUMP: i32 = 0x0A;
const CCL_WRITE_ARRAY_READ_JUMP: i32 = 0x0B;
const CCL_READ_JUMP: i32 = 0x0C;
const CCL_BRANCH: i32 = 0x0D;
const CCL_READ_REGISTER: i32 = 0x0E;
const CCL_WRITE_EXPR_CONST: i32 = 0x0F;
const CCL_READ_BRANCH: i32 = 0x10;
const CCL_WRITE_REGISTER: i32 = 0x11;
const CCL_WRITE_EXPR_REGISTER: i32 = 0x12;
const CCL_CALL: i32 = 0x13;
const CCL_WRITE_CONST_STRING: i32 = 0x14;
const CCL_WRITE_ARRAY: i32 = 0x15;
const CCL_END: i32 = 0x16;
const CCL_EXPR_SELF_CONST: i32 = 0x17;
const CCL_EXPR_SELF_REG: i32 = 0x18;
const CCL_SET_EXPR_CONST: i32 = 0x19;
const CCL_SET_EXPR_REG: i32 = 0x1A;
const CCL_JUMP_COND_EXPR_CONST: i32 = 0x1B;
const CCL_JUMP_COND_EXPR_REG: i32 = 0x1C;
const CCL_READ_JUMP_COND_EXPR_CONST: i32 = 0x1D;
const CCL_READ_JUMP_COND_EXPR_REG: i32 = 0x1E;
const CCL_EXTENSION: i32 = 0x1F;

// Extended commands.
const CCL_READ_MULTIBYTE_CHAR2: i32 = 0x00;
const CCL_WRITE_MULTIBYTE_CHAR2: i32 = 0x01;
const CCL_TRANSLATE_CHARACTER: i32 = 0x02;
const CCL_TRANSLATE_CHARACTER_CONST_TBL: i32 = 0x03;
const CCL_ITERATE_MULTIPLE_MAP: i32 = 0x10;
const CCL_MAP_MULTIPLE: i32 = 0x11;
const CCL_MAP_SINGLE: i32 = 0x12;
const CCL_LOOKUP_INT_CONST_TBL: i32 = 0x13;
const CCL_LOOKUP_CHAR_CONST_TBL: i32 = 0x14;

// Arithmetic and logical operators.
const CCL_PLUS: i32 = 0x00;
const CCL_MINUS: i32 = 0x01;
const CCL_MUL: i32 = 0x02;
const CCL_DIV: i32 = 0x03;
const CCL_MOD: i32 = 0x04;
const CCL_AND: i32 = 0x05;
const CCL_OR: i32 = 0x06;
const CCL_XOR: i32 = 0x07;
const CCL_LSH: i32 = 0x08;
const CCL_RSH: i32 = 0x09;
const CCL_LSH8: i32 = 0x0A;
const CCL_RSH8: i32 = 0x0B;
const CCL_DIVMOD: i32 = 0x0C;
const CCL_LS: i32 = 0x10;
const CCL_GT: i32 = 0x11;
const CCL_EQ: i32 = 0x12;
const CCL_LE: i32 = 0x13;
const CCL_GE: i32 = 0x14;
const CCL_NE: i32 = 0x15;
const CCL_DECODE_SJIS: i32 = 0x16;
const CCL_ENCODE_SJIS: i32 = 0x17;

// ccl.h: how a program's run ended.
pub(crate) const CCL_STAT_SUCCESS: i32 = 0;
pub(crate) const CCL_STAT_SUSPEND_BY_SRC: i32 = 1;
pub(crate) const CCL_STAT_SUSPEND_BY_DST: i32 = 2;
pub(crate) const CCL_STAT_INVALID_CMD: i32 = 3;
pub(crate) const CCL_STAT_QUIT: i32 = 4;

const MAX_MAP_SET_LEVEL: usize = 30;
/// The depth of ccl_prog_stack_struct.
const CCL_PROG_STACK_DEPTH: i32 = 256;
/// Fccl_execute_on_string's CCL_EXECUTE_BUF_SIZE, and the size of
/// decode_coding_ccl's source and encode_coding_ccl's destination buffers.
pub(crate) const CCL_BLOCK: usize = 1024;

/// ccl.h struct ccl_program: a program set up to run, and where its last
/// run left it.  PROG is the compiled code of the program running (a
/// program CCL_Call reached, while that one runs).
#[derive(Clone)]
pub(crate) struct CclProgram {
    idx: i64,
    size: usize,
    prog: Rc<[i32]>,
    pub(crate) ic: i32,
    eof_ic: i32,
    pub(crate) reg: [i32; 8],
    pub(crate) status: i32,
    buf_magnification: i32,
    stack_idx: i32,
    pub(crate) consumed: usize,
    pub(crate) produced: usize,
    pub(crate) last_block: bool,
    pub(crate) quit_silently: bool,
}

impl Default for CclProgram {
    fn default() -> Self {
        CclProgram {
            idx: -1,
            size: 0,
            prog: Rc::from(Vec::new()),
            ic: CCL_HEADER_MAIN,
            eof_ic: 0,
            reg: [0; 8],
            status: CCL_STAT_SUCCESS,
            buf_magnification: 0,
            stack_idx: 0,
            consumed: 0,
            produced: 0,
            last_block: false,
            quit_silently: false,
        }
    }
}

/// A caller CCL_Call left: struct ccl_prog_stack.
#[derive(Clone)]
struct CclFrame {
    prog: Rc<[i32]>,
    ic: i32,
    eof_ic: i32,
}

/// ccl.c's statics, shared by every program as GNU shares them:
/// ccl_prog_stack_struct, where CCL_Call keeps the callers of the program
/// running (a program suspended inside a callee finds its callers there
/// when it resumes, whatever ran in between), and
/// stack_idx_of_map_multiple.  They are the running process's: a copy or
/// a load starts them afresh.
#[derive(Default)]
pub(crate) struct CclStatics {
    prog_stack: RefCell<Vec<CclFrame>>,
    map_multiple_depth: Cell<i32>,
}

impl Clone for CclStatics {
    fn clone(&self) -> Self {
        CclStatics::default()
    }
}

impl CclStatics {
    fn frame(&self, index: i32) -> Option<CclFrame> {
        usize::try_from(index)
            .ok()
            .and_then(|index| self.prog_stack.borrow().get(index).cloned())
    }

    fn set_frame(&self, index: i32, frame: CclFrame) {
        let index = index as usize;
        let mut stack = self.prog_stack.borrow_mut();
        if stack.len() <= index {
            stack.resize(index + 1, frame.clone());
        }
        stack[index] = frame;
    }
}

/// Vccl_program_table as syms_of_ccl makes it.
pub(crate) fn initial_program_table() -> Value {
    Value::vector(std::iter::repeat_n(Value::Nil, 32))
}

/// What ccl_driver asks of the editor besides its interpreter: fns.c's
/// hash_lookup of an integer key, which runs a table's test functions
/// where the caller can.
pub(crate) trait CclHost {
    fn interp(&self) -> &Interpreter;
    /// The value of KEY's entry in TABLE.
    fn hash_lookup(&mut self, table: Value, key: i64) -> Result<Option<Value>, LispError>;
}

/// The primitives' host, which looks a key up as hash_lookup does.
struct LispHost<'a> {
    interp: &'a mut Interpreter,
    env: &'a mut Env,
}

impl CclHost for LispHost<'_> {
    fn interp(&self) -> &Interpreter {
        self.interp
    }

    fn hash_lookup(&mut self, table: Value, key: i64) -> Result<Option<Value>, LispError> {
        let Kind::HashTable(table) = table.kind() else {
            return Ok(None);
        };
        let (_, slot) = hash_table_lookup(self.interp, table, &Value::Integer(key), self.env)?;
        Ok(slot
            .and_then(|slot| table.entry(slot))
            .map(|(_, value)| value))
    }
}

/// The coding systems' host: their decoders and encoders run with the
/// interpreter shared, so a key is found by the identity every standard
/// test gives an integer.  A table whose test `define-hash-table-test'
/// defined is searched the same way, where GNU calls its functions.
pub(crate) struct CodingHost<'a> {
    pub(crate) interp: &'a Interpreter,
}

impl CclHost for CodingHost<'_> {
    fn interp(&self) -> &Interpreter {
        self.interp
    }

    fn hash_lookup(&mut self, table: Value, key: i64) -> Result<Option<Value>, LispError> {
        let Kind::HashTable(table) = table.kind() else {
            return Ok(None);
        };
        Ok(table
            .entries()
            .find(|(_, stored, _)| matches!(stored.kind(), Kind::Integer(n) if n == key))
            .map(|(_, _, value)| value))
    }
}

define_dispatch!(
    pub(super) fn call(
        interp: &mut Interpreter,
        name: &str,
        args: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        match name {
            "ccl-program-p" => {
                need_args(name, args, 1)?;
                Ok(if ccl_program_p(interp, &args[0]) {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "register-ccl-program" => {
                need_args(name, args, 2)?;
                register_ccl_program(interp, &args[0], &args[1])
            }
            "register-code-conversion-map" => {
                need_args(name, args, 2)?;
                register_code_conversion_map(interp, &args[0], &args[1])
            }
            "ccl-execute" => {
                need_args(name, args, 2)?;
                ccl_execute(interp, &args[0], &args[1], env)
            }
            "ccl-execute-on-string" => {
                need_arg_range(name, args, 3, 5)?;
                ccl_execute_on_string(
                    interp,
                    &args[0],
                    &args[1],
                    &args[2],
                    args.get(3).is_some_and(Value::is_truthy),
                    args.get(4).is_some_and(Value::is_truthy),
                    env,
                )
            }
        }
    }
);

fn invalid_program() -> LispError {
    LispError::Signal("Invalid CCL program".into())
}

/// A vector's slots.
fn slots(value: &Value) -> Option<Vec<Value>> {
    match value.kind() {
        Kind::Vector(vector) => Some(vector.slots().collect()),
        _ => None,
    }
}

fn slot(value: &Value, index: i64) -> Option<Value> {
    match value.kind() {
        Kind::Vector(vector) => usize::try_from(index)
            .ok()
            .and_then(|index| vector.get(index)),
        _ => None,
    }
}

fn vector_size(value: &Value) -> Option<usize> {
    match value.kind() {
        Kind::Vector(vector) => Some(vector.len()),
        _ => None,
    }
}

/// TYPE_RANGED_FIXNUMP (int, VALUE).
fn int_fixnum(value: &Value) -> Option<i32> {
    match value.kind() {
        Kind::Integer(n) => i32::try_from(n).ok(),
        _ => None,
    }
}

/// RANGED_FIXNUMP (0, VAL, INT_MAX).
fn is_index(value: &Option<Value>) -> bool {
    value.as_ref().and_then(int_fixnum).is_some_and(|n| n >= 0)
}

/// fns.c:Fget of SYMBOL's PROPERTY: the symbol object's plist, PROPERTY
/// compared as plist_get compares a Rust caller's name.
fn fget(interp: &Interpreter, symbol: &Value, property: &str) -> Option<Value> {
    interp.get_property_of_symbol_value(*symbol, property)
}

/// What resolve_symbol_ccl_program makes of a vector.
enum Resolved {
    /// Qnil: not a CCL program.
    Invalid,
    /// Qt: some symbol has no index yet.
    Unresolved,
    /// A copy with every symbol replaced by its index.
    Program(Value),
}

/// resolve_symbol_ccl_program: the code conversion maps, translation
/// tables and programs CCL names, by (SYMBOL . PROPERTY) or (an older
/// style) by a bare symbol, as their indexes.
fn resolve_symbol_ccl_program(interp: &Interpreter, ccl: &Value) -> Resolved {
    let Some(mut items) = slots(ccl) else {
        return Resolved::Invalid;
    };
    if !((CCL_HEADER_MAIN as usize) < items.len() && items.len() <= i32::MAX as usize) {
        return Resolved::Invalid;
    }
    let mut unresolved = false;
    for item in &mut items {
        if int_fixnum(item).is_some() {
            continue;
        }
        if let Kind::Cons(cell) = item.kind() {
            let (car, cdr) = (cell.car.get(), cell.cdr.get());
            let (Ok(_), Ok(property)) = (car.as_symbol(), cdr.as_symbol()) else {
                return Resolved::Invalid;
            };
            let val = fget(interp, &car, property);
            if is_index(&val) {
                *item = val.expect("an index");
            } else {
                unresolved = true;
            }
            continue;
        }
        if item.as_symbol().is_ok() {
            let found = [
                "translation-table-id",
                "code-conversion-map-id",
                "ccl-program-idx",
            ]
            .into_iter()
            .map(|property| fget(interp, item, property))
            .find(is_index);
            match found {
                Some(val) => *item = val.expect("an index"),
                None => unresolved = true,
            }
            continue;
        }
        return Resolved::Invalid;
    }
    let size = items.len() as i64;
    let header = |index: usize| match items[index].kind() {
        Kind::Integer(n) => Some(n),
        _ => None,
    };
    if !(header(CCL_HEADER_BUF_MAG).is_some_and(|mag| mag >= 0)
        && header(CCL_HEADER_EOF).is_some_and(|eof| (0..=size).contains(&eof)))
    {
        return Resolved::Invalid;
    }
    if unresolved {
        Resolved::Unresolved
    } else {
        Resolved::Program(Value::vector(items))
    }
}

/// The words of a resolved program vector, every one an int.
fn program_words(vector: &Value) -> Option<Rc<[i32]>> {
    let words = slots(vector)?
        .iter()
        .map(int_fixnum)
        .collect::<Option<Vec<i32>>>()?;
    Some(Rc::from(words))
}

/// ccl_get_compiled_code: the compiled code of CCL_PROG, a program's name
/// or code, its symbols resolved (a registered program's once, in its
/// slot), and the index of a registered one (-1 for code).
fn ccl_get_compiled_code(interp: &Interpreter, ccl_prog: &Value) -> Option<(Value, i64)> {
    if matches!(ccl_prog.kind(), Kind::Vector(_)) {
        return match resolve_symbol_ccl_program(interp, ccl_prog) {
            Resolved::Program(code) => Some((code, -1)),
            _ => None,
        };
    }
    ccl_prog.as_symbol().ok()?;
    let table = interp.ccl_program_table;
    let idx = match fget(interp, ccl_prog, "ccl-program-idx").map(|val| val.kind()) {
        Some(Kind::Integer(idx)) if idx >= 0 && (idx as usize) < vector_size(&table)? => idx,
        _ => return None,
    };
    let entry = slot(&table, idx)?;
    if vector_size(&entry) != Some(4) || !matches!(slot(&entry, 1)?.kind(), Kind::Vector(_)) {
        return None;
    }
    if slot(&entry, 2)?.is_nil() {
        let Resolved::Program(code) = resolve_symbol_ccl_program(interp, &slot(&entry, 1)?) else {
            return None;
        };
        aset_vector_value(&entry, 1, code).ok()?;
        aset_vector_value(&entry, 2, Value::T).ok()?;
    }
    Some((slot(&entry, 1)?, idx))
}

/// setup_ccl_program: CCL set up to run CCL_PROG (a name or compiled
/// code) from its start, or, for nil, CCL's state reset; false when
/// CCL_PROG is no program.
pub(crate) fn setup_ccl_program(
    interp: &Interpreter,
    ccl: &mut CclProgram,
    ccl_prog: &Value,
) -> bool {
    if !ccl_prog.is_nil() {
        let Some((code, idx)) = ccl_get_compiled_code(interp, ccl_prog) else {
            return false;
        };
        let Some(words) = program_words(&code) else {
            return false;
        };
        ccl.idx = idx;
        ccl.size = words.len();
        ccl.eof_ic = words[CCL_HEADER_EOF];
        ccl.buf_magnification = words[CCL_HEADER_BUF_MAG];
        ccl.prog = words;
        if ccl.idx >= 0
            && let Some(entry) = slot(&interp.ccl_program_table, ccl.idx)
        {
            let _ = aset_vector_value(&entry, 3, Value::Nil);
        }
    }
    ccl.ic = CCL_HEADER_MAIN;
    ccl.reg = [0; 8];
    ccl.last_block = false;
    ccl.status = 0;
    ccl.stack_idx = 0;
    ccl.quit_silently = false;
    true
}

/// Fccl_program_p.
pub(crate) fn ccl_program_p(interp: &Interpreter, object: &Value) -> bool {
    if matches!(object.kind(), Kind::Vector(_)) {
        return matches!(
            resolve_symbol_ccl_program(interp, object),
            Resolved::Program(_)
        );
    }
    if object.as_symbol().is_err() {
        return false;
    }
    match fget(interp, object, "ccl-program-idx").map(|val| val.kind()) {
        Some(Kind::Integer(idx)) => {
            idx >= 0
                && vector_size(&interp.ccl_program_table).is_some_and(|size| (idx as usize) < size)
        }
        _ => false,
    }
}

/// fns.c:larger_vector (VEC, 1, -1): VEC's slots and half as many nils
/// again, at least one.
fn larger_vector(vec: &Value) -> Value {
    let old = slots(vec).unwrap_or_default();
    let incr = (old.len() >> 1).max(1);
    Value::vector(old.into_iter().chain(std::iter::repeat_n(Value::Nil, incr)))
}

/// Fregister_ccl_program.
fn register_ccl_program(
    interp: &mut Interpreter,
    name: &Value,
    ccl_prog: &Value,
) -> Result<Value, LispError> {
    let symbol = name.as_symbol()?.to_string();
    let mut ccl_prog = *ccl_prog;
    let mut resolved = Value::Nil;
    if !ccl_prog.is_nil() {
        if !matches!(ccl_prog.kind(), Kind::Vector(_)) {
            return Err(LispError::WrongTypeArgument("vectorp".into(), ccl_prog));
        }
        match resolve_symbol_ccl_program(interp, &ccl_prog) {
            Resolved::Invalid => return Err(LispError::Signal("Error in CCL program".into())),
            Resolved::Program(code) => {
                ccl_prog = code;
                resolved = Value::T;
            }
            Resolved::Unresolved => {}
        }
    }
    let table = interp.ccl_program_table;
    let len = vector_size(&table).unwrap_or(0);
    let mut idx = 0;
    while idx < len {
        let entry = slot(&table, idx as i64).unwrap_or(Value::Nil);
        if !matches!(entry.kind(), Kind::Vector(_)) {
            // The first unused slot.
            break;
        }
        if slot(&entry, 0) == Some(*name) {
            aset_vector_value(&entry, 1, ccl_prog)?;
            aset_vector_value(&entry, 2, resolved)?;
            aset_vector_value(&entry, 3, Value::T)?;
            return Ok(Value::Integer(idx as i64));
        }
        idx += 1;
    }
    if idx == len {
        interp.ccl_program_table = larger_vector(&table);
    }
    aset_vector_value(
        &interp.ccl_program_table,
        idx,
        Value::vector([*name, ccl_prog, resolved, Value::T]),
    )?;
    interp.put_symbol_property(&symbol, "ccl-program-idx", Value::Integer(idx as i64));
    Ok(Value::Integer(idx as i64))
}

/// Fregister_code_conversion_map.
fn register_code_conversion_map(
    interp: &mut Interpreter,
    symbol: &Value,
    map: &Value,
) -> Result<Value, LispError> {
    let name = symbol.as_symbol()?.to_string();
    if !matches!(map.kind(), Kind::Vector(_)) {
        return Err(LispError::WrongTypeArgument("vectorp".into(), *map));
    }
    let mut vector = interp
        .symbol_value_cell("code-conversion-map-vector")
        .unwrap_or(Value::Nil);
    let Some(len) = vector_size(&vector) else {
        return Err(LispError::Signal(
            "Invalid code-conversion-map-vector".into(),
        ));
    };
    let mut i = 0;
    while i < len {
        let entry = slot(&vector, i as i64).unwrap_or(Value::Nil);
        let Kind::Cons(cell) = entry.kind() else {
            break;
        };
        if cell.car.get() == *symbol {
            let idx = Value::Integer(i as i64);
            cell.cdr.set(*map);
            interp.put_symbol_property(&name, "code-conversion-map", *map);
            interp.put_symbol_property(&name, "code-conversion-map-id", idx);
            return Ok(idx);
        }
        i += 1;
    }
    if i == len {
        vector = larger_vector(&vector);
        interp.set_c_variable("code-conversion-map-vector", vector);
    }
    let idx = Value::Integer(i as i64);
    interp.put_symbol_property(&name, "code-conversion-map", *map);
    interp.put_symbol_property(&name, "code-conversion-map-id", idx);
    aset_vector_value(&vector, i, Value::cons(*symbol, *map))?;
    Ok(idx)
}

/// An int from a register's initial value: an integer in range, else 0.
fn register_value(value: &Value) -> i32 {
    value
        .integer_value()
        .and_then(|n| i32::try_from(n).ok())
        .unwrap_or(0)
}

/// Fccl_execute.
fn ccl_execute(
    interp: &mut Interpreter,
    ccl_prog: &Value,
    reg: &Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    let mut ccl = CclProgram::default();
    if !setup_ccl_program(interp, &mut ccl, ccl_prog) {
        return Err(invalid_program());
    }
    let Some(registers) = slots(reg) else {
        return Err(LispError::WrongTypeArgument("vectorp".into(), *reg));
    };
    if registers.len() != 8 {
        return Err(LispError::Signal(
            "Length of vector REGISTERS is not 8".into(),
        ));
    }
    for (target, value) in ccl.reg.iter_mut().zip(&registers) {
        *target = register_value(value);
    }
    ccl_driver(
        &mut LispHost {
            interp: &mut *interp,
            env: &mut *env,
        },
        &mut ccl,
        None,
        None,
        None,
    )?;
    interp.maybe_quit(env)?;
    if ccl.status != CCL_STAT_SUCCESS {
        return Err(LispError::Signal(format!(
            "Error in CCL program at {}th code",
            ccl.ic
        )));
    }
    for (index, value) in ccl.reg.iter().enumerate() {
        aset_vector_value(reg, index, Value::Integer(i64::from(*value)))?;
    }
    Ok(Value::Nil)
}

/// character.c:char_resolve_modifier_mask.
pub(crate) fn char_resolve_modifier_mask(mut c: i64) -> i64 {
    const CHAR_SHIFT: i64 = 0x200_0000;
    const CHAR_CTL: i64 = 0x400_0000;
    const CHAR_MODIFIER_MASK: i64 = 0xFC0_0000;
    // A non-ASCII character can't reflect modifier bits to the code.
    if !(0..0x80).contains(&(c & !CHAR_MODIFIER_MASK)) {
        return c;
    }
    if c & CHAR_SHIFT != 0 {
        if (c & 0o377) >= i64::from(b'A') && (c & 0o377) <= i64::from(b'Z') {
            c &= !CHAR_SHIFT;
        } else if (c & 0o377) >= i64::from(b'a') && (c & 0o377) <= i64::from(b'z') {
            c = (c & !CHAR_SHIFT) - i64::from(b'a' - b'A');
        } else if (c & !CHAR_MODIFIER_MASK) <= 0x20 {
            c &= !CHAR_SHIFT;
        }
    }
    if c & CHAR_CTL != 0 {
        if (c & 0o377) == i64::from(b' ') {
            c &= !0o177 & !CHAR_CTL;
        } else if (c & 0o377) == i64::from(b'?') {
            c = 0o177 | (c & !0o177 & !CHAR_CTL);
        } else if ((c & 0o137) >= 0o101 && (c & 0o137) <= 0o132)
            || ((c & 0o177) >= 0o100 && (c & 0o177) <= 0o137)
        {
            c &= 0o37 | (!0o177 & !CHAR_CTL);
        }
    }
    c
}

/// The character CHAR_STRING stores for the int C a CCL program wrote:
/// character.c:char_string's, whose modifier bits resolve as
/// char_resolve_modifier_mask resolves them (a remaining bit dropped);
/// one beyond MAX_CHAR is an error.  A negative C, whose low byte alone
/// CHAR_STRING stores (an invalid multibyte form, which a string here
/// cannot hold), is that byte's raw-byte character.
pub(crate) fn written_character(c: i32) -> Result<u32, LispError> {
    const CHAR_MODIFIER_MASK: u32 = 0xFC0_0000;
    const MAX_CHAR: u32 = 0x3F_FFFF;
    if c < 0 {
        return Ok(0x3F_FF00 + (c as u32 & 0xFF));
    }
    if c < 0x1_0000 {
        return Ok(c as u32);
    }
    let mut c = c as u32;
    if c & CHAR_MODIFIER_MASK != 0 {
        c = char_resolve_modifier_mask(i64::from(c)) as u32;
        c &= !CHAR_MODIFIER_MASK;
    }
    if c <= MAX_CHAR {
        Ok(c)
    } else {
        Err(LispError::Signal(format!("Invalid character: {c:x}")))
    }
}

/// Up to LIMIT of the characters (string_char_advance's, or the bytes as
/// they are when not MULTIBYTE) of BYTES from FROM, into OUT; where they
/// end.
fn string_characters(
    bytes: &[u8],
    multibyte: bool,
    from: usize,
    out: &mut Vec<i32>,
    limit: usize,
) -> usize {
    let mut p = from;
    while out.len() < limit && p < bytes.len() {
        if !multibyte || bytes[p] < 0x80 {
            out.push(i32::from(bytes[p]));
            p += 1;
        } else {
            match crate::lisp::types::string_data::decode_character(&bytes[p..]) {
                Ok((c, len)) => {
                    out.push(c as i32);
                    p += len.max(1);
                }
                Err(_) => {
                    out.push(i32::from(bytes[p]));
                    p += 1;
                }
            }
        }
    }
    p
}

/// CHAR_STRING: the multibyte form of the character C.
fn push_char_string(c: u32, out: &mut Vec<u8>) {
    if c < 0x80 {
        out.push(c as u8);
    } else if c < 0x800 {
        out.extend([0xC0 | (c >> 6) as u8, 0x80 | (c & 0x3F) as u8]);
    } else if c < 0x1_0000 {
        out.extend([
            0xE0 | (c >> 12) as u8,
            0x80 | ((c >> 6) & 0x3F) as u8,
            0x80 | (c & 0x3F) as u8,
        ]);
    } else if c < 0x20_0000 {
        out.extend([
            0xF0 | (c >> 18) as u8,
            0x80 | ((c >> 12) & 0x3F) as u8,
            0x80 | ((c >> 6) & 0x3F) as u8,
            0x80 | (c & 0x3F) as u8,
        ]);
    } else if c < 0x3F_FF80 {
        out.extend([
            0xF8,
            0x80 | ((c >> 18) & 0x0F) as u8,
            0x80 | ((c >> 12) & 0x3F) as u8,
            0x80 | ((c >> 6) & 0x3F) as u8,
            0x80 | (c & 0x3F) as u8,
        ]);
    } else {
        // BYTE8_STRING.
        let byte = (c - 0x3F_FF00) as u8;
        out.extend([0xC0 | ((byte >> 6) & 1), 0x80 | (byte & 0x3F)]);
    }
}

/// Fccl_execute_on_string.
fn ccl_execute_on_string(
    interp: &mut Interpreter,
    ccl_prog: &Value,
    status: &Value,
    string: &Value,
    contin: bool,
    unibyte_p: bool,
    env: &mut Env,
) -> Result<Value, LispError> {
    let mut ccl = CclProgram::default();
    if !setup_ccl_program(interp, &mut ccl, ccl_prog) {
        return Err(invalid_program());
    }
    let Some(initial) = slots(status) else {
        return Err(LispError::WrongTypeArgument("vectorp".into(), *status));
    };
    if initial.len() != 9 {
        return Err(LispError::Signal("Length of vector STATUS is not 9".into()));
    }
    let Kind::StringObject(state) = string.kind() else {
        return Err(LispError::WrongTypeArgument("stringp".into(), *string));
    };
    let (bytes, multibyte, str_chars) = {
        let state = state.borrow();
        (state.bytes().to_vec(), state.is_multibyte(), state.len())
    };
    let str_bytes = bytes.len();
    for (index, value) in initial.iter().take(8).enumerate() {
        let value = if value.is_nil() {
            aset_vector_value(status, index, Value::Integer(0))?;
            Value::Integer(0)
        } else {
            *value
        };
        if let Some(n) = value.integer_value().and_then(|n| i32::try_from(n).ok()) {
            ccl.reg[index] = n;
        }
    }
    if let Some(ic) = initial[8].integer_value()
        && i64::from(ccl.ic) < ic
        && ic < ccl.size as i64
    {
        ccl.ic = ic as i32;
    }

    let mut out = Vec::with_capacity(str_bytes + 256);
    let mut consumed_chars = 0;
    let mut consumed_bytes = 0;
    let mut produced_chars = 0;
    let mut source = Vec::with_capacity(CCL_BLOCK);
    let mut destination = vec![0; CCL_BLOCK];
    let mut host = LispHost { interp, env };
    loop {
        source.clear();
        let bytes_are_chars = str_bytes - consumed_bytes == str_chars - consumed_chars;
        consumed_bytes = string_characters(
            &bytes,
            multibyte && !bytes_are_chars,
            consumed_bytes,
            &mut source,
            CCL_BLOCK,
        );
        consumed_chars += source.len();
        if consumed_bytes == str_bytes {
            ccl.last_block = !contin;
        }
        let mut src = 0;
        loop {
            ccl_driver(
                &mut host,
                &mut ccl,
                Some(&source[src..]),
                Some(&mut destination),
                None,
            )?;
            produced_chars += ccl.produced;
            for &c in &destination[..ccl.produced] {
                if unibyte_p {
                    out.push(c as u8);
                } else {
                    push_char_string(written_character(c)?, &mut out);
                }
            }
            src += ccl.consumed;
            if ccl.status != CCL_STAT_SUSPEND_BY_DST {
                break;
            }
        }
        if ccl.status != CCL_STAT_SUSPEND_BY_SRC || str_chars == consumed_chars {
            break;
        }
    }
    if ccl.status == CCL_STAT_INVALID_CMD {
        return Err(LispError::Signal(format!(
            "Error in CCL program at {}th code",
            ccl.ic
        )));
    }
    if ccl.status == CCL_STAT_QUIT {
        return Err(LispError::Signal(format!(
            "CCL program interrupted at {}th code",
            ccl.ic
        )));
    }
    for (index, value) in ccl.reg.iter().enumerate() {
        aset_vector_value(status, index, Value::Integer(i64::from(*value)))?;
    }
    aset_vector_value(status, 8, Value::Integer(i64::from(ccl.ic)))?;
    crate::lisp::types::StringObjectRef::from_storage(out, produced_chars, !unibyte_p)
        .map(Value::StringObject)
        .map_err(|error| LispError::Signal(error.to_string()))
}

/// charset.c:char_charset: the first charset of LIST (Vcharset_ordered_list
/// when not MAYBE_NULL) that encodes C, with the code; the unicode charset,
/// with no code, where the walk reaches Vcharset_non_preferred_head (the
/// list's end, when that is nil) with a Unicode C; past the list, None for MAYBE_NULL, else `emacs' or
/// `eight-bit' with no code.
fn char_charset(
    interp: &Interpreter,
    c: i32,
    list: &[String],
    maybe_null: bool,
) -> Option<(String, Option<u32>)> {
    let non_preferred_head = interp.charset_non_preferred_head_ref();
    for (index, charset) in list.iter().enumerate() {
        if c >= 0
            && let Some(code) = super::coding::encode_charset_char(interp, charset, c as u32)
        {
            return Some((charset.clone(), Some(code)));
        }
        if !maybe_null
            && c <= super::coding::MAX_UNICODE_CHAR as i32
            && list.get(index + 1).map(String::as_str) == non_preferred_head
        {
            return Some(("unicode".into(), None));
        }
    }
    if maybe_null {
        None
    } else if c <= 0x3F_FF7F {
        Some(("emacs".into(), None))
    } else {
        Some(("eight-bit".into(), None))
    }
}

/// coding.h SJIS_TO_JIS over ints.
fn sjis_to_jis(code: i32) -> i32 {
    let (s1, s2) = (code >> 8, code & 0xFF);
    let (j1, j2) = if s2 >= 0x9F {
        (
            s1.wrapping_mul(2)
                .wrapping_sub(if s1 >= 0xE0 { 0x160 } else { 0xE0 }),
            s2 - 0x7E,
        )
    } else {
        (
            s1.wrapping_mul(2)
                .wrapping_sub(if s1 >= 0xE0 { 0x161 } else { 0xE1 }),
            s2 - if s2 >= 0x7F { 0x20 } else { 0x1F },
        )
    };
    j1.wrapping_shl(8) | j2
}

/// coding.h JIS_TO_SJIS over ints.
fn jis_to_sjis(code: i32) -> i32 {
    let (j1, j2) = (code >> 8, code & 0xFF);
    let (s1, s2) = if j1 & 1 != 0 {
        (
            j1 / 2 + if j1 < 0x5F { 0x71 } else { 0xB1 },
            j2 + if j2 >= 0x60 { 0x20 } else { 0x1F },
        )
    } else {
        (j1 / 2 + if j1 < 0x5F { 0x70 } else { 0xB0 }, j2 + 0x7E)
    };
    s1.wrapping_shl(8) | s2
}

/// How a command leaves ccl_driver's loop.
enum Flow {
    /// goto ccl_repeat: on to the next command.
    Repeat,
    /// goto ccl_finish: suspended or done.
    Finish,
    /// goto ccl_error_handler.
    Error,
    /// A signal from a table's test function.
    Lisp(LispError),
}

/// ccl_driver's state over one run.
struct Driver<'a> {
    reg: [i32; 8],
    ic: i32,
    code: i32,
    prog: Rc<[i32]>,
    source: Option<&'a [i32]>,
    src: usize,
    destination: Option<&'a mut [i32]>,
    /// dst, None for NULL.
    dst: Option<usize>,
    stack_idx: i32,
    this_ic: i32,
    eof_ic: i32,
    /// ccl->eof_ic, where a read at the end of the last block goes.
    main_eof_ic: i32,
    last_block: bool,
    status: i32,
    /// mapping_stack and mapping_stack_pointer.
    mapping: [(i32, i32); MAX_MAP_SET_LEVEL],
    sp: usize,
    charset_list: Option<&'a [String]>,
    /// CCL_ENCODE_CHAR's ncode.
    ncode: u32,
}

impl Driver<'_> {
    fn invalid(&mut self) -> Flow {
        self.status = CCL_STAT_INVALID_CMD;
        Flow::Error
    }

    /// CCL_SUSPEND.
    fn suspend(&mut self, status: i32) -> Flow {
        self.ic = self.ic.wrapping_sub(1);
        self.status = status;
        Flow::Finish
    }

    /// XFIXNUM (ccl_prog[IC]); a word past the code is an invalid command
    /// here.
    fn word(&mut self, ic: i32) -> Result<i32, Flow> {
        match usize::try_from(ic).ok().and_then(|ic| self.prog.get(ic)) {
            Some(&word) => Ok(word),
            None => Err(self.invalid()),
        }
    }

    /// GET_CCL_RANGE (VAR, ccl_prog, ic++, LO, HI): IC moves past the
    /// word first, and a word past the code (which GNU reads from beyond
    /// the vector) is out of range.
    fn next_in_range(&mut self, lo: i64, hi: i64) -> Result<i32, Flow> {
        let word = usize::try_from(self.ic)
            .ok()
            .and_then(|ic| self.prog.get(ic))
            .copied();
        self.ic = self.ic.wrapping_add(1);
        match word {
            Some(word) if lo <= i64::from(word) && i64::from(word) <= hi => Ok(word),
            _ => Err(self.invalid()),
        }
    }

    /// GET_CCL_CODE (code, ccl_prog, ic++).
    fn next_code(&mut self) -> Result<i32, Flow> {
        let code = self.next_in_range(i64::from(CCL_CODE_MIN), i64::from(CCL_CODE_MAX))?;
        self.code = code;
        Ok(code)
    }

    fn advance(&mut self, by: i32) {
        self.ic = self.ic.wrapping_add(by);
    }

    /// CCL_WRITE_CHAR.
    fn write(&mut self, ch: i32) -> Result<(), Flow> {
        let Some(dst) = self.dst else {
            return Err(self.invalid());
        };
        let destination = self
            .destination
            .as_deref_mut()
            .expect("dst within destination");
        if dst < destination.len() {
            destination[dst] = ch;
            self.dst = Some(dst + 1);
            Ok(())
        } else {
            Err(self.suspend(CCL_STAT_SUSPEND_BY_DST))
        }
    }

    /// CCL_WRITE_STRING (LEN): the string at ccl_prog[ic].
    fn write_string(&mut self, len: i32) -> Result<(), Flow> {
        let Some(dst) = self.dst else {
            return Err(self.invalid());
        };
        let room = self.destination.as_deref().map_or(0, <[i32]>::len) - dst;
        if i64::from(len) > room as i64 {
            return Err(self.suspend(CCL_STAT_SUSPEND_BY_DST));
        }
        if len <= 0 {
            return Ok(());
        }
        let mut written = Vec::with_capacity(len as usize);
        if self.word(self.ic)? & 0x100_0000 != 0 {
            for ccli in 0..len {
                written.push(self.word(self.ic.wrapping_add(ccli))? & 0xFF_FFFF);
            }
        } else {
            for ccli in 0..len {
                let word = self.word(self.ic.wrapping_add(ccli / 3))?;
                written.push((word >> ((2 - ccli % 3) * 8)) & 0xFF);
            }
        }
        let destination = self
            .destination
            .as_deref_mut()
            .expect("dst within destination");
        destination[dst..dst + written.len()].copy_from_slice(&written);
        self.dst = Some(dst + written.len());
        Ok(())
    }

    /// CCL_READ_CHAR into the register R (a local, for None).
    fn read(&mut self, r: Option<usize>) -> Result<i32, Flow> {
        let Some(source) = self.source else {
            return Err(self.invalid());
        };
        if self.src < source.len() {
            let c = source[self.src];
            self.src += 1;
            if let Some(r) = r {
                self.reg[r] = c;
            }
            Ok(c)
        } else if self.last_block {
            if let Some(r) = r {
                self.reg[r] = -1;
            }
            self.ic = self.main_eof_ic;
            Err(Flow::Repeat)
        } else {
            Err(self.suspend(CCL_STAT_SUSPEND_BY_SRC))
        }
    }

    fn push_mapping(&mut self, rest_length: i32, orig: i32) -> Result<(), Flow> {
        if self.sp >= MAX_MAP_SET_LEVEL {
            return Err(self.invalid());
        }
        self.mapping[self.sp] = (rest_length, orig);
        self.sp += 1;
        Ok(())
    }

    fn pop_mapping(&mut self) -> Result<(i32, i32), Flow> {
        if self.sp == 0 {
            return Err(self.invalid());
        }
        self.sp -= 1;
        Ok(self.mapping[self.sp])
    }

    /// Back where the outermost caller left off, as CCL_Call and
    /// CCL_CALL_FOR_MAP_INSTRUCTION leave a program whose call fails.
    fn unwind_failed_call(&mut self, statics: &CclStatics) {
        if self.stack_idx > 0
            && let Some(frame) = statics.frame(0)
        {
            self.prog = frame.prog;
            self.ic = frame.ic;
            self.eof_ic = frame.eof_ic;
        }
    }

    /// Enter CALLEE, to come back to RET_IC.
    fn enter(&mut self, statics: &CclStatics, callee: Rc<[i32]>, ret_ic: i32) -> Result<(), Flow> {
        let Some(&eof) = callee.get(CCL_HEADER_EOF) else {
            return Err(self.invalid());
        };
        statics.set_frame(
            self.stack_idx,
            CclFrame {
                prog: self.prog.clone(),
                ic: ret_ic,
                eof_ic: self.eof_ic,
            },
        );
        self.stack_idx += 1;
        self.prog = callee;
        self.ic = CCL_HEADER_MAIN;
        self.eof_ic = eof;
        Ok(())
    }

    /// CCL_CALL_FOR_MAP_INSTRUCTION (SYMBOL, RET_IC).
    fn call_for_map(&mut self, interp: &Interpreter, symbol: &Value, ret_ic: i32) -> Flow {
        let mut called = CclProgram::default();
        if self.stack_idx >= CCL_PROG_STACK_DEPTH || !setup_ccl_program(interp, &mut called, symbol)
        {
            self.unwind_failed_call(&interp.ccl_statics);
            return self.invalid();
        }
        match self.enter(&interp.ccl_statics, called.prog, ret_ic) {
            Ok(()) => Flow::Repeat,
            Err(flow) => flow,
        }
    }

    /// CCL_DECODE_CHAR (ID, CODE).
    fn decode_char(interp: &Interpreter, id: i32, code: i32) -> i32 {
        if id == 0 {
            return code;
        }
        interp
            .charset_name_of_id(i64::from(id))
            .and_then(|charset| super::coding::decode_charset_code(interp, charset, code as u32))
            .map_or(-1, |c| c as i32)
    }

    /// CCL_ENCODE_CHAR (C, charset_list, reg[ID], reg[ENCODED]): the
    /// charset char_charset finds for C among the coding's charsets, else
    /// by priority.  Where char_charset returns a charset without storing
    /// a code (the unicode of Vcharset_non_preferred_head, or the `emacs'
    /// or `eight-bit' past the list), the macro's `ncode' keeps what it
    /// last held, as GNU's does.
    fn encode_char(&mut self, interp: &Interpreter, c: i32, id: usize, encoded: usize) {
        let found = self
            .charset_list
            .and_then(|list| char_charset(interp, c, list, true))
            .or_else(|| char_charset(interp, c, interp.charset_priority(), false));
        if let Some((charset, code)) = found
            && let Some(charset_id) = interp.charset_id(&charset)
        {
            if let Some(code) = code {
                self.ncode = code;
            }
            self.reg[id] = charset_id as i32;
            self.reg[encoded] = self.ncode as i32;
        }
    }

    /// The translation table of id ID (GET_TRANSLATION_TABLE): the cdr of
    /// its entry in `translation-table-vector'.
    fn translation_table(interp: &Interpreter, id: i64) -> Value {
        let vector = interp
            .symbol_value_cell("translation-table-vector")
            .unwrap_or(Value::Nil);
        match slot(&vector, id).map(|entry| entry.kind()) {
            Some(Kind::Cons(cell)) => cell.cdr.get(),
            _ => Value::Nil,
        }
    }

    /// translate_char (TABLE, C), C itself when not a character.
    fn translate(interp: &Interpreter, table: &Value, c: i32) -> i32 {
        if c < 0 {
            return c;
        }
        super::buffers::translate_char(interp, table, c as u32) as i32
    }

    /// The size of the vector VARIABLE holds, -1 when it holds none.
    fn table_vector_size(interp: &Interpreter, variable: &str) -> i64 {
        vector_size(&interp.symbol_value_cell(variable).unwrap_or(Value::Nil))
            .map_or(-1, |size| size as i64)
    }

    /// The hash table of id EOP (GET_HASH_TABLE).
    fn hash_table(&mut self, interp: &Interpreter, eop: i32) -> Result<Value, Flow> {
        let vector = interp
            .symbol_value_cell("translation-hash-table-vector")
            .unwrap_or(Value::Nil);
        match slot(&vector, i64::from(eop)).map(|entry| entry.kind()) {
            Some(Kind::Cons(cell)) if matches!(cell.cdr.get().kind(), Kind::HashTable(_)) => {
                Ok(cell.cdr.get())
            }
            _ => Err(self.invalid()),
        }
    }

    /// ccl_set_expr: reg[RRR] = I OP J, then a write or a jump.
    fn set_expr(
        &mut self,
        rrr: usize,
        op: i32,
        i: i32,
        j: i32,
        jump_address: i32,
    ) -> Result<(), Flow> {
        let reg = &mut self.reg;
        match op {
            CCL_PLUS => reg[rrr] = i.wrapping_add(j),
            CCL_MINUS => reg[rrr] = i.wrapping_sub(j),
            CCL_MUL => reg[rrr] = i.wrapping_mul(j),
            CCL_DIV => {
                if j == 0 {
                    return Err(self.invalid());
                }
                reg[rrr] = if i == i32::MIN && j == -1 { i } else { i / j };
            }
            CCL_MOD => {
                if j == 0 {
                    return Err(self.invalid());
                }
                reg[rrr] = if j == -1 { 0 } else { i % j };
            }
            CCL_AND => reg[rrr] = i & j,
            CCL_OR => reg[rrr] = i | j,
            CCL_XOR => reg[rrr] = i ^ j,
            CCL_LSH => {
                if j < 0 {
                    return Err(self.invalid());
                }
                reg[rrr] = if j < 32 { ((i as u32) << j) as i32 } else { 0 };
            }
            CCL_RSH => {
                if j < 0 {
                    return Err(self.invalid());
                }
                reg[rrr] = i >> j.min(31);
            }
            CCL_LSH8 => reg[rrr] = (((i as u32) << 8) as i32) | j,
            CCL_RSH8 => {
                reg[rrr] = i >> 8;
                reg[7] = i & 0xFF;
            }
            CCL_DIVMOD => {
                if j == 0 {
                    return Err(self.invalid());
                }
                if j == -1 {
                    reg[rrr] = 0i32.wrapping_sub(reg[rrr]);
                    reg[7] = 0;
                } else {
                    reg[rrr] = i / j;
                    reg[7] = i % j;
                }
            }
            CCL_LS => reg[rrr] = i32::from(i < j),
            CCL_GT => reg[rrr] = i32::from(i > j),
            CCL_EQ => reg[rrr] = i32::from(i == j),
            CCL_LE => reg[rrr] = i32::from(i <= j),
            CCL_GE => reg[rrr] = i32::from(i >= j),
            CCL_NE => reg[rrr] = i32::from(i != j),
            CCL_DECODE_SJIS => {
                let code = sjis_to_jis((((i as u32) << 8) as i32) | j);
                reg[rrr] = code >> 8;
                reg[7] = code & 0xFF;
            }
            CCL_ENCODE_SJIS => {
                let code = jis_to_sjis((((i as u32) << 8) as i32) | j);
                reg[rrr] = code >> 8;
                reg[7] = code & 0xFF;
            }
            _ => return Err(self.invalid()),
        }
        self.code &= 0x1F;
        if self.code == CCL_WRITE_EXPR_CONST || self.code == CCL_WRITE_EXPR_REGISTER {
            let i = self.reg[rrr];
            self.write(i)?;
            self.ic = jump_address;
        } else if self.reg[rrr] == 0 {
            self.ic = jump_address;
        }
        Ok(())
    }

    /// ccl_expr_self: reg[RRR] OP= I.
    fn expr_self(&mut self, rrr: usize, op: i32, i: i32) -> Result<(), Flow> {
        let reg = &mut self.reg;
        match op {
            CCL_PLUS => reg[rrr] = reg[rrr].wrapping_add(i),
            CCL_MINUS => reg[rrr] = reg[rrr].wrapping_sub(i),
            CCL_MUL => reg[rrr] = reg[rrr].wrapping_mul(i),
            CCL_DIV => {
                if i == 0 {
                    return Err(self.invalid());
                }
                if !(reg[rrr] == i32::MIN && i == -1) {
                    reg[rrr] /= i;
                }
            }
            CCL_MOD => {
                if i == 0 {
                    return Err(self.invalid());
                }
                reg[rrr] = if i == -1 { 0 } else { reg[rrr] % i };
            }
            CCL_AND => reg[rrr] &= i,
            CCL_OR => reg[rrr] |= i,
            CCL_XOR => reg[rrr] ^= i,
            CCL_LSH => {
                if i < 0 {
                    return Err(self.invalid());
                }
                reg[rrr] = if i < 32 {
                    ((reg[rrr] as u32) << i) as i32
                } else {
                    0
                };
            }
            CCL_RSH => {
                if i < 0 {
                    return Err(self.invalid());
                }
                reg[rrr] >>= i.min(31);
            }
            CCL_LSH8 => {
                reg[rrr] = ((reg[rrr] as u32) << 8) as i32;
                reg[rrr] |= i;
            }
            CCL_RSH8 => {
                reg[7] = reg[rrr] & 0xFF;
                reg[rrr] >>= 8;
            }
            CCL_DIVMOD => {
                if i == 0 {
                    return Err(self.invalid());
                }
                if i == -1 {
                    reg[7] = 0;
                    reg[rrr] = 0i32.wrapping_sub(reg[rrr]);
                } else {
                    reg[7] = reg[rrr] % i;
                    reg[rrr] /= i;
                }
            }
            CCL_LS => reg[rrr] = i32::from(reg[rrr] < i),
            CCL_GT => reg[rrr] = i32::from(reg[rrr] > i),
            CCL_EQ => reg[rrr] = i32::from(reg[rrr] == i),
            CCL_LE => reg[rrr] = i32::from(reg[rrr] <= i),
            CCL_GE => reg[rrr] = i32::from(reg[rrr] >= i),
            CCL_NE => reg[rrr] = i32::from(reg[rrr] != i),
            _ => return Err(self.invalid()),
        }
        Ok(())
    }

    /// What the map at POINT of `code-conversion-map-vector' has for OP,
    /// as CCL_IterateMultipleMap and CCL_MapMultiple read a
    /// [STARTPOINT VAL1 VAL2 ...] or [t VAL STARTPOINT ENDPOINT] map; None
    /// to go on to the next map.
    fn map_content(vector: &Value, point: i64, op: i32) -> Option<Value> {
        let entry = slot(vector, point)?;
        let Kind::Cons(cell) = entry.kind() else {
            return None;
        };
        let map = cell.cdr.get();
        let size = vector_size(&map)? as i64;
        if size <= 1 {
            return None;
        }
        let op = i64::from(op);
        let content = match slot(&map, 0)?.kind() {
            Kind::Integer(start) => {
                if !(start <= op && op - start + 1 < size) {
                    return None;
                }
                slot(&map, op - start + 1)?
            }
            Kind::T => {
                if size != 4 {
                    return None;
                }
                match (slot(&map, 2)?.kind(), slot(&map, 3)?.kind()) {
                    (Kind::Integer(start), Kind::Integer(end)) if start <= op && op < end => {
                        slot(&map, 1)?
                    }
                    _ => return None,
                }
            }
            _ => return None,
        };
        (!content.is_nil()).then_some(content)
    }

    fn is_lambda(value: &Value) -> bool {
        value.symbol_str() == Some("lambda")
    }

    fn code_conversion_maps(interp: &Interpreter) -> Value {
        interp
            .symbol_value_cell("code-conversion-map-vector")
            .unwrap_or(Value::Nil)
    }

    /// CCL_IterateMultipleMap.
    fn iterate_multiple_map(
        &mut self,
        interp: &Interpreter,
        rrr: usize,
        big_rrr: usize,
    ) -> Result<(), Flow> {
        // The number of maps.
        let j = self.word(self.ic)?;
        self.advance(1);
        let fin_ic = self.ic.wrapping_add(j);
        let op = self.reg[rrr];
        let mut i;
        if j > self.reg[big_rrr] && j >= 0 {
            self.advance(self.reg[big_rrr]);
            i = self.reg[big_rrr];
        } else {
            self.reg[big_rrr] = -1;
            self.ic = fin_ic;
            return Ok(());
        }
        let vector = Self::code_conversion_maps(interp);
        while i < j {
            if vector_size(&vector).is_none() {
                i += 1;
                continue;
            }
            let point = self.word(self.ic)?;
            self.advance(1);
            let Some(content) = Self::map_content(&vector, i64::from(point), op) else {
                i += 1;
                continue;
            };
            match content.kind() {
                Kind::Integer(n) if i32::try_from(n).is_ok() => {
                    self.reg[big_rrr] = i;
                    self.reg[rrr] = n as i32;
                    break;
                }
                Kind::T => {
                    self.reg[big_rrr] = i;
                    break;
                }
                _ if Self::is_lambda(&content) => {
                    self.reg[big_rrr] = i;
                    break;
                }
                Kind::Cons(cell) => match (cell.car.get().kind(), cell.cdr.get().kind()) {
                    (Kind::Integer(_), Kind::Integer(value)) if i32::try_from(value).is_ok() => {
                        self.reg[big_rrr] = i;
                        self.reg[rrr] = value as i32;
                        break;
                    }
                    _ => {
                        i += 1;
                        continue;
                    }
                },
                _ if content.as_symbol().is_ok() => {
                    return Err(self.call_for_map(interp, &content, fin_ic));
                }
                _ => return Err(self.invalid()),
            }
        }
        if i == j {
            self.reg[big_rrr] = -1;
        }
        self.ic = fin_ic;
        Ok(())
    }

    /// CCL_MapMultiple.
    fn map_multiple(
        &mut self,
        interp: &Interpreter,
        rrr: usize,
        big_rrr: usize,
    ) -> Result<(), Flow> {
        let statics = &interp.ccl_statics;
        let current_ic = self.this_ic;
        // No recursive call of MapMultiple.
        let depth = statics.map_multiple_depth.get();
        if depth > 0 {
            if depth <= self.stack_idx {
                statics.map_multiple_depth.set(0);
                self.sp = 0;
                return Err(self.invalid());
            }
        } else {
            self.sp = 0;
        }
        statics.map_multiple_depth.set(0);

        // The number of maps and separators.
        let mut rest = self.word(self.ic)?;
        self.advance(1);
        let fin_ic = self.ic.wrapping_add(rest);
        let mut op = self.reg[rrr];
        let mut i;
        if rest > self.reg[big_rrr] && self.reg[big_rrr] >= 0 {
            self.advance(self.reg[big_rrr]);
            i = self.reg[big_rrr];
            rest = rest.wrapping_sub(i);
        } else {
            self.ic = fin_ic;
            self.reg[big_rrr] = -1;
            self.sp = 0;
            return Ok(());
        }

        if self.sp <= 1 {
            self.sp = 0;
            self.push_mapping(0, op)?;
            self.reg[big_rrr] = -1;
        } else {
            // Back from a program a map called.
            let (_, orig_op) = self.pop_mapping()?;
            let (length, value) = self.pop_mapping()?;
            rest = length;
            self.reg[rrr] = value;
            match op {
                -1 => {
                    // As nil.
                    op = orig_op;
                    i = i.wrapping_add(1);
                    self.advance(1);
                    rest = rest.wrapping_sub(1);
                }
                -2 => {
                    // As t.
                    op = self.reg[rrr];
                    i = i.wrapping_add(1);
                    self.advance(1);
                    rest = rest.wrapping_sub(1);
                }
                -3 => {
                    // As lambda.
                    op = orig_op;
                    i = i.wrapping_add(rest);
                    self.advance(rest);
                    rest = 0;
                }
                _ => {
                    // As a mapped value.
                    i = i.wrapping_add(rest);
                    self.advance(rest);
                    let (length, value) = self.pop_mapping()?;
                    rest = length;
                    self.reg[rrr] = value;
                }
            }
        }
        let vector = Self::code_conversion_maps(interp);
        let Some(map_vector_size) = vector_size(&vector) else {
            return Err(self.invalid());
        };

        loop {
            while rest > 0 {
                let point = i64::from(self.word(self.ic)?);
                if point < 0 {
                    // +1 for the separator.
                    let point = (-point + 1) as i32;
                    let reg = self.reg[rrr];
                    self.push_mapping(rest.wrapping_sub(point), reg)?;
                    rest = point;
                    self.reg[rrr] = op;
                } else if (point as usize) < map_vector_size
                    && let Some(content) = Self::map_content(&vector, point, op)
                {
                    self.reg[big_rrr] = i;
                    match content.kind() {
                        Kind::Integer(n) if i32::try_from(n).is_ok() => {
                            op = n as i32;
                            i = i.wrapping_add(rest - 1);
                            self.advance(rest - 1);
                            let (length, value) = self.pop_mapping()?;
                            rest = length.wrapping_add(1);
                            self.reg[rrr] = value;
                        }
                        Kind::Cons(cell) => {
                            if let (Kind::Integer(_), Kind::Integer(value)) =
                                (cell.car.get().kind(), cell.cdr.get().kind())
                                && let Ok(value) = i32::try_from(value)
                            {
                                op = value;
                                i = i.wrapping_add(rest - 1);
                                self.advance(rest - 1);
                                let (length, value) = self.pop_mapping()?;
                                rest = length.wrapping_add(1);
                                self.reg[rrr] = value;
                            }
                        }
                        Kind::T => op = self.reg[rrr],
                        _ if Self::is_lambda(&content) => {
                            i = i.wrapping_add(rest);
                            self.advance(rest);
                            break;
                        }
                        _ if content.as_symbol().is_ok() => {
                            let reg = self.reg[rrr];
                            self.push_mapping(rest, reg)?;
                            self.push_mapping(rest, op)?;
                            statics.map_multiple_depth.set(self.stack_idx + 1);
                            return Err(self.call_for_map(interp, &content, current_ic));
                        }
                        _ => return Err(self.invalid()),
                    }
                }
                i = i.wrapping_add(1);
                self.advance(1);
                rest = rest.wrapping_sub(1);
            }
            if self.sp <= 1 {
                break;
            }
            let (length, value) = self.pop_mapping()?;
            rest = length;
            self.reg[rrr] = value;
            i = i.wrapping_add(rest);
            self.advance(rest);
            let (length, value) = self.pop_mapping()?;
            rest = length;
            self.reg[rrr] = value;
        }
        self.ic = fin_ic;
        self.reg[rrr] = op;
        Ok(())
    }

    /// CCL_MapSingle.
    fn map_single(&mut self, interp: &Interpreter, rrr: usize, big_rrr: usize) -> Result<(), Flow> {
        // The map's id.
        let j = self.word(self.ic)?;
        self.advance(1);
        let op = i64::from(self.reg[rrr]);
        let vector = Self::code_conversion_maps(interp);
        let map = match slot(&vector, i64::from(j)).map(|entry| entry.kind()) {
            Some(Kind::Cons(cell)) => cell.cdr.get(),
            _ => {
                self.reg[big_rrr] = -1;
                return Ok(());
            }
        };
        let start = match (vector_size(&map), slot(&map, 0).map(|start| start.kind())) {
            (Some(size), Some(Kind::Integer(start)))
                if size > 0 && start <= op && op - start + 1 < size as i64 =>
            {
                start
            }
            _ => {
                self.reg[big_rrr] = -1;
                return Ok(());
            }
        };
        self.reg[big_rrr] = 0;
        let content = slot(&map, op - start + 1).unwrap_or(Value::Nil);
        match content.kind() {
            Kind::Nil => self.reg[big_rrr] = -1,
            Kind::Integer(n) if i32::try_from(n).is_ok() => self.reg[rrr] = n as i32,
            Kind::T => {}
            Kind::Cons(cell) => {
                if let (Kind::Integer(_), Kind::Integer(value)) =
                    (cell.car.get().kind(), cell.cdr.get().kind())
                    && let Ok(value) = i32::try_from(value)
                {
                    self.reg[rrr] = value;
                }
            }
            _ if content.as_symbol().is_ok() => {
                let ic = self.ic;
                return Err(self.call_for_map(interp, &content, ic));
            }
            _ => self.reg[big_rrr] = -1,
        }
        Ok(())
    }

    /// One command, from ccl_repeat to the end of its case.
    fn step<H: CclHost + ?Sized>(&mut self, host: &mut H) -> Result<(), Flow> {
        self.this_ic = self.ic;
        let code = self.next_code()?;
        let mut field1 = code >> 8;
        let mut rrr = ((code & 0xFF) >> 5) as usize;
        let big_rrr = (field1 & 7) as usize;
        let rrr3 = ((field1 >> 3) & 7) as usize;
        match code & 0x1F {
            CCL_SET_REGISTER => self.reg[rrr] = self.reg[big_rrr],
            CCL_SET_SHORT_CONST => self.reg[rrr] = field1,
            CCL_SET_CONST => {
                self.reg[rrr] = self.word(self.ic)?;
                self.advance(1);
            }
            CCL_SET_ARRAY => {
                let i = self.reg[big_rrr];
                let j = field1 >> 3;
                if 0 <= i && i < j {
                    self.reg[rrr] = self.word(self.ic.wrapping_add(i))?;
                }
                self.advance(j);
            }
            CCL_JUMP => self.advance(field1),
            CCL_JUMP_COND => {
                if self.reg[rrr] == 0 {
                    self.advance(field1);
                }
            }
            CCL_WRITE_REGISTER_JUMP => {
                self.write(self.reg[rrr])?;
                self.advance(field1);
            }
            CCL_WRITE_REGISTER_READ_JUMP => {
                self.write(self.reg[rrr])?;
                self.advance(1);
                self.read(Some(rrr))?;
                self.advance(field1 - 1);
            }
            CCL_WRITE_CONST_JUMP => {
                let i = self.word(self.ic)?;
                self.write(i)?;
                self.advance(field1);
            }
            CCL_WRITE_CONST_READ_JUMP => {
                let i = self.word(self.ic)?;
                self.write(i)?;
                self.advance(1);
                self.read(Some(rrr))?;
                self.advance(field1 - 1);
            }
            CCL_WRITE_STRING_JUMP => {
                let j = self.word(self.ic)?;
                self.advance(1);
                self.write_string(j)?;
                self.advance(field1 - 1);
            }
            CCL_WRITE_ARRAY_READ_JUMP => {
                let i = self.reg[rrr];
                let j = self.word(self.ic)?;
                if 0 <= i && i < j {
                    let i = self.word(self.ic.wrapping_add(1).wrapping_add(i))?;
                    self.write(i)?;
                }
                self.advance(j.wrapping_add(2));
                self.read(Some(rrr))?;
                self.advance(field1.wrapping_sub(j.wrapping_add(2)));
            }
            CCL_READ_JUMP => {
                self.read(Some(rrr))?;
                self.advance(field1);
            }
            command @ (CCL_READ_BRANCH | CCL_BRANCH) => {
                if command == CCL_READ_BRANCH {
                    self.read(Some(rrr))?;
                }
                let ioff = if 0 <= self.reg[rrr] && self.reg[rrr] < field1 {
                    self.reg[rrr]
                } else {
                    field1
                };
                let incr = self.word(self.ic.wrapping_add(ioff))?;
                self.advance(incr);
            }
            CCL_READ_REGISTER => loop {
                self.read(Some(rrr))?;
                if field1 == 0 {
                    break;
                }
                let code = self.next_code()?;
                field1 = code >> 8;
                rrr = ((code & 0xFF) >> 5) as usize;
            },
            CCL_WRITE_EXPR_CONST => {
                let i = self.reg[big_rrr];
                let j = self.word(self.ic)?;
                let jump_address = self.ic.wrapping_add(1);
                self.set_expr(7, field1 >> 6, i, j, jump_address)?;
            }
            CCL_WRITE_REGISTER => loop {
                self.write(self.reg[rrr])?;
                if field1 == 0 {
                    break;
                }
                let code = self.next_code()?;
                field1 = code >> 8;
                rrr = ((code & 0xFF) >> 5) as usize;
            },
            CCL_WRITE_EXPR_REGISTER => {
                let (i, j) = (self.reg[big_rrr], self.reg[rrr3]);
                let jump_address = self.ic;
                self.set_expr(7, field1 >> 6, i, j, jump_address)?;
            }
            CCL_CALL => {
                let interp = host.interp();
                // A nonzero FFF puts the program's id in the next word.
                let prog_id = if rrr != 0 {
                    let id = self.word(self.ic)?;
                    self.advance(1);
                    id
                } else {
                    field1
                };
                let callee = if self.stack_idx >= CCL_PROG_STACK_DEPTH {
                    None
                } else {
                    slot(&interp.ccl_program_table, i64::from(prog_id))
                        .filter(|entry| matches!(entry.kind(), Kind::Vector(_)))
                        .and_then(|entry| slot(&entry, 1))
                        .and_then(|code| program_words(&code))
                };
                let Some(callee) = callee else {
                    self.unwind_failed_call(&interp.ccl_statics);
                    return Err(self.invalid());
                };
                let ic = self.ic;
                self.enter(&interp.ccl_statics, callee, ic)?;
            }
            CCL_WRITE_CONST_STRING => {
                if rrr == 0 {
                    self.write(field1)?;
                } else {
                    self.write_string(field1)?;
                    self.advance((field1 + 2) / 3);
                }
            }
            CCL_WRITE_ARRAY => {
                let i = self.reg[rrr];
                if 0 <= i && i < field1 {
                    let j = self.word(self.ic.wrapping_add(i))?;
                    self.write(j)?;
                }
                self.advance(field1);
            }
            CCL_END => {
                if self.stack_idx > 0 {
                    self.stack_idx -= 1;
                    let frame = host
                        .interp()
                        .ccl_statics
                        .frame(self.stack_idx)
                        .expect("a caller for each call");
                    self.prog = frame.prog;
                    self.ic = frame.ic;
                    self.eof_ic = frame.eof_ic;
                } else {
                    if let Some(source) = self.source {
                        self.src = source.len();
                    }
                    // ccl->ic stays at this command, to do no more.
                    self.ic = self.ic.wrapping_sub(1);
                    self.status = CCL_STAT_SUCCESS;
                    return Err(Flow::Finish);
                }
            }
            CCL_EXPR_SELF_CONST => {
                let i = self.word(self.ic)?;
                self.advance(1);
                self.expr_self(rrr, field1 >> 6, i)?;
            }
            CCL_EXPR_SELF_REG => {
                let i = self.reg[big_rrr];
                self.expr_self(rrr, field1 >> 6, i)?;
            }
            CCL_SET_EXPR_CONST => {
                let i = self.reg[big_rrr];
                let j = self.word(self.ic)?;
                self.advance(1);
                let jump_address = self.ic;
                self.set_expr(rrr, field1 >> 6, i, j, jump_address)?;
            }
            CCL_SET_EXPR_REG => {
                let (i, j) = (self.reg[big_rrr], self.reg[rrr3]);
                let jump_address = self.ic;
                self.set_expr(rrr, field1 >> 6, i, j, jump_address)?;
            }
            command @ (CCL_READ_JUMP_COND_EXPR_CONST | CCL_JUMP_COND_EXPR_CONST) => {
                if command == CCL_READ_JUMP_COND_EXPR_CONST {
                    self.read(Some(rrr))?;
                }
                let i = self.reg[rrr];
                let jump_address = self.ic.wrapping_add(field1);
                let op = self.word(self.ic)?;
                self.advance(1);
                let j = self.word(self.ic)?;
                self.advance(1);
                self.set_expr(7, op, i, j, jump_address)?;
            }
            command @ (CCL_READ_JUMP_COND_EXPR_REG | CCL_JUMP_COND_EXPR_REG) => {
                if command == CCL_READ_JUMP_COND_EXPR_REG {
                    self.read(Some(rrr))?;
                }
                let i = self.reg[rrr];
                let jump_address = self.ic.wrapping_add(field1);
                let op = self.word(self.ic)?;
                self.advance(1);
                let j = self.next_in_range(0, 7)?;
                let j = self.reg[j as usize];
                self.set_expr(7, op, i, j, jump_address)?;
            }
            CCL_EXTENSION => self.extension(host, field1 >> 6, rrr, big_rrr, rrr3)?,
            _ => return Err(self.invalid()),
        }
        Ok(())
    }

    /// CCL_Extension's commands.
    fn extension<H: CclHost + ?Sized>(
        &mut self,
        host: &mut H,
        excmd: i32,
        rrr: usize,
        big_rrr: usize,
        rrr3: usize,
    ) -> Result<(), Flow> {
        match excmd {
            CCL_READ_MULTIBYTE_CHAR2 => {
                if self.source.is_none() {
                    return Err(self.invalid());
                }
                let i = self.read(None)?;
                self.encode_char(host.interp(), i, big_rrr, rrr);
            }
            CCL_WRITE_MULTIBYTE_CHAR2 => {
                if self.dst.is_none() {
                    return Err(self.invalid());
                }
                let i = Self::decode_char(host.interp(), self.reg[big_rrr], self.reg[rrr]);
                self.write(i)?;
            }
            CCL_TRANSLATE_CHARACTER => {
                let interp = host.interp();
                let i = Self::decode_char(interp, self.reg[big_rrr], self.reg[rrr]);
                let table = Self::translation_table(interp, i64::from(self.reg[rrr3]));
                let op = Self::translate(interp, &table, i);
                self.encode_char(interp, op, big_rrr, rrr);
            }
            CCL_TRANSLATE_CHARACTER_CONST_TBL => {
                let interp = host.interp();
                let size = Self::table_vector_size(interp, "translation-table-vector");
                let eop = self.next_in_range(0, size)?;
                let i = Self::decode_char(interp, self.reg[big_rrr], self.reg[rrr]);
                let table = Self::translation_table(interp, i64::from(eop));
                let op = Self::translate(interp, &table, i);
                self.encode_char(interp, op, big_rrr, rrr);
            }
            CCL_LOOKUP_INT_CONST_TBL => {
                let size = Self::table_vector_size(host.interp(), "translation-hash-table-vector");
                let eop = self.next_in_range(0, size)?;
                let table = self.hash_table(host.interp(), eop)?;
                let key = i64::from(self.reg[big_rrr]);
                match host.hash_lookup(table, key).map_err(Flow::Lisp)? {
                    Some(opl) => {
                        let Some(c) = int_fixnum(&opl).filter(|c| (0..=0x3F_FFFF).contains(c))
                        else {
                            return Err(self.invalid());
                        };
                        self.reg[big_rrr] = host.interp().charset_id("unicode").unwrap_or(2) as i32;
                        self.reg[rrr] = c;
                        // r7 true for success.
                        self.reg[7] = 1;
                    }
                    None => self.reg[7] = 0,
                }
            }
            CCL_LOOKUP_CHAR_CONST_TBL => {
                let size = Self::table_vector_size(host.interp(), "translation-hash-table-vector");
                let eop = self.next_in_range(0, size)?;
                let i = Self::decode_char(host.interp(), self.reg[big_rrr], self.reg[rrr]);
                let table = self.hash_table(host.interp(), eop)?;
                match host.hash_lookup(table, i64::from(i)).map_err(Flow::Lisp)? {
                    Some(opl) => {
                        let Some(value) = int_fixnum(&opl) else {
                            return Err(self.invalid());
                        };
                        self.reg[big_rrr] = value;
                        self.reg[7] = 1;
                    }
                    None => self.reg[7] = 0,
                }
            }
            CCL_ITERATE_MULTIPLE_MAP => self.iterate_multiple_map(host.interp(), rrr, big_rrr)?,
            CCL_MAP_MULTIPLE => self.map_multiple(host.interp(), rrr, big_rrr)?,
            CCL_MAP_SINGLE => self.map_single(host.interp(), rrr, big_rrr)?,
            _ => return Err(self.invalid()),
        }
        Ok(())
    }
}

/// ccl_driver: run CCL over SOURCE (characters, or bytes) into
/// DESTINATION, either None to permit only operations on registers.
/// CCL's `consumed' and `produced' count what the run read and wrote, its
/// status says how the run ended, and an error's message goes to
/// DESTINATION where it has room.  CHARSET_LIST (None for nil) holds the
/// charsets a character read is encoded in first.
pub(crate) fn ccl_driver<H: CclHost + ?Sized>(
    host: &mut H,
    ccl: &mut CclProgram,
    source: Option<&[i32]>,
    destination: Option<&mut [i32]>,
    charset_list: Option<&[String]>,
) -> Result<(), LispError> {
    let has_destination = destination.is_some();
    let mut driver = Driver {
        reg: ccl.reg,
        ic: ccl.ic,
        code: 0,
        prog: ccl.prog.clone(),
        source,
        src: 0,
        // We can't read or produce any bytes.
        dst: (has_destination && ccl.buf_magnification != 0).then_some(0),
        destination,
        stack_idx: ccl.stack_idx,
        this_ic: 0,
        eof_ic: ccl.eof_ic,
        main_eof_ic: ccl.eof_ic,
        last_block: ccl.last_block,
        status: ccl.status,
        mapping: [(0, 0); MAX_MAP_SET_LEVEL],
        sp: 0,
        charset_list,
        ncode: 0,
    };

    let error = loop {
        let interp = host.interp();
        if !interp.quit_flag_is_nil() && !interp.inhibit_quit_is_truthy() {
            // Not a signal here: stop as if the whole source were done,
            // leaving quit-flag for a safer place.
            if let Some(source) = driver.source {
                driver.src = source.len();
            }
            driver.status = CCL_STAT_QUIT;
            break true;
        }
        match driver.step(host) {
            Ok(()) | Err(Flow::Repeat) => {}
            Err(Flow::Finish) => break false,
            Err(Flow::Error) => break true,
            Err(Flow::Lisp(error)) => return Err(error),
        }
    };

    if error && has_destination {
        // The message goes in where there is room for it.
        let dst = driver.dst.unwrap_or(0);
        let message = match driver.status {
            CCL_STAT_INVALID_CMD => format!(
                "\nCCL: Invalid command {:x} (ccl_code = {:x}) at {}.",
                driver.code as u32 & 0x1F,
                driver.code as u32,
                driver.this_ic
            ),
            CCL_STAT_QUIT if ccl.quit_silently => String::new(),
            CCL_STAT_QUIT => "\nCCL: Quitted.".to_string(),
            status => format!("\nCCL: Unknown error type ({status})"),
        };
        let destination = driver.destination.as_deref_mut().expect("a destination");
        let mut end = dst;
        if message.len() <= destination.len() - dst {
            for byte in message.bytes() {
                destination[end] = i32::from(byte);
                end += 1;
            }
        }
        driver.dst = Some(end);
        if driver.status == CCL_STAT_INVALID_CMD
            && let Some(source) = driver.source
        {
            // Everything counts as consumed.
            driver.src = source.len();
        }
    }

    ccl.reg = driver.reg;
    ccl.ic = driver.ic;
    ccl.stack_idx = driver.stack_idx;
    ccl.prog = driver.prog;
    ccl.status = driver.status;
    ccl.consumed = if driver.source.is_some() {
        driver.src
    } else {
        0
    };
    ccl.produced = driver.dst.unwrap_or(0);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vector(items: impl IntoIterator<Item = Value>) -> Value {
        Value::vector(items)
    }

    #[test]
    fn pgg_crc24_program_updates_status_like_gnu_ccl() {
        let mut interp = Interpreter::new();
        let program = vector(
            [
                1, 30, 14, 114744, 114775, 0, 161, 131127, 1, 148217, 15, 82167, 1, 1848, 131159,
                1, 1595, 5, 256, 114743, 390, 114775, 19707, 1467, 16, 7, 183, 1, -5628, -7164, 22,
            ]
            .into_iter()
            .map(Value::Integer),
        );
        let status = vector(
            [Value::Nil, Value::Integer(183), Value::Integer(1230)]
                .into_iter()
                .chain(std::iter::repeat_n(Value::Nil, 6)),
        );
        let result = ccl_execute_on_string(
            &mut interp,
            &program,
            &status,
            &Value::String("foo".into()),
            false,
            false,
            &mut crate::lisp::types::Env::new(),
        )
        .expect("execute GNU's pgg CRC24 program");

        assert_eq!(result, Value::String(String::new().into()));
        let values = vector_items(&status).expect("status vector");
        assert_eq!(
            values,
            vec![
                Value::Integer(-1),
                Value::Integer(79),
                Value::Integer(687_194_709),
                Value::Integer(0),
                Value::Integer(0),
                Value::Integer(7),
                Value::Integer(0),
                Value::Integer(0),
                Value::Integer(30),
            ]
        );
    }

    #[test]
    fn symbolic_hash_operand_resolves_when_the_table_is_defined() {
        let mut interp = Interpreter::new();
        let table = json::make_hash_table(
            &mut interp,
            "eq",
            vec![(Value::Integer(17), Value::Integer(16))],
        );
        interp.set_global_binding(
            "translation-hash-table-vector",
            vector([Value::cons(Value::Symbol("table".into()), table)]),
        );
        interp.put_symbol_property("table", "translation-hash-table-id", Value::Integer(0));
        let program = vector([
            Value::Integer(2),
            Value::Integer(4),
            Value::Integer(311_359),
            Value::cons(
                Value::Symbol("table".into()),
                Value::Symbol("translation-hash-table-id".into()),
            ),
            Value::Integer(22),
        ]);
        let registers = vector(
            [Value::Integer(17)]
                .into_iter()
                .chain(std::iter::repeat_n(Value::Integer(0), 7)),
        );

        ccl_execute(
            &mut interp,
            &program,
            &registers,
            &mut crate::lisp::types::Env::new(),
        )
        .expect("execute lookup-integer");
        assert_eq!(
            vector_items(&registers).expect("register vector"),
            vec![
                Value::Integer(2),
                Value::Integer(16),
                Value::Integer(0),
                Value::Integer(0),
                Value::Integer(0),
                Value::Integer(0),
                Value::Integer(0),
                Value::Integer(1),
            ]
        );
    }
}
