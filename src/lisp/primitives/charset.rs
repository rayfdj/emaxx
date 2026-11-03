//! charset.c's Lisp interface: Fdefine_charset_internal's checks of a
//! definition and the primitives that read and change the charset table,
//! its aliases, the ordered list and ISO_CHARSET_TABLE.  The table itself
//! (the interpreter's charset registrations and charset_table.rs's struct
//! charset) is unchanged by a charset's plist.

use super::*;
use crate::lisp::primitives::coding::charset_table::{self, CharsetInfo, charset_info};
use crate::lisp::types::Kind;

const CHARSET_ARG_MAX: usize = 17;
const MAX_CHAR: u32 = 0x3F_FFFF;

fn args_out_of_range_3(a: Value, b: Value, c: Value) -> LispError {
    LispError::SignalValue(Value::list([Value::symbol("args-out-of-range"), a, b, c]))
}

/// check_integer_range: X an integer from LO to HI.
fn check_integer_range(x: &Value, lo: i64, hi: i64) -> Result<i64, LispError> {
    match x.kind() {
        Kind::Integer(n) if (lo..=hi).contains(&n) => Ok(n),
        Kind::Integer(_) | Kind::BigInteger(_) => Err(args_out_of_range_3(
            *x,
            Value::Integer(lo),
            Value::Integer(hi),
        )),
        _ => Err(wrong_type_argument("integerp", *x)),
    }
}

fn check_fixnum(x: &Value) -> Result<i64, LispError> {
    match x.kind() {
        Kind::Integer(n) => Ok(n),
        _ => Err(wrong_type_argument("fixnump", *x)),
    }
}

fn check_fixnat(x: &Value) -> Result<i64, LispError> {
    match x.kind() {
        Kind::Integer(n) if n >= 0 => Ok(n),
        _ => Err(wrong_type_argument("wholenump", *x)),
    }
}

fn check_character(x: &Value) -> Result<u32, LispError> {
    match x.kind() {
        Kind::Integer(n) if (0..=i64::from(MAX_CHAR)).contains(&n) => Ok(n as u32),
        _ => Err(wrong_type_argument("characterp", *x)),
    }
}

/// CHECK_CHARSET_GET_ID / CHECK_CHARSET_GET_ATTR: the canonical name of
/// the charset the symbol X names.
pub(crate) fn check_charset_get_id(interp: &Interpreter, x: &Value) -> Result<String, LispError> {
    let name = match x.kind() {
        Kind::Nil => "nil".to_string(),
        Kind::T => "t".to_string(),
        Kind::Symbol(name) => name.to_string(),
        _ => return Err(wrong_type_argument("charsetp", *x)),
    };
    interp
        .charset_canonical_name(&name)
        .ok_or_else(|| wrong_type_argument("charsetp", *x))
}

/// cons_to_unsigned with UINT_MAX.
fn code_point(x: &Value) -> Result<u32, LispError> {
    super::system::cons_to_unsigned(x, u64::from(u32::MAX)).map(|code| code as u32)
}

/// struct charset's code space as Fdefine_charset_internal builds it:
/// each byte's (min, max, count) and the running products.
struct CodeSpace {
    bytes: [i64; 16],
    linear: bool,
    min_code: u32,
    max_code: u32,
    char_index_offset: i64,
}

impl CodeSpace {
    fn byte_in(&self, byte: u32, dimension: usize) -> bool {
        let (min, max) = (self.bytes[dimension * 4], self.bytes[dimension * 4 + 1]);
        (min..=max).contains(&i64::from(byte))
    }

    /// CODE_POINT_TO_INDEX.
    fn index(&self, code: u32) -> i64 {
        if self.linear {
            return i64::from(code.wrapping_sub(self.min_code) as i32);
        }
        let b = |shift: u32| (code >> shift) & 0xFF;
        if !(self.byte_in(code >> 24, 3)
            && self.byte_in(b(16), 2)
            && self.byte_in(b(8), 1)
            && self.byte_in(b(0), 0))
        {
            return -1;
        }
        ((i64::from(code >> 24) - self.bytes[12]) * self.bytes[11])
            + ((i64::from(b(16)) - self.bytes[8]) * self.bytes[7])
            + ((i64::from(b(8)) - self.bytes[4]) * self.bytes[3])
            + (i64::from(b(0)) - self.bytes[0])
            - self.char_index_offset
    }
}

/// The charset map MAP checked as load_charset does when a map charset is
/// defined: a vector's even length and characters, a file's presence on
/// `charset-map-path'.
fn check_charset_map(
    interp: &mut Interpreter,
    map: &Value,
    env: &mut Env,
) -> Result<(), LispError> {
    if map.is_string() {
        let name = string_text(map)?;
        if charset_table::charset_map_file(interp, &name).is_none() {
            return Err(LispError::SignalValue(Value::list([
                Value::symbol("file-missing"),
                Value::string("Loading charset map"),
                Value::string("No such file or directory"),
                *map,
            ])));
        }
        return Ok(());
    }
    let items = vector_items(map)?;
    if items.len() % 2 == 1 {
        // add_to_log ("Failure in loading charset map: %V", vec), whose
        // Fformat_message rejects the %V.
        return Err(LispError::Signal("Invalid format operation %V".into()));
    }
    for pair in items.chunks(2) {
        check_fixnat(&pair[1])?;
    }
    let _ = env;
    Ok(())
}

/// Fdefine_charset_internal.
pub(crate) fn define_charset_internal(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    if args.len() < CHARSET_ARG_MAX {
        return Err(LispError::WrongNumberOfArgs(
            "define-charset-internal".into(),
            args.len(),
        ));
    }
    if args.len() != CHARSET_ARG_MAX {
        // Fsignal (Qwrong_number_of_arguments, Fcons (Qdefine_charset_internal,
        // make_fixnum (nargs))).
        return Err(LispError::SignalValue(Value::cons(
            Value::symbol("wrong-number-of-arguments"),
            Value::cons(
                Value::symbol("define-charset-internal"),
                Value::Integer(args.len() as i64),
            ),
        )));
    }
    let name = args[0];
    if !matches!(name.kind(), Kind::Nil | Kind::T | Kind::Symbol(_)) {
        return Err(wrong_type_argument("symbolp", name));
    }

    // The code space: four (min, max) byte pairs.
    let space = args[2];
    let mut bytes = [0i64; 16];
    let mut dimension = 0;
    let mut nchars: i64 = 1;
    for i in 0..4 {
        let min_obj = super::call(interp, "aref", &[space, Value::Integer(i as i64 * 2)], env)?;
        let max_obj = super::call(
            interp,
            "aref",
            &[space, Value::Integer(i as i64 * 2 + 1)],
            env,
        )?;
        let min_byte = check_integer_range(&min_obj, 0, 255)?;
        let max_byte = check_integer_range(&max_obj, min_byte, 255)?;
        bytes[i * 4] = min_byte;
        bytes[i * 4 + 1] = max_byte;
        bytes[i * 4 + 2] = max_byte - min_byte + 1;
        if max_byte > 0 {
            dimension = i as i64 + 1;
        }
        if i == 3 {
            break;
        }
        nchars *= bytes[i * 4 + 2];
        bytes[i * 4 + 3] = nchars;
    }
    let dimension = if args[1].is_nil() {
        dimension
    } else {
        check_integer_range(&args[1], 1, 4)?
    };
    let linear = dimension == 1
        || (bytes[2] == 256
            && (dimension == 2 || (bytes[6] == 256 && (dimension == 3 || bytes[10] == 256))));
    let corner = |at: usize| {
        (bytes[at] as u32)
            | ((bytes[at + 4] as u32) << 8)
            | ((bytes[at + 8] as u32) << 16)
            | ((bytes[at + 12] as u32) << 24)
    };
    let mut cs = CodeSpace {
        bytes,
        linear,
        min_code: corner(0),
        max_code: corner(1),
        char_index_offset: 0,
    };
    let range_error = |cs: &CodeSpace, value: Value| {
        args_out_of_range_3(
            Value::Integer(i64::from(cs.min_code)),
            Value::Integer(i64::from(cs.max_code)),
            value,
        )
    };
    if !args[3].is_nil() {
        let code = code_point(&args[3])?;
        if code < cs.min_code || code > cs.max_code {
            return Err(range_error(&cs, args[3]));
        }
        cs.char_index_offset = cs.index(code);
        cs.min_code = code;
    }
    if !args[4].is_nil() {
        let code = code_point(&args[4])?;
        if code < cs.min_code || code > cs.max_code {
            return Err(range_error(&cs, args[4]));
        }
        cs.max_code = code;
    }
    let invalid_code = if args[10].is_nil() {
        if cs.min_code > 0 {
            0
        } else if cs.max_code < u32::MAX {
            cs.max_code + 1
        } else {
            return Err(LispError::Signal(
                "Attribute :invalid-code must be specified".into(),
            ));
        }
    } else {
        code_point(&args[10])?
    };
    let iso_final = if args[5].is_nil() {
        Value::Nil
    } else {
        let value = check_fixnum(&args[5])?;
        if value < i64::from(b'0') || value > 127 {
            return Err(LispError::Signal(format!(
                "Invalid iso-final-char: {value}"
            )));
        }
        Value::Integer(value)
    };
    let iso_revision = if args[6].is_nil() {
        -1
    } else {
        check_integer_range(&args[6], -1, 63)?
    };
    let emacs_mule_id = if args[7].is_nil() {
        Value::Nil
    } else {
        let value = check_fixnat(&args[7])?;
        if (value > 0 && value <= 128) || value >= 256 {
            return Err(LispError::Signal(format!("Invalid emacs-mule-id: {value}")));
        }
        Value::Integer(value)
    };

    // The method: a code offset, a map, a subset or a superset.
    let mut code_offset = Value::Nil;
    let mut map = Value::Nil;
    let mut subset = Value::Nil;
    let mut superset = Value::Nil;
    if !args[11].is_nil() {
        let offset = check_character(&args[11])?;
        let index = cs.index(cs.max_code);
        if i64::from(MAX_CHAR) - i64::from(offset) < index {
            // The C's charset.max_char is still zero here.
            return Err(LispError::Signal("Unsupported max char: 0".into()));
        }
        code_offset = Value::Integer(i64::from(offset));
    } else if !args[12].is_nil() {
        map = args[12];
    } else if !args[13].is_nil() {
        let val = args[13];
        let parent = super::call(interp, "car", &[val], env)?;
        let parent = check_charset_get_id(interp, &parent)?;
        let parent_min = super::call(interp, "nth", &[Value::Integer(1), val], env)?;
        check_fixnat(&parent_min)?;
        let parent_max = super::call(interp, "nth", &[Value::Integer(2), val], env)?;
        check_fixnat(&parent_max)?;
        let parent_offset = super::call(interp, "nth", &[Value::Integer(3), val], env)?;
        check_fixnum(&parent_offset)?;
        subset = Value::list([
            Value::symbol(&parent),
            parent_min,
            parent_max,
            parent_offset,
        ]);
    } else if !args[14].is_nil() {
        let mut val = super::call(interp, "copy-sequence", &[args[14]], env)?;
        let mut members = Vec::new();
        while !val.is_nil() {
            let elt = super::call(interp, "car", &[val], env)?;
            let member = if let Some((car, cdr)) = elt.cons_values() {
                let id = check_charset_get_id(interp, &car)?;
                let offset = check_integer_range(&cdr, i64::from(i32::MIN), i64::from(i32::MAX))?;
                Value::cons(Value::symbol(&id), Value::Integer(offset))
            } else {
                let id = check_charset_get_id(interp, &elt)?;
                Value::cons(Value::symbol(&id), Value::Integer(0))
            };
            members.push(member);
            val = super::call(interp, "cdr", &[val], env)?;
        }
        superset = Value::list(members);
    } else {
        return Err(LispError::Signal(
            "None of :code-offset, :map, :parents are specified".into(),
        ));
    }

    let unify_map = args[15];
    if !unify_map.is_nil() && !unify_map.is_string() && !is_vector_value(&unify_map) {
        return Err(wrong_type_argument("vectorp", unify_map));
    }
    let plist = args[16];
    if !plist.is_nil() && plist.cons_values().is_none() {
        return Err(wrong_type_argument("listp", plist));
    }

    let flag = |value: &Value| if value.is_nil() { Value::Nil } else { Value::T };
    let mut space_items = [Value::Nil; 8];
    for (i, item) in space_items.iter_mut().enumerate() {
        *item = Value::Integer(cs.bytes[(i / 2) * 4 + i % 2]);
    }
    let mut def = vec![Value::Nil; charset_table::DEF_SLOTS];
    def[charset_table::DEF_DIMENSION] = Value::Integer(dimension);
    def[charset_table::DEF_CODE_SPACE] = Value::vector(space_items);
    def[charset_table::DEF_MIN_CODE] = Value::Integer(i64::from(cs.min_code));
    def[charset_table::DEF_MAX_CODE] = Value::Integer(i64::from(cs.max_code));
    def[charset_table::DEF_ISO_FINAL] = iso_final;
    def[charset_table::DEF_ISO_REVISION] = Value::Integer(iso_revision);
    def[charset_table::DEF_EMACS_MULE_ID] = emacs_mule_id;
    def[charset_table::DEF_ASCII_COMPATIBLE_P] = flag(&args[8]);
    def[charset_table::DEF_SUPPLEMENTARY_P] = flag(&args[9]);
    def[charset_table::DEF_INVALID_CODE] = Value::Integer(i64::from(invalid_code));
    def[charset_table::DEF_CODE_OFFSET] = code_offset;
    def[charset_table::DEF_MAP] = map;
    def[charset_table::DEF_SUBSET] = subset;
    def[charset_table::DEF_SUPERSET] = superset;
    def[charset_table::DEF_UNIFY_MAP] = unify_map;
    let name = name.symbol_str().map(str::to_string).unwrap_or_else(|| {
        if name.is_nil() {
            "nil".to_string()
        } else {
            "t".to_string()
        }
    });
    let def = Value::vector(def);
    // The hash table and charset_table hold the charset before a map
    // charset's map loads (a failing load leaves it registered).
    let (id, new_definition) = interp.register_charset(&name, def, plist);
    if !map.is_nil() {
        check_charset_map(interp, &map, env)?;
    }
    interp.finish_charset_definition(&name, id, new_definition, def);
    Ok(Value::Nil)
}

/// Fdefine_charset_alias.
pub(crate) fn define_charset_alias(
    interp: &mut Interpreter,
    alias: &Value,
    charset: &Value,
) -> Result<Value, LispError> {
    let target = check_charset_get_id(interp, charset)?;
    let alias = match alias.kind() {
        Kind::Nil => "nil".to_string(),
        Kind::T => "t".to_string(),
        Kind::Symbol(name) => name.to_string(),
        // Fputhash takes any key, which no charset lookup reaches (they
        // want a symbol); `charset-list' gains it all the same.
        _ => {
            interp.push_charset_list(*alias);
            return Ok(Value::Nil);
        }
    };
    interp.define_charset_alias(&alias, &target)?;
    Ok(Value::Nil)
}

/// Fset_charset_plist.
pub(crate) fn set_charset_plist(
    interp: &mut Interpreter,
    charset: &Value,
    plist: Value,
) -> Result<Value, LispError> {
    let name = check_charset_get_id(interp, charset)?;
    interp.set_charset_plist_value(&name, plist)?;
    Ok(plist)
}

/// Funify_charset.
pub(crate) fn unify_charset(
    interp: &mut Interpreter,
    charset: &Value,
    unify_map: Option<&Value>,
    deunify: bool,
) -> Result<Value, LispError> {
    let name = check_charset_get_id(interp, charset)?;
    let unified = interp.charset_is_unified(&name);
    let deunifier = interp
        .charset_def_value(&name)
        .and_then(|def| crate::lisp::primitives::vector_or_list_items(&def).ok())
        .and_then(|slots| slots.get(charset_table::DEF_DEUNIFIER).copied())
        .is_some_and(|deunifier| !deunifier.is_nil());
    if if deunify {
        !unified
    } else {
        unified && deunifier
    } {
        return Ok(Value::Nil);
    }
    // CHARSET_UNIFIED_P is cleared before the checks below, which leave it
    // so when they signal.
    interp.set_charset_unified(&name, false);
    if deunify {
        return Ok(Value::Nil);
    }
    let info = charset_info(interp, &name);
    let offset = info.as_ref().and_then(|info| info.code_offset);
    if info.as_ref().is_none_or(|info| info.has_direct_map) || offset.is_none_or(|o| o < 0x11_0000)
    {
        let shown = charset.symbol_str().unwrap_or("nil").to_string();
        return Err(LispError::Signal(format!("Can't unify charset: {shown}")));
    }
    if let Some(map) = unify_map.filter(|map| !map.is_nil()) {
        if !map.is_string() && !is_vector_value(map) {
            return Err(LispError::SignalValue(Value::list([
                Value::symbol("error"),
                Value::string("Bad unify-map"),
                *map,
            ])));
        }
        interp.set_charset_def_slot(&name, charset_table::DEF_UNIFY_MAP, *map);
    }
    interp.set_charset_unified(&name, true);
    Ok(Value::Nil)
}

/// check_iso_charset_parameter: whether the set is of 96 characters.
fn check_iso_charset_parameter(
    dimension: &Value,
    chars: &Value,
    final_char: &Value,
) -> Result<(i64, bool, u32), LispError> {
    let dimension = check_fixnum(dimension)?;
    let chars = check_fixnum(chars)?;
    let final_char = check_character(final_char)?;
    if !(1..=3).contains(&dimension) {
        return Err(LispError::Signal(format!(
            "Invalid DIMENSION {dimension}, it should be 1, 2, or 3"
        )));
    }
    let chars_96 = chars == 96;
    if !(chars_96 || chars == 94) {
        return Err(LispError::Signal(format!(
            "Invalid CHARS {chars}, it should be 94 or 96"
        )));
    }
    if !(u32::from(b'0')..=u32::from(b'~')).contains(&final_char) {
        let shown = char::from_u32(final_char).unwrap_or('\u{FFFD}');
        return Err(LispError::Signal(format!(
            "Invalid FINAL-CHAR `{shown}', it should be `0'..`~'"
        )));
    }
    Ok((dimension, chars_96, final_char))
}

/// Fget_unused_iso_final_char.
pub(crate) fn get_unused_iso_final_char(
    interp: &Interpreter,
    dimension: &Value,
    chars: &Value,
) -> Result<Value, LispError> {
    let (dimension, chars_96, _) =
        check_iso_charset_parameter(dimension, chars, &Value::Integer(i64::from(b'0')))?;
    let chars = if chars_96 { 96 } else { 94 };
    Ok((u32::from(b'0')..=u32::from(b'?'))
        .find(|&final_char| interp.iso_charset(dimension, chars, final_char).is_none())
        .map_or(Value::Nil, |final_char| {
            Value::Integer(i64::from(final_char))
        }))
}

/// Fdeclare_equiv_charset.
pub(crate) fn declare_equiv_charset(
    interp: &mut Interpreter,
    args: &[Value],
) -> Result<Value, LispError> {
    let charset = check_charset_get_id(interp, &args[3])?;
    let (dimension, chars_96, final_char) =
        check_iso_charset_parameter(&args[0], &args[1], &args[2])?;
    interp.declare_iso_charset(
        dimension,
        if chars_96 { 96 } else { 94 },
        final_char,
        &charset,
    );
    Ok(Value::Nil)
}

/// Fiso_charset.
pub(crate) fn iso_charset(interp: &Interpreter, args: &[Value]) -> Result<Value, LispError> {
    let (dimension, chars_96, final_char) =
        check_iso_charset_parameter(&args[0], &args[1], &args[2])?;
    Ok(interp
        .iso_charset(dimension, if chars_96 { 96 } else { 94 }, final_char)
        .map_or(Value::Nil, |name| Value::symbol(&name)))
}

/// Fset_charset_priority.
pub(crate) fn set_charset_priority(
    interp: &mut Interpreter,
    args: &[Value],
) -> Result<Value, LispError> {
    if args.is_empty() {
        return Err(LispError::WrongNumberOfArgs(
            "set-charset-priority".into(),
            0,
        ));
    }
    let mut names = Vec::with_capacity(args.len());
    for arg in args {
        names.push(check_charset_get_id(interp, arg)?);
    }
    interp.set_charset_priority(&names);
    Ok(Value::Nil)
}

/// Fsort_charsets: CHARSETS sorted in place by the ordered list.
pub(crate) fn sort_charsets(
    interp: &mut Interpreter,
    charsets: &Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    // list_length: FOR_EACH_TAIL, then CHECK_LIST_END of the tail it
    // stopped at.
    let mut cells = Vec::new();
    let mut tail = *charsets;
    while let Some((elt, rest)) = tail.cons_values() {
        cells.push((tail, elt));
        tail = rest;
    }
    if !tail.is_nil() {
        return Err(wrong_type_argument("listp", tail));
    }
    if cells.is_empty() {
        return Ok(Value::Nil);
    }
    let mut keyed = Vec::with_capacity(cells.len());
    for (_, elt) in &cells {
        let name = check_charset_get_id(interp, elt)?;
        keyed.push((interp.charset_priority_rank(&name), *elt));
    }
    keyed.sort_by_key(|(rank, _)| *rank);
    for ((cell, _), (_, elt)) in cells.iter().zip(keyed) {
        super::call(interp, "setcar", &[*cell, elt], env)?;
    }
    Ok(*charsets)
}

/// Fdecode_char.
pub(crate) fn decode_char(
    interp: &Interpreter,
    charset: &Value,
    code: &Value,
) -> Result<Value, LispError> {
    let name = check_charset_get_id(interp, charset)?;
    let code = code_point(code)?;
    Ok(super::coding::decode_charset_code(interp, &name, code)
        .map_or(Value::Nil, |c| Value::Integer(i64::from(c))))
}

/// Fencode_char: nil for CHARSET_INVALID_CODE as for no code.
pub(crate) fn encode_char(
    interp: &Interpreter,
    ch: &Value,
    charset: &Value,
) -> Result<Value, LispError> {
    let name = check_charset_get_id(interp, charset)?;
    let c = check_character(ch)?;
    let invalid =
        charset_info(interp, &name).and_then(|info: std::rc::Rc<CharsetInfo>| info.invalid_code);
    Ok(match super::coding::encode_charset_char(interp, &name, c) {
        Some(code) if Some(code) != invalid => Value::Integer(i64::from(code)),
        _ => Value::Nil,
    })
}
