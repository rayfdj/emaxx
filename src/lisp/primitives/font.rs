//! font.c's font-spec model: the weight, slant and width tables and the
//! numbers font_style_to_value stores for them, the property validators,
//! font_put_extra's sorted extra alist, the XLFD and fontconfig name
//! parsers with the cairo X build's filter_properties, the XLFD
//! unparsers and font_match_p.  A font is a Font record of FONT_SPEC_MAX
//! slots whose symbol type is `font-spec', `font-entity' or
//! `font-object'; only font-specs are ever made, no frame having a font
//! driver.

use super::*;
use crate::lisp::types::{Kind, SymbolName};

pub(crate) const FONT_TYPE_INDEX: usize = 0;
pub(crate) const FONT_FOUNDRY_INDEX: usize = 1;
pub(crate) const FONT_FAMILY_INDEX: usize = 2;
pub(crate) const FONT_ADSTYLE_INDEX: usize = 3;
pub(crate) const FONT_REGISTRY_INDEX: usize = 4;
pub(crate) const FONT_WEIGHT_INDEX: usize = 5;
pub(crate) const FONT_SLANT_INDEX: usize = 6;
pub(crate) const FONT_WIDTH_INDEX: usize = 7;
pub(crate) const FONT_SIZE_INDEX: usize = 8;
pub(crate) const FONT_DPI_INDEX: usize = 9;
pub(crate) const FONT_SPACING_INDEX: usize = 10;
pub(crate) const FONT_AVGWIDTH_INDEX: usize = 11;
pub(crate) const FONT_EXTRA_INDEX: usize = 12;
pub(crate) const FONT_SPEC_MAX: usize = 13;

const FONT_SPACING_PROPORTIONAL: i64 = 0;
const FONT_SPACING_DUAL: i64 = 90;
const FONT_SPACING_MONO: i64 = 100;
const FONT_SPACING_CHARCELL: i64 = 110;

/// font_property_table's keys, in its order: the FONT_EXTRA_INDEX
/// properties first, then :lang, :script and :otf.
const FONT_PROPERTY_KEYS: [&str; 15] = [
    ":type",
    ":foundry",
    ":family",
    ":adstyle",
    ":registry",
    ":weight",
    ":slant",
    ":width",
    ":size",
    ":dpi",
    ":spacing",
    ":avgwidth",
    ":lang",
    ":script",
    ":otf",
];

/// The kinds of PVEC_FONT: FONT_SPEC_P, FONT_ENTITY_P, FONT_OBJECT_P.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum FontKind {
    Spec,
    Entity,
    Object,
}

/// VALUE's kind of font, or None when it is no font (FONTP).
pub(crate) fn font_kind(interp: &Interpreter, value: &Value) -> Option<FontKind> {
    let Kind::Record(id) = value.kind() else {
        return None;
    };
    let record = interp
        .find_record(id)
        .filter(|record| record.kind == crate::lisp::eval::RecordKind::Font)?;
    Some(match record.symbol_type_name() {
        Some("font-entity") => FontKind::Entity,
        Some("font-object") => FontKind::Object,
        _ => FontKind::Spec,
    })
}

fn check_font_kind(
    interp: &Interpreter,
    value: &Value,
    kind: Option<FontKind>,
    predicate: &str,
) -> Result<(), LispError> {
    match font_kind(interp, value) {
        Some(found) if kind.is_none_or(|kind| kind == found) => Ok(()),
        _ => Err(wrong_type_argument(predicate, *value)),
    }
}

/// font.h's CHECK_FONT.
pub(crate) fn check_font(interp: &Interpreter, value: &Value) -> Result<(), LispError> {
    check_font_kind(interp, value, None, "font")
}

/// font.h's CHECK_FONT_SPEC.
pub(crate) fn check_font_spec(interp: &Interpreter, value: &Value) -> Result<(), LispError> {
    check_font_kind(interp, value, Some(FontKind::Spec), "font-spec")
}

/// font.h's CHECK_FONT_ENTITY.
pub(crate) fn check_font_entity(interp: &Interpreter, value: &Value) -> Result<(), LispError> {
    check_font_kind(interp, value, Some(FontKind::Entity), "font-entity")
}

/// font.h's CHECK_FONT_OBJECT.
pub(crate) fn check_font_object(interp: &Interpreter, value: &Value) -> Result<(), LispError> {
    check_font_kind(interp, value, Some(FontKind::Object), "font-object")
}

/// AREF (FONT, INDEX) of a font known to be one.
pub(crate) fn aref(interp: &Interpreter, font: &Value, index: usize) -> Value {
    let Kind::Record(id) = font.kind() else {
        return Value::Nil;
    };
    interp
        .find_record(id)
        .and_then(|record| record.slots.get(index).copied())
        .unwrap_or(Value::Nil)
}

/// ASET (FONT, INDEX, VALUE) of a font known to be one.
pub(crate) fn aset(interp: &mut Interpreter, font: &Value, index: usize, value: Value) {
    let Kind::Record(id) = font.kind() else {
        unreachable!("a font is a record");
    };
    let record = interp.find_record_mut(id).expect("a live font record");
    if record.slots.len() < FONT_SPEC_MAX {
        record.slots.resize(FONT_SPEC_MAX, Value::Nil);
    }
    record.slots[index] = value;
}

/// font_make_spec: a font-spec with every property nil.
pub(crate) fn font_make_spec(interp: &mut Interpreter) -> Value {
    interp.create_pseudovector(
        crate::lisp::eval::RecordKind::Font,
        "font-spec",
        vec![Value::Nil; FONT_SPEC_MAX],
    )
}

fn symbol_name_text(value: &Value) -> Option<String> {
    match value.kind() {
        Kind::Nil => Some("nil".into()),
        Kind::T => Some("t".into()),
        Kind::Symbol(symbol) => Some(symbol.as_str().to_string()),
        _ => None,
    }
}

/// SDATA (SYMBOL_NAME (SYMBOL)) of a symbol, as its internal bytes.
fn symbol_name_bytes(value: &Value) -> Option<Vec<u8>> {
    let text = symbol_name_text(value)?;
    let multibyte = !text.is_ascii();
    super::text::internal_text_bytes(&text, multibyte).ok()
}

fn fixnum(value: &Value) -> Option<i64> {
    match value.kind() {
        Kind::Integer(number) => Some(number),
        _ => None,
    }
}

fn check_fixnum(value: &Value) -> Result<i64, LispError> {
    fixnum(value).ok_or_else(|| wrong_type_argument("fixnump", *value))
}

fn is_symbol(value: &Value) -> bool {
    matches!(value.kind(), Kind::Nil | Kind::T | Kind::Symbol(_))
}

fn keyword(name: &str) -> Value {
    Value::symbol(name)
}

/// get_font_prop_index: KEY's place in font_property_table, or None.
/// Only a symbol (one with position too, while `symbols-with-pos-enabled'
/// is set) can be EQ to a key.
pub(crate) fn get_font_prop_index(key: &Value) -> Option<usize> {
    if !matches!(
        crate::lisp::types::maybe_remove_pos(*key).kind(),
        Kind::Symbol(_)
    ) {
        return None;
    }
    FONT_PROPERTY_KEYS
        .iter()
        .position(|name| crate::lisp::types::eq(*key, keyword(name)))
}

/// assq_no_quit: the first cons of LIST whose car is KEY, elements that
/// are no cons passed over.
fn assq_no_quit(key: &Value, list: Value) -> Option<Value> {
    let mut tail = list;
    while let Some((entry, rest)) = tail.cons_values() {
        if entry
            .cons_values()
            .is_some_and(|(car, _)| crate::lisp::types::eq(car, *key))
        {
            return Some(entry);
        }
        tail = rest;
    }
    None
}

// ---------------------------------------------------------------------
// The style tables.

const STYLE_TABLE_NAMES: [&str; 3] = ["font-weight-table", "font-slant-table", "font-width-table"];

/// font_style_table, made from the three tables' values (syms_of_font's
/// CALLN (Fvector, ...)) when a style is first registered in it.
fn font_style_table(interp: &mut Interpreter) -> Value {
    if interp.font_style_table.is_nil() {
        let tables = STYLE_TABLE_NAMES.map(|name| {
            interp
                .symbol_value_symbol(&SymbolName::intern_str(name))
                .unwrap_or(Value::Nil)
        });
        interp.font_style_table = Value::vector(tables);
    }
    interp.font_style_table
}

/// AREF (font_style_table, PROP - FONT_WEIGHT_INDEX) with CHECK_VECTOR:
/// the variable's value itself while no style has been registered.
fn style_table(interp: &Interpreter, prop: usize) -> Result<Vec<Value>, LispError> {
    let index = prop - FONT_WEIGHT_INDEX;
    let table = if interp.font_style_table.is_nil() {
        interp
            .symbol_value_symbol(&SymbolName::intern_str(STYLE_TABLE_NAMES[index]))
            .unwrap_or(Value::Nil)
    } else {
        vector_items(&interp.font_style_table)?
            .get(index)
            .copied()
            .unwrap_or(Value::Nil)
    };
    vector_items(&table)
}

/// c_strcasecmp's equality of two C strings: the bytes to the first NUL,
/// ASCII letters folded.
fn c_strcaseeq(a: &[u8], b: &[u8]) -> bool {
    let end = |bytes: &[u8]| bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    let (a, b) = (&a[..end(a)], &b[..end(b)]);
    a.eq_ignore_ascii_case(b)
}

/// font_style_to_value: the number of PROP's style VAL to store in a font
/// vector: (NUMERIC << 8) | (ENTRY << 4) | (NAME - 1).  An unknown symbol
/// is -1 unless NOERROR, which registers it in font_style_table as a
/// style of 100; a number not in the table is -1 unless NOERROR, which
/// answers the closest entry's.
pub(crate) fn font_style_to_value(
    interp: &mut Interpreter,
    prop: usize,
    val: &Value,
    noerror: bool,
) -> Result<i64, LispError> {
    let table = style_table(interp, prop)?;
    let len = table.len();
    // SYMBOLP and SYMBOL_NAME take a symbol with position while
    // `symbols-with-pos-enabled' is set; the table registers VAL itself.
    let symbol = crate::lisp::types::maybe_remove_pos(*val);
    if is_symbol(&symbol) {
        // At first try exact match.
        for (i, entry) in table.iter().enumerate() {
            let names = vector_items(entry)?;
            for (j, name) in names.iter().enumerate().skip(1) {
                if crate::lisp::types::eq(*val, *name) {
                    let numeric = check_fixnum(&names[0])?;
                    return Ok((numeric << 8) | ((i as i64) << 4) | (j as i64 - 1));
                }
            }
        }
        // Try also with case-folding match.
        let s = symbol_name_bytes(&symbol).unwrap_or_default();
        for (i, entry) in table.iter().enumerate() {
            let names = vector_items(entry)?;
            for (j, name) in names.iter().enumerate().skip(1) {
                if symbol_name_bytes(name).is_some_and(|name| c_strcaseeq(&s, &name)) {
                    let numeric = check_fixnum(&names[0])?;
                    return Ok((numeric << 8) | ((i as i64) << 4) | (j as i64 - 1));
                }
            }
        }
        if !noerror {
            return Ok(-1);
        }
        let elt = Value::vector([Value::Integer(100), *val]);
        let extended = Value::vector(table.iter().copied().chain([elt]));
        let tables = font_style_table(interp);
        aset_vector_value(&tables, prop - FONT_WEIGHT_INDEX, extended)?;
        return Ok((100 << 8) | ((len as i64) << 4));
    }
    let numeric = check_fixnum(val)?;
    let mut last_n = -1;
    for (i, entry) in table.iter().enumerate() {
        let names = vector_items(entry)?;
        let n = check_fixnum(names.first().unwrap_or(&Value::Nil))?;
        if numeric == n {
            return Ok((n << 8) | ((i as i64) << 4));
        }
        if numeric < n {
            if !noerror {
                return Ok(-1);
            }
            let i = i as i64;
            return Ok(if i == 0 || n - numeric < numeric - last_n {
                (n << 8) | (i << 4)
            } else {
                (last_n << 8) | ((i - 1) << 4)
            });
        }
        last_n = n;
    }
    if !noerror {
        return Ok(-1);
    }
    Ok((last_n << 8) | ((len as i64 - 1) << 4))
}

/// font_style_symbolic: the name of the style FONT's PROP holds, the
/// entry's first (face attribute) name when FOR_FACE.
pub(crate) fn font_style_symbolic(
    interp: &mut Interpreter,
    font: &Value,
    prop: usize,
    for_face: bool,
) -> Result<Value, LispError> {
    let val = aref(interp, font, prop);
    style_symbolic_of(interp, &val, prop, for_face)
}

/// font_style_symbolic of the slot value VAL.
pub(crate) fn style_symbolic_of(
    interp: &Interpreter,
    val: &Value,
    prop: usize,
    for_face: bool,
) -> Result<Value, LispError> {
    if val.is_nil() {
        return Ok(Value::Nil);
    }
    let table = style_table(interp, prop)?;
    let i = check_fixnum(val)? & 0xFF;
    let entry = table
        .get(((i >> 4) & 0xF) as usize)
        .copied()
        .ok_or_else(|| args_out_of_range(*val))?;
    let names = vector_items(&entry)?;
    let index = if for_face { 1 } else { (i & 0xF) as usize + 1 };
    let name = names
        .get(index)
        .copied()
        .ok_or_else(|| args_out_of_range(*val))?;
    if !is_symbol(&name) {
        return Err(wrong_type_argument("symbolp", name));
    }
    Ok(name)
}

fn args_out_of_range(value: Value) -> LispError {
    LispError::SignalValue(Value::list([Value::symbol("args-out-of-range"), value]))
}

/// FONT_WEIGHT_NAME_NUMERIC and its kin: PROP's numeric style of NAME,
/// or -1.
pub(crate) fn style_name_numeric(
    interp: &mut Interpreter,
    prop: usize,
    name: &Value,
) -> Result<i64, LispError> {
    Ok(font_style_to_value(interp, prop, name, false)? >> 8)
}

/// FONT_SET_STYLE.
fn font_set_style(
    interp: &mut Interpreter,
    font: &Value,
    prop: usize,
    val: &Value,
) -> Result<(), LispError> {
    let n = font_style_to_value(interp, prop, val, true)?;
    aset(interp, font, prop, Value::Integer(n));
    Ok(())
}

// ---------------------------------------------------------------------
// The validators.

/// Qerror, a validator's answer for an invalid value.
enum Validated {
    Value(Value),
    Error,
}

fn validate_symbol(
    interp: &mut Interpreter,
    prop: &Value,
    val: Value,
    env: &mut Env,
) -> Result<Validated, LispError> {
    let val = if val.is_string() {
        super::call(interp, "intern", &[val], env)?
    } else {
        val
    };
    if !is_symbol(&val) {
        return Ok(Validated::Error);
    }
    if crate::lisp::types::eq(*prop, keyword(":registry")) {
        let name = super::call(interp, "symbol-name", &[val], env)?;
        let down = super::call(interp, "downcase", &[name], env)?;
        return Ok(Validated::Value(super::call(
            interp,
            "intern",
            &[down],
            env,
        )?));
    }
    Ok(Validated::Value(val))
}

fn validate_style(
    interp: &mut Interpreter,
    style: &Value,
    val: Value,
) -> Result<Validated, LispError> {
    let prop = if crate::lisp::types::eq(*style, keyword(":weight")) {
        FONT_WEIGHT_INDEX
    } else if crate::lisp::types::eq(*style, keyword(":slant")) {
        FONT_SLANT_INDEX
    } else {
        FONT_WIDTH_INDEX
    };
    if let Some(n) = fixnum(&val) {
        let table = style_table(interp, prop)?;
        let Some(entry) = table.get(((n >> 4) & 0xF) as usize) else {
            return Ok(Validated::Error);
        };
        let names = vector_items(entry)?;
        if (n & 0xF) as usize + 1 >= names.len() {
            return Ok(Validated::Error);
        }
        if check_fixnum(&names[0])? != n >> 8 {
            return Ok(Validated::Error);
        }
        return Ok(Validated::Value(val));
    }
    if is_symbol(&val) {
        let n = font_style_to_value(interp, prop, &val, false)?;
        return Ok(if n >= 0 {
            Validated::Value(Value::Integer(n))
        } else {
            Validated::Error
        });
    }
    Ok(Validated::Error)
}

fn validate_non_neg(val: Value) -> Validated {
    match val.kind() {
        Kind::Integer(n) if n >= 0 => Validated::Value(val),
        Kind::Float(x) if x.get() >= 0.0 => Validated::Value(val),
        _ => Validated::Error,
    }
}

/// font_prop_validate_spacing.
fn validate_spacing(val: Value) -> Validated {
    match val.kind() {
        Kind::Nil => return Validated::Value(val),
        Kind::Integer(n) if (0..=FONT_SPACING_CHARCELL).contains(&n) => {
            return Validated::Value(val);
        }
        _ => {}
    }
    if let Some(name) = symbol_name_bytes(&val)
        && name.len() == 1
    {
        let spacing = match name[0] {
            b'c' | b'C' => FONT_SPACING_CHARCELL,
            b'm' | b'M' => FONT_SPACING_MONO,
            b'p' | b'P' => FONT_SPACING_PROPORTIONAL,
            b'd' | b'D' => FONT_SPACING_DUAL,
            _ => return Validated::Error,
        };
        return Validated::Value(Value::Integer(spacing));
    }
    Validated::Error
}

/// font_prop_validate_otf: (SCRIPT [LANGSYS [GSUB [GPOS]]]), the
/// feature lists proper lists of symbols.  As GNU's, the check meant for
/// LANGSYS tests SCRIPT again.
fn validate_otf(val: Value) -> Validated {
    let Some((script, mut tail)) = val.cons_values() else {
        return Validated::Error;
    };
    if !is_symbol(&script) {
        return Validated::Error;
    }
    if tail.is_nil() {
        return Validated::Value(val);
    }
    if tail.cons_values().is_none() {
        return Validated::Error;
    }
    for _ in 0..2 {
        tail = tail.cons_values().map_or(Value::Nil, |(_, rest)| rest);
        if tail.is_nil() {
            return Validated::Value(val);
        }
        let Some((features, _)) = tail.cons_values() else {
            return Validated::Error;
        };
        let mut tmp = features;
        while let Some((feature, rest)) = tmp.cons_values() {
            if !is_symbol(&feature) {
                return Validated::Error;
            }
            tmp = rest;
        }
        if !tmp.is_nil() {
            return Validated::Error;
        }
    }
    Validated::Value(val)
}

/// font_prop_validate: VAL regularized for the property named PROP, or
/// for font_property_table's IDXth when PROP is nil; a property that is
/// no known one is any value.  An invalid value signals.
pub(crate) fn font_prop_validate(
    interp: &mut Interpreter,
    idx: usize,
    prop: Value,
    val: Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    if val.is_nil() {
        return Ok(val);
    }
    let (idx, prop) = if prop.is_nil() {
        (idx, keyword(FONT_PROPERTY_KEYS[idx]))
    } else {
        match get_font_prop_index(&prop) {
            Some(idx) => (idx, prop),
            None => return Ok(val),
        }
    };
    let validated = match idx {
        FONT_TYPE_INDEX..=FONT_REGISTRY_INDEX | 12 | 13 => {
            validate_symbol(interp, &prop, val, env)?
        }
        FONT_WEIGHT_INDEX..=FONT_WIDTH_INDEX => validate_style(interp, &prop, val)?,
        FONT_SIZE_INDEX | FONT_DPI_INDEX | FONT_AVGWIDTH_INDEX => validate_non_neg(val),
        FONT_SPACING_INDEX => validate_spacing(val),
        _ => validate_otf(val),
    };
    match validated {
        Validated::Value(value) => Ok(value),
        Validated::Error => Err(signal_error(
            "invalid font property",
            Value::cons(prop, val),
        )),
    }
}

/// font_put_extra: VAL as FONT's extra property PROP, a new entry kept in
/// `string-lessp' order of the keys; VAL None (Qunbound) deletes PROP's.
pub(crate) fn font_put_extra(
    interp: &mut Interpreter,
    font: &Value,
    prop: Value,
    val: Option<Value>,
    env: &mut Env,
) -> Result<(), LispError> {
    let extra = aref(interp, font, FONT_EXTRA_INDEX);
    let slot = if extra.is_nil() {
        None
    } else {
        assq_no_quit(&prop, extra)
    };
    let Some(slot) = slot else {
        let Some(val) = val else {
            return Ok(());
        };
        let mut prev: Option<Value> = None;
        let mut tail = extra;
        while let Some((entry, rest)) = tail.cons_values() {
            let key = entry
                .cons_values()
                .map(|(key, _)| key)
                .ok_or_else(|| wrong_type_argument("listp", entry))?;
            if !super::call(interp, "string-lessp", &[prop, key], env)?.is_nil() {
                break;
            }
            prev = Some(tail);
            tail = rest;
        }
        let cell = Value::cons(Value::cons(prop, val), tail);
        match prev {
            None => aset(interp, font, FONT_EXTRA_INDEX, cell),
            Some(prev) => prev.set_cdr(cell)?,
        }
        return Ok(());
    };
    match val {
        Some(val) => slot.set_cdr(val)?,
        None => {
            // Fdelq (SLOT, EXTRA).
            let mut head = extra;
            let mut prev: Option<Value> = None;
            let mut tail = extra;
            while let Some((entry, rest)) = tail.cons_values() {
                if entry.eq_value(slot) {
                    match prev {
                        None => head = rest,
                        Some(prev) => prev.set_cdr(rest)?,
                    }
                } else {
                    prev = Some(tail);
                }
                tail = rest;
            }
            aset(interp, font, FONT_EXTRA_INDEX, head);
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------
// Name parsing.

/// font_intern_prop: the property value of the name text STR: nil for
/// "*", a fixnum for digits (unless FORCE_SYMBOL), else the symbol of
/// that name in the obarray `obarray' names, made as make_string makes a
/// string of those bytes.
fn font_intern_prop(
    interp: &mut Interpreter,
    str: &[u8],
    force_symbol: bool,
    env: &mut Env,
) -> Result<Value, LispError> {
    if str == b"*" {
        return Ok(Value::Nil);
    }
    let make_name = |bytes: &[u8]| {
        super::strings::make_string(bytes.to_vec())
            .map_err(|error| LispError::Signal(error.to_string()))
    };
    if !force_symbol
        && str.first().is_some_and(u8::is_ascii_digit)
        && str.iter().all(u8::is_ascii_digit)
    {
        let mut n: i64 = 0;
        let mut i = 0;
        loop {
            n += i64::from(str[i] - b'0');
            i += 1;
            if n > crate::lisp::primitives::pdumper::image::MOST_POSITIVE_FIXNUM {
                break;
            }
            if i == str.len() {
                return Ok(Value::Integer(n));
            }
            match n.checked_mul(10) {
                Some(m) => n = m,
                None => break,
            }
        }
        return Err(LispError::SignalValue(Value::list([
            Value::symbol("overflow-error"),
            make_name(str)?,
        ])));
    }
    let name = make_name(str)?;
    let text = string_text(&name)?;
    let obarray = interp
        .symbol_value_symbol(&SymbolName::intern_str("obarray"))
        .unwrap_or(Value::Nil);
    let _ = env;
    super::completion::intern_in_obarray_with_name(interp, &obarray, &text, Some(name), |_| {
        Ok(name)
    })
}

/// The byte at P of NAME read as a C string: NUL past its end.
fn at(name: &[u8], p: isize) -> u8 {
    if p < 0 {
        return 0;
    }
    name.get(p as usize).copied().unwrap_or(0)
}

/// The value of the hexadecimal digit C, which the caller has checked.
fn hex_digit(c: u8) -> f64 {
    f64::from((c as char).to_digit(16).unwrap_or(0))
}

/// C strtod on NAME from P, after leading white space: the value and
/// where its text ends, P itself when there is no number.
fn strtod(name: &[u8], p: usize) -> (f64, usize) {
    let mut q = p;
    while matches!(
        at(name, q as isize),
        b' ' | b'\t' | b'\n' | 0x0b | 0x0c | b'\r'
    ) {
        q += 1;
    }
    let start = q;
    let mut negative = false;
    if matches!(at(name, q as isize), b'+' | b'-') {
        negative = at(name, q as isize) == b'-';
        q += 1;
    }
    let rest = |from: usize| -> &[u8] {
        let end = name[from.min(name.len())..]
            .iter()
            .position(|&b| b == 0)
            .map_or(name.len(), |n| from + n);
        &name[from.min(end)..end]
    };
    let tail = rest(q);
    let lower = |n: usize| -> Vec<u8> {
        tail.iter()
            .take(n)
            .map(u8::to_ascii_lowercase)
            .collect::<Vec<_>>()
    };
    let sign = |x: f64| if negative { -x } else { x };
    if lower(8) == b"infinity" {
        return (sign(f64::INFINITY), q + 8);
    }
    if lower(3) == b"inf" {
        return (sign(f64::INFINITY), q + 3);
    }
    if lower(3) == b"nan" {
        let mut end = q + 3;
        if at(name, end as isize) == b'(' {
            let mut k = end + 1;
            while at(name, k as isize).is_ascii_alphanumeric() || at(name, k as isize) == b'_' {
                k += 1;
            }
            if at(name, k as isize) == b')' {
                end = k + 1;
            }
        }
        return (sign(f64::NAN), end);
    }
    if lower(2) == b"0x"
        && (at(name, q as isize + 2).is_ascii_hexdigit()
            || (at(name, q as isize + 2) == b'.' && at(name, q as isize + 3).is_ascii_hexdigit()))
    {
        let mut k = q + 2;
        let mut mantissa = 0f64;
        let mut exponent: i64 = 0;
        let mut any = false;
        while at(name, k as isize).is_ascii_hexdigit() {
            mantissa = mantissa * 16.0 + hex_digit(at(name, k as isize));
            any = true;
            k += 1;
        }
        if at(name, k as isize) == b'.' {
            k += 1;
            while at(name, k as isize).is_ascii_hexdigit() {
                mantissa = mantissa * 16.0 + hex_digit(at(name, k as isize));
                exponent -= 4;
                any = true;
                k += 1;
            }
        }
        if any {
            if matches!(at(name, k as isize), b'p' | b'P') {
                let mut e = k + 1;
                let mut eneg = false;
                if matches!(at(name, e as isize), b'+' | b'-') {
                    eneg = at(name, e as isize) == b'-';
                    e += 1;
                }
                if at(name, e as isize).is_ascii_digit() {
                    let mut value: i64 = 0;
                    while at(name, e as isize).is_ascii_digit() {
                        value = value
                            .saturating_mul(10)
                            .saturating_add(i64::from(at(name, e as isize) - b'0'));
                        e += 1;
                    }
                    exponent += if eneg { -value } else { value };
                    k = e;
                }
            }
            let value = mantissa * 2f64.powi(exponent.clamp(-2000, 2000) as i32);
            return (sign(value), k);
        }
    }
    let mut k = q;
    let mut digits = false;
    while at(name, k as isize).is_ascii_digit() {
        digits = true;
        k += 1;
    }
    if at(name, k as isize) == b'.' {
        let mut f = k + 1;
        while at(name, f as isize).is_ascii_digit() {
            digits = true;
            f += 1;
        }
        if digits {
            k = f;
        }
    }
    if !digits {
        return (0.0, p);
    }
    if matches!(at(name, k as isize), b'e' | b'E') {
        let mut e = k + 1;
        if matches!(at(name, e as isize), b'+' | b'-') {
            e += 1;
        }
        if at(name, e as isize).is_ascii_digit() {
            while at(name, e as isize).is_ascii_digit() {
                e += 1;
            }
            k = e;
        }
    }
    let text = std::str::from_utf8(&name[start..k]).unwrap_or("0");
    (text.parse::<f64>().unwrap_or(0.0), k)
}

/// The C conversion of a double to int on x86-64 (cvttsd2si): the
/// truncation, or INT_MIN when it does not fit.
fn double_to_int(x: f64) -> i32 {
    if x.is_finite() && x > f64::from(i32::MIN) - 1.0 && x < f64::from(i32::MAX) + 1.0 {
        x.trunc() as i32
    } else {
        i32::MIN
    }
}

/// glibc atoi: strtol's value cast to int.
fn atoi(name: &[u8], p: usize) -> i32 {
    let mut q = p;
    while matches!(
        at(name, q as isize),
        b' ' | b'\t' | b'\n' | 0x0b | 0x0c | b'\r'
    ) {
        q += 1;
    }
    let mut negative = false;
    if matches!(at(name, q as isize), b'+' | b'-') {
        negative = at(name, q as isize) == b'-';
        q += 1;
    }
    let mut value: i64 = 0;
    let mut overflow = false;
    while at(name, q as isize).is_ascii_digit() {
        match value
            .checked_mul(10)
            .and_then(|v| v.checked_add(i64::from(at(name, q as isize) - b'0')))
        {
            Some(v) => value = v,
            None => overflow = true,
        }
        q += 1;
    }
    let value = if overflow {
        if negative { i64::MIN } else { i64::MAX }
    } else if negative {
        -value
    } else {
        value
    };
    value as i32
}

/// parse_matrix: the scalar of the XLFD size field `[A B C D]' at P, D,
/// or -1.
fn parse_matrix(name: &[u8], p: usize) -> i32 {
    let mut matrix = [0f64; 4];
    let mut i = 0;
    let mut p = p + 1;
    while i < 4 && at(name, p as isize) != 0 && at(name, p as isize) != b']' {
        let (value, end) = if at(name, p as isize) == b'~' {
            let (value, end) = strtod(name, p + 1);
            // strtod's end is P + 1 when nothing was read.
            (-value, end)
        } else {
            strtod(name, p)
        };
        matrix[i] = value;
        p = end;
        i += 1;
    }
    if i == 4 { double_to_int(matrix[3]) } else { -1 }
}

const XLFD_FOUNDRY_INDEX: usize = 0;
const XLFD_FAMILY_INDEX: usize = 1;
const XLFD_WEIGHT_INDEX: usize = 2;
const XLFD_SLANT_INDEX: usize = 3;
const XLFD_SWIDTH_INDEX: usize = 4;
const XLFD_ADSTYLE_INDEX: usize = 5;
const XLFD_PIXEL_INDEX: usize = 6;
const XLFD_POINT_INDEX: usize = 7;
const XLFD_RESX_INDEX: usize = 8;
const XLFD_RESY_INDEX: usize = 9;
const XLFD_SPACING_INDEX: usize = 10;
const XLFD_AVGWIDTH_INDEX: usize = 11;
const XLFD_REGISTRY_INDEX: usize = 12;
const XLFD_ENCODING_INDEX: usize = 13;
const XLFD_LAST_INDEX: usize = 14;

const XLFD_FOUNDRY_MASK: u32 = 0x0001;
const XLFD_FAMILY_MASK: u32 = 0x0002;
const XLFD_WEIGHT_MASK: u32 = 0x0004;
const XLFD_SLANT_MASK: u32 = 0x0008;
const XLFD_SWIDTH_MASK: u32 = 0x0010;
const XLFD_ADSTYLE_MASK: u32 = 0x0020;
const XLFD_PIXEL_MASK: u32 = 0x0040;
const XLFD_POINT_MASK: u32 = 0x0080;
const XLFD_RESX_MASK: u32 = 0x0100;
const XLFD_RESY_MASK: u32 = 0x0200;
const XLFD_SPACING_MASK: u32 = 0x0400;
const XLFD_AVGWIDTH_MASK: u32 = 0x0800;
const XLFD_REGISTRY_MASK: u32 = 0x1000;
const XLFD_ENCODING_MASK: u32 = 0x2000;

const XLFD_SYMBOL_MASK: u32 =
    XLFD_FOUNDRY_MASK | XLFD_FAMILY_MASK | XLFD_ADSTYLE_MASK | XLFD_REGISTRY_MASK;
const XLFD_NULL_MASK: u32 = XLFD_FOUNDRY_MASK | XLFD_ADSTYLE_MASK;
const XLFD_LARGENUM_MASK: u32 =
    XLFD_POINT_MASK | XLFD_RESX_MASK | XLFD_RESY_MASK | XLFD_AVGWIDTH_MASK;
const XLFD_REGENC_MASK: u32 = XLFD_REGISTRY_MASK | XLFD_ENCODING_MASK;

#[derive(Clone, Copy, Default)]
struct FieldRange {
    from: i32,
    to: i32,
    mask: u32,
}

fn bit(n: i32) -> u32 {
    1u32 << n
}

/// font_expand_wildcards: the N fields of FIELD spread over the 14 XLFD
/// fields, each wildcard standing for as many as the others' contents
/// leave it; false when no placement is possible.
fn font_expand_wildcards(
    interp: &mut Interpreter,
    field: &mut [Value; XLFD_LAST_INDEX],
    n: usize,
    env: &mut Env,
) -> Result<bool, LispError> {
    let tmp: [Value; XLFD_LAST_INDEX] = *field;
    let mut range = [FieldRange::default(); XLFD_LAST_INDEX];
    let n_i = n as i32;
    let mut range_mask: u32 = 0;
    for _ in 0..=(14 - n_i) {
        range_mask = (range_mask << 1) | 1;
    }
    let mut range_from: i32 = 0;
    for i in 0..n {
        // The C's RANGE_TO, one more each field.
        let range_to: i32 = 14 - n_i + i as i32;
        let val = tmp[i];
        if val.is_nil() {
            // Wildcard.
            range[i] = FieldRange {
                from: range_from,
                to: range_to,
                mask: range_mask,
            };
        } else {
            let (mut from, mut to, mut mask): (i32, i32, u32);
            if let Some(numeric) = fixnum(&val) {
                if i + 1 == n {
                    (from, to, mask) = (
                        XLFD_ENCODING_INDEX as i32,
                        XLFD_ENCODING_INDEX as i32,
                        XLFD_ENCODING_MASK,
                    );
                } else if numeric == 0 {
                    (from, to, mask) = (
                        XLFD_PIXEL_INDEX as i32,
                        XLFD_AVGWIDTH_INDEX as i32,
                        XLFD_PIXEL_MASK | XLFD_LARGENUM_MASK,
                    );
                } else if numeric <= 48 {
                    (from, to, mask) = (
                        XLFD_PIXEL_INDEX as i32,
                        XLFD_PIXEL_INDEX as i32,
                        XLFD_PIXEL_MASK,
                    );
                } else {
                    (from, to, mask) = (
                        XLFD_POINT_INDEX as i32,
                        XLFD_AVGWIDTH_INDEX as i32,
                        XLFD_LARGENUM_MASK,
                    );
                }
            } else {
                let name = symbol_name_bytes(&val).unwrap_or_default();
                if name.is_empty() {
                    (from, to, mask) = (
                        XLFD_FOUNDRY_INDEX as i32,
                        XLFD_ADSTYLE_INDEX as i32,
                        XLFD_NULL_MASK,
                    );
                } else if i == 0 {
                    (from, to, mask) = (
                        XLFD_FOUNDRY_INDEX as i32,
                        XLFD_FOUNDRY_INDEX as i32,
                        XLFD_FOUNDRY_MASK,
                    );
                } else if i + 1 == n {
                    if name.last() == Some(&b'*') {
                        (from, to, mask) = (
                            XLFD_REGISTRY_INDEX as i32,
                            XLFD_ENCODING_INDEX as i32,
                            XLFD_REGENC_MASK,
                        );
                    } else {
                        (from, to, mask) = (
                            XLFD_ENCODING_INDEX as i32,
                            XLFD_ENCODING_INDEX as i32,
                            XLFD_ENCODING_MASK,
                        );
                    }
                } else if range_from <= XLFD_WEIGHT_INDEX as i32
                    && range_to >= XLFD_WEIGHT_INDEX as i32
                    && style_name_numeric(interp, FONT_WEIGHT_INDEX, &val)? >= 0
                {
                    (from, to, mask) = (
                        XLFD_WEIGHT_INDEX as i32,
                        XLFD_WEIGHT_INDEX as i32,
                        XLFD_WEIGHT_MASK,
                    );
                } else if range_from <= XLFD_SLANT_INDEX as i32
                    && range_to >= XLFD_SLANT_INDEX as i32
                    && style_name_numeric(interp, FONT_SLANT_INDEX, &val)? >= 0
                {
                    (from, to, mask) = (
                        XLFD_SLANT_INDEX as i32,
                        XLFD_SLANT_INDEX as i32,
                        XLFD_SLANT_MASK,
                    );
                } else if range_from <= XLFD_SWIDTH_INDEX as i32
                    && range_to >= XLFD_SWIDTH_INDEX as i32
                    && style_name_numeric(interp, FONT_WIDTH_INDEX, &val)? >= 0
                {
                    (from, to, mask) = (
                        XLFD_SWIDTH_INDEX as i32,
                        XLFD_SWIDTH_INDEX as i32,
                        XLFD_SWIDTH_MASK,
                    );
                } else if ["c", "m", "p", "d"]
                    .iter()
                    .any(|spacing| val.eq_value(Value::symbol(spacing)))
                {
                    (from, to, mask) = (
                        XLFD_SPACING_INDEX as i32,
                        XLFD_SPACING_INDEX as i32,
                        XLFD_SPACING_MASK,
                    );
                } else {
                    (from, to, mask) = (
                        XLFD_FOUNDRY_INDEX as i32,
                        XLFD_ENCODING_INDEX as i32,
                        XLFD_SYMBOL_MASK,
                    );
                }
            }
            // Merge position-based and value-based restrictions.
            mask &= range_mask;
            while from < range_from {
                mask &= !bit(from);
                from += 1;
            }
            while from < 14 && mask & bit(from) == 0 {
                from += 1;
            }
            while to > range_to {
                mask &= !bit(to);
                to -= 1;
            }
            while to >= 0 && mask & bit(to) == 0 {
                to -= 1;
            }
            if from > to {
                return Ok(false);
            }
            range[i] = FieldRange { from, to, mask };
            if from > range_from || to < range_to {
                // The range is narrowed by value-based restrictions.
                // Reflect it to the other fields.
                range_from = from;
                let (mut from, mut to) = (from - 1, to - 1);
                for j in (0..i).rev() {
                    // Check FROM for non-wildcard field.
                    if !tmp[j].is_nil() && range[j].from < from {
                        while range[j].from < from {
                            range[j].mask &= !bit(range[j].from);
                            range[j].from += 1;
                        }
                        while from < 14 && range[j].mask & bit(from) == 0 {
                            from += 1;
                        }
                        range[j].from = from;
                    } else {
                        from = range[j].from;
                    }
                    if range[j].to > to {
                        while range[j].to > to {
                            range[j].mask &= !bit(range[j].to);
                            range[j].to -= 1;
                        }
                        while to >= 0 && range[j].mask & bit(to) == 0 {
                            to -= 1;
                        }
                        range[j].to = to;
                    } else {
                        to = range[j].to;
                    }
                    if from > to {
                        return Ok(false);
                    }
                    from -= 1;
                    to -= 1;
                }
            }
        }
        range_from += 1;
        range_mask <<= 1;
    }
    // Decide all fields from restrictions in RANGE.
    let mut j: i32 = 0;
    for i in 0..n {
        if j < range[i].from {
            if i == 0 || !tmp[i - 1].is_nil() {
                // None of TMP[X] corresponds to Jth field.
                return Ok(false);
            }
            for slot in field
                .iter_mut()
                .take(range[i].from as usize)
                .skip(j as usize)
            {
                *slot = Value::Nil;
            }
            j = range[i].from;
        }
        field[j as usize] = tmp[i];
        j += 1;
    }
    if !tmp[n - 1].is_nil() && j < XLFD_REGISTRY_INDEX as i32 {
        return Ok(false);
    }
    for slot in field.iter_mut().skip(j as usize) {
        *slot = Value::Nil;
    }
    if let Some(number) = fixnum(&field[XLFD_ENCODING_INDEX]) {
        let name = Value::string(&number.to_string());
        field[XLFD_ENCODING_INDEX] = super::call(interp, "intern", &[name], env)?;
    }
    Ok(true)
}

/// font_parse_xlfd_1: NAME parsed as an XLFD into the font-spec FONT,
/// SEGMENTS beyond 14 dashes gathered into the family; false when it is
/// none.
fn font_parse_xlfd_1(
    interp: &mut Interpreter,
    name: &[u8],
    font: &Value,
    mut segments: i64,
    env: &mut Env,
) -> Result<bool, LispError> {
    let len = name.len();
    // Reject empty XLFDs.
    if len == 0 {
        return Ok(false);
    }
    let mut f = [0usize; XLFD_LAST_INDEX + 1];
    // Accept "*-.." as a fully specified XLFD.
    let mut i = if name[0] == b'*' && (len == 1 || name[1] == b'-') {
        f[XLFD_FOUNDRY_INDEX] = 0;
        1
    } else {
        0
    };
    // Split into segments.
    let mut p = i;
    while at(name, p as isize) != 0 {
        if name[p] == b'-' {
            // If we have too many segments, then gather them up into the
            // FAMILY part of the name.  This allows using fonts with
            // dashes in the FAMILY bit.
            if segments > XLFD_LAST_INDEX as i64 && i == XLFD_WEIGHT_INDEX {
                segments -= 1;
            } else {
                f[i] = p + 1;
                i += 1;
                if i == XLFD_LAST_INDEX {
                    break;
                }
            }
        }
        p += 1;
    }
    f[i] = len;
    // INTERN_FIELD: the field from f[N] to the dash before f[N + 1].
    let field = |n: usize| &name[f[n]..f[n + 1].saturating_sub(1).max(f[n])];
    if i == XLFD_LAST_INDEX {
        // Fully specified XLFD.
        let value = font_intern_prop(interp, field(XLFD_FOUNDRY_INDEX), true, env)?;
        aset(interp, font, FONT_FOUNDRY_INDEX, value);
        let value = font_intern_prop(interp, field(XLFD_FAMILY_INDEX), true, env)?;
        aset(interp, font, FONT_FAMILY_INDEX, value);
        for (xlfd, prop) in (XLFD_WEIGHT_INDEX..=XLFD_SWIDTH_INDEX).zip(FONT_WEIGHT_INDEX..) {
            let val = font_intern_prop(interp, field(xlfd), true, env)?;
            if !val.is_nil() {
                let n = font_style_to_value(interp, prop, &val, false)?;
                if n < 0 {
                    return Ok(false);
                }
                aset(interp, font, prop, Value::Integer(n));
            }
        }
        let value = font_intern_prop(interp, field(XLFD_ADSTYLE_INDEX), true, env)?;
        aset(interp, font, FONT_ADSTYLE_INDEX, value);
        let registry = &name[f[XLFD_REGISTRY_INDEX]..];
        let registry_c = &registry[..registry
            .iter()
            .position(|&b| b == 0)
            .unwrap_or(registry.len())];
        if registry_c == b"*-*" {
            aset(interp, font, FONT_REGISTRY_INDEX, Value::Nil);
        } else {
            let value = font_intern_prop(
                interp,
                &name[f[XLFD_REGISTRY_INDEX]..f[XLFD_LAST_INDEX]],
                true,
                env,
            )?;
            aset(interp, font, FONT_REGISTRY_INDEX, value);
        }
        let p = f[XLFD_PIXEL_INDEX];
        let pixel_size = if at(name, p as isize) == b'[' {
            parse_matrix(name, p)
        } else {
            -1
        };
        if at(name, p as isize) == b'[' && pixel_size >= 0 {
            aset(
                interp,
                font,
                FONT_SIZE_INDEX,
                Value::Integer(i64::from(pixel_size)),
            );
        } else {
            let val = font_intern_prop(interp, field(XLFD_PIXEL_INDEX), false, env)?;
            if fixnum(&val).is_some() {
                aset(interp, font, FONT_SIZE_INDEX, val);
            } else if font_kind(interp, font) == Some(FontKind::Entity) {
                return Ok(false);
            } else {
                let mut point_size = -1.0;
                let p = f[XLFD_POINT_INDEX];
                if at(name, p as isize) == b'[' {
                    point_size = f64::from(parse_matrix(name, p));
                } else if at(name, p as isize).is_ascii_digit() {
                    point_size = f64::from(atoi(name, p));
                    point_size /= 10.0;
                }
                if point_size >= 0.0 {
                    aset(interp, font, FONT_SIZE_INDEX, Value::float(point_size));
                }
            }
        }
        let val = font_intern_prop(interp, field(XLFD_RESY_INDEX), false, env)?;
        if !val.is_nil() && fixnum(&val).is_none() {
            return Ok(false);
        }
        aset(interp, font, FONT_DPI_INDEX, val);
        let val = font_intern_prop(interp, field(XLFD_SPACING_INDEX), false, env)?;
        if !val.is_nil() {
            let Validated::Value(val) = validate_spacing(val) else {
                return Ok(false);
            };
            if fixnum(&val).is_none() {
                return Ok(false);
            }
            aset(interp, font, FONT_SPACING_INDEX, val);
        }
        let mut p = f[XLFD_AVGWIDTH_INDEX];
        if at(name, p as isize) == b'~' {
            p += 1;
        }
        let end = f[XLFD_REGISTRY_INDEX].saturating_sub(1).max(p);
        let val = font_intern_prop(interp, &name[p..end], false, env)?;
        if !val.is_nil() && fixnum(&val).is_none() {
            return Ok(false);
        }
        aset(interp, font, FONT_AVGWIDTH_INDEX, val);
    } else {
        let mut wild_card_found = false;
        let mut prop = [Value::Nil; XLFD_LAST_INDEX];
        if font_kind(interp, font) == Some(FontKind::Entity) {
            return Ok(false);
        }
        for j in 0..i {
            if at(name, f[j] as isize) == b'*' {
                let next = at(name, f[j] as isize + 1);
                if next != 0 && next != b'-' {
                    return Ok(false);
                }
                prop[j] = Value::Nil;
                wild_card_found = true;
            } else if j + 1 < i {
                prop[j] = font_intern_prop(interp, field(j), false, env)?;
            } else {
                prop[j] = font_intern_prop(interp, &name[f[j]..f[i]], false, env)?;
            }
        }
        if !wild_card_found {
            return Ok(false);
        }
        if !font_expand_wildcards(interp, &mut prop, i, env)? {
            return Ok(false);
        }
        aset(interp, font, FONT_FOUNDRY_INDEX, prop[XLFD_FOUNDRY_INDEX]);
        aset(interp, font, FONT_FAMILY_INDEX, prop[XLFD_FAMILY_INDEX]);
        for (xlfd, font_prop) in (XLFD_WEIGHT_INDEX..=XLFD_SWIDTH_INDEX).zip(FONT_WEIGHT_INDEX..) {
            if !prop[xlfd].is_nil() {
                let n = font_style_to_value(interp, font_prop, &prop[xlfd], true)?;
                if n < 0 {
                    return Ok(false);
                }
                aset(interp, font, font_prop, Value::Integer(n));
            }
        }
        aset(interp, font, FONT_ADSTYLE_INDEX, prop[XLFD_ADSTYLE_INDEX]);
        let symbol_name = |value: &Value| symbol_name_text(value).unwrap_or_default();
        let registry = prop[XLFD_REGISTRY_INDEX];
        let encoding = prop[XLFD_ENCODING_INDEX];
        let val = if registry.is_nil() {
            (!encoding.is_nil()).then(|| format!("*-{}", symbol_name(&encoding)))
        } else if encoding.is_nil() {
            Some(format!("{}-*", symbol_name(&registry)))
        } else {
            Some(format!(
                "{}-{}",
                symbol_name(&registry),
                symbol_name(&encoding)
            ))
        };
        if let Some(val) = val {
            let val = super::call(interp, "intern", &[Value::string(&val)], env)?;
            aset(interp, font, FONT_REGISTRY_INDEX, val);
        }
        if fixnum(&prop[XLFD_PIXEL_INDEX]).is_some() {
            aset(interp, font, FONT_SIZE_INDEX, prop[XLFD_PIXEL_INDEX]);
        } else if let Some(point) = fixnum(&prop[XLFD_POINT_INDEX]) {
            aset(
                interp,
                font,
                FONT_SIZE_INDEX,
                Value::float(point as f64 / 10.0),
            );
        }
        if fixnum(&prop[XLFD_RESX_INDEX]).is_some() {
            aset(interp, font, FONT_DPI_INDEX, prop[XLFD_RESY_INDEX]);
        }
        if !prop[XLFD_SPACING_INDEX].is_nil() {
            let Validated::Value(val) = validate_spacing(prop[XLFD_SPACING_INDEX]) else {
                return Ok(false);
            };
            if fixnum(&val).is_none() {
                return Ok(false);
            }
            aset(interp, font, FONT_SPACING_INDEX, val);
        }
        if fixnum(&prop[XLFD_AVGWIDTH_INDEX]).is_some() {
            aset(interp, font, FONT_AVGWIDTH_INDEX, prop[XLFD_AVGWIDTH_INDEX]);
        }
    }
    Ok(true)
}

/// font_parse_xlfd: NAME as an XLFD, retried with the dashes beyond 14
/// taken into the family.
pub(crate) fn font_parse_xlfd(
    interp: &mut Interpreter,
    name: &[u8],
    font: &Value,
    env: &mut Env,
) -> Result<bool, LispError> {
    if font_parse_xlfd_1(interp, name, font, -1, env)? {
        return Ok(true);
    }
    // Count how many segments we have.
    let c_name = &name[..name.iter().position(|&b| b == 0).unwrap_or(name.len())];
    let segments = c_name.iter().filter(|&&b| b == b'-').count() as i64;
    // If we have a surplus of segments, then we try to parse again, in
    // case there's a font with dashes in the family name.
    if segments > XLFD_LAST_INDEX as i64 {
        font_parse_xlfd_1(interp, name, font, segments, env)
    } else {
        Ok(false)
    }
}

/// ftfont.c's ftfont_booleans.
const FTFONT_BOOLEANS: &[&str] = &[
    ":antialias",
    ":hinting",
    ":verticallayout",
    ":autohint",
    ":globaladvance",
    ":outline",
    ":scalable",
    ":minspace",
    ":embolden",
];

/// ftfont.c's ftfont_non_booleans.
const FTFONT_NON_BOOLEANS: &[&str] = &[
    ":family",
    ":familylang",
    ":style",
    ":stylelang",
    ":fullname",
    ":fullnamelang",
    ":slant",
    ":weight",
    ":size",
    ":width",
    ":aspect",
    ":pixelsize",
    ":spacing",
    ":foundry",
    ":hintstyle",
    ":file",
    ":index",
    ":ftface",
    ":rasterizer",
    ":scale",
    ":dpi",
    ":rgba",
    ":lcdfilter",
    ":charset",
    ":lang",
    ":fontversion",
    ":capability",
];

/// font_filter_properties with the ftfont lists, the filter_properties of
/// each of the cairo X build's global drivers (ftfont, its HarfBuzz copy,
/// ftcrfont and ftcrhbfont): the boolean properties of ALIST put as t or
/// nil, the others as they are.  A second pass puts the same values, so
/// one stands for the four.
fn font_filter_properties(
    interp: &mut Interpreter,
    font: &Value,
    alist: &[(Value, Value)],
    env: &mut Env,
) -> Result<(), LispError> {
    for property in FTFONT_BOOLEANS {
        for (key, val) in alist {
            if symbol_name_text(key).as_deref() == Some(*property) {
                let str = match val.kind() {
                    Kind::Integer(n) => (if n != 0 { "true" } else { "false" }).to_string(),
                    _ if is_symbol(val) => symbol_name_text(val).unwrap_or_default(),
                    _ => "true".into(),
                };
                let val = if matches!(
                    str.as_str(),
                    "false" | "False" | "FALSE" | "FcFalse" | "off" | "OFF" | "Off"
                ) {
                    Value::Nil
                } else {
                    Value::T
                };
                font_put(interp, font, *key, val, env)?;
            }
        }
    }
    for property in FTFONT_NON_BOOLEANS {
        for (key, val) in alist {
            if symbol_name_text(key).as_deref() == Some(*property) {
                font_put(interp, font, *key, *val, env)?;
            }
        }
    }
    Ok(())
}

/// font_parse_fcname: NAME as a fontconfig name (FAMILY[-SIZE][:PROPS])
/// or a GTK one (FAMILY [STYLES...] [SIZE]) into FONT.
fn font_parse_fcname(
    interp: &mut Interpreter,
    name: &[u8],
    font: &Value,
    env: &mut Env,
) -> Result<bool, LispError> {
    let len = name.len() as isize;
    if len == 0 {
        return Ok(false);
    }
    let c = |p: isize| at(name, p);
    let mut size_beg: Option<isize> = None;
    let mut props_beg: Option<isize> = None;
    let mut family_end: Option<isize> = None;
    let mut p: isize = 0;
    while c(p) != 0 {
        if c(p) == b'\\' && c(p + 1) != 0 {
            p += 1;
        } else if c(p) == b':' {
            props_beg = Some(p);
            family_end = Some(p);
            break;
        } else if c(p) == b'-' {
            let mut decimal = false;
            let mut size_found = true;
            let mut q = p + 1;
            while c(q) != 0 && c(q) != b':' {
                if !c(q).is_ascii_digit() {
                    if c(q) != b'.' || decimal {
                        size_found = false;
                        break;
                    }
                    decimal = true;
                }
                q += 1;
            }
            if size_found {
                family_end = Some(p);
                size_beg = Some(p + 1);
                break;
            }
        }
        p += 1;
    }

    if let Some(family_end) = family_end {
        let mut extra_props: Vec<(Value, Value)> = Vec::new();
        // A fontconfig name with size and/or property data.
        if family_end > 0 {
            let family = font_intern_prop(interp, &name[..family_end as usize], true, env)?;
            aset(interp, font, FONT_FAMILY_INDEX, family);
        }
        if let Some(size_beg) = size_beg {
            let (point_size, size_end) = strtod(name, size_beg as usize);
            aset(interp, font, FONT_SIZE_INDEX, Value::float(point_size));
            let size_end = size_end as isize;
            if c(size_end) == b':' && c(size_end + 1) != 0 {
                props_beg = Some(size_end);
            }
        }
        if let Some(props_beg) = props_beg {
            // Now parse ":KEY=VAL" patterns.
            let mut p = props_beg;
            while c(p) != 0 {
                let mut q = p + 1;
                while c(q) != 0 && c(q) != b'=' && c(q) != b':' {
                    q += 1;
                }
                if c(q) != b'=' {
                    // Must be an enumerated value.
                    let word = &name[(p + 1) as usize..q as usize];
                    let val = font_intern_prop(interp, word, true, env)?;
                    match word {
                        b"thin" | b"ultra-light" | b"light" | b"semi-light" | b"book"
                        | b"medium" | b"normal" | b"semibold" | b"demibold" | b"bold"
                        | b"ultra-bold" | b"black" | b"heavy" | b"ultra-heavy" => {
                            font_set_style(interp, font, FONT_WEIGHT_INDEX, &val)?;
                        }
                        b"roman" | b"italic" | b"oblique" => {
                            font_set_style(interp, font, FONT_SLANT_INDEX, &val)?;
                        }
                        b"charcell" => aset(
                            interp,
                            font,
                            FONT_SPACING_INDEX,
                            Value::Integer(FONT_SPACING_CHARCELL),
                        ),
                        b"mono" => aset(
                            interp,
                            font,
                            FONT_SPACING_INDEX,
                            Value::Integer(FONT_SPACING_MONO),
                        ),
                        b"proportional" => aset(
                            interp,
                            font,
                            FONT_SPACING_INDEX,
                            Value::Integer(FONT_SPACING_PROPORTIONAL),
                        ),
                        _ => {}
                    }
                } else {
                    // KEY=VAL pairs.
                    let key_text = &name[p as usize..q as usize];
                    let (key, prop) = if key_text == b":pixelsize" {
                        (Value::Nil, Some(FONT_SIZE_INDEX))
                    } else {
                        let key = font_intern_prop(interp, key_text, true, env)?;
                        let prop = get_font_prop_index(&key);
                        (key, prop)
                    };
                    let vp = q + 1;
                    let mut vq = vp;
                    while c(vq) != 0 && c(vq) != b':' {
                        vq += 1;
                    }
                    let val =
                        font_intern_prop(interp, &name[vp as usize..vq as usize], false, env)?;
                    match prop {
                        Some(prop) if (FONT_FOUNDRY_INDEX..FONT_EXTRA_INDEX).contains(&prop) => {
                            let val = font_prop_validate(interp, prop, Value::Nil, val, env)?;
                            aset(interp, font, prop, val);
                        }
                        _ => extra_props.push((key, val)),
                    }
                    q = vq;
                }
                p = q;
            }
        }
        if !extra_props.is_empty() {
            font_filter_properties(interp, font, &extra_props, env)?;
        }
    } else {
        // Either a fontconfig-style name with no size and property data,
        // or a GTK-style name.
        let mut weight = Value::Nil;
        let mut slant = Value::Nil;
        let mut width = Value::Nil;
        let mut size = Value::Nil;
        let mut family_end: Option<isize> = None;
        // Scan backwards from the end, looking for a size.
        let byte = |p: isize| name[p as usize];
        let mut p = len - 1;
        while p >= 0 {
            if !byte(p).is_ascii_digit() {
                break;
            }
            p -= 1;
        }
        if p < len - 1 && (p + 1 == 0 || byte(p) == b' ') {
            // Found a font size.
            size = Value::float(strtod(name, (p + 1) as usize).0);
        } else {
            p = len;
        }
        // Now P points to the termination of the string, sans size.  Scan
        // backwards, looking for font properties.
        while p > 0 {
            let mut q = p - 1;
            while q >= 0 {
                if q > 0 && byte(q - 1) == b'\\' {
                    // Skip quoting backslashes.
                    q -= 1;
                } else if byte(q) == b' ' {
                    break;
                }
                q -= 1;
            }
            let word = &name[(q + 1) as usize..p as usize];
            let mut save = |var: &mut Value, str: &str| -> Result<(), LispError> {
                if var.is_nil() {
                    *var = font_intern_prop(interp, str.as_bytes(), true, env)?;
                }
                Ok(())
            };
            match word {
                b"Ultra-Light" => save(&mut weight, "ultra-light")?,
                b"Light" => save(&mut weight, "light")?,
                b"Book" => save(&mut weight, "book")?,
                b"Medium" => save(&mut weight, "medium")?,
                b"Semi-Bold" => save(&mut weight, "semi-bold")?,
                b"Bold" => save(&mut weight, "bold")?,
                b"Italic" => save(&mut slant, "italic")?,
                b"Oblique" => save(&mut slant, "oblique")?,
                b"Semi-Condensed" => save(&mut width, "semi-condensed")?,
                b"Condensed" => save(&mut width, "condensed")?,
                // An unknown word must be part of the font name.
                _ => {
                    family_end = Some(p);
                    break;
                }
            }
            p = q;
        }
        if let Some(family_end) = family_end {
            let family = font_intern_prop(interp, &name[..family_end as usize], true, env)?;
            aset(interp, font, FONT_FAMILY_INDEX, family);
        }
        if !size.is_nil() {
            aset(interp, font, FONT_SIZE_INDEX, size);
        }
        if !weight.is_nil() {
            font_set_style(interp, font, FONT_WEIGHT_INDEX, &weight)?;
        }
        if !slant.is_nil() {
            font_set_style(interp, font, FONT_SLANT_INDEX, &slant)?;
        }
        if !width.is_nil() {
            font_set_style(interp, font, FONT_WIDTH_INDEX, &width)?;
        }
    }
    Ok(true)
}

/// font_parse_name: NAME, a Lisp string's bytes, into FONT, as an XLFD
/// when it starts with `-' or has a wildcard, else as a fontconfig or GTK
/// name.  False when it parses as neither.
pub(crate) fn font_parse_name(
    interp: &mut Interpreter,
    name: &[u8],
    font: &Value,
    env: &mut Env,
) -> Result<bool, LispError> {
    let c_name = &name[..name.iter().position(|&b| b == 0).unwrap_or(name.len())];
    if at(name, 0) == b'-' || c_name.contains(&b'*') || c_name.contains(&b'?') {
        return font_parse_xlfd(interp, name, font, env);
    }
    font_parse_fcname(interp, name, font, env)
}

/// font_parse_family_registry: FAMILY (perhaps FOUNDRY-FAMILY) and
/// REGISTRY, strings or nil, merged into the font-spec FONT_SPEC.
pub(crate) fn font_parse_family_registry(
    interp: &mut Interpreter,
    family: &Value,
    registry: &Value,
    font_spec: &Value,
    env: &mut Env,
) -> Result<(), LispError> {
    if !family.is_nil() && aref(interp, font_spec, FONT_FAMILY_INDEX).is_nil() {
        let bytes = string_sdata(family)?;
        let c_len = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
        if let Some(p1) = bytes[..c_len].iter().position(|&b| b == b'-') {
            if bytes[0] != b'*' && p1 > 0 && aref(interp, font_spec, FONT_FOUNDRY_INDEX).is_nil() {
                let foundry = font_intern_prop(interp, &bytes[..p1], true, env)?;
                font_put(interp, font_spec, keyword(":foundry"), foundry, env)?;
            }
            let family = font_intern_prop(interp, &bytes[p1 + 1..], true, env)?;
            font_put(interp, font_spec, keyword(":family"), family, env)?;
        } else {
            let family = super::call(interp, "intern", std::slice::from_ref(family), env)?;
            aset(interp, font_spec, FONT_FAMILY_INDEX, family);
        }
    }
    if !registry.is_nil() {
        // Convert "XXX" and "XXX*" to "XXX*-*".
        let bytes = string_sdata(registry)?;
        let c_len = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
        let mut registry = *registry;
        if !bytes[..c_len].contains(&b'-') {
            let asterisk = bytes.last() == Some(&b'*');
            let extra = Value::string(if asterisk { "-*" } else { "*-*" });
            registry = super::call(interp, "concat", &[registry, extra], env)?;
        }
        let registry = super::call(interp, "downcase", &[registry], env)?;
        let registry = super::call(interp, "intern", &[registry], env)?;
        aset(interp, font_spec, FONT_REGISTRY_INDEX, registry);
    }
    Ok(())
}

/// SDATA of the string STRING.
pub(crate) fn string_sdata(string: &Value) -> Result<Vec<u8>, LispError> {
    let like = string_like(string).ok_or_else(|| wrong_type_argument("stringp", *string))?;
    super::text::internal_string_bytes(&like)
}

/// font_spec_from_name: the font-spec FONT_NAME parses to, with :name
/// and :user-spec, or nil.
pub(crate) fn font_spec_from_name(
    interp: &mut Interpreter,
    font_name: &Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    let spec = font_make_spec(interp);
    let bytes = string_sdata(font_name)?;
    if !font_parse_name(interp, &bytes, &spec, env)? {
        return Ok(Value::Nil);
    }
    font_put_extra(interp, &spec, keyword(":name"), Some(*font_name), env)?;
    font_put_extra(interp, &spec, keyword(":user-spec"), Some(*font_name), env)?;
    Ok(spec)
}

// ---------------------------------------------------------------------
// The XLFD unparser.

/// font_dynamic_unparse_xlfd: the XLFD name of FONT, PIXEL_SIZE standing
/// for a size that is no positive integer.
pub(crate) fn font_dynamic_unparse_xlfd(
    interp: &mut Interpreter,
    font: &Value,
    pixel_size: i64,
) -> Result<Vec<u8>, LispError> {
    let name_of = |value: &Value| -> Vec<u8> {
        if let Some(bytes) = symbol_name_bytes(value) {
            return bytes;
        }
        string_sdata(value).unwrap_or_default()
    };
    let c_str = |bytes: Vec<u8>| -> Vec<u8> {
        let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
        bytes[..end].to_vec()
    };
    let mut fields: Vec<Vec<u8>> = Vec::with_capacity(11);
    for index in [FONT_FOUNDRY_INDEX, FONT_FAMILY_INDEX] {
        let val = aref(interp, font, index);
        fields.push(if val.is_nil() {
            b"*".to_vec()
        } else {
            c_str(name_of(&val))
        });
    }
    for prop in [FONT_WEIGHT_INDEX, FONT_SLANT_INDEX, FONT_WIDTH_INDEX] {
        let val = font_style_symbolic(interp, font, prop, false)?;
        fields.push(if val.is_nil() {
            b"*".to_vec()
        } else {
            // Copy the name while excluding '-', '?', ',', and '"'.
            c_str(name_of(&val))
                .into_iter()
                .filter(|c| !matches!(c, b'-' | b'?' | b',' | b'"'))
                .collect()
        });
    }
    let adstyle = aref(interp, font, FONT_ADSTYLE_INDEX);
    fields.push(if adstyle.is_nil() {
        b"*".to_vec()
    } else {
        c_str(name_of(&adstyle))
    });
    let size = aref(interp, font, FONT_SIZE_INDEX);
    fields.push(match size.kind() {
        Kind::Integer(v) => {
            let v = if v > 0 { v } else { pixel_size };
            if v > 0 {
                format!("{v}-*").into_bytes()
            } else {
                b"*-*".to_vec()
            }
        }
        Kind::BigInteger(_) => {
            if pixel_size > 0 {
                format!("{pixel_size}-*").into_bytes()
            } else {
                b"*-*".to_vec()
            }
        }
        Kind::Float(v) => format!("*-{:.0}", v.get() * 10.0).into_bytes(),
        _ => b"*-*".to_vec(),
    });
    fields.push(match fixnum(&aref(interp, font, FONT_DPI_INDEX)) {
        Some(v) => format!("{v}-{v}").into_bytes(),
        None => b"*-*".to_vec(),
    });
    fields.push(
        match fixnum(&aref(interp, font, FONT_SPACING_INDEX)) {
            Some(spacing) if spacing <= FONT_SPACING_PROPORTIONAL => "p",
            Some(spacing) if spacing <= FONT_SPACING_DUAL => "d",
            Some(spacing) if spacing <= FONT_SPACING_MONO => "m",
            Some(_) => "c",
            None => "*",
        }
        .as_bytes()
        .to_vec(),
    );
    fields.push(match fixnum(&aref(interp, font, FONT_AVGWIDTH_INDEX)) {
        Some(v) => v.to_string().into_bytes(),
        None => b"*".to_vec(),
    });
    let registry = aref(interp, font, FONT_REGISTRY_INDEX);
    fields.push(if registry.is_nil() {
        b"*-*".to_vec()
    } else {
        let mut name = c_str(name_of(&registry));
        if !name.contains(&b'-') {
            // Change "jisx0208*" and "jisx0208" to "jisx0208*-*".
            if name.last() != Some(&b'*') {
                name.push(b'*');
            }
            name.extend_from_slice(b"-*");
        }
        name
    });
    let mut out = Vec::new();
    for field in fields {
        out.push(b'-');
        out.extend_from_slice(&field);
    }
    Ok(out)
}

/// font_unparse_xlfd into a buffer of NBYTES: the name, or None when it
/// does not fit.
pub(crate) fn font_unparse_xlfd(
    interp: &mut Interpreter,
    font: &Value,
    pixel_size: i64,
    nbytes: usize,
) -> Result<Option<Vec<u8>>, LispError> {
    let name = font_dynamic_unparse_xlfd(interp, font, pixel_size)?;
    Ok((name.len() < nbytes).then_some(name))
}

/// Ffont_xlfd_name.
pub(crate) fn font_xlfd_name(
    interp: &mut Interpreter,
    font: &Value,
    fold_wildcards: bool,
    long_xlfds: bool,
) -> Result<Value, LispError> {
    check_font(interp, font)?;
    let pixel_size = 0;
    let name = if long_xlfds {
        font_dynamic_unparse_xlfd(interp, font, pixel_size)?
    } else {
        match font_unparse_xlfd(interp, font, pixel_size, 256)? {
            Some(name) => name,
            None => return Ok(Value::Nil),
        }
    };
    let mut name = name;
    if fold_wildcards {
        let mut p0 = 0;
        while let Some(found) = name[p0..].windows(4).position(|w| w == b"-*-*") {
            let p1 = p0 + found;
            name.drain(p1..p1 + 2);
            p0 = p1;
        }
    }
    super::strings::make_string(name).map_err(|error| LispError::Signal(error.to_string()))
}

// ---------------------------------------------------------------------
// Font matching.

/// FRAME_RES on the terminal frames Emaxx has: GNU reads a window-system
/// display's resolution there (and crashes); this is the value of a build
/// without a window system, default_pixels_per_inch_y.
pub(crate) fn frame_res(interp: &Interpreter) -> f64 {
    let v = interp
        .symbol_value_symbol(&SymbolName::intern_str("display-pixels-per-inch"))
        .unwrap_or(Value::Nil);
    let v = v.cons_values().map_or(v, |(_, cdr)| cdr);
    let numval = match v.kind() {
        Kind::Integer(n) => n as f64,
        Kind::Float(x) => x.get(),
        _ => 0.0,
    };
    if numval > 0.0 { numval } else { 72.0 }
}

/// font_rescale_ratio: the ratio face-font-rescale-alist gives FONT.
fn font_rescale_ratio(
    interp: &mut Interpreter,
    font: &Value,
    env: &mut Env,
) -> Result<f64, LispError> {
    let alist = interp
        .symbol_value_symbol(&SymbolName::intern_str("face-font-rescale-alist"))
        .unwrap_or(Value::Nil);
    let mut name = Value::Nil;
    let mut tail = alist;
    while let Some((elt, rest)) = tail.cons_values() {
        if let Some((pattern, ratio)) = elt.cons_values()
            && let Kind::Float(ratio) = ratio.kind()
        {
            if pattern.is_string() {
                if name.is_nil() {
                    name = font_xlfd_name(interp, font, false, true)?;
                }
                // N.B. that `name' is set to nil if the resulting XLFD is
                // too long.
                if !name.is_nil() {
                    let canon = interp.standard_case_tables().canon;
                    if super::search::fast_string_match(
                        interp,
                        env,
                        &pattern,
                        &name,
                        Some(canon),
                        false,
                    )?
                    .is_some()
                    {
                        return Ok(ratio.get());
                    }
                }
            } else if font_kind(interp, &pattern) == Some(FontKind::Spec)
                && font_match_p(interp, &pattern, font, env)?
            {
                return Ok(ratio.get());
            }
        }
        tail = rest;
    }
    Ok(1.0)
}

/// font_score's verdict on ENTITY against the properties SPEC_PROP of a
/// spec: whether any style, the size or the resolution differs (every
/// difference shifts in nonzero bits).
fn font_score_differs(
    interp: &mut Interpreter,
    entity: &Value,
    spec_prop: &[Value],
    env: &mut Env,
) -> Result<bool, LispError> {
    // Score three style numeric fields.
    for (i, spec_value) in spec_prop
        .iter()
        .enumerate()
        .take(FONT_WIDTH_INDEX + 1)
        .skip(FONT_WEIGHT_INDEX)
    {
        let entity_value = aref(interp, entity, i);
        if !spec_value.is_nil()
            && !entity_value.eq_value(*spec_value)
            && let Some(entity_value) = fixnum(&entity_value)
        {
            let spec_value = fixnum(spec_value).unwrap_or(0);
            if (entity_value >> 8) - (spec_value >> 8) != 0 {
                return Ok(true);
            }
        }
    }
    // Score the size.
    let entity_size = aref(interp, entity, FONT_SIZE_INDEX);
    // XFIXNUM of a float is its tagged address, a large positive number.
    let entity_size = match entity_size.kind() {
        Kind::Integer(n) => n,
        Kind::Float(_) => i64::MAX / 4,
        _ => 0,
    };
    if !spec_prop[FONT_SIZE_INDEX].is_nil() && entity_size > 0 {
        let mut pixel_size = fixnum(&spec_prop[FONT_SIZE_INDEX]).unwrap_or(0);
        let alist = interp
            .symbol_value_symbol(&SymbolName::intern_str("face-font-rescale-alist"))
            .unwrap_or(Value::Nil);
        if alist.cons_values().is_some() {
            pixel_size = (pixel_size as f64 * font_rescale_ratio(interp, entity, env)?) as i64;
        }
        if pixel_size.saturating_mul(2) < entity_size || entity_size.saturating_mul(2) < pixel_size
        {
            // This size is wrong by more than a factor 2: reject it!
            return Ok(true);
        }
        let mut diff = (pixel_size - entity_size).abs() << 1;
        if !spec_prop[FONT_DPI_INDEX].is_nil()
            && !spec_prop[FONT_DPI_INDEX].eq_value(aref(interp, entity, FONT_DPI_INDEX))
        {
            diff |= 1;
        }
        if !spec_prop[FONT_AVGWIDTH_INDEX].is_nil()
            && !spec_prop[FONT_AVGWIDTH_INDEX].eq_value(aref(interp, entity, FONT_AVGWIDTH_INDEX))
        {
            diff |= 1;
        }
        if diff != 0 {
            return Ok(true);
        }
    }
    Ok(false)
}

/// font_match_p: whether FONT matches the font-spec SPEC.  A :script
/// whose representative characters the font must encode, or an :otf, is
/// matched only by a font-object (GNU's font_encode_char of any other
/// font crashes); Emaxx opens none.
pub(crate) fn font_match_p(
    interp: &mut Interpreter,
    spec: &Value,
    font: &Value,
    env: &mut Env,
) -> Result<bool, LispError> {
    for i in FONT_FOUNDRY_INDEX..=FONT_REGISTRY_INDEX {
        let expected = aref(interp, spec, i);
        let actual = aref(interp, font, i);
        if !expected.is_nil() && !actual.is_nil() && !crate::lisp::types::eq(expected, actual) {
            return Ok(false);
        }
    }
    let mut props: Vec<Value> = (0..FONT_SPEC_MAX).map(|i| aref(interp, spec, i)).collect();
    if matches!(props[FONT_SIZE_INDEX].kind(), Kind::Float(_)) {
        // font_pixel_size of a float size on a frame that is no window
        // system's.
        props[FONT_SIZE_INDEX] = Value::Integer(1);
        for slot in props.iter_mut().skip(FONT_SIZE_INDEX + 1) {
            *slot = Value::Nil;
        }
    }
    if font_score_differs(interp, font, &props, env)? {
        return Ok(false);
    }
    let font_extra = aref(interp, font, FONT_EXTRA_INDEX);
    let mut extra = aref(interp, spec, FONT_EXTRA_INDEX);
    while let Some((entry, rest)) = extra.cons_values() {
        let (key, val) = entry.cons_values().unwrap_or((Value::Nil, Value::Nil));
        if crate::lisp::types::eq(key, keyword(":lang")) {
            let Some(val2) = assq_no_quit(&key, font_extra) else {
                return Ok(false);
            };
            let val2 = val2.cons_values().map_or(Value::Nil, |(_, cdr)| cdr);
            // A spec's :lang is a symbol (font_prop_validate_symbol), so
            // GNU's branch for a list of languages is never taken.
            if let Some((_, rest)) = val2.cons_values() {
                if super::call(interp, "memq", &[val, rest], env)?.is_nil() {
                    return Ok(false);
                }
            } else if !crate::lisp::types::eq(val, val2) {
                return Ok(false);
            }
        } else if crate::lisp::types::eq(key, keyword(":script")) {
            let chars = interp
                .symbol_value_symbol(&SymbolName::intern_str("script-representative-chars"))
                .unwrap_or(Value::Nil);
            if let Some(val2) = assq_no_quit(&val, chars) {
                let val2 = val2.cons_values().map_or(Value::Nil, |(_, cdr)| cdr);
                let is_char = |value: &Value| matches!(value.kind(), Kind::Integer(c) if (0..=0x3f_ffff).contains(&c));
                if val2.cons_values().is_some() {
                    // All characters in the list must be supported.
                    let mut tail = val2;
                    while let Some((c, rest)) = tail.cons_values() {
                        if is_char(&c) {
                            return Ok(false);
                        }
                        tail = rest;
                    }
                } else if is_vector_value(&val2) {
                    // At most one character in the vector must be
                    // supported.
                    return Ok(false);
                }
            }
        } else if crate::lisp::types::eq(key, keyword(":otf")) {
            return Ok(false);
        }
        extra = rest;
    }
    Ok(true)
}

// ---------------------------------------------------------------------
// The DEFUNs.

/// Ffont_spec.
pub(crate) fn font_spec(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    let spec = font_make_spec(interp);
    let mut i = 0;
    while i < args.len() {
        let key = args[i];
        // CHECK_SYMBOL, a symbol with position passing while
        // `symbols-with-pos-enabled' is set; KEY itself is what is stored.
        if !is_symbol(&crate::lisp::types::maybe_remove_pos(key)) {
            return Err(wrong_type_argument("symbolp", key));
        }
        let Some(&val) = args.get(i + 1) else {
            // error ("No value for key `%s'", SDATA (SYMBOL_NAME (key))).
            let name = crate::lisp::primitives::call(interp, "symbol-name", &[key], env)?;
            return Err(crate::lisp::primitives::values::doprnt_error_sdata(
                Some(interp),
                "No value for key `{}'",
                &[name],
            ));
        };
        if crate::lisp::types::eq(key, keyword(":name")) {
            let bytes = string_sdata(&val)?;
            if !font_parse_name(interp, &bytes, &spec, env)? {
                // error ("Invalid font name: %s", SSDATA (val)).
                return Err(crate::lisp::primitives::values::doprnt_error_sdata(
                    None,
                    "Invalid font name: {}",
                    &[val],
                ));
            }
            font_put_extra(interp, &spec, key, Some(val), env)?;
        } else {
            match get_font_prop_index(&key) {
                Some(idx) => {
                    let val = font_prop_validate(interp, idx, Value::Nil, val, env)?;
                    if idx < FONT_EXTRA_INDEX {
                        aset(interp, &spec, idx, val);
                    } else {
                        font_put_extra(interp, &spec, key, Some(val), env)?;
                    }
                }
                None => {
                    let val = font_prop_validate(interp, 0, key, val, env)?;
                    font_put_extra(interp, &spec, key, Some(val), env)?;
                }
            }
        }
        i += 2;
    }
    Ok(spec)
}

/// Ffont_get.
pub(crate) fn font_get(
    interp: &mut Interpreter,
    font: &Value,
    key: &Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    check_font(interp, font)?;
    // CHECK_SYMBOL, a symbol with position passing while
    // `symbols-with-pos-enabled' is set.
    if !is_symbol(&crate::lisp::types::maybe_remove_pos(key)) {
        return Err(wrong_type_argument("symbolp", *key));
    }
    let idx = get_font_prop_index(key);
    if let Some(idx) = idx {
        if (FONT_WEIGHT_INDEX..=FONT_WIDTH_INDEX).contains(&idx) {
            return font_style_symbolic(interp, font, idx, false);
        }
        if idx < FONT_EXTRA_INDEX {
            return Ok(aref(interp, font, idx));
        }
    }
    let extra = aref(interp, font, FONT_EXTRA_INDEX);
    let val = super::call(interp, "assq", &[*key, extra], env)?;
    super::call(interp, "cdr", &[val], env)
}

/// Ffont_put.
pub(crate) fn font_put(
    interp: &mut Interpreter,
    font: &Value,
    prop: Value,
    val: Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    match get_font_prop_index(&prop) {
        Some(idx) if idx < FONT_EXTRA_INDEX => {
            check_font_spec(interp, font)?;
            let validated = font_prop_validate(interp, idx, Value::Nil, val, env)?;
            aset(interp, font, idx, validated);
        }
        _ => {
            if [":name", ":script", ":lang", ":otf"]
                .iter()
                .any(|name| crate::lisp::types::eq(prop, keyword(name)))
            {
                check_font_spec(interp, font)?;
            } else {
                check_font(interp, font)?;
            }
            let validated = font_prop_validate(interp, 0, prop, val, env)?;
            font_put_extra(interp, font, prop, Some(validated), env)?;
        }
    }
    Ok(val)
}

/// Ffont_face_attributes after decode_live_frame: the face attributes
/// FONT, a font or a font name, gives.
pub(crate) fn font_face_attributes(
    interp: &mut Interpreter,
    font: &Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    let font = if font.is_string() {
        // GNU looks the fontset up, then parses NAME itself, the fontset's
        // ASCII font unused.
        super::fontset::fs_query_fontset(interp, font, 0, env)?;
        let spec = font_spec_from_name(interp, font, env)?;
        if font_kind(interp, &spec).is_none() {
            return Err(signal_error("Invalid font name", *font));
        }
        spec
    } else if font_kind(interp, font).is_none() {
        return Err(signal_error("Invalid font object", *font));
    } else {
        *font
    };
    let mut plist = Vec::new();
    let family = aref(interp, &font, FONT_FAMILY_INDEX);
    if !family.is_nil() {
        plist.push(keyword(":family"));
        plist.push(super::call(interp, "symbol-name", &[family], env)?);
    }
    let size = aref(interp, &font, FONT_SIZE_INDEX);
    match size.kind() {
        Kind::Integer(size) => {
            let dpi = match fixnum(&aref(interp, &font, FONT_DPI_INDEX)) {
                Some(dpi) => dpi as i32,
                None => frame_res(interp) as i32,
            };
            // PIXEL_TO_POINT (SIZE * 10, DPI).
            let height = (size * 10) as f64 * 72.27 / f64::from(dpi) + 0.5;
            plist.push(keyword(":height"));
            plist.push(Value::Integer(height as i64));
        }
        Kind::Float(size) => {
            plist.push(keyword(":height"));
            plist.push(Value::Integer(10 * i64::from(double_to_int(size.get()))));
        }
        _ => {}
    }
    for (prop, key) in [
        (FONT_WEIGHT_INDEX, ":weight"),
        (FONT_SLANT_INDEX, ":slant"),
        (FONT_WIDTH_INDEX, ":width"),
    ] {
        let val = font_style_symbolic(interp, &font, prop, true)?;
        if !val.is_nil() {
            plist.push(keyword(key));
            plist.push(val);
        }
    }
    Ok(Value::list(plist))
}

/// copy_font_spec: a font-spec of FONT's properties but its type, its
/// extra alist's cells copied but for `:font-entity' ones.
pub(crate) fn copy_font_spec(interp: &mut Interpreter, font: &Value) -> Result<Value, LispError> {
    check_font(interp, font)?;
    let mut copy = vec![Value::Nil; FONT_SPEC_MAX];
    for (index, slot) in copy
        .iter_mut()
        .enumerate()
        .take(FONT_EXTRA_INDEX)
        .skip(FONT_FOUNDRY_INDEX)
    {
        *slot = aref(interp, font, index);
    }
    let mut entries = Vec::new();
    let mut tail = aref(interp, font, FONT_EXTRA_INDEX);
    while let Some((entry, rest)) = tail.cons_values() {
        let (key, value) = entry
            .cons_values()
            .ok_or_else(|| wrong_type_argument("listp", entry))?;
        if !crate::lisp::types::eq(key, keyword(":font-entity")) {
            entries.push(Value::cons(key, value));
        }
        tail = rest;
    }
    copy[FONT_EXTRA_INDEX] = Value::list(entries);
    Ok(interp.create_pseudovector(crate::lisp::eval::RecordKind::Font, "font-spec", copy))
}

/// font_clear_prop: the font in the lface ATTRS made a copy with PROP and
/// the properties that go with it cleared, its `:name' gone.
pub(crate) fn font_clear_prop(
    interp: &mut Interpreter,
    attrs: &mut [Value],
    prop: usize,
    env: &mut Env,
) -> Result<(), LispError> {
    const LFACE_FONT_INDEX: usize = 15;
    let mut font = attrs[LFACE_FONT_INDEX];
    if font_kind(interp, &font).is_none() {
        return Ok(());
    }
    if !font_get(interp, &font, &keyword(":name"), env)?.is_nil() {
        font = copy_font_spec(interp, &font)?;
        font_put_extra(interp, &font, keyword(":name"), None, env)?;
    }
    if aref(interp, &font, prop).is_nil()
        && !matches!(
            prop,
            FONT_FAMILY_INDEX | FONT_FOUNDRY_INDEX | FONT_WIDTH_INDEX | FONT_SIZE_INDEX
        )
    {
        return Ok(());
    }
    if font.eq_value(attrs[LFACE_FONT_INDEX]) {
        font = copy_font_spec(interp, &font)?;
    }
    let cleared: &[usize] = match prop {
        FONT_FAMILY_INDEX => &[
            FONT_FAMILY_INDEX,
            FONT_FOUNDRY_INDEX,
            FONT_WIDTH_INDEX,
            FONT_ADSTYLE_INDEX,
            FONT_REGISTRY_INDEX,
            FONT_SIZE_INDEX,
            FONT_DPI_INDEX,
            FONT_SPACING_INDEX,
            FONT_AVGWIDTH_INDEX,
        ],
        FONT_FOUNDRY_INDEX => &[
            FONT_FOUNDRY_INDEX,
            FONT_ADSTYLE_INDEX,
            FONT_REGISTRY_INDEX,
            FONT_SIZE_INDEX,
            FONT_DPI_INDEX,
            FONT_SPACING_INDEX,
            FONT_AVGWIDTH_INDEX,
        ],
        FONT_SIZE_INDEX => &[
            FONT_SIZE_INDEX,
            FONT_DPI_INDEX,
            FONT_SPACING_INDEX,
            FONT_AVGWIDTH_INDEX,
        ],
        FONT_WIDTH_INDEX => &[FONT_WIDTH_INDEX, FONT_AVGWIDTH_INDEX],
        _ => std::slice::from_ref(&prop),
    };
    for &index in cleared {
        aset(interp, &font, index, Value::Nil);
    }
    attrs[LFACE_FONT_INDEX] = font;
    Ok(())
}
