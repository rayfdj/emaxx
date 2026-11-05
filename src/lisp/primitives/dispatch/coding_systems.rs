//! coding.c's coding system objects: Fdefine_coding_system_internal's
//! argument checks and attribute vector, Fcoding_system_put,
//! Fdefine_coding_system_alias and the checks CHECK_CODING_SYSTEM and
//! CHECK_CODING_SYSTEM_GET_SPEC make through Fcheck_coding_system.

use super::*;
use crate::lisp::eval::CodingSystemState;
use crate::lisp::eval::coding::{
    CODING_CATEGORY_BIG5, CODING_CATEGORY_CCL, CODING_CATEGORY_CHARSET, CODING_CATEGORY_EMACS_MULE,
    CODING_CATEGORY_ISO_8_1, CODING_CATEGORY_ISO_8_2, CODING_CATEGORY_NAMES,
    CODING_CATEGORY_RAW_TEXT, CODING_CATEGORY_SJIS, CODING_CATEGORY_UNDECIDED,
    CODING_CATEGORY_UTF_8_AUTO, CODING_CATEGORY_UTF_8_NOSIG, CODING_CATEGORY_UTF_8_SIG,
    CODING_CATEGORY_UTF_16_AUTO, CODING_CATEGORY_UTF_16_BE, CODING_CATEGORY_UTF_16_BE_NOSIG,
    CODING_CATEGORY_UTF_16_LE, CODING_CATEGORY_UTF_16_LE_NOSIG,
};
use crate::lisp::primitives::coding::charset_table::charset_info;
use crate::lisp::types::Kind;

// coding.c `enum define_coding_system_arg_index'.
const ARG_NAME: usize = 0;
const ARG_MNEMONIC: usize = 1;
const ARG_CODING_TYPE: usize = 2;
const ARG_CHARSET_LIST: usize = 3;
const ARG_ASCII_COMPATIBLE_P: usize = 4;
const ARG_DECODE_TRANSLATION_TABLE: usize = 5;
const ARG_ENCODE_TRANSLATION_TABLE: usize = 6;
const ARG_POST_READ_CONVERSION: usize = 7;
const ARG_PRE_WRITE_CONVERSION: usize = 8;
const ARG_DEFAULT_CHAR: usize = 9;
const ARG_FOR_UNIBYTE: usize = 10;
const ARG_PLIST: usize = 11;
const ARG_EOL_TYPE: usize = 12;
const ARG_MAX: usize = 13;
const ARG_CCL_MAX: usize = 16;
const ARG_ISO2022_MAX: usize = 17;
const ARG_UTF8_MAX: usize = 14;
const ARG_UTF16_MAX: usize = 15;
const ARG_UNDECIDED_MAX: usize = 16;

/// The symbol name of VALUE when it is a symbol, nil and t included.
fn symbol_name(value: &Value) -> Option<String> {
    match value.kind() {
        Kind::Nil => Some("nil".into()),
        Kind::T => Some("t".into()),
        Kind::Symbol(name) => Some(name.to_string()),
        _ => None,
    }
}

/// XSYMBOL of a value CHECK_SYMBOL passed: the symbol itself, nil and t
/// as theirs.
fn symbol_object(value: &Value) -> Option<crate::lisp::types::SymbolName> {
    match value.kind() {
        Kind::Nil => Some(crate::lisp::types::symbols::builtin_boolean(false)),
        Kind::T => Some(crate::lisp::types::symbols::builtin_boolean(true)),
        Kind::Symbol(symbol) => Some(symbol),
        _ => None,
    }
}

/// CHECK_SYMBOL.
fn check_symbol(value: &Value) -> Result<(), LispError> {
    symbol_name(value)
        .map(|_| ())
        .ok_or_else(|| wrong_type_argument("symbolp", *value))
}

/// A translation table argument: a char-table, a cons or a symbol.
fn check_translation_table(value: &Value) -> Result<(), LispError> {
    if matches!(value.kind(), Kind::CharTable(_) | Kind::Cons(_)) {
        return Ok(());
    }
    check_symbol(value)
}

/// The mnemonic argument: a string's first character, else
/// CHECK_CHARACTER.
fn check_mnemonic(value: &Value) -> Result<i64, LispError> {
    if let Some(text) = string_like(value) {
        return Ok(text
            .text
            .chars()
            .next()
            .map_or(0, |c| i64::from(u32::from(c))));
    }
    check_character(value).map(i64::from)
}

/// coding.c Fcheck_coding_system: a pending `coding-system-define-form'
/// is cleared and evaluated through safe_eval first; CODING-SYSTEM itself
/// when it is a coding system, else `coding-system-error'.
pub(crate) fn check_coding_system(
    interp: &mut Interpreter,
    coding_system: &Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    // Fget's CHECK_SYMBOL, then the symbol's own property.
    let symbol = match coding_system.kind() {
        Kind::Symbol(symbol) => symbol,
        Kind::Nil => crate::lisp::types::symbols::builtin_boolean(false),
        Kind::T => crate::lisp::types::symbols::builtin_boolean(true),
        _ => return Err(wrong_type_argument("symbolp", *coding_system)),
    };
    let define_form = interp
        .get_property_of_symbol_value(*coding_system, "coding-system-define-form")
        .unwrap_or(Value::Nil);
    if !define_form.is_nil() {
        interp.put_symbol_property_on(&symbol, "coding-system-define-form", Value::Nil);
        interp.safe_funcall(Value::symbol("eval"), &[define_form], env)?;
    }
    if crate::lisp::primitives::coding::coding_system_p(interp, coding_system) {
        return Ok(*coding_system);
    }
    Err(crate::lisp::primitives::coding::coding_system_error(
        *coding_system,
    ))
}

/// CHECK_CODING_SYSTEM_GET_SPEC: X's spec, by index, after
/// Fcheck_coding_system when the table has none.
pub(crate) fn checked_coding_spec(
    interp: &mut Interpreter,
    x: &Value,
    env: &mut Env,
) -> Result<usize, LispError> {
    let lookup = |interp: &Interpreter| {
        symbol_object(x).and_then(|symbol| interp.coding_spec_index_symbol(&symbol))
    };
    if let Some(spec) = lookup(interp) {
        return Ok(spec);
    }
    check_coding_system(interp, x, env)?;
    lookup(interp).ok_or_else(|| wrong_type_argument("coding-system-p", *x))
}

/// CHECK_CODING_SYSTEM.
fn check_coding_system_macro(
    interp: &mut Interpreter,
    x: &Value,
    env: &mut Env,
) -> Result<(), LispError> {
    if symbol_object(x).is_some_and(|symbol| interp.has_coding_system_symbol(&symbol)) {
        return Ok(());
    }
    if check_coding_system(interp, x, env)?.is_nil() {
        return Err(wrong_type_argument("coding-system-p", *x));
    }
    Ok(())
}

/// A :bom argument of utf-8 or utf-16: nil, t, or a cons of two coding
/// systems.
fn check_bom(interp: &mut Interpreter, bom: &Value, env: &mut Env) -> Result<(), LispError> {
    if bom.is_nil() || matches!(bom.kind(), Kind::T) {
        return Ok(());
    }
    let (with, without) = bom
        .cons_values()
        .ok_or_else(|| wrong_type_argument("consp", *bom))?;
    check_coding_system_macro(interp, &with, env)?;
    check_coding_system_macro(interp, &without, env)
}

/// check_integer_range (VALUE, FROM, TO).
fn check_integer_range(value: Value, from: i64, to: i64) -> Result<i64, LispError> {
    let Some(n) = value.integer_value() else {
        return Err(wrong_type_argument("integerp", value));
    };
    if (from..=to).contains(&n) {
        Ok(n)
    } else {
        Err(LispError::SignalValue(Value::list([
            Value::symbol("args-out-of-range"),
            value,
            Value::Integer(from),
            Value::Integer(to),
        ])))
    }
}

/// The charset X names, by canonical name (CHECK_CHARSET_GET_CHARSET).
fn checked_charset(interp: &Interpreter, x: &Value) -> Result<String, LispError> {
    crate::lisp::primitives::charset::check_charset_get_id(interp, x)
}

fn charset_dimension(interp: &Interpreter, charset: &str) -> u32 {
    charset_info(interp, charset).map_or(1, |info| info.dimension)
}

fn charset_ascii_compatible(interp: &Interpreter, charset: &str) -> bool {
    crate::lisp::primitives::coding::charset_ascii_compatible_p(interp, charset)
}

/// The elements of a proper list, as list_length walks it.
fn list_elements(list: &Value) -> Result<Vec<Value>, LispError> {
    list.to_vec()
        .map_err(|_| wrong_type_argument("listp", *list))
}

/// coding.c Fdefine_coding_system_internal.
pub(crate) fn define_coding_system_internal(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    let short_args = || {
        LispError::SignalValue(Value::cons(
            Value::symbol("wrong-number-of-arguments"),
            Value::cons(
                Value::symbol("define-coding-system-internal"),
                Value::Integer(args.len() as i64),
            ),
        ))
    };
    if args.len() < ARG_MAX {
        return Err(short_args());
    }
    let name_value = args[ARG_NAME];
    let name_symbol =
        symbol_object(&name_value).ok_or_else(|| wrong_type_argument("symbolp", name_value))?;
    let mnemonic = check_mnemonic(&args[ARG_MNEMONIC])?;
    let coding_type_value = args[ARG_CODING_TYPE];
    let coding_type = symbol_name(&coding_type_value)
        .ok_or_else(|| wrong_type_argument("symbolp", coding_type_value))?;

    let invalid_charset_list = || LispError::Signal("Invalid charset-list".into());
    let charset_arg = args[ARG_CHARSET_LIST];
    let charset_list = match symbol_name(&charset_arg) {
        Some(symbol) => {
            // The symbols `iso-2022' and `emacs-mule' stand for those
            // lists of charset ids; any other symbol (nil included) has
            // no elements to check.
            if symbol == "iso-2022" && coding_type != "iso-2022"
                || symbol == "emacs-mule" && coding_type != "emacs-mule"
            {
                return Err(invalid_charset_list());
            }
            charset_arg
        }
        None => {
            let copy =
                crate::lisp::primitives::sequences::copy_sequence_value(interp, &charset_arg)?;
            let mut names = Vec::new();
            let mut tail = copy;
            while let Some((charset, rest)) = tail.cons_values() {
                let canonical = checked_charset(interp, &charset)?;
                let info = charset_info(interp, &canonical);
                let unhandled = match coding_type.as_str() {
                    "iso-2022" => info.as_ref().is_none_or(|info| info.iso_final.is_none()),
                    "emacs-mule" => info
                        .as_ref()
                        .is_none_or(|info| info.emacs_mule_id.is_none()),
                    _ => false,
                };
                if unhandled {
                    return Err(crate::lisp::primitives::values::doprnt_error(
                        interp,
                        "Can't handle charset `{}'",
                        &[&canonical],
                    ));
                }
                names.push(Value::symbol(&canonical));
                tail = rest;
            }
            if matches!(copy.kind(), Kind::Cons(_) | Kind::Nil) {
                Value::list(names)
            } else {
                copy
            }
        }
    };

    let mut ascii_compatible = args[ARG_ASCII_COMPATIBLE_P];
    let decode_table = args[ARG_DECODE_TRANSLATION_TABLE];
    check_translation_table(&decode_table)?;
    let encode_table = args[ARG_ENCODE_TRANSLATION_TABLE];
    check_translation_table(&encode_table)?;
    let post_read = args[ARG_POST_READ_CONVERSION];
    check_symbol(&post_read)?;
    let pre_write = args[ARG_PRE_WRITE_CONVERSION];
    check_symbol(&pre_write)?;
    let default_char = if args[ARG_DEFAULT_CHAR].is_nil() {
        u32::from(b' ')
    } else {
        check_character(&args[ARG_DEFAULT_CHAR])?
    };
    let for_unibyte = !args[ARG_FOR_UNIBYTE].is_nil();
    let plist = args[ARG_PLIST];
    if !plist.is_nil() && plist.cons_values().is_none() {
        return Err(wrong_type_argument("listp", plist));
    }

    let charset_names = || -> Vec<String> {
        charset_list
            .to_vec()
            .unwrap_or_default()
            .iter()
            .filter_map(|charset| charset.symbol_str().map(str::to_string))
            .collect()
    };
    let mut type_args = Vec::new();
    let category = match coding_type.as_str() {
        "charset" => {
            if charset_names()
                .iter()
                .any(|charset| charset_ascii_compatible(interp, charset))
            {
                ascii_compatible = Value::T;
            }
            CODING_CATEGORY_CHARSET
        }
        "ccl" => {
            if args.len() < ARG_CCL_MAX {
                return Err(short_args());
            }
            type_args = ccl_coding_args(interp, args)?;
            CODING_CATEGORY_CCL
        }
        "utf-16" => {
            ascii_compatible = Value::Nil;
            if args.len() < ARG_UTF16_MAX {
                return Err(short_args());
            }
            let bom = args[13];
            check_bom(interp, &bom, env)?;
            let endian_value = args[14];
            let endian = symbol_name(&endian_value)
                .ok_or_else(|| wrong_type_argument("symbolp", endian_value))?;
            let big = match endian.as_str() {
                "nil" | "big" => true,
                "little" => false,
                _ => return Err(LispError::Signal(format!("Invalid endian: {endian}"))),
            };
            type_args = vec![bom, Value::symbol(if big { "big" } else { "little" })];
            match bom.kind() {
                Kind::Cons(_) => CODING_CATEGORY_UTF_16_AUTO,
                Kind::Nil if big => CODING_CATEGORY_UTF_16_BE_NOSIG,
                Kind::Nil => CODING_CATEGORY_UTF_16_LE_NOSIG,
                _ if big => CODING_CATEGORY_UTF_16_BE,
                _ => CODING_CATEGORY_UTF_16_LE,
            }
        }
        "iso-2022" => {
            if args.len() < ARG_ISO2022_MAX {
                return Err(short_args());
            }
            let initial =
                crate::lisp::primitives::sequences::copy_sequence_value(interp, &args[13])?;
            let Kind::Vector(designations) = initial.kind() else {
                return Err(wrong_type_argument("vectorp", initial));
            };
            for (index, designation) in designations.slots().take(4).enumerate() {
                if designation.is_nil() {
                    continue;
                }
                let charset = checked_charset(interp, &designation)?;
                if index == 0 && charset_ascii_compatible(interp, &charset) {
                    ascii_compatible = Value::T;
                }
            }
            let reg_usage = args[14];
            let (reg94, reg96) = reg_usage
                .cons_values()
                .ok_or_else(|| wrong_type_argument("consp", reg_usage))?;
            for usage in [reg94, reg96] {
                if !matches!(usage.kind(), Kind::Integer(_)) {
                    return Err(wrong_type_argument("fixnump", usage));
                }
            }
            let request =
                crate::lisp::primitives::sequences::copy_sequence_value(interp, &args[15])?;
            let mut tail = request;
            while let Some((element, rest)) = tail.cons_values() {
                let (charset, register) = element
                    .cons_values()
                    .ok_or_else(|| wrong_type_argument("consp", element))?;
                checked_charset(interp, &charset)?;
                check_integer_range(register, 0, 3)?;
                tail = rest;
            }
            let flags = args[16];
            if !matches!(flags.kind(), Kind::Integer(n) if n >= 0) {
                return Err(wrong_type_argument("wholenump", flags));
            }
            type_args = vec![initial, reg_usage, request, flags];
            let category = interp.coding_category_index("iso-2022", &charset_arg, &type_args);
            if !matches!(category, CODING_CATEGORY_ISO_8_1 | CODING_CATEGORY_ISO_8_2) {
                ascii_compatible = Value::Nil;
            }
            category
        }
        "emacs-mule" => {
            ascii_compatible = Value::T;
            CODING_CATEGORY_EMACS_MULE
        }
        "shift-jis" | "big5" => {
            let charsets = list_elements(&charset_list)?;
            let shift_jis = coding_type == "shift-jis";
            let counts_ok = if shift_jis {
                matches!(charsets.len(), 3 | 4)
            } else {
                charsets.len() == 2
            };
            if !counts_ok {
                return Err(LispError::Signal(
                    if shift_jis {
                        "There should be three or four charsets"
                    } else {
                        "There should be just two charsets"
                    }
                    .into(),
                ));
            }
            let dimensions: &[u32] = if shift_jis { &[1, 1, 2, 2] } else { &[1, 2] };
            for (index, (charset, dimension)) in charsets.iter().zip(dimensions).enumerate() {
                let charset = charset.symbol_str().unwrap_or_default().to_string();
                if charset_dimension(interp, &charset) != *dimension {
                    return Err(LispError::Signal(format!(
                        "Dimension of charset {charset} is not {}",
                        if *dimension == 1 { "one" } else { "two" }
                    )));
                }
                if index == 0 && charset_ascii_compatible(interp, &charset) {
                    ascii_compatible = Value::T;
                }
            }
            if shift_jis {
                CODING_CATEGORY_SJIS
            } else {
                CODING_CATEGORY_BIG5
            }
        }
        "raw-text" => {
            ascii_compatible = Value::T;
            CODING_CATEGORY_RAW_TEXT
        }
        "utf-8" => {
            if args.len() < ARG_UTF8_MAX {
                return Err(short_args());
            }
            let bom = args[13];
            check_bom(interp, &bom, env)?;
            if bom.is_nil() {
                ascii_compatible = Value::T;
            }
            type_args = vec![bom];
            match bom.kind() {
                Kind::Cons(_) => CODING_CATEGORY_UTF_8_AUTO,
                Kind::Nil => CODING_CATEGORY_UTF_8_NOSIG,
                _ => CODING_CATEGORY_UTF_8_SIG,
            }
        }
        "undecided" => {
            if args.len() < ARG_UNDECIDED_MAX {
                return Err(short_args());
            }
            type_args = args[13..16].to_vec();
            CODING_CATEGORY_UNDECIDED
        }
        _ => {
            return Err(LispError::Signal(format!(
                "Invalid coding system type: {coding_type}"
            )));
        }
    };

    // The plist gains `:category' and then `:ascii-compatible-p' in front.
    let plist = Value::cons(
        Value::symbol(":ascii-compatible-p"),
        Value::cons(
            ascii_compatible,
            Value::cons(
                Value::symbol(":category"),
                Value::cons(Value::symbol(CODING_CATEGORY_NAMES[category]), plist),
            ),
        ),
    );
    let eol_type = args[ARG_EOL_TYPE];
    if !matches!(
        symbol_name(&eol_type).as_deref(),
        Some("nil" | "unix" | "dos" | "mac")
    ) {
        return Err(LispError::Signal("Invalid eol-type".into()));
    }
    interp.define_coding_system(
        name_symbol,
        CodingSystemState {
            base: name_symbol,
            mnemonic,
            kind: coding_type,
            ascii_compatible,
            decode_table,
            encode_table,
            post_read,
            pre_write,
            for_unibyte,
            plist,
            category,
            charset_list,
            default_char,
            type_args,
            derived: Default::default(),
        },
        eol_type,
    );
    Ok(Value::Nil)
}

/// Fdefine_coding_system_internal's arguments of a `ccl' coding: the
/// decoder and the encoder, each a CCL program (CHECK_CCL_PROGRAM, a
/// vector copied), and the valid bytes, each a byte or a (FROM . TO)
/// range of them.
fn ccl_coding_args(interp: &Interpreter, args: &[Value]) -> Result<Vec<Value>, LispError> {
    let program = |value: &Value| -> Result<Value, LispError> {
        if !crate::lisp::primitives::ccl::ccl_program_p(interp, value) {
            return Err(LispError::WrongTypeArgument("cclp".into(), *value));
        }
        Ok(match value.kind() {
            Kind::Vector(vector) => Value::vector(vector.slots()),
            _ => *value,
        })
    };
    let decoder = program(&args[13])?;
    let encoder = program(&args[14])?;
    let mut tail = args[15];
    while let Kind::Cons(cell) = tail.kind() {
        let val = cell.car.get();
        match val.kind() {
            Kind::Integer(byte) => {
                if !(0..=255).contains(&byte) {
                    return Err(LispError::SignalValue(Value::list([
                        Value::symbol("args-out-of-range"),
                        val,
                        Value::Integer(0),
                        Value::Integer(255),
                    ])));
                }
            }
            Kind::Cons(range) => {
                let from = check_integer_range(range.car.get(), 0, 255)?;
                check_integer_range(range.cdr.get(), from, 255)?;
            }
            _ => return Err(LispError::WrongTypeArgument("consp".into(), val)),
        }
        tail = cell.cdr.get();
    }
    Ok(vec![decoder, encoder, args[15]])
}

/// coding.c Fcoding_system_put: the attribute PROP stands for, when it
/// is one of those `define-coding-system' sets, then the plist.
pub(crate) fn coding_system_put(
    interp: &mut Interpreter,
    coding_system: &Value,
    prop: &Value,
    val: &Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    let spec = checked_coding_spec(interp, coding_system, env)?;
    let attrs = interp.coding_specs[spec].attrs;
    let mut val = *val;
    match prop.symbol_str() {
        Some(":mnemonic") => {
            let mnemonic = check_mnemonic(&val)?;
            val = Value::Integer(mnemonic);
            interp.coding_attrs[attrs].mnemonic = mnemonic;
        }
        Some(":default-char") => {
            if val.is_nil() {
                val = Value::Integer(i64::from(b' '));
            }
            interp.coding_attrs[attrs].default_char = check_character(&val)?;
        }
        Some(":decode-translation-table") => {
            check_translation_table(&val)?;
            interp.coding_attrs[attrs].decode_table = val;
        }
        Some(":encode-translation-table") => {
            check_translation_table(&val)?;
            interp.coding_attrs[attrs].encode_table = val;
        }
        Some(":post-read-conversion") => {
            check_symbol(&val)?;
            interp.coding_attrs[attrs].post_read = val;
        }
        Some(":pre-write-conversion") => {
            check_symbol(&val)?;
            interp.coding_attrs[attrs].pre_write = val;
        }
        Some(":ascii-compatible-p") => interp.coding_attrs[attrs].ascii_compatible = val,
        _ => {}
    }
    interp.coding_attrs[attrs].derived.clear();
    let plist = interp.coding_attrs[attrs].plist;
    let plist = super::collections::plist_put_eq(interp, plist, *prop, val, env)?;
    interp.coding_attrs[attrs].plist = plist;
    Ok(val)
}

/// coding.c Fdefine_coding_system_alias.
pub(crate) fn define_coding_system_alias(
    interp: &mut Interpreter,
    alias: &Value,
    coding_system: &Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    let alias = symbol_object(alias).ok_or_else(|| wrong_type_argument("symbolp", *alias))?;
    let spec = checked_coding_spec(interp, coding_system, env)?;
    interp.define_coding_system_alias(alias, spec)?;
    Ok(Value::Nil)
}
