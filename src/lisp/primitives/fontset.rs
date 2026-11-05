//! fontset.c's base fontsets.  Vfontset_table holds them by id, each a
//! char-table of purpose `fontset' whose element for a character is a
//! vector of font_defs [FONT-SPEC ENCODING REPERTORY] (nil for a font set
//! to nothing) and whose extra slots are FONTSET_ID, FONTSET_NAME,
//! FONTSET_ASCII, FONTSET_BASE, FONTSET_FRAME, FONTSET_NOFONT_FACE,
//! FONTSET_DEFAULT and FONTSET_FALLBACK.  No frame realizes a fontset, a
//! terminal frame using none.

use super::*;
use crate::lisp::primitives::font;
use crate::lisp::types::{CharTableRef, Kind, SymbolName};

const FONTSET_ID: usize = 0;
const FONTSET_NAME: usize = 1;
const FONTSET_ASCII: usize = 2;
const FONTSET_BASE: usize = 3;
const FONTSET_FALLBACK: usize = 7;

const DEFAULT_FONTSET_NAME: &str = "-*-*-*-*-*-*-*-*-*-*-*-*-fontset-default";

/// syms_of_fontset: Vfontset_table of 32 slots, the default fontset at
/// id 0 named DEFAULT_FONTSET_NAME.
pub(crate) fn make_fontset_table(interp: &mut Interpreter) -> Value {
    let default = interp.make_char_table(Some("fontset".into()), Value::Nil);
    let table = Value::vector(std::iter::repeat_n(Value::Nil, 32));
    if let Kind::CharTable(fontset) = default.kind() {
        fontset.set_extra(FONTSET_ID, Value::Integer(0));
        fontset.set_extra(FONTSET_NAME, Value::string(DEFAULT_FONTSET_NAME));
    }
    aset_vector_value(&table, 0, default).expect("a fresh fontset table");
    table
}

fn table_items(interp: &Interpreter) -> Vec<Value> {
    vector_items(&interp.fontset_table).unwrap_or_default()
}

fn char_table(fontset: &Value) -> CharTableRef {
    match fontset.kind() {
        Kind::CharTable(table) => table,
        _ => unreachable!("a fontset is a char-table"),
    }
}

fn extra(fontset: &Value, slot: usize) -> Value {
    char_table(fontset).extra(slot).unwrap_or(Value::Nil)
}

fn set_extra(fontset: &Value, slot: usize, value: Value) {
    char_table(fontset).set_extra(slot, value);
}

/// FONTSET_FROM_ID.
fn fontset_from_id(interp: &Interpreter, id: usize) -> Value {
    table_items(interp).get(id).copied().unwrap_or(Value::Nil)
}

/// Vdefault_fontset.
fn default_fontset(interp: &Interpreter) -> Value {
    fontset_from_id(interp, 0)
}

/// BASE_FONTSET_P.
fn base_fontset_p(fontset: &Value) -> bool {
    extra(fontset, FONTSET_BASE).is_nil()
}

/// make_fontset of a base fontset NAME: the first free id of
/// Vfontset_table, the table grown while its last slot would be taken.
fn make_fontset(interp: &mut Interpreter, name: Value) -> Value {
    let items = table_items(interp);
    let id = items.iter().position(Value::is_nil).unwrap_or(items.len());
    if id + 1 >= items.len() {
        let grown = items.len().max(1) * 2;
        interp.fontset_table = Value::vector(
            items
                .iter()
                .copied()
                .chain(std::iter::repeat_n(Value::Nil, grown - items.len())),
        );
    }
    let fontset = interp.make_char_table(Some("fontset".into()), Value::Nil);
    set_extra(&fontset, FONTSET_ID, Value::Integer(id as i64));
    set_extra(&fontset, FONTSET_NAME, name);
    aset_vector_value(&interp.fontset_table, id, fontset).expect("a free fontset slot");
    fontset
}

/// FONTSET_REF: FONTSET's element for C, a base fontset's nil one looked
/// up in the default fontset.
fn fontset_ref(interp: &Interpreter, fontset: &Value, c: u32) -> Value {
    let elt = char_table(fontset).get(c);
    let default = default_fontset(interp);
    if elt.is_nil() && !fontset.eq_value(default) && base_fontset_p(fontset) {
        return char_table(&default).get(c);
    }
    elt
}

/// fontset_pattern_regexp: the regexp of the fontset name PATTERN with
/// `*' and `?' wildcards, or None when it has neither.
fn fontset_pattern_regexp(pattern: &Value) -> Result<Option<Value>, LispError> {
    let bytes = font::string_sdata(pattern)?;
    let bytes = &bytes[..bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len())];
    if !bytes.contains(&b'*') && !bytes.contains(&b'?') {
        // PATTERN does not contain any wild cards.
        return Ok(None);
    }
    let ndashes = bytes.iter().filter(|&&b| b == b'-').count();
    let mut regex = vec![b'^'];
    for &c in bytes {
        match c {
            // If PATTERN is not full XLFD we convert "*" to ".*".
            // Otherwise we convert "*" to "[^-]*" which is much faster in
            // regular expression matching.
            b'*' if ndashes < 14 => regex.extend_from_slice(b".*"),
            b'*' => regex.extend_from_slice(b"[^-]*"),
            b'?' => regex.push(b'.'),
            b'[' | b'.' | b'\\' | b'+' | b'^' | b'$' => {
                regex.push(b'\\');
                regex.push(c);
            }
            _ => regex.push(c),
        }
    }
    regex.push(b'$');
    super::strings::make_string(regex)
        .map(Some)
        .map_err(|error| LispError::Signal(error.to_string()))
}

/// fs_query_fontset: the id of the base fontset NAME names, its
/// fontset-alias-alist alias resolved; with NAME_PATTERN 0 a name with
/// wildcards is a pattern, with 1 NAME is a regexp.
pub(crate) fn fs_query_fontset(
    interp: &mut Interpreter,
    name: &Value,
    mut name_pattern: i32,
    env: &mut Env,
) -> Result<Option<usize>, LispError> {
    let mut name = super::call(interp, "downcase", std::slice::from_ref(name), env)?;
    if name_pattern != 1 {
        let aliases = interp
            .symbol_value_symbol(&SymbolName::intern_str("fontset-alias-alist"))
            .unwrap_or(Value::Nil);
        let mut tem = super::call(interp, "rassoc", &[name, aliases], env)?;
        if tem.is_nil() {
            tem = super::call(interp, "assoc", &[name, aliases], env)?;
        }
        if let Some((alias, _)) = tem.cons_values()
            && alias.is_string()
        {
            name = alias;
        } else if name_pattern == 0
            && let Some(regexp) = fontset_pattern_regexp(&name)?
        {
            name = regexp;
            name_pattern = 1;
        }
    }
    let text = string_text(&name)?;
    for (id, fontset) in table_items(interp).into_iter().enumerate() {
        if fontset.is_nil() || !base_fontset_p(&fontset) {
            continue;
        }
        let this_name = extra(&fontset, FONTSET_NAME);
        let found = if name_pattern == 1 {
            let canon = interp.standard_case_tables().canon;
            super::search::fast_string_match(interp, env, &name, &this_name, Some(canon), false)?
                .is_some()
        } else {
            string_text(&this_name)?.eq_ignore_ascii_case(&text)
        };
        if found {
            return Ok(Some(id));
        }
    }
    Ok(None)
}

/// check_fontset_name: the fontset NAME names after FRAME is decoded as a
/// live frame: t the default one, nil the frame's own (none on a terminal
/// frame), a string the base fontset of that name, or else as a pattern.
pub(crate) fn check_fontset_name(
    interp: &mut Interpreter,
    name: &Value,
    frame: Option<&Value>,
    env: &mut Env,
) -> Result<Value, LispError> {
    super::dispatch::decode_live_frame(interp, frame)?;
    match name.kind() {
        Kind::T => return Ok(default_fontset(interp)),
        Kind::Nil => {
            return Err(crate::lisp::primitives::values::doprnt_error(
                interp,
                "Can't use fontsets in non-GUI frames",
                &[],
            ));
        }
        _ => {}
    }
    if !name.is_string() {
        return Err(wrong_type_argument("stringp", *name));
    }
    // First try NAME as literal.
    let id = match fs_query_fontset(interp, name, 2, env)? {
        Some(id) => id,
        // For backward compatibility, try again NAME as pattern.
        None => match fs_query_fontset(interp, name, 0, env)? {
            Some(id) => id,
            None => {
                return Err(crate::lisp::primitives::values::doprnt_error(
                    interp,
                    "Fontset `{}' does not exist",
                    &[&string_text(name)?],
                ));
            }
        },
    };
    Ok(fontset_from_id(interp, id))
}

/// FONTSET_ADD: RANGE's characters, or the fallback for None, given the
/// vector [ELT] when ADD is nil, or ELT appended (`append') or prepended
/// (any other ADD) to each run of their elements (fontset_add).
fn fontset_add(fontset: &Value, range: Option<(u32, u32)>, elt: Value, add: &Value) {
    let new = Value::vector([elt]);
    let table = char_table(fontset);
    if add.is_nil() {
        match range {
            None => set_extra(fontset, FONTSET_FALLBACK, new),
            Some((from, to)) => {
                if from <= to {
                    table.set_range(from, to, new);
                }
            }
        }
        return;
    }
    let append = add.eq_value(Value::symbol("append"));
    let merged = |old: Value| -> Value {
        if old.is_nil() {
            return new;
        }
        let old_items = vector_items(&old).unwrap_or_default();
        let new_items = vector_items(&new).unwrap_or_default();
        if append {
            Value::vector(old_items.into_iter().chain(new_items))
        } else {
            Value::vector(new_items.into_iter().chain(old_items))
        }
    };
    match range {
        None => {
            let old = extra(fontset, FONTSET_FALLBACK);
            set_extra(fontset, FONTSET_FALLBACK, merged(old));
        }
        Some((mut from, to)) => {
            if from > to {
                return;
            }
            loop {
                let (old, _, to1) = table.ref_and_range(from, from, to);
                table.set_range(from, to1, merged(old));
                if to1 >= to {
                    break;
                }
                from = to1 + 1;
            }
        }
    }
}

/// find_font_encoding: the cdr of the first font-encoding-alist entry
/// whose regexp matches FONTNAME and whose cdr is a charset or a cons of
/// one, or nil.
fn find_font_encoding(
    interp: &mut Interpreter,
    fontname: &Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    let alist = interp
        .symbol_value_symbol(&SymbolName::intern_str("font-encoding-alist"))
        .unwrap_or(Value::Nil);
    let mut tail = alist;
    while let Some((elt, rest)) = tail.cons_values() {
        if let Some((regexp, encoding)) = elt.cons_values()
            && regexp.is_string()
        {
            let canon = interp.standard_case_tables().canon;
            if super::search::fast_string_match(interp, env, &regexp, fontname, Some(canon), false)?
                .is_some()
            {
                let charsetp =
                    |value: &Value| value.as_symbol().is_ok_and(|name| interp.has_charset(name));
                let valid = match encoding.kind() {
                    Kind::Nil | Kind::T | Kind::Symbol(_) => charsetp(&encoding),
                    _ => encoding
                        .cons_values()
                        .is_some_and(|(charset, _)| charsetp(&charset)),
                };
                if valid {
                    return Ok(encoding);
                }
            }
        }
        tail = rest;
    }
    Ok(Value::Nil)
}

/// CHECK_CHARSET then CHARSET_SYMBOL_ID.
fn charset_symbol_id(interp: &Interpreter, charset: &Value) -> Result<i64, LispError> {
    let name = super::coding::check_charset(interp, charset)?;
    interp
        .charset_id(name)
        .ok_or_else(|| wrong_type_argument("charsetp", *charset))
}

/// CHAR_CHARSET_P of the charset whose id is ID.
fn char_charset_p(interp: &Interpreter, c: u32, id: i64) -> bool {
    let Some(name) = interp.charset_name_of_id(id) else {
        return false;
    };
    if c < 0x80 && super::coding::charset_ascii_compatible_p(interp, name) {
        return true;
    }
    super::coding::encode_charset_char(interp, name, c).is_some()
}

/// Fset_fontset_font's set_fontset_font, map_charset_chars' callback: a
/// range of the charset's characters given FONT_DEF, after the script
/// ranges below it that SCRIPT_RANGES still holds, which a charset range
/// covering them consumes; an ASCII-preserving call skips the ASCII part.
fn set_fontset_font_range(
    fontset: &Value,
    font_def: Value,
    add: &Value,
    ascii_changed: bool,
    script_ranges: &mut Vec<(u32, u32)>,
    from: u32,
    to: u32,
) {
    let (mut from, to) = (from, to);
    if !ascii_changed && from < 0x80 {
        if to < 0x80 {
            return;
        }
        from = 0x80;
    }
    let mut range_from = from;
    while let Some(&(script_from, script_to)) = script_ranges.first()
        && script_to < from
    {
        fontset_add(fontset, Some((script_from, script_to)), font_def, add);
        script_ranges.remove(0);
    }
    if let Some(&(script_from, _)) = script_ranges.first() {
        if script_from < from {
            range_from = script_from;
        }
        while let Some(&(_, script_to)) = script_ranges.first()
            && script_to <= to
        {
            script_ranges.remove(0);
        }
        if let Some(first) = script_ranges.first_mut()
            && first.0 <= to
        {
            first.0 = to + 1;
        }
    }
    fontset_add(fontset, Some((range_from, to)), font_def, add);
}

/// The ranges of char-script-table whose value is SCRIPT, in order
/// (map_char_table with accumulate_script_ranges, reversed).
fn script_ranges(
    interp: &mut Interpreter,
    script: &Value,
    env: &mut Env,
) -> Result<Vec<(u32, u32)>, LispError> {
    let table = interp
        .lookup_var_key(cached_symbol!("char-script-table"), env)
        .unwrap_or(Value::Nil);
    let Kind::CharTable(table) = table.kind() else {
        return Ok(Vec::new());
    };
    Ok(table
        .effective_ranges()
        .into_iter()
        .filter(|entry| entry.value.eq_value(*script))
        .map(|entry| (entry.start, entry.end))
        .collect())
}

/// Fset_fontset_font.
pub(crate) fn set_fontset_font(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    let (fontset, characters, font_spec) = (args[0], args[1], args[2]);
    let frame = args.get(3).copied();
    let add = args.get(4).copied().unwrap_or(Value::Nil);
    let fontset_obj = check_fontset_name(interp, &fontset, frame.as_ref(), env)?;

    let mut fontname = Value::Nil;
    let font_spec = if let Some((family, registry)) = font_spec.cons_values() {
        let spec = font::font_make_spec(interp);
        font::font_parse_family_registry(interp, &family, &registry, &spec, env)?;
        fontname = font::font_xlfd_name(interp, &spec, false, true)?;
        spec
    } else if font_spec.is_string() {
        fontname = font_spec;
        font::font_spec(interp, &[Value::symbol(":name"), fontname], env)?
    } else if font::font_kind(interp, &font_spec) == Some(font::FontKind::Spec) {
        fontname = font::font_xlfd_name(interp, &font_spec, false, true)?;
        font_spec
    } else if !font_spec.is_nil() {
        return Err(LispError::SignalValue(Value::list([
            Value::symbol("font"),
            Value::string("Invalid font-spec"),
            font_spec,
        ])));
    } else {
        Value::Nil
    };

    let font_def = if font_spec.is_nil() {
        Value::Nil
    } else {
        let name_of = |interp: &mut Interpreter, value: Value, env: &mut Env| {
            if value.is_nil() {
                Ok(Value::Nil)
            } else {
                super::call(interp, "symbol-name", &[value], env)
            }
        };
        let family = font::aref(interp, &font_spec, font::FONT_FAMILY_INDEX);
        let family = name_of(interp, family, env)?;
        let registry = font::aref(interp, &font_spec, font::FONT_REGISTRY_INDEX);
        let mut registry = name_of(interp, registry, env)?;
        if !registry.is_nil() {
            registry = super::call(interp, "downcase", &[registry], env)?;
        }
        let name = super::call(
            interp,
            "concat",
            &[family, Value::string("-"), registry],
            env,
        )?;
        let mut encoding = find_font_encoding(interp, &name, env)?;
        if encoding.is_nil() {
            encoding = Value::symbol("ascii");
        }
        let (encoding, repertory) = if let Some((charset, repertory)) = encoding.cons_values() {
            let encoding = Value::Integer(charset_symbol_id(interp, &charset)?);
            let repertory =
                if !repertory.is_nil() && matches!(repertory.kind(), Kind::T | Kind::Symbol(_)) {
                    Value::Integer(charset_symbol_id(interp, &repertory)?)
                } else {
                    repertory
                };
            (encoding, repertory)
        } else {
            let id = Value::Integer(charset_symbol_id(interp, &encoding)?);
            (id, id)
        };
        // font_def_new.
        Value::vector([font_spec, encoding, repertory])
    };

    let is_character =
        |value: &Value| matches!(value.kind(), Kind::Integer(c) if (0..=0x3f_ffff).contains(&c));
    let mut ascii_changed = false;
    let mut charset: Option<String> = None;
    let mut range_list: Vec<Option<(u32, u32)>> = Vec::new();
    let mut script_range_list: Vec<(u32, u32)> = Vec::new();
    if let Kind::Integer(c) = characters.kind()
        && is_character(&characters)
    {
        if c < 0x80 {
            return Err(crate::lisp::primitives::values::doprnt_error(
                interp,
                "Can't set a font for partial ASCII range",
                &[],
            ));
        }
        range_list.push(Some((c as u32, c as u32)));
    } else if let Some((from, to)) = characters.cons_values() {
        if !is_character(&from) {
            return Err(wrong_type_argument("characterp", from));
        }
        if !is_character(&to) {
            return Err(wrong_type_argument("characterp", to));
        }
        let (Kind::Integer(from), Kind::Integer(to)) = (from.kind(), to.kind()) else {
            unreachable!("characters are fixnums");
        };
        if from < 0x80 {
            if from != 0 || to < 0x7f {
                return Err(crate::lisp::primitives::values::doprnt_error(
                    interp,
                    "Can't set a font for partial ASCII range",
                    &[],
                ));
            }
            ascii_changed = true;
        }
        range_list.push(Some((from as u32, to as u32)));
    } else if let Some(name) = match characters.kind() {
        Kind::Symbol(name) => Some(name.to_string()),
        Kind::T => Some("t".to_string()),
        _ => None,
    } {
        let table = interp
            .lookup_var_key(cached_symbol!("char-script-table"), env)
            .unwrap_or(Value::Nil);
        let script_list = super::call(
            interp,
            "char-table-extra-slot",
            &[table, Value::Integer(0)],
            env,
        )?;
        if !super::call(interp, "memq", &[characters, script_list], env)?.is_nil() {
            if name == "latin" {
                ascii_changed = true;
            }
            script_range_list = script_ranges(interp, &characters, env)?;
        }
        if interp.has_charset(&name) {
            if super::coding::charset_ascii_compatible_p(interp, &name) {
                ascii_changed = true;
            }
            charset = Some(name);
        } else if script_range_list.is_empty() {
            return Err(LispError::Signal(format!(
                "Invalid script or charset name: {name}"
            )));
        }
    } else if characters.is_nil() {
        range_list.push(None);
    } else {
        return Err(LispError::Signal(
            "Invalid second argument for setting a font in a fontset".into(),
        ));
    }

    if ascii_changed {
        if font_spec.is_nil() {
            return Err(crate::lisp::primitives::values::doprnt_error(
                interp,
                "Can't set ASCII font to nil",
                &[],
            ));
        }
        let val = char_table(&fontset_obj).get(0);
        if !val.is_nil() && add.eq_value(Value::symbol("append")) {
            // We are going to change just an additional font for ASCII.
            ascii_changed = false;
        }
    }

    if let Some(charset) = charset {
        let (min_code, max_code) = super::coding::charset_code_bounds(interp, &charset)
            .ok_or_else(|| wrong_type_argument("charsetp", characters))?;
        let ranges = super::coding::map_charset_char_ranges(interp, &charset, min_code, max_code)
            .unwrap_or_default();
        for (from, to) in ranges {
            set_fontset_font_range(
                &fontset_obj,
                font_def,
                &add,
                ascii_changed,
                &mut script_range_list,
                from as u32,
                to as u32,
            );
        }
    }
    for range in script_range_list {
        range_list.push(Some(range));
    }
    for range in range_list {
        fontset_add(&fontset_obj, range, font_def, &add);
    }
    if ascii_changed {
        set_extra(&fontset_obj, FONTSET_ASCII, fontname);
        // Every frame is the initial or a termcap one, whose default
        // face's font is not changed.
    }
    Ok(Value::Nil)
}

/// Fnew_fontset.
pub(crate) fn new_fontset(
    interp: &mut Interpreter,
    name: &Value,
    fontlist: &Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    if !name.is_string() {
        return Err(wrong_type_argument("stringp", *name));
    }
    let name = super::call(interp, "downcase", std::slice::from_ref(name), env)?;
    match fs_query_fontset(interp, &name, 0, env)? {
        None => {
            let font_spec = font::font_make_spec(interp);
            let bytes = font::string_sdata(&name)?;
            if !font::font_parse_xlfd(interp, &bytes, &font_spec, env)? {
                return Err(LispError::Signal(
                    "Fontset name must be in XLFD format".into(),
                ));
            }
            let short_name = font::aref(interp, &font_spec, font::FONT_REGISTRY_INDEX);
            let short_name = super::call(interp, "symbol-name", &[short_name], env)?;
            let short = font::string_sdata(&short_name)?;
            if !short.starts_with(b"fontset-") || short.len() < 9 {
                return Err(LispError::Signal(
                    "Registry field of fontset name must be \"fontset-*\"".into(),
                ));
            }
            let aliases = interp
                .symbol_value_symbol(&SymbolName::intern_str("fontset-alias-alist"))
                .unwrap_or(Value::Nil);
            interp.set_global_binding_key(
                cached_symbol!("fontset-alias-alist"),
                Value::cons(Value::cons(name, short_name), aliases),
            );
            font::aset(
                interp,
                &font_spec,
                font::FONT_REGISTRY_INDEX,
                Value::symbol("iso8859-1"),
            );
            let fontset = make_fontset(interp, name);
            let ascii = font::font_dynamic_unparse_xlfd(interp, &font_spec, 0)?;
            let ascii = super::strings::make_specified_string(ascii, false)
                .map_err(|error| LispError::Signal(error.to_string()))?;
            set_extra(&fontset, FONTSET_ASCII, ascii);
        }
        Some(id) => {
            let fontset = fontset_from_id(interp, id);
            char_table(&fontset).set_all(Value::Nil);
        }
    }
    let mut tail = *fontlist;
    while let Some((elt, rest)) = tail.cons_values() {
        let script = super::call(interp, "car", &[elt], env)?;
        let elt = super::call(interp, "cdr", &[elt], env)?;
        let append = Value::symbol("append");
        if let Some((_, after)) = elt.cons_values()
            && (after.is_nil() || after.cons_values().is_some())
        {
            let mut specs = elt;
            while let Some((spec, more)) = specs.cons_values() {
                set_fontset_font(interp, &[name, script, spec, Value::Nil, append], env)?;
                specs = more;
            }
        } else {
            set_fontset_font(interp, &[name, script, elt, Value::Nil, append], env)?;
        }
        tail = rest;
    }
    if !tail.is_nil() {
        return Err(wrong_type_argument("listp", *fontlist));
    }
    Ok(name)
}

/// Ffontset_font: the (FAMILY . REGISTRY) of the first font of NAME's
/// fontset for CH whose encoding charset has CH, or all of them, the
/// default fontset's after a base fontset's.
pub(crate) fn fontset_font(
    interp: &mut Interpreter,
    name: &Value,
    ch: &Value,
    all: bool,
    env: &mut Env,
) -> Result<Value, LispError> {
    let mut fontset = check_fontset_name(interp, name, None, env)?;
    let c = match ch.kind() {
        Kind::Integer(c) if (0..=0x3f_ffff).contains(&c) => c as u32,
        _ => return Err(wrong_type_argument("characterp", *ch)),
    };
    let default = default_fontset(interp);
    let mut list = Vec::new();
    loop {
        for elt in [
            if fontset.eq_value(default) {
                char_table(&fontset).get(c)
            } else {
                fontset_ref(interp, &fontset, c)
            },
            extra(&fontset, FONTSET_FALLBACK),
        ] {
            let Ok(defs) = vector_items(&elt) else {
                continue;
            };
            for val in defs {
                if val.is_nil() {
                    return Ok(Value::Nil);
                }
                let def = vector_items(&val)?;
                // GNU reads the font_def's ENCODING here as its repertory.
                let repertory = def.get(1).copied().unwrap_or(Value::Nil);
                match repertory.kind() {
                    Kind::Integer(id) if !char_charset_p(interp, c, id) => continue,
                    Kind::CharTable(table) if table.get(c).is_nil() => continue,
                    _ => {}
                }
                let spec = def.first().copied().unwrap_or(Value::Nil);
                let name_of = |interp: &mut Interpreter, value: Value, env: &mut Env| {
                    if value.is_nil() {
                        Ok(Value::Nil)
                    } else {
                        super::call(interp, "symbol-name", &[value], env)
                    }
                };
                let family = font::aref(interp, &spec, font::FONT_FAMILY_INDEX);
                let family = name_of(interp, family, env)?;
                let registry = font::aref(interp, &spec, font::FONT_REGISTRY_INDEX);
                let registry = name_of(interp, registry, env)?;
                let val = Value::cons(family, registry);
                if !all {
                    return Ok(val);
                }
                list.push(val);
            }
        }
        if fontset.eq_value(default) {
            break;
        }
        fontset = default;
    }
    Ok(Value::list(list))
}

/// Ffontset_list: the base fontsets' names, the last made first.
pub(crate) fn fontset_list(interp: &Interpreter) -> Value {
    let mut list = Value::Nil;
    for fontset in table_items(interp) {
        if !fontset.is_nil() && base_fontset_p(&fontset) {
            list = Value::cons(extra(&fontset, FONTSET_NAME), list);
        }
    }
    list
}
