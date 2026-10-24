//! keymap.c's accessors and mutators, ported: get_keymap, access_keymap_1,
//! get_keyelt, lookup_key_1, Fdefine_key with store_in_keymap and
//! define_as_prefix, keymap_parent, Fset_keymap_parent, map_keymap and
//! Fkeymap_prompt; and keyboard.c's reorder_modifiers with the caches
//! parse_modifiers and apply_modifiers keep on symbols.

use super::*;
use crate::lisp::alloc::RootedVec;
use crate::lisp::types::Kind;

/// CHAR_META.
const CHAR_META: i64 = KEY_DESCRIPTION_META_BIT;
/// CHAR_MODIFIER_MASK.
const CHAR_MODIFIER_MASK: i64 = KEY_DESCRIPTION_MODIFIER_MASK;
/// MAX_CHAR.
const MAX_CHAR: i64 = 0x3f_ffff;

/// Qkeymap.
#[inline]
fn qkeymap() -> Value {
    cached_symbol!("keymap").with(|symbol| Value::Symbol(*symbol))
}

#[inline]
fn qautoload() -> Value {
    cached_symbol!("autoload").with(|symbol| Value::Symbol(*symbol))
}

#[inline]
fn qmenu_item() -> Value {
    cached_symbol!("menu-item").with(|symbol| Value::Symbol(*symbol))
}

#[inline]
fn qcfilter() -> Value {
    cached_symbol!(":filter").with(|symbol| Value::Symbol(*symbol))
}

/// `meta-prefix-char', as C reads it.
fn meta_prefix_char(interp: &Interpreter) -> Value {
    interp
        .symbol_value_key(cached_symbol!("meta-prefix-char"))
        .unwrap_or(Value::Integer(KEY_DESCRIPTION_META_PREFIX))
}

/// CONSP (X) && EQ (XCAR (X), Qkeymap).
#[inline]
fn is_keymap_list(value: Value) -> bool {
    value
        .cons_ref()
        .is_some_and(|cell| cell.car.get().eq_value(qkeymap()))
}

/// CHARACTERP.
#[inline]
fn characterp(value: Value) -> bool {
    matches!(value.kind(), Kind::Integer(code) if (0..=MAX_CHAR).contains(&code))
}

/// EVENT_HEAD.
#[inline]
fn event_head(event: Value) -> Value {
    event.cons_ref().map_or(event, |cell| cell.car.get())
}

/// SYMBOLP, which with `symbols-with-pos-enabled' also takes a symbol with
/// position as its bare symbol (XSYMBOL).
fn bare_symbol(interp: &Interpreter, value: Value, env: &Env) -> Option<Value> {
    if value.is_symbol() {
        return Some(value);
    }
    symbols_with_pos_enabled(interp, env)
        .then(|| symbol_with_pos_parts(interp, &value).map(|(symbol, _)| symbol))
        .flatten()
}

/// indirect_function: the end of SYMBOL's chain of function cells, nil when
/// one is void.  A non-symbol is itself.
#[inline]
fn indirect_function(interp: &Interpreter, object: Value, env: &Env) -> Value {
    match object.symbol_cell() {
        Some(symbol) => interp
            .lookup_function_symbol(&symbol, env)
            .unwrap_or(Value::Nil),
        // nil and t have no function.
        None if object.is_symbol() => Value::Nil,
        None => object,
    }
}

/// Fnth (4, AUTOLOAD-FORM): the autoload's TYPE.
fn autoload_type(form: Value) -> Result<Value, LispError> {
    let mut tail = form;
    for _ in 0..4 {
        match tail.kind() {
            Kind::Cons(cell) => tail = cell.cdr.get(),
            Kind::Nil => return Ok(Value::Nil),
            _ => return Err(wrong_type_argument("listp", form)),
        }
    }
    match tail.kind() {
        Kind::Cons(cell) => Ok(cell.car.get()),
        Kind::Nil => Ok(Value::Nil),
        _ => Err(wrong_type_argument("listp", tail)),
    }
}

/// get_keymap (OBJECT, ERROR_IF_NOT_KEYMAP, AUTOLOAD): a `(keymap ...)'
/// list is itself; a symbol whose function indirection is one yields that
/// list; a symbol whose function is an autoload of TYPE `keymap' is loaded
/// and tried again when AUTOLOAD, and is itself otherwise (KEYMAPP holds
/// for it, though nothing is loaded).  Anything else is nil, or
/// `(wrong-type-argument keymapp OBJECT)' when ERROR_IF_NOT_KEYMAP.
pub(crate) fn get_keymap(
    interp: &mut Interpreter,
    object: Value,
    error_if_not_keymap: bool,
    autoload: bool,
    env: &mut Env,
) -> Result<Value, LispError> {
    loop {
        if let Some(cell) = object.cons_ref() {
            // A list is its own indirect_function, and only a symbol is
            // autoloaded.
            if cell.car.get().eq_value(qkeymap()) {
                return Ok(object);
            }
        } else if object.symbol_cell().is_some() {
            let tem = indirect_function(interp, object, env);
            if let Some(cell) = tem.cons_ref() {
                let head = cell.car.get();
                if head.eq_value(qkeymap()) {
                    return Ok(tem);
                }
                // Autoload forms for keymaps have Qkeymap as their fifth
                // element.
                if (autoload || !error_if_not_keymap)
                    && head.eq_value(qautoload())
                    && autoload_type(tem)?.eq_value(qkeymap())
                {
                    if !autoload {
                        return Ok(object);
                    }
                    interp.with_lisp_stack_roots(&(object, tem), |interp| {
                        super::call(interp, "autoload-do-load", &[tem, object, Value::Nil], env)
                    })?;
                    continue;
                }
            }
        }
        if error_if_not_keymap {
            return Err(wrong_type_argument("keymapp", object));
        }
        return Ok(Value::Nil);
    }
}

/// get_keymap (OBJECT, false, false), which neither loads nor signals
/// except for a malformed autoload form.
pub(crate) fn get_keymap_noload(interp: &Interpreter, object: Value, env: &Env) -> Value {
    if let Some(cell) = object.cons_ref() {
        return if cell.car.get().eq_value(qkeymap()) {
            object
        } else {
            Value::Nil
        };
    }
    if object.symbol_cell().is_none() {
        return Value::Nil;
    }
    let tem = indirect_function(interp, object, env);
    if let Some(cell) = tem.cons_ref() {
        let head = cell.car.get();
        if head.eq_value(qkeymap()) {
            return tem;
        }
        if head.eq_value(qautoload())
            && autoload_type(tem).is_ok_and(|kind| kind.eq_value(qkeymap()))
        {
            return object;
        }
    }
    Value::Nil
}

/// KEYMAPP.
pub(crate) fn keymapp(interp: &Interpreter, object: Value, env: &Env) -> bool {
    !get_keymap_noload(interp, object, env).is_nil()
}

/// keyboard.c:parse_modifiers for a symbol: (UNMODIFIED MASK), cached in
/// SYMBOL's `event-symbol-element-mask' with `event-symbol-elements' beside
/// it.
pub(crate) fn parse_modifiers(
    interp: &mut Interpreter,
    symbol: Value,
) -> Result<(Value, i64), LispError> {
    let name = symbol.as_symbol()?.to_string();
    if let Some(elements) = interp.get_symbol_property(&name, "event-symbol-element-mask")
        && let Some((base, rest)) = elements.cons_values()
    {
        let mask = rest.car_if_list().unwrap_or(Value::Nil);
        return Ok((base, mask.as_integer().unwrap_or(0)));
    }
    parse_event_symbol_modifiers(interp, &symbol)?;
    let elements = interp
        .get_symbol_property(&name, "event-symbol-element-mask")
        .unwrap_or(Value::Nil);
    let (base, rest) = elements.cons_values().expect("parsed modifiers");
    Ok((base, rest.car()?.as_integer()?))
}

/// keyboard.c:apply_modifiers: BASE with MODIFIERS, through BASE's
/// `modifier-cache' (the click modifier never figures in its indices), the
/// new symbol taking BASE's `event-kind' when it has none.
fn apply_modifiers(
    interp: &mut Interpreter,
    modifiers: i64,
    base: Value,
) -> Result<Value, LispError> {
    if let Kind::Integer(code) = base.kind() {
        return Ok(Value::Integer(code | modifiers));
    }
    const CLICK: i64 = 1 << 3;
    let base_name = base.as_symbol()?.to_string();
    let cache = interp
        .get_symbol_property(&base_name, "modifier-cache")
        .unwrap_or(Value::Nil);
    let index = Value::Integer(modifiers & !CLICK);
    let mut tail = cache;
    let mut cached = None;
    while let Some((entry, rest)) = tail.cons_values() {
        if let Some((key, symbol)) = entry.cons_values()
            && key.eq_value(index)
        {
            cached = Some(symbol);
            break;
        }
        tail = rest;
    }
    let new_symbol = match cached {
        Some(symbol) => symbol,
        None => {
            let symbol = Value::symbol(&modified_event_symbol_name(modifiers, &base_name));
            interp.put_symbol_property(
                &base_name,
                "modifier-cache",
                Value::cons(Value::cons(index, symbol), cache),
            );
            symbol
        }
    };
    let new_name = new_symbol.as_symbol()?.to_string();
    if interp
        .get_symbol_property(&new_name, "event-kind")
        .is_none_or(|kind| kind.is_nil())
        && let Some(kind) = interp
            .get_symbol_property(&base_name, "event-kind")
            .filter(|kind| !kind.is_nil())
    {
        interp.put_symbol_property(&new_name, "event-kind", kind);
    }
    Ok(new_symbol)
}

/// keyboard.c:reorder_modifiers: SYMBOL with its modifiers in the canonical
/// order.
pub(crate) fn reorder_modifiers(
    interp: &mut Interpreter,
    symbol: Value,
) -> Result<Value, LispError> {
    let (base, modifiers) = parse_modifiers(interp, symbol)?;
    apply_modifiers(interp, modifiers, base)
}

/// An event as access_keymap_1 and store_in_keymap compare it: a symbol's
/// modifiers reordered, a fixnum's bits above Meta cleared.
fn canonical_event(interp: &mut Interpreter, idx: Value, env: &Env) -> Result<Value, LispError> {
    if let Some(symbol) = bare_symbol(interp, idx, env) {
        return reorder_modifiers(interp, symbol);
    }
    if let Kind::Integer(code) = idx.kind() {
        return Ok(Value::Integer(code & (CHAR_META | (CHAR_META - 1))));
    }
    Ok(idx)
}

/// get_keyelt: the definition OBJECT holds, past `(menu-item NAME DEFN
/// . PROPS)' (whose `:filter' runs on DEFN when AUTOLOAD) and the obsolete
/// `(STRING . DEFN)' menu items.
pub(crate) fn get_keyelt(
    interp: &mut Interpreter,
    mut object: Value,
    autoload: bool,
    env: &mut Env,
) -> Result<Value, LispError> {
    loop {
        let Some(cell) = object.cons_ref() else {
            return Ok(object);
        };
        let head = cell.car.get();
        if head.eq_value(qmenu_item()) {
            let rest = cell.cdr.get();
            let Some(rest) = rest.cons_ref() else {
                // Invalid keymap.
                return Ok(object);
            };
            object = rest.cdr.get();
            let mut tem = object;
            if let Some(defn) = object.cons_ref() {
                object = defn.car.get();
            }
            // `:filter FILTER' gives the real definition.
            while let Some(cell) = tem.cons_ref() {
                let next = cell.cdr.get();
                let Some(next_cell) = next.cons_ref() else {
                    break;
                };
                if autoload && cell.car.get().eq_value(qcfilter()) {
                    let filter = next_cell.car.get();
                    let form = Value::list([filter, Value::list([Value::symbol("quote"), object])]);
                    object = interp.menu_item_eval_property(form, env)?;
                    break;
                }
                tem = next;
            }
        } else if head.is_string() {
            object = cell.cdr.get();
        } else {
            return Ok(object);
        }
    }
}

/// access_keymap_1: IDX's binding in MAP, `Value::Unbound' for none (GNU's
/// Qunbound).  T_OK takes a `t' default; NOINHERIT stops at the parent;
/// AUTOLOAD loads autoloaded submaps and runs menu filters.
pub(crate) fn access_keymap_1(
    interp: &mut Interpreter,
    map: Value,
    idx: Value,
    mut t_ok: bool,
    noinherit: bool,
    autoload: bool,
    env: &mut Env,
) -> Result<Value, LispError> {
    let keymap_symbol = qkeymap();
    let mut map = map;
    let mut idx = canonical_event(interp, event_head(idx), env)?;

    // The special meta -> esc mapping.
    if let Kind::Integer(code) = idx.kind()
        && code & CHAR_META != 0
    {
        let mut meta_prefix_char = meta_prefix_char(interp);
        // A strange value in which Meta is set would cause infinite
        // recursion.
        if meta_prefix_char.as_integer()? & CHAR_META != 0 {
            meta_prefix_char = Value::Integer(KEY_DESCRIPTION_META_PREFIX);
            interp.set_global_binding("meta-prefix-char", meta_prefix_char);
        }
        let roots = RootedVec::from_vec(vec![map]);
        let event_meta_binding = access_keymap_1(
            interp,
            map,
            meta_prefix_char,
            t_ok,
            noinherit,
            autoload,
            env,
        )?;
        let event_meta_map = if event_meta_binding.eq_value(Value::Unbound) {
            Value::Nil
        } else {
            interp.with_lisp_stack_roots(&event_meta_binding, |interp| {
                get_keymap(interp, event_meta_binding, false, autoload, env)
            })?
        };
        drop(roots);
        if event_meta_map.is_cons() {
            map = event_meta_map;
            idx = Value::Integer(code & !CHAR_META);
        } else if t_ok {
            // Find only a default binding.
            idx = Value::T;
        } else {
            // An explicit nil binding, or no binding at all.
            return Ok(if event_meta_binding.is_nil() {
                Value::Nil
            } else {
                Value::Unbound
            });
        }
    }

    // The map, then tail, retval, retval_tail, t_binding, idx and the
    // binding in hand, all reachable across the Lisp a filter or an
    // autoload runs.
    const TAIL: usize = 1;
    const RETVAL: usize = 2;
    const RETVAL_TAIL: usize = 3;
    const T_BINDING: usize = 4;
    const IDX: usize = 5;
    const VAL: usize = 6;
    let tail = match map.cons_ref() {
        Some(cell) if cell.car.get().eq_value(keymap_symbol) => cell.cdr.get(),
        _ => map,
    };
    let mut roots = RootedVec::from_vec(vec![
        map,
        tail,
        Value::Unbound,
        Value::Nil,
        Value::Unbound,
        idx,
        Value::Nil,
    ]);
    loop {
        if !roots[TAIL].is_cons() {
            roots[TAIL] = get_keymap(interp, roots[TAIL], false, autoload, env)?;
            if !roots[TAIL].is_cons() {
                break;
            }
        }
        let binding = roots[TAIL].car()?;
        // get_keymap (binding, 0, autoload): a list is a keymap by its car
        // alone, and only a symbol needs its function looked at.
        let submap = match binding.cons_ref() {
            Some(cell) if cell.car.get().eq_value(keymap_symbol) => binding,
            Some(_) => Value::Nil,
            None if binding.symbol_cell().is_some() => {
                get_keymap(interp, binding, false, autoload, env)?
            }
            None => Value::Nil,
        };
        let mut val = Value::Unbound;
        if binding.eq_value(keymap_symbol) {
            if noinherit || roots[RETVAL].is_nil() {
                // If NOINHERIT, stop here, the rest is inherited.
                break;
            } else if !roots[RETVAL].eq_value(Value::Unbound) {
                let parent =
                    access_keymap_1(interp, roots[TAIL], roots[IDX], t_ok, false, autoload, env)?;
                let parent_entry = if parent.eq_value(Value::Unbound) {
                    Value::Nil
                } else {
                    interp.with_lisp_stack_roots(&parent, |interp| {
                        get_keymap(interp, parent, false, autoload, env)
                    })?
                };
                if keymapp(interp, parent_entry, env) {
                    if roots[RETVAL_TAIL].is_cons() {
                        roots[RETVAL_TAIL].set_cdr(parent_entry)?;
                    } else {
                        roots[RETVAL_TAIL] = Value::cons(roots[RETVAL], parent_entry);
                        roots[RETVAL] = Value::cons(keymap_symbol, roots[RETVAL_TAIL]);
                    }
                }
                break;
            }
        } else if submap.is_cons() {
            roots[VAL] = submap;
            val = access_keymap_1(interp, submap, roots[IDX], t_ok, noinherit, autoload, env)?;
        } else if let Some(cell) = binding.cons_ref() {
            let key = cell.car.get();
            if key.eq_value(roots[IDX]) {
                val = cell.cdr.get();
            } else if t_ok && key.eq_value(Value::T) {
                roots[T_BINDING] = cell.cdr.get();
                t_ok = false;
            }
        } else if let Kind::Vector(vector) = binding.kind() {
            if let Kind::Integer(index) = roots[IDX].kind()
                && let Ok(index) = usize::try_from(index)
                && index < vector.len()
            {
                val = vector.get(index).expect("index within the vector");
            }
        } else if let Kind::CharTable(table) = binding.kind()
            && let Kind::Integer(code) = roots[IDX].kind()
            && code >= 0
            && code & CHAR_MODIFIER_MASK == 0
        {
            // Character codes with modifiers are not included in a
            // char-table; nil there records an unbound entry.
            val = table.get(code as u32);
            if val.is_nil() {
                val = Value::Unbound;
            }
        }

        // If we found a binding, clean it up and return it.
        if !val.eq_value(Value::Unbound) {
            if val.eq_value(Value::T) {
                // A t binding is an explicit nil binding: it shadows any
                // parent binding but not bindings in keymaps of lower
                // precedence.
                val = Value::Nil;
            }
            roots[VAL] = val;
            let val = get_keyelt(interp, val, autoload, env)?;
            roots[VAL] = val;
            let unset = roots[RETVAL].is_nil() || roots[RETVAL].eq_value(Value::Unbound);
            if !keymapp(interp, val, env) {
                if unset {
                    roots[RETVAL] = val;
                }
                if !val.is_nil() {
                    // Shadows everything that follows.
                    break;
                }
            } else if unset {
                roots[RETVAL] = val;
            } else if roots[RETVAL_TAIL].is_cons() {
                let next = Value::list([val]);
                roots[RETVAL_TAIL].set_cdr(next)?;
                roots[RETVAL_TAIL] = next;
            } else {
                roots[RETVAL_TAIL] = Value::list([val]);
                roots[RETVAL] = Value::cons(
                    keymap_symbol,
                    Value::cons(roots[RETVAL], roots[RETVAL_TAIL]),
                );
            }
        }
        interp.maybe_quit(env)?;
        roots[TAIL] = roots[TAIL].cdr()?;
    }
    if roots[RETVAL].eq_value(Value::Unbound) {
        get_keyelt(interp, roots[T_BINDING], autoload, env)
    } else {
        Ok(roots[RETVAL])
    }
}

/// access_keymap: access_keymap_1 with nil for no binding.
pub(crate) fn access_keymap(
    interp: &mut Interpreter,
    map: Value,
    idx: Value,
    t_ok: bool,
    noinherit: bool,
    autoload: bool,
    env: &mut Env,
) -> Result<Value, LispError> {
    let value = access_keymap_1(interp, map, idx, t_ok, noinherit, autoload, env)?;
    Ok(if value.eq_value(Value::Unbound) {
        Value::Nil
    } else {
        value
    })
}

/// keymap.c:possibly_translate_key_sequence: a vector holding one string
/// is that string's `key-parse' when `key-valid-p' accepts it.
pub(crate) fn possibly_translate_key_sequence(
    interp: &mut Interpreter,
    key: Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    let Kind::Vector(vector) = key.kind() else {
        return Ok(key);
    };
    if vector.len() != 1 {
        return Ok(key);
    }
    let Some(description) = vector.get(0).filter(Value::is_string) else {
        return Ok(key);
    };
    if interp.lookup_function("key-valid-p", env).is_err() {
        return Err(LispError::SignalValue(Value::list([
            Value::symbol("error"),
            Value::string("`key-valid-p' is not defined, so this syntax can't be used: %s"),
            key,
        ])));
    }
    // If key-valid-p is unhappy about KEY, it is returned as it is: menu
    // items may bind strings meant for insertion (bug#64927).
    let valid = interp.call_function_value(
        Value::symbol("key-valid-p"),
        Some("key-valid-p"),
        &[description],
        env,
    )?;
    if valid.is_nil() {
        return Ok(key);
    }
    let parsed = interp.call_function_value(
        Value::symbol("key-parse"),
        Some("key-parse"),
        &[description],
        env,
    )?;
    if check_vector_or_string(interp, parsed)? == 0 {
        return Err(LispError::SignalValue(Value::list([
            Value::symbol("error"),
            Value::string("Invalid `key-parse' syntax: %S"),
            parsed,
        ])));
    }
    Ok(parsed)
}

/// CHECK_VECTOR_OR_STRING: the length, or `(wrong-type-argument arrayp X)'.
fn check_vector_or_string(interp: &Interpreter, key: Value) -> Result<usize, LispError> {
    if !is_vector_value(&key) && !key.is_string() {
        return Err(wrong_type_argument("arrayp", key));
    }
    Ok(sequence_length_value(interp, &key)? as usize)
}

/// Faref (KEY, INDEX) of a key sequence.
fn key_event(key: Value, index: usize) -> Result<Value, LispError> {
    match key.kind() {
        Kind::Vector(vector) => vector.get(index),
        _ => string_char_code_at_in_place(&key, index).map(Value::Integer),
    }
    .ok_or_else(|| {
        LispError::SignalValue(Value::list([
            Value::symbol("args-out-of-range"),
            key,
            Value::Integer(index as i64),
        ]))
    })
}

/// message_with_string ("Key sequence contains invalid event %s", C, 1),
/// whose CHECK_STRING signals for anything but a string.
fn invalid_event_message(
    interp: &mut Interpreter,
    event: Value,
    env: &mut Env,
) -> Result<(), LispError> {
    if !event.is_string() {
        return Err(wrong_type_argument("stringp", event));
    }
    super::dispatch::display::message_with_string(
        interp,
        "Key sequence contains invalid event %s",
        event,
        env,
    )
}

/// lookup_key_1 over LENGTH events READ_EVENT yields: the definition, or
/// the number of events that reach a non-prefix key.
pub(crate) fn lookup_key_1(
    interp: &mut Interpreter,
    keymap: Value,
    length: usize,
    t_ok: bool,
    mut read_event: impl FnMut(&mut Interpreter, usize, &mut Env) -> Result<Value, LispError>,
    env: &mut Env,
) -> Result<Value, LispError> {
    let keymap = if !keymap.is_cons() && !keymap.is_nil() {
        get_keymap(interp, keymap, true, true, env)?
    } else {
        keymap
    };
    if length == 0 {
        return Ok(keymap);
    }
    // A composed prefix can be freshly allocated and dropped from its map
    // by a filter; keep the current map and event reachable.
    let mut roots = RootedVec::from_vec(vec![keymap, Value::Nil]);
    let mut idx = 0;
    loop {
        let c = read_event(interp, idx, env)?;
        idx += 1;
        roots[1] = c;
        // A string is allowed: `menu-bar-select-buffer' binds a buffer's
        // name in the key sequence.
        if !matches!(c.kind(), Kind::Integer(_) | Kind::Cons(_))
            && !c.is_symbol()
            && !c.is_string()
            && bare_symbol(interp, c, env).is_none()
        {
            invalid_event_message(interp, c, env)?;
        }
        let cmd = access_keymap(interp, roots[0], c, t_ok, false, true, env)?;
        if idx == length {
            return Ok(cmd);
        }
        roots[1] = cmd;
        let next = get_keymap(interp, cmd, false, true, env)?;
        if !next.is_cons() {
            return Ok(Value::Integer(idx as i64));
        }
        roots[0] = next;
        interp.maybe_quit(env)?;
    }
}

/// keymap.c:keymap_parent: KEYMAP's parent, or nil.
pub(crate) fn keymap_parent(
    interp: &mut Interpreter,
    keymap: Value,
    autoload: bool,
    env: &mut Env,
) -> Result<Value, LispError> {
    let keymap = get_keymap(interp, keymap, true, autoload, env)?;
    // Skip past the initial element `keymap'.
    let mut list = keymap.cdr()?;
    while let Some(cell) = list.cons_ref() {
        // See if there is another `keymap'.
        if is_keymap_list(list) {
            return Ok(list);
        }
        list = cell.cdr.get();
    }
    get_keymap(interp, list, false, autoload, env)
}

/// keymap.c:keymap_memberp: MAP is MAPS or one of its parents.
fn keymap_memberp(
    interp: &mut Interpreter,
    map: Value,
    mut maps: Value,
    env: &mut Env,
) -> Result<bool, LispError> {
    if map.is_nil() {
        return Ok(false);
    }
    while keymapp(interp, maps, env) && !map.eq_value(maps) {
        maps = keymap_parent(interp, maps, false, env)?;
    }
    Ok(map.eq_value(maps))
}

/// Fset_keymap_parent.
pub(crate) fn set_keymap_parent(
    interp: &mut Interpreter,
    keymap: Value,
    parent: Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    // Flush any reverse-map cache.
    interp.where_is_cache = Value::Nil;
    interp.where_is_cache_keymaps = Value::T;
    let keymap = get_keymap(interp, keymap, true, true, env)?;
    let mut roots = RootedVec::from_vec(vec![keymap, parent]);
    if !parent.is_nil() {
        roots[1] = get_keymap(interp, parent, true, false, env)?;
        // Check for cycles.
        if keymap_memberp(interp, keymap, roots[1], env)? {
            return Err(LispError::Signal("Cyclic keymap inheritance".into()));
        }
    }
    let parent = roots[1];
    // Skip past the initial element `keymap'.
    let mut prev = keymap;
    loop {
        let list = prev.cdr()?;
        // If there is a parent keymap here, replace it.  If we came to the
        // end, add the parent in PREV.
        if !list.is_cons() || is_keymap_list(list) {
            prev.set_cdr(parent)?;
            return Ok(parent);
        }
        prev = list;
    }
}

/// keymap.c:map_keymap: FUN on each binding of MAP and its parents, MAP
/// first loaded when AUTOLOAD.
pub(crate) fn map_keymap(
    interp: &mut Interpreter,
    map: Value,
    fun: &mut KeymapEntryCallback<'_>,
    autoload: bool,
    env: &mut Env,
) -> Result<(), LispError> {
    let map = get_keymap(interp, map, true, autoload, env)?;
    let mut roots = RootedVec::from_vec(vec![map]);
    while let Some(cell) = roots[0].cons_ref() {
        let head = cell.car.get();
        if keymapp(interp, head, env) {
            map_keymap(interp, head, fun, autoload, env)?;
            roots[0] = roots[0].cdr()?;
        } else {
            roots[0] = map_keymap_own_entries(interp, roots[0], fun, env)?;
        }
        if !roots[0].is_cons() {
            roots[0] = get_keymap(interp, roots[0], false, autoload, env)?;
        }
    }
    Ok(())
}

/// Fkeymap_prompt: the first prompt string in MAP, its included keymaps
/// and its parents.
pub(crate) fn keymap_prompt(
    interp: &mut Interpreter,
    map: Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    let mut map = get_keymap_noload(interp, map, env);
    while let Some(cell) = map.cons_ref() {
        let tem = cell.car.get();
        if tem.is_string() {
            return Ok(tem);
        } else if keymapp(interp, tem, env) {
            let tem = keymap_prompt(interp, tem, env)?;
            if !tem.is_nil() {
                return Ok(tem);
            }
        }
        map = cell.cdr.get();
    }
    Ok(Value::Nil)
}

/// Fdelq (ELT, LIST) on the list after LIST's head, which is never ELT.
fn delq_after(list: Value, elt: Value) -> Result<(), LispError> {
    let mut prev = list;
    let mut tail = list.cdr()?;
    while let Some(cell) = tail.cons_ref() {
        let next = cell.cdr.get();
        if cell.car.get().eq_value(elt) {
            prev.set_cdr(next)?;
        } else {
            prev = tail;
        }
        tail = next;
    }
    Ok(())
}

/// keymap.c:store_in_keymap: bind IDX to DEF in KEYMAP's own list, its
/// vector or its char-table, before the inherited tail.  REMOVE deletes a
/// sparse binding.
pub(crate) fn store_in_keymap(
    interp: &mut Interpreter,
    keymap: Value,
    idx: Value,
    def: Value,
    remove: bool,
    env: &Env,
) -> Result<Value, LispError> {
    // Flush any reverse-map cache.
    interp.where_is_cache = Value::Nil;
    interp.where_is_cache_keymaps = Value::T;
    if idx.eq_value(qkeymap()) {
        return Err(LispError::Signal(
            "`keymap' is reserved for embedded parent maps".into(),
        ));
    }
    if !is_keymap_list(keymap) {
        return Err(LispError::Signal(
            "attempt to define a key in a non-keymap".into(),
        ));
    }
    // A cons whose car is a character is (FROM-CHAR . TO-CHAR); any other
    // list is an event whose head is the index.
    let range = match idx.cons_values() {
        Some((from, to)) if characterp(from) => {
            if !characterp(to) {
                return Err(wrong_type_argument("characterp", to));
            }
            Some((from.as_integer()?, to.as_integer()?))
        }
        _ => None,
    };
    let idx = match range {
        Some(_) => idx,
        None => canonical_event(interp, event_head(idx), env)?,
    };
    let natnum = match idx.kind() {
        Kind::Integer(code) if code >= 0 => Some(code),
        _ => None,
    };
    let unbound_or = |def: Value| if def.is_nil() { Value::T } else { def };

    // The cons after which new bindings go: after a table element, so the
    // table stays towards the front.
    let mut insertion_point = keymap;
    let mut tail = keymap.cdr()?;
    while let Some(cell) = tail.cons_ref() {
        let elt = cell.car.get();
        match elt.kind() {
            Kind::Vector(vector) => {
                if let Some(index) = natnum
                    && (index as usize) < vector.len()
                {
                    vector.set(index as usize, def);
                    return Ok(def);
                } else if let Some((from, to)) = range {
                    let last = to.min(vector.len() as i64 - 1);
                    for index in from..=last {
                        vector.set(index as usize, def);
                    }
                    if last == to {
                        // We have defined all keys in IDX.
                        return Ok(def);
                    }
                }
                insertion_point = tail;
            }
            Kind::CharTable(table) => {
                // nil in a char-table means no binding; t records an
                // explicitly unbound entry.
                let sdef = if remove { Value::Nil } else { unbound_or(def) };
                if let Some(code) = natnum
                    && code & CHAR_MODIFIER_MASK == 0
                {
                    interp.char_table_set(table, code as u32, sdef)?;
                    return Ok(def);
                } else if let Some((from, to)) = range {
                    if from <= to {
                        interp.char_table_set_range(table, from as u32, to as u32, sdef)?;
                    }
                    return Ok(def);
                }
                insertion_point = tail;
            }
            Kind::Cons(entry) => {
                let key = entry.car.get();
                if key.eq_value(qkeymap()) {
                    // A sub keymap: a lookup that found two matching
                    // bindings built a temporary map, so update this one.
                    tail = elt;
                    insertion_point = elt;
                } else if key.eq_value(idx) {
                    if remove {
                        delq_after(insertion_point, elt)?;
                    } else {
                        elt.set_cdr(def)?;
                    }
                    return Ok(def);
                } else if let Some((from, to)) = range
                    && characterp(key)
                {
                    let code = key.as_integer()?;
                    if from <= code && to >= code {
                        if remove {
                            delq_after(insertion_point, elt)?;
                        } else {
                            elt.set_cdr(def)?;
                        }
                        if from == to {
                            return Ok(def);
                        }
                    }
                }
            }
            // A `keymap' symbol in the spine starts a parent keymap used as
            // KEYMAP's tail: the binding goes before it.
            _ if elt.eq_value(qkeymap()) => break,
            _ => {}
        }
        tail = tail.cdr()?;
    }

    // Not found: add a binding.
    if !remove {
        let elt = match range {
            Some((from, to)) => {
                // Not all of IDX's characters were handled, so this keymap
                // has no char-table: insert one.
                let table = interp.make_char_table(Some("keymap".into()), Value::Nil);
                let Kind::CharTable(table_ref) = table.kind() else {
                    unreachable!("make-char-table returns a char-table")
                };
                if from <= to {
                    interp.char_table_set_range(
                        table_ref,
                        from as u32,
                        to as u32,
                        unbound_or(def),
                    )?;
                }
                table
            }
            None => Value::cons(idx, def),
        };
        insertion_point.set_cdr(Value::cons(elt, insertion_point.cdr()?))?;
    }
    Ok(def)
}

/// keymap.c:define_as_prefix: a new sparse keymap bound to C in KEYMAP.
fn define_as_prefix(
    interp: &mut Interpreter,
    keymap: Value,
    c: Value,
    env: &Env,
) -> Result<Value, LispError> {
    let cmd = make_runtime_keymap(interp, Value::Nil);
    store_in_keymap(interp, keymap, c, cmd, false, env)?;
    Ok(cmd)
}

/// keymap.c:silly_event_symbol_error: RET, M-RET, C-DEL and the like name
/// characters, not function keys.
fn silly_event_symbol_error(interp: &mut Interpreter, c: Value) -> Result<(), LispError> {
    let (base, modifiers) = parse_modifiers(interp, c)?;
    let keystring = match base.as_symbol()? {
        "DEL" => "\\d",
        "TAB" => "\\t",
        "RET" => "\\r",
        "ESC" => "\\e",
        "SPC" => " ",
        _ => return Ok(()),
    };
    let mut new_mods = String::new();
    for (bit, prefix) in [
        (KEY_DESCRIPTION_ALT_BIT, "\\A-"),
        (KEY_DESCRIPTION_CTRL_BIT, "\\C-"),
        (KEY_DESCRIPTION_HYPER_BIT, "\\H-"),
        (KEY_DESCRIPTION_META_BIT, "\\M-"),
        (KEY_DESCRIPTION_SHIFT_BIT, "\\S-"),
        (KEY_DESCRIPTION_SUPER_BIT, "\\s-"),
    ] {
        if modifiers & bit != 0 {
            new_mods.push_str(prefix);
        }
    }
    let c = reorder_modifiers(interp, c)?;
    let name = c.as_symbol()?;
    Err(LispError::Signal(format!(
        "To bind the key {name}, use [?{new_mods}{keystring}], not [{name}]"
    )))
}

/// Fdefine_key.
pub(crate) fn define_key(
    interp: &mut Interpreter,
    keymap: Value,
    key: Value,
    def: Value,
    remove: bool,
    env: &mut Env,
) -> Result<Value, LispError> {
    let mut metized = false;
    let keymap = get_keymap(interp, keymap, true, true, env)?;
    let mut length = check_vector_or_string(interp, key)?;
    if length == 0 {
        return Ok(Value::Nil);
    }
    let meta_bit = if is_vector_value(&key) || string_argument_multibyte(&key) {
        CHAR_META
    } else {
        0x80
    };
    // Keymap, key, def, the event and the binding in hand.
    let mut roots = RootedVec::from_vec(vec![keymap, key, def, Value::Nil, Value::Nil]);
    if let Kind::Vector(vector) = def.kind()
        && vector.len() > 0
        && vector.get(0).is_some_and(|first| first.is_cons())
    {
        // DEF is apparently an XEmacs-style keyboard macro.
        let tmp = Value::vector(std::iter::repeat_n(Value::Nil, vector.len()));
        roots[2] = tmp;
        let Kind::Vector(converted) = tmp.kind() else {
            unreachable!("a new vector")
        };
        for i in (0..vector.len()).rev() {
            let mut defi = vector.get(i).expect("index within the vector");
            if lucid_event_type_list_p(&defi) {
                defi = event_convert_list_value(interp, &defi)?;
            }
            converted.set(i, defi);
        }
    }
    roots[1] = possibly_translate_key_sequence(interp, key, env)?;
    let key = roots[1];
    if !key.eq_value(roots[1]) {
        length = check_vector_or_string(interp, key)?;
    }

    let mut idx = 0usize;
    loop {
        let mut c = key_event(key, idx)?;
        if c.is_cons() {
            // C may be a Lucid style event type list or a cons (FROM . TO)
            // specifying a range of characters.
            if lucid_event_type_list_p(&c) {
                c = event_convert_list_value(interp, &c)?;
            } else if characterp(c.car()?) && !characterp(c.cdr()?) {
                return Err(wrong_type_argument("characterp", c.cdr()?));
            }
        }
        if let Some(symbol) = bare_symbol(interp, c, env) {
            silly_event_symbol_error(interp, symbol)?;
        }
        if let Kind::Integer(code) = c.kind()
            && code & meta_bit != 0
            && !metized
        {
            c = meta_prefix_char(interp);
            metized = true;
        } else {
            if let Kind::Integer(code) = c.kind() {
                c = Value::Integer(code & !meta_bit);
            }
            metized = false;
            idx += 1;
        }
        roots[3] = c;

        let fixnum = matches!(c.kind(), Kind::Integer(_));
        if !fixnum
            && bare_symbol(interp, c, env).is_none()
            && (!c.is_cons()
                // If C is a range, it must be a leaf.
                || (matches!(c.car()?.kind(), Kind::Integer(_)) && idx != length))
        {
            invalid_event_message(interp, c, env)?;
        }

        if idx == length {
            return store_in_keymap(interp, roots[0], c, roots[2], remove, env);
        }

        let mut cmd = access_keymap(interp, roots[0], c, false, true, true, env)?;
        // If this key is undefined, make it a prefix.
        if cmd.is_nil() {
            cmd = define_as_prefix(interp, roots[0], c, env)?;
        }
        roots[4] = cmd;
        roots[0] = get_keymap(interp, cmd, false, true, env)?;
        if !roots[0].is_cons() {
            let trailing_esc = if c.eq_value(meta_prefix_char(interp)) && metized {
                if idx == 0 { "ESC" } else { " ESC" }
            } else {
                ""
            };
            let whole = super::call(interp, "key-description", &[key, Value::Nil], env)?;
            let head = super::call(
                interp,
                "substring",
                &[key, Value::Integer(0), Value::Integer(idx as i64)],
                env,
            )?;
            let head = super::call(interp, "key-description", &[head, Value::Nil], env)?;
            let text = |value: &Value| string_like(value).map(|s| s.text).unwrap_or_default();
            return Err(LispError::Signal(format!(
                "Key sequence {} starts with non-prefix key {}{trailing_esc}",
                text(&whole),
                text(&head)
            )));
        }
    }
}

/// Faref (SEQ, INDEX) of a vector.
fn vector_ref(seq: Value, index: usize) -> Value {
    match seq.kind() {
        Kind::Vector(vector) => vector.get(index).unwrap_or(Value::Nil),
        _ => Value::Nil,
    }
}

/// Flength of a key vector.
fn vector_length(seq: Value) -> usize {
    match seq.kind() {
        Kind::Vector(vector) => vector.len(),
        _ => 0,
    }
}

/// append_key: SEQ with KEY appended, always a vector.
fn append_key(seq: Value, key: Value) -> Value {
    let length = vector_length(seq);
    Value::vector(
        (0..length)
            .map(|i| vector_ref(seq, i))
            .chain(std::iter::once(key)),
    )
}

/// Fcopy_sequence of SEQ with its last element set to LAST.
fn copy_with_last(seq: Value, last: Value) -> Value {
    let length = vector_length(seq);
    Value::vector((0..length).map(|i| {
        if i + 1 == length {
            last
        } else {
            vector_ref(seq, i)
        }
    }))
}

/// nconc2 (LIST, TAIL) for a non-empty LIST.
fn nconc_onto(list: Value, tail: Value) -> Result<(), LispError> {
    let mut last = list;
    while let Some(next) = last.cdr()?.cons_ref().map(|_| last.cdr()) {
        last = next?;
    }
    last.set_cdr(tail)
}

/// keymap.c's struct accessible_keymaps_data.
#[derive(Clone, Copy)]
struct AccessibleKeymapsData {
    maps: Value,
    tail: Value,
    thisseq: Value,
    /// Does the current sequence end in the meta-prefix-char?
    is_metized: bool,
}

/// keymap.c:accessible_keymaps_1: queue CMD's keymap after THISSEQ + KEY
/// unless a shorter prefix of THISSEQ already reached it (a cycle).
fn accessible_keymaps_1(
    interp: &mut Interpreter,
    key: Value,
    cmd: Value,
    data: AccessibleKeymapsData,
    env: &mut Env,
) -> Result<(), LispError> {
    let AccessibleKeymapsData {
        maps,
        tail,
        thisseq,
        is_metized,
    } = data;
    let is_metized = is_metized && matches!(key.kind(), Kind::Integer(_));
    let cmd = get_keyelt(interp, cmd, false, env)?;
    let cmd = get_keymap_noload(interp, cmd, env);
    if cmd.is_nil() {
        return Ok(());
    }
    // Look for and break cycles.
    let thislen = vector_length(thisseq);
    let mut rest = maps;
    while let Some(cell) = rest.cons_ref() {
        let tem = cell.car.get();
        rest = cell.cdr.get();
        let Some((prefix, map)) = tem.cons_values() else {
            continue;
        };
        if !map.eq_value(cmd) {
            continue;
        }
        let lim = vector_length(prefix);
        if lim <= thislen
            && (0..lim).all(|i| vector_ref(prefix, i).eq_value(vector_ref(thisseq, i)))
        {
            // `prefix' is a prefix of `thisseq': a cycle.
            return Ok(());
        }
    }
    if is_metized {
        // The last key of THISSEQ is meta-prefix-char: the event is an
        // ASCII keystroke of a keymap table, so it becomes M-KEY, the same
        // length as THISSEQ and queued right after it.
        let code = key.as_integer()?;
        let tem = copy_with_last(thisseq, Value::Integer(code | CHAR_META));
        tail.set_cdr(Value::cons(Value::cons(tem, cmd), tail.cdr()?))?;
    } else {
        let tem = append_key(thisseq, key);
        nconc_onto(tail, Value::list([Value::cons(tem, cmd)]))?;
    }
    Ok(())
}

/// Faccessible_keymaps: (KEYS . MAP) for every keymap reachable from
/// KEYMAP through prefix keys, breadth first, starting at PREFIX's map.
pub(crate) fn accessible_keymaps(
    interp: &mut Interpreter,
    keymap: Value,
    prefix: Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    let prefixlen = if prefix.is_nil() {
        0
    } else {
        sequence_length_value(interp, &prefix)? as usize
    };
    let mut roots = RootedVec::from_vec(vec![keymap, prefix, Value::Nil, Value::Nil]);
    if !prefix.is_nil() {
        // Start with PREFIX's keymap, if any.  Flookup_key may give nil, a
        // number, or a list that is not a keymap; an autoloaded keymap is
        // not a cons but is still returned.
        let tem = super::call(interp, "lookup-key", &[keymap, prefix, Value::T], env)?;
        let tem = get_keymap_noload(interp, tem, env);
        if tem.is_nil() {
            return Ok(Value::Nil);
        }
        // PREFIX as a vector, a unibyte string's 8th bit as Meta.
        let prefix = if prefix.is_string() {
            let multibyte = string_argument_multibyte(&prefix);
            Value::vector((0..prefixlen).map(|i| {
                let c = string_char_code_at_in_place(&prefix, i).unwrap_or(0);
                Value::Integer(if !multibyte && c & 0o200 != 0 {
                    c ^ (0o200 | CHAR_META)
                } else {
                    c
                })
            }))
        } else {
            prefix
        };
        roots[1] = prefix;
        roots[2] = Value::list([Value::cons(prefix, tem)]);
    } else {
        let map = get_keymap(interp, keymap, true, false, env)?;
        roots[2] = Value::list([Value::cons(Value::vector([]), map)]);
    }
    let maps = roots[2];
    let meta_prefix = meta_prefix_char(interp);
    // A breadth-first traversal: TAIL is the queue, MAPS every node seen.
    let mut tail = maps;
    while tail.is_cons() {
        roots[3] = tail;
        let entry = tail.car()?;
        let thismap = entry.cdr()?;
        let thisseq = entry.car()?;
        let length = vector_length(thisseq);
        // Does the current sequence end in the meta-prefix-char?  The last
        // char of PREFIX is not metized.
        let is_metized =
            length > prefixlen && vector_ref(thisseq, length - 1).eq_value(meta_prefix);
        // An autoloaded map cannot be scanned without running Lisp.
        if thismap.is_cons() {
            map_keymap(
                interp,
                thismap,
                &mut |interp, key, cmd, env| {
                    let data = AccessibleKeymapsData {
                        maps,
                        tail,
                        thisseq,
                        is_metized,
                    };
                    accessible_keymaps_1(interp, key, cmd, data, env)
                },
                false,
                env,
            )?;
        }
        tail = roots[3].cdr()?;
    }
    Ok(maps)
}

/// keymap.c:preferred_sequence_p: 0 for a sequence with non-character
/// events or other modifiers than PREFERRED (Meta aside), 2 for one using
/// PREFERRED, 1 otherwise.
fn preferred_sequence_p(seq: Value, preferred: i64) -> i32 {
    let mut result = 1;
    for i in 0..vector_length(seq) {
        let Kind::Integer(code) = vector_ref(seq, i).kind() else {
            return 0;
        };
        let modifiers = code & (CHAR_MODIFIER_MASK & !CHAR_META);
        if modifiers == preferred {
            result = 2;
        } else if modifiers != 0 {
            return 0;
        }
    }
    result
}

/// keymap.c:shadow_lookup: Flookup_key, nil for a key too long, and with
/// REMAP a command's remapping.
pub(crate) fn shadow_lookup(
    interp: &mut Interpreter,
    keymap: Value,
    key: Value,
    accept_default: bool,
    remap: bool,
    env: &mut Env,
) -> Result<Value, LispError> {
    let value = super::call(
        interp,
        "lookup-key",
        &[
            keymap,
            key,
            if accept_default { Value::T } else { Value::Nil },
        ],
        env,
    )?;
    if matches!(value.kind(), Kind::Integer(n) if n >= 0) {
        return Ok(Value::Nil);
    }
    if !value.is_nil() && remap && value.is_symbol() {
        let remapping = super::call(
            interp,
            "command-remapping",
            &[value, Value::Nil, keymap],
            env,
        )?;
        return Ok(if remapping.is_nil() { value } else { remapping });
    }
    Ok(value)
}

/// keymap.c's Vmouse_events.
fn mouse_event_p(symbol: Value) -> bool {
    matches!(
        symbol.as_symbol(),
        Ok("menu-bar"
            | "tab-bar"
            | "tool-bar"
            | "tab-line"
            | "header-line"
            | "mode-line"
            | "mouse-1"
            | "mouse-2"
            | "mouse-3"
            | "mouse-4"
            | "mouse-5")
    )
}

/// keymap.c:where_is_internal: the sequences in KEYMAPS bound to DEFINITION,
/// longest to shortest, possibly shadowed or duplicated.  With NOMENUS and
/// not NOINDIRECT the answer comes from `where_is_cache', filled for every
/// binding at once and kept until a keymap changes or KEYMAPS differ.
fn where_is_internal(
    interp: &mut Interpreter,
    definition: Value,
    keymaps: Value,
    noindirect: bool,
    nomenus: bool,
    env: &mut Env,
) -> Result<Value, LispError> {
    let mut roots = RootedVec::from_vec(vec![definition, keymaps, Value::Nil, Value::Nil]);
    if nomenus && !noindirect {
        // Check heuristic-consistency of the cache.
        let cached_keymaps = interp.where_is_cache_keymaps;
        if !values_equal(interp, &keymaps, &cached_keymaps) {
            interp.where_is_cache = Value::Nil;
        }
        if interp.where_is_cache.is_nil() {
            // Create the cache.
            interp.where_is_cache = super::call(interp, "make-hash-table", &[], env)?;
            interp.where_is_cache_keymaps = Value::T;
        } else {
            // Reuse the cache.
            let cache = interp.where_is_cache;
            return super::call(interp, "gethash", &[definition, cache, Value::Nil], env);
        }
    } else {
        // Kill the cache so that where_is_internal_1 does not fill it.
        interp.where_is_cache = Value::Nil;
    }

    let mut found = keymaps;
    let mut maps_list: Vec<Value> = Vec::new();
    while let Some(cell) = found.cons_ref() {
        let map = get_keymap(interp, cell.car.get(), true, false, env)?;
        let accessible = accessible_keymaps(interp, map, Value::Nil, env)?;
        let mut tail = accessible;
        while let Some(entry) = tail.cons_ref() {
            maps_list.push(entry.car.get());
            tail = entry.cdr.get();
        }
        roots[2] = Value::list(maps_list.iter().copied());
        found = cell.cdr.get();
    }
    let meta_prefix = meta_prefix_char(interp);
    let mut sequences = Value::Nil;
    for &entry in &maps_list {
        // The key sequence to reach MAP, and MAP.
        let this = entry.car()?;
        let map = entry.cdr()?;
        let length = vector_length(this);
        // [META-PREFIX-CHAR CHAR] folds into [M-CHAR].
        let last_is_meta = length > 0 && vector_ref(this, length - 1).eq_value(meta_prefix);
        // Without menu entries, skip the keymaps bound to `menu-bar',
        // `tool-bar' and non-ASCII prefixes like `C-down-mouse-2'.
        if nomenus && length > 0 {
            let first = vector_ref(this, 0);
            if first.is_symbol() && mouse_event_p(parse_modifiers(interp, first)?.0) {
                continue;
            }
        }
        interp.maybe_quit(env)?;
        if map.is_cons() {
            map_keymap(
                interp,
                map,
                &mut |interp, key, binding, env| {
                    // Search through indirections unless that's not wanted.
                    let binding = if noindirect {
                        binding
                    } else {
                        get_keyelt(interp, binding, false, env)?
                    };
                    let filling = !interp.where_is_cache.is_nil();
                    if !(filling
                        || binding.eq_value(definition)
                        || (definition.is_cons() && values_equal(interp, &binding, &definition)))
                    {
                        return Ok(());
                    }
                    // A match: the key sequence where it was found.
                    let sequence = match key.kind() {
                        Kind::Integer(code) if last_is_meta => {
                            copy_with_last(this, Value::Integer(code | CHAR_META))
                        }
                        _ => {
                            let key = match key.cons_values() {
                                Some((from, to)) => Value::cons(from, to),
                                None => key,
                            };
                            append_key(this, key)
                        }
                    };
                    if filling {
                        let cache = interp.where_is_cache;
                        let known =
                            super::call(interp, "gethash", &[binding, cache, Value::Nil], env)?;
                        super::call(
                            interp,
                            "puthash",
                            &[binding, Value::cons(sequence, known), cache],
                            env,
                        )?;
                    } else {
                        sequences = Value::cons(sequence, sequences);
                        roots[3] = sequences;
                    }
                    Ok(())
                },
                false,
                env,
            )?;
        }
    }
    if nomenus && !noindirect {
        // The keymaps this cache was built for, set late so that the cache
        // reads as unfilled until it is complete.
        interp.where_is_cache_keymaps = keymaps;
        let cache = interp.where_is_cache;
        return super::call(interp, "gethash", &[definition, cache, Value::Nil], env);
    }
    Ok(sequences)
}

/// Freverse of a list.
fn reverse_list(list: Value) -> Value {
    let mut result = Value::Nil;
    let mut tail = list;
    while let Some(cell) = tail.cons_ref() {
        result = Value::cons(cell.car.get(), result);
        tail = cell.cdr.get();
    }
    result
}

/// Fwhere_is_internal.
pub(crate) fn where_is_internal_command(
    interp: &mut Interpreter,
    definition: Value,
    keymap: Value,
    firstonly: Value,
    noindirect: bool,
    no_remap: bool,
    env: &mut Env,
) -> Result<Value, LispError> {
    let non_ascii = firstonly.eq_value(Value::symbol("non-ascii"));
    // Ignore all menu bindings entirely.
    let nomenus = !firstonly.is_nil() && !non_ascii;
    let preferred = interp
        .lookup_var("where-is-preferred-modifier", env)
        .filter(Value::is_symbol)
        .and_then(|symbol| symbol.as_symbol().ok().map(solitary_event_modifier))
        .unwrap_or(0);

    // The keymaps in which to search.
    let keymaps = if let Some(cell) = keymap.cons_ref()
        && keymapp(interp, cell.car.get(), env)
    {
        keymap
    } else if !keymap.is_nil() {
        Value::list([keymap, interp.current_global_map_value()])
    } else {
        Value::list(current_active_maps(interp, env, false, None)?)
    };
    // Definition, keymaps, the remapping, sequences, remapped sequences,
    // found, and the sequence in hand.
    let mut roots = RootedVec::from_vec(vec![
        definition,
        keymaps,
        Value::Nil,
        Value::Nil,
        Value::Nil,
        Value::Nil,
        Value::Nil,
    ]);
    let tem = super::call(
        interp,
        "command-remapping",
        &[definition, Value::Nil, keymaps],
        env,
    )?;
    roots[2] = tem;
    // Find the keys bound, maybe through remapping, to TEM: right for menu
    // shortcuts, the most common use.
    let definition = if !no_remap && !tem.is_nil() {
        tem
    } else {
        definition
    };
    roots[0] = definition;

    if definition.is_symbol() && !firstonly.is_nil() {
        let name = definition.as_symbol()?.to_string();
        let mut tem = interp
            .get_symbol_property(&name, ":advertised-binding")
            .unwrap_or(Value::Nil);
        if !tem.is_nil() {
            // A list of advertised bindings.
            roots[6] = tem;
            while let Some(cell) = tem.cons_ref() {
                let candidate = cell.car.get();
                if shadow_lookup(interp, keymaps, candidate, false, false, env)?
                    .eq_value(definition)
                {
                    return Ok(candidate);
                }
                tem = cell.cdr.get();
            }
            if shadow_lookup(interp, keymaps, tem, false, false, env)?.eq_value(definition) {
                return Ok(tem);
            }
        }
    }

    roots[3] = reverse_list(where_is_internal(
        interp, definition, keymaps, noindirect, nomenus, env,
    )?);
    let mut remapped = false;
    loop {
        if !roots[3].is_cons() {
            // At the end of the sequences: process the remapped ones.
            if remapped {
                break;
            }
            roots[3] = roots[4];
            remapped = true;
            if !roots[3].is_cons() {
                break;
            }
        }
        let sequence = roots[3].car()?;
        roots[3] = roots[3].cdr()?;
        roots[6] = sequence;

        // Not shadowed by another binding for the same key.
        let looked_up = shadow_lookup(interp, keymaps, sequence, false, remapped, env)?;
        if !values_equal(interp, &looked_up, &definition) {
            continue;
        }

        // For [remap COMMAND], the key sequences which run COMMAND.
        if !no_remap
            && !remapped
            && let Kind::Vector(vector) = sequence.kind()
            && vector.len() == 2
            && vector
                .get(0)
                .is_some_and(|head| head.eq_value(Value::symbol("remap")))
            && let Some(function) = vector.get(1).filter(Value::is_symbol)
        {
            let seqs = where_is_internal(interp, function, keymaps, noindirect, nomenus, env)?;
            let reversed = reverse_list(seqs);
            if reversed.is_cons() {
                nconc_onto(reversed, roots[4])?;
                roots[4] = reversed;
            }
            continue;
        }

        // Menu strings, such as Paste from Kill Menu's entries, all read as
        // "(any string)".
        if let Kind::Vector(vector) = sequence.kind()
            && let Some(last) = vector.len().checked_sub(1)
            && vector.get(last).is_some_and(|item| item.is_string())
        {
            vector.set(last, Value::string("(any string)"));
        }

        // Record a true unshadowed match unless already seen (as with
        // inherited keymaps), and not a non-key event.
        let mut seen = false;
        let mut tail = roots[5];
        while let Some(cell) = tail.cons_ref() {
            if values_equal(interp, &cell.car.get(), &sequence) {
                seen = true;
                break;
            }
            tail = cell.cdr.get();
        }
        let non_key_event = if let Kind::Vector(vector) = sequence.kind()
            && vector.len() == 1
            && let Some(event) = vector.get(0).filter(Value::is_symbol)
        {
            interp
                .get_symbol_property(event.as_symbol()?, "non-key-event")
                .is_some_and(|value| !value.is_nil())
        } else {
            false
        };
        if !seen && !non_key_event {
            roots[5] = Value::cons(sequence, roots[5]);
        }

        // `non-ascii' takes the first binding; another FIRSTONLY the first
        // ASCII-only one.
        if non_ascii || (!firstonly.is_nil() && preferred_sequence_p(sequence, preferred) == 2) {
            return Ok(sequence);
        }
    }

    let found = reverse_list(roots[5]);
    // FIRSTONLY without an all-ASCII sequence: the best found.
    if firstonly.is_nil() {
        return Ok(found);
    }
    if preferred != 0 {
        let mut bindings = found;
        while let Some(cell) = bindings.cons_ref() {
            if preferred_sequence_p(cell.car.get(), preferred) != 0 {
                return Ok(cell.car.get());
            }
            bindings = cell.cdr.get();
        }
    }
    Ok(found.car_if_list().unwrap_or(Value::Nil))
}
