//! keyboard.c's menu items -- parse_menu_item and the item_properties
//! vector it fills, menu_bar_items, show_help_echo,
//! read_char_minibuf_menu_prompt and read_char_x_menu_prompt -- and
//! menu.c: the menu_items vector built from keymaps (keymap_panes,
//! single_keymap_panes, single_menu_item) and from old-fashioned menus
//! (list_of_panes), x_popup_menu_1 and its DEFUNs, with term.c's
//! tty_menu_show turning menu_items into the tty_menu that
//! tty_menu_activate shows.

use super::*;
use crate::lisp::alloc::RootedVec;
use crate::lisp::types::Kind;

// keyboard.h's ITEM_PROPERTY_* slots of item_properties.
pub(crate) const ITEM_PROPERTY_ITEM: usize = 0;
pub(crate) const ITEM_PROPERTY_NAME: usize = 1;
pub(crate) const ITEM_PROPERTY_DEF: usize = 2;
pub(crate) const ITEM_PROPERTY_MAP: usize = 3;
pub(crate) const ITEM_PROPERTY_TYPE: usize = 4;
pub(crate) const ITEM_PROPERTY_KEYEQ: usize = 5;
pub(crate) const ITEM_PROPERTY_SELECTED: usize = 6;
pub(crate) const ITEM_PROPERTY_HELP: usize = 7;
pub(crate) const ITEM_PROPERTY_ENABLE: usize = 8;

// keyboard.h's MENU_ITEMS_* layout of menu_items.
pub(crate) const MENU_ITEMS_PANE_NAME: usize = 1;
pub(crate) const MENU_ITEMS_PANE_PREFIX: usize = 2;
pub(crate) const MENU_ITEMS_PANE_LENGTH: usize = 3;
pub(crate) const MENU_ITEMS_ITEM_NAME: usize = 0;
pub(crate) const MENU_ITEMS_ITEM_ENABLE: usize = 1;
pub(crate) const MENU_ITEMS_ITEM_VALUE: usize = 2;
pub(crate) const MENU_ITEMS_ITEM_EQUIV_KEY: usize = 3;
pub(crate) const MENU_ITEMS_ITEM_DEFINITION: usize = 4;
pub(crate) const MENU_ITEMS_ITEM_TYPE: usize = 5;
pub(crate) const MENU_ITEMS_ITEM_SELECTED: usize = 6;
pub(crate) const MENU_ITEMS_ITEM_HELP: usize = 7;
pub(crate) const MENU_ITEMS_ITEM_LENGTH: usize = 8;

// menu.h's menuflags.
pub(crate) const MENU_KEYMAPS: u32 = 0x1;
pub(crate) const MENU_FOR_CLICK: u32 = 0x2;
pub(crate) const MENU_KBD_NAVIGATION: u32 = 0x4;

/// The static state keyboard.c and menu.c keep for menus, staticpro'd
/// there: item_properties (the last item parse_menu_item parsed),
/// menu_items with menu_items_inuse, _allocated, _used, _n_panes and
/// menu_items_submenu_depth, and menu_bar_items' vector, index and
/// menu_bar_one_keymap_changed_items.
#[derive(Clone, Debug)]
pub(crate) struct MenuState {
    pub(crate) item_properties: Value,
    pub(crate) menu_items: Value,
    pub(crate) menu_items_inuse: bool,
    pub(crate) menu_items_allocated: usize,
    pub(crate) menu_items_used: usize,
    pub(crate) menu_items_n_panes: usize,
    pub(crate) menu_items_submenu_depth: usize,
    pub(crate) menu_bar_items_vector: Value,
    pub(crate) menu_bar_items_index: usize,
    pub(crate) menu_bar_one_keymap_changed_items: Value,
    /// keyboard.c's help_echo_showing_p: a help string shown last.
    pub(crate) help_echo_showing_p: bool,
}

impl Default for MenuState {
    fn default() -> Self {
        MenuState {
            item_properties: Value::Nil,
            menu_items: Value::Nil,
            menu_items_inuse: false,
            menu_items_allocated: 0,
            menu_items_used: 0,
            menu_items_n_panes: 0,
            menu_items_submenu_depth: 0,
            menu_bar_items_vector: Value::Nil,
            menu_bar_items_index: 0,
            menu_bar_one_keymap_changed_items: Value::Nil,
            help_echo_showing_p: false,
        }
    }
}

impl MenuState {
    pub(crate) fn mark(&self, mark: &mut impl FnMut(&Value)) {
        mark(&self.item_properties);
        mark(&self.menu_items);
        mark(&self.menu_bar_items_vector);
        mark(&self.menu_bar_one_keymap_changed_items);
    }
}

#[inline]
fn sym(name: crate::lisp::types::symbols::SymbolKey) -> Value {
    name.with(|symbol| Value::Symbol(*symbol))
}

fn nil_vector(len: usize) -> Value {
    Value::Vector(crate::lisp::alloc::VectorRef::filled(len, Value::Nil))
}

fn aref(vector: Value, index: usize) -> Value {
    match vector.kind() {
        Kind::Vector(slots) => slots.get(index).unwrap_or(Value::Nil),
        _ => Value::Nil,
    }
}

fn aset(vector: Value, index: usize, value: Value) {
    if let Kind::Vector(slots) = vector.kind()
        && index < slots.len()
    {
        slots.set(index, value);
    }
}

fn asize(vector: Value) -> usize {
    match vector.kind() {
        Kind::Vector(slots) => slots.len(),
        _ => 0,
    }
}

/// alloc.c:larger_vector (VEC, INCR_MIN, -1): VEC's contents in a vector
/// at least INCR_MIN slots longer, and half again as long when that is
/// more.
fn larger_vector(vector: Value, incr_min: usize) -> Value {
    let old_size = asize(vector);
    let incr = incr_min.max(old_size >> 1);
    let bigger = nil_vector(old_size + incr);
    for index in 0..old_size {
        aset(bigger, index, aref(vector, index));
    }
    bigger
}

/// The first byte of STRING's data (SREF (STRING, 0)), its terminating
/// NUL when it is empty.
fn sref0(string: &Value) -> u8 {
    super::sdata_c_string(string).first().copied().unwrap_or(0)
}

fn schars(string: &Value) -> usize {
    match string.kind() {
        Kind::StringObject(state) => state.borrow().len(),
        _ => 0,
    }
}

fn concat2(
    interp: &mut Interpreter,
    a: Value,
    b: Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    super::call(interp, "concat", &[a, b], env)
}

fn item_prop(interp: &Interpreter, index: usize) -> Value {
    aref(interp.menu.item_properties, index)
}

fn set_item_prop(interp: &Interpreter, index: usize, value: Value) {
    aset(interp.menu.item_properties, index, value);
}

/// Whether `enable-disabled-menus-and-buttons' is non-nil.
fn enable_disabled_menus_and_buttons(interp: &Interpreter, env: &Env) -> bool {
    interp
        .lookup_var_key(cached_symbol!("enable-disabled-menus-and-buttons"), env)
        .is_some_and(|value| value.is_truthy())
}

/// keyboard.c:help_echo_substitute_command_keys: HELP's key
/// descriptions and quotes substituted, unless its first character has
/// a non-nil `help-echo-inhibit-substitution' property.
pub(crate) fn help_echo_substitute_command_keys(
    interp: &mut Interpreter,
    help: Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    if help.is_string()
        && schars(&help) > 0
        && super::call(
            interp,
            "get-text-property",
            &[
                Value::Integer(0),
                sym(cached_symbol!("help-echo-inhibit-substitution")),
                help,
            ],
            env,
        )?
        .is_truthy()
    {
        return Ok(help);
    }
    interp.call_function_value(
        sym(cached_symbol!("substitute-command-keys")),
        None,
        &[help],
        env,
    )
}

/// keyboard.c:parse_menu_item: whether ITEM, a key binding, is a menu
/// item, leaving its parse in item_properties.  INMENUBAR > 0 parses a
/// menu-bar top-level entry, < 0 an entry of a keyboard menu.
pub(crate) fn parse_menu_item(
    interp: &mut Interpreter,
    item: Value,
    inmenubar: i32,
    env: &mut Env,
) -> Result<bool, LispError> {
    let mut filter = Value::Nil;
    let mut keyhint = Value::Nil;
    let Some((item_string, rest)) = item.cons_values() else {
        return Ok(false);
    };
    // Create item_properties vector if necessary.
    if interp.menu.item_properties.is_nil() {
        interp.menu.item_properties = nil_vector(ITEM_PROPERTY_ENABLE + 1);
    }
    // Initialize optional entries.
    for index in ITEM_PROPERTY_DEF..ITEM_PROPERTY_ENABLE {
        set_item_prop(interp, index, Value::Nil);
    }
    set_item_prop(interp, ITEM_PROPERTY_ENABLE, Value::T);
    // Save the item here to protect it from GC.
    set_item_prop(interp, ITEM_PROPERTY_ITEM, item);
    let mut item = rest;
    if item_string.is_string() {
        // Old format menu item.
        set_item_prop(interp, ITEM_PROPERTY_NAME, item_string);
        // Maybe help string.
        if let Some((help, next)) = item.cons_values()
            && help.is_string()
        {
            let help = help_echo_substitute_command_keys(interp, help, env)?;
            set_item_prop(interp, ITEM_PROPERTY_HELP, help);
            item = next;
        }
        // Maybe an obsolete key binding cache.
        if let Some((cache, next)) = item.cons_values()
            && let Some((head, _)) = cache.cons_values()
            && (head.is_nil() || matches!(head.kind(), Kind::Vector(_)))
        {
            item = next;
        }
        // This is the real definition--the function to run.
        set_item_prop(interp, ITEM_PROPERTY_DEF, item);
        // Get enable property, if any.
        if item.is_symbol() {
            let tem = super::symbol_get(interp, item, cached_symbol!("menu-enable"));
            if enable_disabled_menus_and_buttons(interp, env) {
                set_item_prop(interp, ITEM_PROPERTY_ENABLE, Value::T);
            } else if !tem.is_nil() {
                set_item_prop(interp, ITEM_PROPERTY_ENABLE, tem);
            }
        }
    } else if crate::lisp::types::eq(item_string, sym(cached_symbol!("menu-item")))
        && let Some((name, start)) = item.cons_values()
    {
        // New format menu item.
        set_item_prop(interp, ITEM_PROPERTY_NAME, name);
        if let Some((def, tail)) = start.cons_values() {
            // We have a real binding.
            set_item_prop(interp, ITEM_PROPERTY_DEF, def);
            item = tail;
            // Is there an obsolete cache list with key equivalences.
            if let Some((cache, next)) = item.cons_values()
                && cache.is_cons()
            {
                item = next;
            }
            // Parse properties.
            let mut cycle = super::values::ForEachTail::new(item);
            while let Some((tem, next)) = item.cons_values() {
                item = next;
                let Some((value, _)) = item.cons_values() else {
                    break;
                };
                if crate::lisp::types::eq(tem, sym(cached_symbol!(":enable"))) {
                    if enable_disabled_menus_and_buttons(interp, env) {
                        set_item_prop(interp, ITEM_PROPERTY_ENABLE, Value::T);
                    } else {
                        set_item_prop(interp, ITEM_PROPERTY_ENABLE, value);
                    }
                } else if crate::lisp::types::eq(tem, sym(cached_symbol!(":visible"))) {
                    // If got a visible property and that evaluates to nil
                    // then ignore this item.
                    if interp.menu_item_eval_property(value, env)?.is_nil() {
                        return Ok(false);
                    }
                } else if crate::lisp::types::eq(tem, sym(cached_symbol!(":help"))) {
                    let mut help = value;
                    if help.is_string() {
                        help = help_echo_substitute_command_keys(interp, help, env)?;
                    }
                    set_item_prop(interp, ITEM_PROPERTY_HELP, help);
                } else if crate::lisp::types::eq(tem, sym(cached_symbol!(":filter"))) {
                    filter = item;
                } else if crate::lisp::types::eq(tem, sym(cached_symbol!(":key-sequence"))) {
                    if value.is_symbol()
                        || value.is_string()
                        || matches!(value.kind(), Kind::Vector(_))
                    {
                        // Be GC protected.  Set keyhint to item instead
                        // of tem.
                        keyhint = item;
                    }
                } else if crate::lisp::types::eq(tem, sym(cached_symbol!(":keys"))) {
                    if super::function_value_p(interp, &value, env) {
                        let keyeq = interp.call_function_value(value, None, &[], env)?;
                        set_item_prop(interp, ITEM_PROPERTY_KEYEQ, keyeq);
                    } else if value.is_cons() || value.is_string() {
                        set_item_prop(interp, ITEM_PROPERTY_KEYEQ, value);
                    }
                } else if crate::lisp::types::eq(tem, sym(cached_symbol!(":button")))
                    && let Some((kind, selected)) = value.cons_values()
                    && (crate::lisp::types::eq(kind, sym(cached_symbol!(":toggle")))
                        || crate::lisp::types::eq(kind, sym(cached_symbol!(":radio"))))
                {
                    set_item_prop(interp, ITEM_PROPERTY_SELECTED, selected);
                    set_item_prop(interp, ITEM_PROPERTY_TYPE, kind);
                }
                // FOR_EACH_TAIL's step, with its cycle and quit checks.
                item = item.cdr()?;
                if cycle.step(item) {
                    return Err(crate::lisp::types::circular_list(item));
                }
            }
        } else if inmenubar != 0 || !start.is_nil() {
            return Ok(false);
        }
    } else {
        // Not a menu item.
        return Ok(false);
    }

    // If item string is not a string, evaluate it to get string.  If we
    // don't get a string, skip this item.
    let item_string = item_prop(interp, ITEM_PROPERTY_NAME);
    if !item_string.is_string() {
        let item_string = interp.menu_item_eval_property(item_string, env)?;
        if !item_string.is_string() {
            return Ok(false);
        }
        set_item_prop(interp, ITEM_PROPERTY_NAME, item_string);
    }

    // If got a filter apply it on definition.
    let mut def = item_prop(interp, ITEM_PROPERTY_DEF);
    if let Some((filter, _)) = filter.cons_values() {
        let form = Value::list([filter, Value::list([sym(cached_symbol!("quote")), def])]);
        def = interp.menu_item_eval_property(form, env)?;
        set_item_prop(interp, ITEM_PROPERTY_DEF, def);
    }

    // Enable or disable selection of item.
    let tem = item_prop(interp, ITEM_PROPERTY_ENABLE);
    if !crate::lisp::types::eq(tem, Value::T) {
        let tem = interp.menu_item_eval_property(tem, env)?;
        if inmenubar != 0 && tem.is_nil() {
            // Ignore disabled items in menu bar.
            return Ok(false);
        }
        set_item_prop(interp, ITEM_PROPERTY_ENABLE, tem);
    }

    // If we got no definition, this item is just unselectable text which
    // is OK in a submenu but not in the menubar.
    if def.is_nil() {
        return Ok(inmenubar == 0);
    }

    // See if this is a separate pane or a submenu.
    let def = item_prop(interp, ITEM_PROPERTY_DEF);
    let tem = super::get_keymap(interp, def, false, true, env)?;
    // For a subkeymap, just record its details and exit.
    if tem.is_cons() {
        set_item_prop(interp, ITEM_PROPERTY_MAP, tem);
        set_item_prop(interp, ITEM_PROPERTY_DEF, tem);
        return Ok(true);
    }

    // At the top level in the menu bar, do likewise for commands also.
    // The menu bar does not display equivalent key bindings anyway.
    // ITEM_PROPERTY_DEF is already set up properly.
    if inmenubar > 0 {
        return Ok(true);
    }

    // This is a command.  See if there is an equivalent key binding.
    let space_space = Value::string("  ");
    let mut keyeq = item_prop(interp, ITEM_PROPERTY_KEYEQ);
    // The previous code preferred :key-sequence to :keys, so we
    // preserve this behavior.
    if keyeq.is_string() && !keyhint.is_cons() {
        let substituted = interp.call_function_value(
            sym(cached_symbol!("substitute-command-keys")),
            None,
            &[keyeq],
            env,
        )?;
        keyeq = concat2(interp, space_space, substituted, env)?;
    } else {
        let mut prefix = keyeq;
        let mut keys = Value::Nil;
        let def = if let Some((def, rest)) = prefix.cons_values() {
            prefix = rest;
            def
        } else {
            item_prop(interp, ITEM_PROPERTY_DEF)
        };
        if let Some((hint, _)) = keyhint.cons_values()
            && !hint.is_nil()
        {
            keys = hint;
            let tem = super::call(
                interp,
                "key-binding",
                &[keys, Value::Nil, Value::Nil, Value::Nil],
                env,
            )?;
            // We have a suggested key.  Is it bound to the command?
            if tem.is_nil()
                || (!crate::lisp::types::eq(tem, def)
                    // If the command is an alias for another (such as
                    // lmenu.el set it up), check if the original command
                    // matches the cached command.
                    && !(def.is_symbol()
                        && crate::lisp::types::eq(
                            tem,
                            super::call(interp, "symbol-function", &[def], env)?,
                        )))
            {
                keys = Value::Nil;
            }
        }
        if keys.is_nil() {
            keys = super::call(
                interp,
                "where-is-internal",
                &[def, Value::Nil, Value::T, Value::Nil, Value::Nil],
                env,
            )?;
        }
        keyeq = if keys.is_nil() {
            Value::Nil
        } else {
            let mut tem = super::call(interp, "key-description", &[keys, Value::Nil], env)?;
            if let Some((before, after)) = prefix.cons_values() {
                if before.is_string() {
                    tem = concat2(interp, before, tem, env)?;
                }
                if after.is_string() {
                    tem = concat2(interp, tem, after, env)?;
                }
            }
            concat2(interp, space_space, tem, env)?
        };
    }
    // If we have an equivalent key binding, use that.
    set_item_prop(interp, ITEM_PROPERTY_KEYEQ, keyeq);

    // Handle radio buttons or toggle boxes.
    let tem = item_prop(interp, ITEM_PROPERTY_SELECTED);
    if !tem.is_nil() {
        let selected = interp.menu_item_eval_property(tem, env)?;
        set_item_prop(interp, ITEM_PROPERTY_SELECTED, selected);
    }
    Ok(true)
}

/// The item_properties slot INDEX of the item parse_menu_item parsed
/// last.
#[cfg(test)]
pub(crate) fn item_property(interp: &Interpreter, index: usize) -> Value {
    item_prop(interp, index)
}

/// keyboard.c:map_keymap_canonical: FUN on each binding of MAP's
/// `keymap-canonicalize' (called safely, as from redisplay), which has
/// no parent.
fn map_keymap_canonical(
    interp: &mut Interpreter,
    map: Value,
    fun: &mut super::KeymapEntryCallback<'_>,
    env: &mut Env,
) -> Result<(), LispError> {
    let map = interp.safe_funcall(sym(cached_symbol!("keymap-canonicalize")), &[map], env)?;
    super::map_keymap_own_entries(interp, map, fun, env)?;
    Ok(())
}

/// keyboard.c:menu_bar_items: the menu bar's items, four slots each
/// (KEY, the item's NAME, the list of its definitions in the maps, and
/// its column on the bar, set by display_menu_bar), from every active
/// keymap's `menu-bar' prefix, the keys of `menu-bar-final-items' moved
/// to the end, and four nils after the last.  OLD is the vector to
/// reuse.
pub(crate) fn menu_bar_items(
    interp: &mut Interpreter,
    old: Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    // In order to build the menus, we need to call the keymap
    // accessors.  They all call maybe_quit.  But this function is called
    // during redisplay, during which a quit is fatal.  So inhibit
    // quitting while building the menus.  We do this instead of specbind
    // because (1) errors will clear it anyway and (2) this avoids risk of
    // specpdl overflow.
    let oquit = interp
        .forwarded_c_value("inhibit-quit", &Env::new())
        .unwrap_or(Value::Nil);
    interp.set_c_variable("inhibit-quit", Value::T);
    let result = menu_bar_items_1(interp, old, env);
    interp.set_c_variable("inhibit-quit", oquit);
    result
}

fn menu_bar_items_1(
    interp: &mut Interpreter,
    old: Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    interp.menu.menu_bar_items_vector = if old.is_nil() { nil_vector(24) } else { old };
    interp.menu.menu_bar_items_index = 0;

    // Build our list of keymaps.
    let mut maps = RootedVec::<Value>::new();
    let otlm = interp
        .lookup_var_key(cached_symbol!("overriding-terminal-local-map"), env)
        .unwrap_or(Value::Nil);
    let olm = interp
        .lookup_var_key(cached_symbol!("overriding-local-map"), env)
        .unwrap_or(Value::Nil);
    let menu_flag = interp
        .lookup_var_key(cached_symbol!("overriding-local-map-menu-flag"), env)
        .is_some_and(|value| value.is_truthy());
    if menu_flag && !olm.is_nil() {
        // Yes, use them (if non-nil) as well as the global map.
        if !otlm.is_nil() {
            maps.push(otlm);
        }
        if !olm.is_nil() {
            maps.push(olm);
        }
    } else {
        // No, so use major and minor mode keymaps and keymap property.
        if !otlm.is_nil() && menu_flag {
            maps.push(otlm);
        }
        let pt = interp.buffer.borrow().point() as i64;
        let tem = super::get_local_map(interp, pt, "keymap")?;
        if !tem.is_nil() {
            maps.push(tem);
        }
        for (_, map) in super::current_minor_maps(interp, env)? {
            maps.push(map);
        }
        maps.push(super::get_local_map(interp, pt, "local-map")?);
    }
    maps.push(interp.current_global_map_value());

    // Look up in each map the dummy prefix key `menu-bar'.
    for mapno in (0..maps.len()).rev() {
        if maps[mapno].is_nil() {
            continue;
        }
        let def = super::access_keymap(
            interp,
            maps[mapno],
            sym(cached_symbol!("menu-bar")),
            true,
            false,
            true,
            env,
        )?;
        let def = super::get_keymap(interp, def, false, true, env)?;
        if def.is_cons() {
            interp.menu.menu_bar_one_keymap_changed_items = Value::Nil;
            map_keymap_canonical(
                interp,
                def,
                &mut |interp, key, item, env| menu_bar_item(interp, key, item, env),
                env,
            )?;
        }
    }

    // Move to the end those items that should be at the end.
    let mut tail = interp
        .lookup_var_key(cached_symbol!("menu-bar-final-items"), env)
        .unwrap_or(Value::Nil);
    let mut cycle = super::values::ForEachTail::new(tail);
    while let Some((head, next)) = tail.cons_values() {
        let vector = interp.menu.menu_bar_items_vector;
        let end = interp.menu.menu_bar_items_index;
        let mut i = 0;
        while i < end {
            if crate::lisp::types::eq(head, aref(vector, i)) {
                // Move the item at index I to the end, shifting all the
                // others forward.
                let tems = [
                    aref(vector, i),
                    aref(vector, i + 1),
                    aref(vector, i + 2),
                    aref(vector, i + 3),
                ];
                for index in i..end - 4 {
                    aset(vector, index, aref(vector, index + 4));
                }
                for (offset, tem) in tems.into_iter().enumerate() {
                    aset(vector, end - 4 + offset, tem);
                }
                break;
            }
            i += 4;
        }
        tail = next;
        if cycle.step(tail) {
            return Err(crate::lisp::types::circular_list(tail));
        }
    }

    // Add nil, nil, nil, nil at the end.
    let i = interp.menu.menu_bar_items_index;
    if i + 4 > asize(interp.menu.menu_bar_items_vector) {
        interp.menu.menu_bar_items_vector = larger_vector(interp.menu.menu_bar_items_vector, 4);
    }
    let vector = interp.menu.menu_bar_items_vector;
    for offset in 0..4 {
        aset(vector, i + offset, Value::Nil);
    }
    interp.menu.menu_bar_items_index = i + 4;
    Ok(vector)
}

/// keyboard.c:menu_bar_item: add one item to menu_bar_items_vector, for
/// KEY, ITEM_STRING and DEF; if there's already an item for KEY, add
/// this DEF to it.
fn menu_bar_item(
    interp: &mut Interpreter,
    key: Value,
    item: Value,
    env: &mut Env,
) -> Result<(), LispError> {
    if crate::lisp::types::eq(item, sym(cached_symbol!("undefined"))) {
        // If a map has an explicit `undefined' as definition, discard any
        // previously made menu bar item.
        let vector = interp.menu.menu_bar_items_vector;
        let mut i = 0;
        while i < interp.menu.menu_bar_items_index {
            if crate::lisp::types::eq(key, aref(vector, i)) {
                let index = interp.menu.menu_bar_items_index;
                for slot in i..index.saturating_sub(4) {
                    aset(vector, slot, aref(vector, slot + 4));
                }
                interp.menu.menu_bar_items_index -= 4;
            }
            i += 4;
        }
    }
    // If this keymap has already contributed to this KEY, don't contribute
    // to it a second time.
    let changed = interp.menu.menu_bar_one_keymap_changed_items;
    if super::call(interp, "memq", &[key, changed], env)?.is_truthy() || item.is_nil() {
        return Ok(());
    }
    interp.menu.menu_bar_one_keymap_changed_items = Value::cons(key, changed);
    // We add to menu_bar_one_keymap_changed_items before doing the
    // parse_menu_item, so that if it turns out it wasn't a menu item, it
    // still correctly hides any further menu item.
    if !parse_menu_item(interp, item, 1, env)? {
        return Ok(());
    }
    let item = item_prop(interp, ITEM_PROPERTY_DEF);
    // Find any existing item for this KEY.
    let vector = interp.menu.menu_bar_items_vector;
    let index = interp.menu.menu_bar_items_index;
    let mut i = 0;
    while i < index {
        if crate::lisp::types::eq(key, aref(vector, i)) {
            break;
        }
        i += 4;
    }
    if i == index {
        // If we did not find this KEY, add it at the end.
        if i + 4 > asize(vector) {
            interp.menu.menu_bar_items_vector = larger_vector(vector, 4);
        }
        let vector = interp.menu.menu_bar_items_vector;
        aset(vector, i, key);
        aset(vector, i + 1, item_prop(interp, ITEM_PROPERTY_NAME));
        aset(vector, i + 2, Value::list([item]));
        aset(vector, i + 3, Value::Integer(0));
        interp.menu.menu_bar_items_index = i + 4;
    } else {
        // We did find an item for this KEY.  Add ITEM to its list of maps.
        let old = aref(vector, i + 2);
        // If the new and the old items are not both keymaps, the lookup
        // will only find `item'.
        let both = super::keymapp(interp, item, env)
            && super::keymapp(interp, old.car_if_list().unwrap_or(Value::Nil), env);
        aset(
            vector,
            i + 2,
            Value::cons(item, if both { old } else { Value::Nil }),
        );
    }
    Ok(())
}

/// keyboard.c:show_help_echo: HELP (a string, nil to clear, a function
/// called with WINDOW, OBJECT and POS, or a form) shown by
/// `show-help-function' after `mouse-fixup-help-message'.
pub(crate) fn show_help_echo(
    interp: &mut Interpreter,
    mut help: Value,
    window: Value,
    object: Value,
    pos: Value,
    env: &mut Env,
) -> Result<(), LispError> {
    if !help.is_nil() && !help.is_string() {
        help = if super::function_value_p(interp, &help, env) {
            interp.safe_funcall(help, &[window, object, pos], env)?
        } else {
            interp.safe_funcall(sym(cached_symbol!("eval")), &[help, Value::T], env)?
        };
        if !help.is_string() {
            return Ok(());
        }
    }
    let noninteractive = interp
        .forwarded_c_value("noninteractive", &Env::new())
        .is_some_and(|value| value.is_truthy());
    if !noninteractive && help.is_string() {
        help = interp.call_function_value(
            sym(cached_symbol!("mouse-fixup-help-message")),
            None,
            &[help],
            env,
        )?;
    }
    if help.is_string() || help.is_nil() {
        let function = interp
            .lookup_var_key(cached_symbol!("show-help-function"), env)
            .unwrap_or(Value::Nil);
        if !function.is_nil() {
            let help = help_echo_substitute_command_keys(interp, help, env)?;
            interp.call_function_value(function, None, &[help], env)?;
        }
        interp.menu.help_echo_showing_p = help.is_string();
    }
    Ok(())
}

// ---------------------------------------------------------------------
// menu.c: the menu_items vector.

/// menu.c:init_menu_items.
fn init_menu_items(interp: &mut Interpreter) -> Result<(), LispError> {
    if interp.menu.menu_items_inuse {
        return Err(LispError::Signal(
            "Trying to use a menu from within a menu-entry".into(),
        ));
    }
    if interp.menu.menu_items.is_nil() {
        interp.menu.menu_items_allocated = 60;
        interp.menu.menu_items = nil_vector(60);
    }
    interp.menu.menu_items_inuse = true;
    interp.menu.menu_items_used = 0;
    interp.menu.menu_items_n_panes = 0;
    interp.menu.menu_items_submenu_depth = 0;
    Ok(())
}

/// menu.c:unuse_menu_items.
fn unuse_menu_items(interp: &mut Interpreter) {
    interp.menu.menu_items_inuse = false;
}

/// menu.c:discard_menu_items: free the structure if it is especially
/// large; otherwise hold on to it, to save time.
fn discard_menu_items(interp: &mut Interpreter) {
    if interp.menu.menu_items_allocated > 200 {
        interp.menu.menu_items = Value::Nil;
        interp.menu.menu_items_allocated = 0;
    }
}

/// menu.c:ensure_menu_items: room for ITEMS more slots.
fn ensure_menu_items(interp: &mut Interpreter, items: usize) {
    let free = interp.menu.menu_items_allocated - interp.menu.menu_items_used;
    if items > free {
        interp.menu.menu_items = larger_vector(interp.menu.menu_items, items - free);
        interp.menu.menu_items_allocated = asize(interp.menu.menu_items);
    }
}

fn push_slot(interp: &mut Interpreter, value: Value) {
    aset(interp.menu.menu_items, interp.menu.menu_items_used, value);
    interp.menu.menu_items_used += 1;
}

/// menu.c:push_left_right_boundary.
fn push_left_right_boundary(interp: &mut Interpreter) {
    ensure_menu_items(interp, 1);
    push_slot(interp, sym(cached_symbol!("quote")));
}

/// menu.c:push_menu_pane: a new pane NAME with prefix key PREFIX_VEC.
fn push_menu_pane(interp: &mut Interpreter, name: Value, prefix_vec: Value) {
    ensure_menu_items(interp, MENU_ITEMS_PANE_LENGTH);
    if interp.menu.menu_items_submenu_depth == 0 {
        interp.menu.menu_items_n_panes += 1;
    }
    push_slot(interp, Value::T);
    push_slot(interp, name);
    push_slot(interp, prefix_vec);
}

/// push_menu_item's arguments, in its order.
struct MenuItemArgs {
    name: Value,
    enable: Value,
    key: Value,
    def: Value,
    equiv: Value,
    type_: Value,
    selected: Value,
    help: Value,
}

/// menu.c:push_menu_item.
fn push_menu_item(interp: &mut Interpreter, item: MenuItemArgs) {
    let MenuItemArgs {
        name,
        enable,
        key,
        def,
        equiv,
        type_,
        selected,
        help,
    } = item;
    ensure_menu_items(interp, MENU_ITEMS_ITEM_LENGTH);
    let used = interp.menu.menu_items_used;
    let items = interp.menu.menu_items;
    aset(items, used + MENU_ITEMS_ITEM_NAME, name);
    aset(items, used + MENU_ITEMS_ITEM_ENABLE, enable);
    aset(items, used + MENU_ITEMS_ITEM_VALUE, key);
    aset(items, used + MENU_ITEMS_ITEM_EQUIV_KEY, equiv);
    aset(items, used + MENU_ITEMS_ITEM_DEFINITION, def);
    aset(items, used + MENU_ITEMS_ITEM_TYPE, type_);
    aset(items, used + MENU_ITEMS_ITEM_SELECTED, selected);
    aset(items, used + MENU_ITEMS_ITEM_HELP, help);
    interp.menu.menu_items_used = used + MENU_ITEMS_ITEM_LENGTH;
}

/// FRAME_TERMCAP_P: FRAME is on a text terminal (not the initial one).
pub(crate) fn frame_termcap_p(frame: Value) -> bool {
    match frame.kind() {
        Kind::Frame(frame) => frame
            .borrow()
            .terminal
            .is_some_and(|terminal| terminal.borrow().kind.is_some()),
        _ => false,
    }
}

/// FRAME_INITIAL_P: FRAME is on the initial (batch) terminal.
fn frame_initial_p(frame: Value) -> bool {
    match frame.kind() {
        Kind::Frame(frame) => frame
            .borrow()
            .terminal
            .is_none_or(|terminal| terminal.borrow().kind.is_none()),
        _ => true,
    }
}

fn menu_updating_frame(interp: &Interpreter, env: &Env) -> Value {
    interp
        .lookup_var_key(cached_symbol!("menu-updating-frame"), env)
        .unwrap_or(Value::Nil)
}

/// menu.c's struct skp: the args single_keymap_panes passes to
/// single_menu_item.
struct Skp {
    pending_maps: Value,
    notbuttons: usize,
}

/// menu.c:single_keymap_panes: a pane PANE_NAME for KEYMAP, whose
/// `@'-named submenus become panes after it, MAXDEPTH levels deep.
fn single_keymap_panes(
    interp: &mut Interpreter,
    keymap: Value,
    pane_name: Value,
    prefix: Value,
    maxdepth: i32,
    env: &mut Env,
) -> Result<(), LispError> {
    // skp.maxdepth serves only the toolkits' submenus, which a text
    // terminal does not build.
    let mut skp = Skp {
        pending_maps: Value::Nil,
        notbuttons: 0,
    };
    if maxdepth <= 0 {
        return Ok(());
    }
    push_menu_pane(interp, pane_name, prefix);
    // have_boxes is false: remember index for first item in this pane so
    // we can go back and add a prefix when (if) we see the first button.
    // After that, notbuttons is set to 0, to mark that we have seen a
    // button and all non button items need a prefix.
    skp.notbuttons = interp.menu.menu_items_used;
    map_keymap_canonical(
        interp,
        keymap,
        &mut |interp, key, item, env| single_menu_item(interp, key, item, &mut skp, env),
        env,
    )?;
    // Process now any submenus which want to be panes at this level.
    while let Some((elt, rest)) = skp.pending_maps.cons_values() {
        let (map, eltcdr) = elt.cons_values().unwrap_or((Value::Nil, Value::Nil));
        let (string, key) = eltcdr.cons_values().unwrap_or((Value::Nil, Value::Nil));
        // We no longer discard the @ from the beginning of the string
        // here.  Instead, we do this in *menu_show.
        single_keymap_panes(interp, map, string, key, maxdepth - 1, env)?;
        skp.pending_maps = rest;
    }
    Ok(())
}

/// menu.c:single_menu_item: one keymap entry, KEY bound to ITEM.
fn single_menu_item(
    interp: &mut Interpreter,
    key: Value,
    item: Value,
    skp: &mut Skp,
    env: &mut Env,
) -> Result<(), LispError> {
    // Parse the menu item and leave the result in item_properties.
    if !parse_menu_item(interp, item, 0, env)? {
        // Not a menu item.
        return Ok(());
    }
    let map = item_prop(interp, ITEM_PROPERTY_MAP);
    let enabled = item_prop(interp, ITEM_PROPERTY_ENABLE);
    let mut item_string = item_prop(interp, ITEM_PROPERTY_NAME);
    if !map.is_nil() && sref0(&item_string) == b'@' {
        if !enabled.is_nil() {
            // An enabled separate pane.  Remember this to handle it later.
            skp.pending_maps = Value::cons(
                Value::cons(map, Value::cons(item_string, key)),
                skp.pending_maps,
            );
        }
        return Ok(());
    }
    // Simulate radio buttons and toggle boxes by putting a prefix in
    // front of them (have_boxes is false).
    let mut prefix = None;
    let type_ = item_prop(interp, ITEM_PROPERTY_TYPE);
    if !type_.is_nil() {
        let selected = item_prop(interp, ITEM_PROPERTY_SELECTED);
        if skp.notbuttons != 0 {
            // The first button.  Line up previous items in this menu.
            let mut idx = skp.notbuttons;
            let mut submenu = 0i32;
            while idx < interp.menu.menu_items_used {
                let tem = aref(interp.menu.menu_items, idx + MENU_ITEMS_ITEM_NAME);
                if tem.is_nil() {
                    idx += 1;
                    // Skip sub menu.
                    submenu += 1;
                } else if crate::lisp::types::eq(tem, sym(cached_symbol!("lambda"))) {
                    idx += 1;
                    // End sub menu.
                    submenu -= 1;
                } else if crate::lisp::types::eq(tem, Value::T) {
                    // Skip new pane marker.
                    idx += 3;
                } else if crate::lisp::types::eq(tem, sym(cached_symbol!("quote"))) {
                    // Skip a left, right divider.
                    idx += 1;
                } else {
                    if submenu == 0 && sref0(&tem) != 0 && sref0(&tem) != b'-' {
                        let spaced = concat2(interp, Value::string("    "), tem, env)?;
                        aset(interp.menu.menu_items, idx + MENU_ITEMS_ITEM_NAME, spaced);
                    }
                    idx += MENU_ITEMS_ITEM_LENGTH;
                }
            }
            skp.notbuttons = 0;
        }
        // Calculate prefix, if any, for this item.
        if crate::lisp::types::eq(type_, sym(cached_symbol!(":toggle"))) {
            prefix = Some(if selected.is_nil() { "[ ] " } else { "[X] " });
        } else if crate::lisp::types::eq(type_, sym(cached_symbol!(":radio"))) {
            prefix = Some(if selected.is_nil() { "( ) " } else { "(*) " });
        }
    } else if skp.notbuttons == 0 && sref0(&item_string) != 0 && sref0(&item_string) != b'-' {
        // Not a button.  If we have earlier buttons, then we need a
        // prefix.
        prefix = Some("    ");
    }
    if let Some(prefix) = prefix {
        item_string = concat2(interp, Value::string(prefix), item_string, env)?;
    }
    if frame_termcap_p(menu_updating_frame(interp, env)) && !map.is_nil() {
        // Indicate visually that this is a submenu.
        item_string = concat2(interp, item_string, Value::string(" >"), env)?;
    }
    push_menu_item(
        interp,
        MenuItemArgs {
            name: item_string,
            enable: enabled,
            key,
            def: item_prop(interp, ITEM_PROPERTY_DEF),
            equiv: item_prop(interp, ITEM_PROPERTY_KEYEQ),
            type_: item_prop(interp, ITEM_PROPERTY_TYPE),
            selected: item_prop(interp, ITEM_PROPERTY_SELECTED),
            help: item_prop(interp, ITEM_PROPERTY_HELP),
        },
    );
    Ok(())
}

/// menu.c:keymap_panes: a pane for each of KEYMAPS.
fn keymap_panes(
    interp: &mut Interpreter,
    keymaps: &[Value],
    env: &mut Env,
) -> Result<(), LispError> {
    init_menu_items(interp)?;
    // Loop over the given keymaps, making a pane for each map.  But don't
    // make a pane that is empty--ignore that map instead.
    for &map in keymaps {
        let prompt = super::keymap_prompt(interp, map, env)?;
        single_keymap_panes(interp, map, prompt, Value::Nil, 10, env)?;
    }
    Ok(())
}

/// menu.c:list_of_items: the items of a single pane defined by the
/// alist PANE.
fn list_of_items(interp: &mut Interpreter, pane: Value) -> Result<(), LispError> {
    let mut tail = pane;
    while let Some((item, next)) = tail.cons_values() {
        if item.is_string() {
            push_menu_item(
                interp,
                MenuItemArgs {
                    name: item,
                    enable: Value::Nil,
                    key: Value::Nil,
                    def: Value::T,
                    equiv: Value::Nil,
                    type_: Value::Nil,
                    selected: Value::Nil,
                    help: Value::Nil,
                },
            );
        } else if let Some((item1, value)) = item.cons_values() {
            if !item1.is_string() {
                return Err(wrong_type_argument("stringp", item1));
            }
            push_menu_item(
                interp,
                MenuItemArgs {
                    name: item1,
                    enable: Value::T,
                    key: value,
                    def: Value::T,
                    equiv: Value::Nil,
                    type_: Value::Nil,
                    selected: Value::Nil,
                    help: Value::Nil,
                },
            );
        } else {
            push_left_right_boundary(interp);
        }
        tail = next;
    }
    Ok(())
}

/// menu.c:list_of_panes: all the panes and items of a menu described by
/// the alist-of-alists MENU (old-fashioned calls to x-popup-menu).
fn list_of_panes(interp: &mut Interpreter, menu: Value) -> Result<(), LispError> {
    init_menu_items(interp)?;
    let mut tail = menu;
    while let Some((elt, next)) = tail.cons_values() {
        let pane_name = elt
            .car_if_list()
            .ok_or_else(|| wrong_type_argument("listp", elt))?;
        if !pane_name.is_string() {
            return Err(wrong_type_argument("stringp", pane_name));
        }
        push_menu_pane(interp, pane_name, Value::Nil);
        let pane_data = elt.cdr_if_list().unwrap_or(Value::Nil);
        if !pane_data.is_cons() {
            return Err(wrong_type_argument("consp", pane_data));
        }
        list_of_items(interp, pane_data)?;
        tail = next;
    }
    Ok(())
}

// ---------------------------------------------------------------------
// menu.c: x_popup_menu_1.

fn check_integer_range(value: Value, lo: i64, hi: i64) -> Result<i64, LispError> {
    if !value.is_integer() {
        return Err(wrong_type_argument("integerp", value));
    }
    match value.kind() {
        Kind::Integer(i) if lo <= i && i <= hi => Ok(i),
        _ => Err(LispError::SignalValue(Value::list([
            Value::symbol("args-out-of-range"),
            value,
            Value::Integer(lo),
            Value::Integer(hi),
        ]))),
    }
}

/// The first element of a list, or nil (Fcar, for a list).
fn car_of(interp: &mut Interpreter, value: Value, env: &mut Env) -> Result<Value, LispError> {
    super::call(interp, "car", &[value], env)
}

fn cdr_of(interp: &mut Interpreter, value: Value, env: &mut Env) -> Result<Value, LispError> {
    super::call(interp, "cdr", &[value], env)
}

/// menu.c:x_popup_menu_1: POSITION decoded, MENU parsed into menu_items
/// and shown by the frame's terminal; its selection.
pub(crate) fn x_popup_menu_1(
    interp: &mut Interpreter,
    position: Value,
    menu: Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    if position.is_nil() {
        // This is an obsolete call, which wants us to precompute the
        // keybinding equivalents, but we don't do that any more anyway.
        return Ok(Value::Nil);
    }
    let mut menuflags = 0u32;
    let mut tem2 = Value::Nil;
    let mut window = Value::Nil;
    let mut x = Value::Nil;
    let mut y = Value::Nil;
    // Decode the first argument: find the window and the coordinates.
    let head = position.car_if_list().unwrap_or(Value::Nil);
    let mut get_current_pos_p = crate::lisp::types::eq(position, Value::T)
        || (position.is_cons()
            && (crate::lisp::types::eq(head, sym(cached_symbol!("menu-bar")))
                || crate::lisp::types::eq(head, sym(cached_symbol!("tab-bar")))
                || position
                    .cdr_if_list()
                    .and_then(|rest| rest.car_if_list())
                    .is_some_and(|second| {
                        crate::lisp::types::eq(second, sym(cached_symbol!("tab-bar")))
                    })
                || crate::lisp::types::eq(head, sym(cached_symbol!("tool-bar")))));
    if !get_current_pos_p {
        let tem = car_of(interp, position, env)?;
        if let Some((tx, rest)) = tem.cons_values() {
            let after = cdr_of(interp, position, env)?;
            window = car_of(interp, after, env)?;
            x = tx;
            y = car_of(interp, rest, env)?;
        } else {
            menuflags |= MENU_FOR_CLICK;
            // EVENT_START (position).
            let start = position
                .cdr_if_list()
                .and_then(|rest| rest.car_if_list())
                .unwrap_or(Value::Nil);
            window = car_of(interp, start, env)?;
            let rest = cdr_of(interp, start, env)?;
            tem2 = car_of(interp, rest, env)?;
            // The MENU_KBD_NAVIGATION field is set when the menu was
            // invoked by F10, which probably means they have no mouse.
            // We set it when POSITION and last_nonmenu_event are
            // different, which means we constructed POSITION by hand (in
            // popup-menu, see menu-bar.el) to look like a mouse click on
            // the menu bar event.
            let posn_posn = |value: Value| {
                value
                    .cdr_if_list()
                    .and_then(|rest| rest.car_if_list())
                    .unwrap_or(Value::Nil)
            };
            let last = interp
                .lookup_var_key(cached_symbol!("last-nonmenu-event"), env)
                .unwrap_or(Value::Nil);
            if !crate::lisp::types::eq(posn_posn(last), posn_posn(position))
                && tem2.car_if_list().is_some_and(|head| {
                    crate::lisp::types::eq(head, sym(cached_symbol!("menu-bar")))
                })
            {
                menuflags |= MENU_KBD_NAVIGATION;
            }
            // POSN_WINDOW_POSN (tem).
            let rest = cdr_of(interp, start, env)?;
            let rest = cdr_of(interp, rest, env)?;
            let tem = car_of(interp, rest, env)?;
            x = car_of(interp, tem, env)?;
            y = cdr_of(interp, tem, env)?;
        }
        // If a click happens in an external tool bar or a detached tool
        // bar, x and y is NIL.  In that case, use the current mouse
        // position.
        if x.is_nil() && y.is_nil() {
            get_current_pos_p = true;
        }
    }
    if get_current_pos_p {
        // Use the mouse's current position: a text terminal has no
        // mouse_position_hook, so the frame's origin.
        x = Value::Integer(0);
        y = Value::Integer(0);
        window = interp.selected_frame_value();
    }
    // Decode where to put the menu.
    let frame;
    let (xpos, ypos);
    if matches!(window.kind(), Kind::Frame(_)) {
        frame = window;
        xpos = 0;
        ypos = 0;
    } else if super::call(interp, "windowp", &[window], env)?.is_truthy() {
        if super::call(interp, "window-live-p", &[window], env)?.is_nil() {
            return Err(wrong_type_argument("window-live-p", window));
        }
        frame = super::call(interp, "window-frame", &[window], env)?;
        let integer = |value: Value| value.as_integer().unwrap_or(0);
        // WINDOW_LEFT_EDGE_X and WINDOW_TOP_EDGE_Y.
        let left = integer(super::call(interp, "window-pixel-left", &[window], env)?);
        let top = integer(super::call(interp, "window-pixel-top", &[window], env)?);
        if tem2.is_integer() {
            // Clicks in the text area, where TEM2 is a buffer position,
            // are relative to the top-left edge of the text area:
            // window_box_left (a text window's left margin past its
            // edge), and the tab and header lines below its top.
            let margins = super::call(interp, "window-margins", &[window], env)?;
            let margin = integer(margins.car_if_list().unwrap_or(Value::Nil));
            let tab_line = integer(super::call(
                interp,
                "window-tab-line-height",
                &[window],
                env,
            )?);
            let header_line = integer(super::call(
                interp,
                "window-header-line-height",
                &[window],
                env,
            )?);
            xpos = left + margin;
            ypos = top + tab_line + header_line;
        } else {
            xpos = left;
            ypos = top;
        }
    } else {
        return Err(wrong_type_argument("windowp", window));
    }
    let int_min = i64::from(i32::MIN);
    let int_max = i64::from(i32::MAX);
    let xpos = xpos + check_integer_range(x, int_min - xpos, int_max - xpos)?;
    let ypos = ypos + check_integer_range(y, int_min - ypos, int_max - ypos)?;
    interp.set_c_variable("menu-updating-frame", frame);

    // Now parse the lisp menus.  unuse_menu_items is this part's unwind.
    let parsed = parse_popup_menu(interp, menu, &mut menuflags, env);
    unuse_menu_items(interp);
    let title = parsed?;

    // discard_menu_items is the display's unwind.
    let result = (|| {
        super::run_hook(
            interp,
            &cached_symbol!("x-pre-popup-menu-hook").with(|symbol| *symbol),
            env,
        )?;
        // Display them in a menu, but not if F is the initial frame that
        // doesn't have its hooks set (e.g., in a batch session), because
        // such a frame cannot display menus.
        if frame_initial_p(frame) {
            return Ok((Value::Nil, None));
        }
        tty_menu_show(interp, frame, xpos, ypos, menuflags, title, env)
    })();
    discard_menu_items(interp);
    let (selection, error_name) = result?;
    if let Some(error_name) = error_name {
        return Err(LispError::Signal(error_name.into()));
    }
    Ok(selection)
}

/// x_popup_menu_1's decoding of MENU into menu_items: its title.
fn parse_popup_menu(
    interp: &mut Interpreter,
    menu: Value,
    menuflags: &mut u32,
    env: &mut Env,
) -> Result<Value, LispError> {
    let mut title = Value::Nil;
    let keymap = super::get_keymap(interp, menu, false, false, env)?;
    if keymap.is_cons() {
        // We were given a keymap.  Extract menu info from the keymap.
        keymap_panes(interp, &[menu], env)?;
        // Search for a string appearing directly as an element of the
        // keymap.  That string is the title of the menu.
        let prompt = super::keymap_prompt(interp, keymap, env)?;
        if !prompt.is_nil() {
            title = prompt;
            // Make that be the pane title of the first pane.
            aset(interp.menu.menu_items, MENU_ITEMS_PANE_NAME, prompt);
        }
        *menuflags |= MENU_KEYMAPS;
    } else if let Some((first, _)) = menu.cons_values()
        && super::keymapp(interp, first, env)
    {
        // We were given a list of keymaps: list_length's count (Flength of
        // a cons is list_length), a dotted list's wrong-type-argument
        // listp and a circular one's circular-list signaled before any
        // keymap is looked at.
        super::call(interp, "length", &[menu], env)?;
        let mut maps = RootedVec::<Value>::new();
        // The first keymap that has a prompt string supplies the menu
        // title.
        let mut tem = menu;
        while let Some((map, next)) = tem.cons_values() {
            let keymap = super::get_keymap(interp, map, true, false, env)?;
            maps.push(keymap);
            let prompt = super::keymap_prompt(interp, keymap, env)?;
            if title.is_nil() && !prompt.is_nil() {
                title = prompt;
            }
            tem = next;
        }
        // Extract the detailed info to make one pane.
        let maps: Vec<Value> = maps.iter().copied().collect();
        let maps = RootedVec::from_vec(maps);
        keymap_panes(interp, &maps, env)?;
        // Make the title be the pane title of the first pane.
        if !title.is_nil() {
            aset(interp.menu.menu_items, MENU_ITEMS_PANE_NAME, title);
        }
        *menuflags |= MENU_KEYMAPS;
    } else {
        // We were given an old-fashioned menu.
        title = car_of(interp, menu, env)?;
        if !title.is_string() {
            return Err(wrong_type_argument("stringp", title));
        }
        let panes = cdr_of(interp, menu, env)?;
        list_of_panes(interp, panes)?;
        *menuflags &= !MENU_KEYMAPS;
    }
    Ok(title)
}

/// menu.c:emulate_dialog_with_menu: CONTENTS as a menu with its prompt
/// centred on FRAME (a text terminal's dialog).
pub(crate) fn emulate_dialog_with_menu(
    interp: &mut Interpreter,
    frame: Value,
    contents: Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    let prompt = car_of(interp, contents, env)?;
    // FRAME_COLS.
    let mut x_coord = super::call(interp, "frame-text-cols", &[frame], env)?.as_integer()?;
    // Center the title at frame middle.  (TTY menus have their upper-left
    // corner at the given position.)
    if prompt.is_string() {
        x_coord -= schars(&prompt) as i64;
    }
    let y_coord = super::call(interp, "frame-total-lines", &[frame], env)?.as_integer()?;
    let newpos = Value::list([
        Value::list([Value::Integer(x_coord / 2), Value::Integer(y_coord / 2)]),
        frame,
    ]);
    super::init_raw_keybuf_count(interp);
    x_popup_menu_1(interp, newpos, Value::list([prompt, contents]), env)
}

// ---------------------------------------------------------------------
// term.c: the tty_menu built from menu_items.

/// term.c's TTYM_* results of tty_menu_activate.
pub(crate) const TTYM_FAILURE: i32 = -1;
pub(crate) const TTYM_SUCCESS: i32 = 1;
pub(crate) const TTYM_NO_SELECT: i32 = 2;
pub(crate) const TTYM_IA_SELECT: i32 = 3;
pub(crate) const TTYM_NEXT: i32 = 4;
pub(crate) const TTYM_PREV: i32 = 5;

/// term.c's tty_menu: COUNT items, each its text (the C string, bytes
/// in Emacs's internal encoding), its submenu, its pane number (also
/// the enabled flag) and its help text; the pane count and the widest
/// item's width.
#[derive(Debug, Default)]
pub(crate) struct TtyMenu {
    pub(crate) text: Vec<Vec<u8>>,
    pub(crate) submenu: Vec<Option<Box<TtyMenu>>>,
    pub(crate) panenumber: Vec<i32>,
    pub(crate) help_text: Vec<Option<Vec<u8>>>,
    pub(crate) panecount: i32,
    pub(crate) width: usize,
}

impl TtyMenu {
    pub(crate) fn count(&self) -> usize {
        self.text.len()
    }

    /// term.c:tty_menu_search_pane.
    fn search_pane(&mut self, pane: i32) -> Option<&mut TtyMenu> {
        let index = (0..self.count()).find(|&i| {
            self.submenu[i].is_some()
                && (pane == self.panenumber[i]
                    || self.submenu[i]
                        .as_ref()
                        .is_some_and(|sub| sub.contains_pane(pane)))
        })?;
        let found_here = pane == self.panenumber[index];
        let submenu = self.submenu[index].as_deref_mut()?;
        if found_here {
            Some(submenu)
        } else {
            submenu.search_pane(pane)
        }
    }

    fn contains_pane(&self, pane: i32) -> bool {
        (0..self.count()).any(|i| {
            self.submenu[i]
                .as_ref()
                .is_some_and(|sub| pane == self.panenumber[i] || sub.contains_pane(pane))
        })
    }

    /// term.c:tty_menu_calc_size: (WIDTH, HEIGHT) the menu needs.
    pub(crate) fn calc_size(&self) -> (usize, usize) {
        let mut maxsubwidth = self.width;
        let mut maxheight = self.count();
        for i in 0..self.count() {
            if let Some(sub) = &self.submenu[i] {
                let (w2, h2) = sub.calc_size();
                maxsubwidth = maxsubwidth.max(w2);
                maxheight = maxheight.max(i + h2);
            }
        }
        (maxsubwidth, maxheight)
    }
}

/// menu.c:menu_item_width: the columns of the C string STR's
/// characters.
pub(crate) fn menu_item_width(interp: &Interpreter, bytes: &[u8]) -> usize {
    let env = Env::new();
    let mut len = 0;
    let mut p = 0;
    while p < bytes.len() && bytes[p] != 0 {
        let (ch, ch_len) = super::syntax::string_char_and_length(&bytes[p..]);
        len += super::character_width(interp, ch as u32, &env);
        p += ch_len;
    }
    len
}

/// SBYTES.
fn sbytes(string: &Value) -> usize {
    match string.kind() {
        Kind::StringObject(state) => state.borrow().bytes().len(),
        _ => 0,
    }
}

/// FRAME_HAS_MINIBUF_P: F has a minibuffer window of its own.
fn frame_has_minibuffer(interp: &Interpreter, f: crate::lisp::types::FrameRef) -> bool {
    let mini = f.borrow().minibuffer_window_id();
    mini != 0 && interp.window_frame_id(mini) == Some(f)
}

/// term.c:tty_menu_add_pane: a new pane, on the outer-most level.
fn tty_menu_add_pane(interp: &Interpreter, menu: &mut TtyMenu, txt: Vec<u8>) -> i32 {
    let len = menu_item_width(interp, &txt);
    menu.submenu.push(Some(Box::default()));
    menu.text.push(txt);
    menu.panecount += 1;
    menu.panenumber.push(menu.panecount);
    menu.help_text.push(None);
    // Update the menu width, if necessary.
    menu.width = menu.width.max(len);
    menu.panecount
}

/// term.c:tty_menu_add_selection: a new item in a menu pane.
fn tty_menu_add_selection(
    interp: &Interpreter,
    menu: &mut TtyMenu,
    pane: i32,
    txt: Vec<u8>,
    enable: bool,
    help_text: Option<Vec<u8>>,
) -> bool {
    let menu = if pane != 0 {
        match menu.search_pane(pane) {
            Some(menu) => menu,
            None => return false,
        }
    } else {
        menu
    };
    let len = menu_item_width(interp, &txt);
    menu.submenu.push(None);
    menu.text.push(txt);
    menu.panenumber.push(i32::from(enable));
    menu.help_text.push(help_text);
    // Update the menu width, if necessary.
    menu.width = menu.width.max(len);
    true
}

/// The frame's `menu-bar-lines' items as display_menu_bar placed them:
/// (KEY, NAME, column) of each, up to the first nil name.
fn frame_menu_bar_items(frame: Value) -> Value {
    match frame.kind() {
        Kind::Frame(frame) => frame.borrow().menu_bar_items,
        _ => Value::Nil,
    }
}

/// term.c:tty_menu_last_menubar_item: the index of FRAME's last menu-bar
/// item.
fn tty_menu_last_menubar_item(frame: Value) -> usize {
    let items = frame_menu_bar_items(frame);
    let mut i = 0usize;
    while i < asize(items) {
        if aref(items, i + 1).is_nil() {
            break;
        }
        i += 4;
    }
    // Went one too far!
    i.wrapping_sub(4)
}

/// term.c:tty_menu_new_item_coords: the column of the menu-bar item
/// after (WHICH TTYM_NEXT) or before the one at X.
fn tty_menu_new_item_coords(interp: &Interpreter, frame: Value, which: i32, x: &mut i64) {
    let items = frame_menu_bar_items(frame);
    let last_i = tty_menu_last_menubar_item(frame);
    let mut prev_x = -1i64;
    let mut i = 0usize;
    while i < asize(items) {
        let string = aref(items, i + 1);
        let pos = aref(items, i + 3);
        if string.is_nil() {
            return;
        }
        let ix = pos.as_integer().unwrap_or(0);
        let width = menu_item_width(interp, &super::sdata_c_string(&string)) as i64;
        // We use <= so the blank between 2 items on a TTY is considered
        // part of the previous item.
        if ix <= *x && *x <= ix + width {
            // Found current item.  Now compute the X coordinate of the
            // previous or next item.
            if which == TTYM_NEXT {
                *x = if i < last_i {
                    aref(items, i + 4 + 3).as_integer().unwrap_or(0)
                } else {
                    // Wrap around to the first item.
                    0
                };
            } else if prev_x < 0 {
                // Wrap around to the last item.
                *x = aref(items, last_i + 3).as_integer().unwrap_or(0);
            } else {
                *x = prev_x;
            }
            return;
        }
        prev_x = ix;
        i += 4;
    }
}

/// term.c:tty_menu_help_callback: HELP_STRING (none to clear) shown for
/// the item ITEM of pane PANE.
pub(crate) fn tty_menu_help_callback(
    interp: &mut Interpreter,
    help_string: Option<&[u8]>,
    pane: i32,
    item: i32,
    env: &mut Env,
) -> Result<(), LispError> {
    let first = aref(interp.menu.menu_items, 0);
    let pane_name = if crate::lisp::types::eq(first, Value::T) {
        aref(interp.menu.menu_items, MENU_ITEMS_PANE_NAME)
    } else if crate::lisp::types::eq(first, sym(cached_symbol!("quote"))) {
        // This shouldn't happen, see xmenu_show.
        Value::string("")
    } else {
        aref(interp.menu.menu_items, MENU_ITEMS_ITEM_NAME)
    };
    // (menu-item MENU-NAME PANE-NUMBER)
    let menu_object = Value::list([
        sym(cached_symbol!("menu-item")),
        pane_name,
        Value::Integer(i64::from(pane)),
    ]);
    let help = match help_string {
        Some(bytes) => super::strings::make_string(bytes.to_vec())
            .map_err(|_| LispError::Signal("Memory exhausted".into()))?,
        None => Value::Nil,
    };
    show_help_echo(
        interp,
        help,
        Value::Nil,
        menu_object,
        Value::Integer(i64::from(item)),
        env,
    )
}

/// What the frontend's tty_menu_activate answers: its status (TTYM_*),
/// and the pane and item index chosen.
pub(crate) struct TtyMenuActivation {
    pub(crate) status: i32,
    pub(crate) pane: i32,
    pub(crate) selidx: i32,
}

/// term.c:tty_menu_activate as the frontend runs it: MENU shown at X0,
/// Y0 (one-based) under the navigation map until a choice or a quit.
pub(crate) type TtyMenuActivate = Box<
    dyn FnMut(
        &mut Interpreter,
        &mut Env,
        &mut TtyMenu,
        i64,
        i64,
        bool,
    ) -> Result<TtyMenuActivation, LispError>,
>;

thread_local! {
    static TTY_MENU_ACTIVATE: std::cell::RefCell<Option<TtyMenuActivate>> =
        const { std::cell::RefCell::new(None) };
}

pub(crate) fn set_tty_menu_activate(activate: Option<TtyMenuActivate>) {
    TTY_MENU_ACTIVATE.with_borrow_mut(|slot| *slot = activate);
}

fn run_tty_menu_activate(
    interp: &mut Interpreter,
    env: &mut Env,
    menu: &mut TtyMenu,
    x0: i64,
    y0: i64,
    kbd_navigation: bool,
) -> Result<TtyMenuActivation, LispError> {
    let Some(mut activate) = TTY_MENU_ACTIVATE.with_borrow_mut(|slot| slot.take()) else {
        // A frame of a terminal whose frontend draws no menu.
        return Ok(TtyMenuActivation {
            status: TTYM_FAILURE,
            pane: 0,
            selidx: 0,
        });
    };
    let outcome = activate(interp, env, menu, x0, y0, kbd_navigation);
    TTY_MENU_ACTIVATE.with_borrow_mut(|slot| {
        if slot.is_none() {
            *slot = Some(activate);
        }
    });
    outcome
}

/// tty_menu_show's construction of the tty_menu from menu_items: a pane
/// per pane, an item per item, each item's text its name padded to the
/// widest name's byte count before its key equivalent; the error to
/// signal for an item before any pane.
fn build_tty_menu(interp: &Interpreter, menuflags: u32) -> Result<TtyMenu, &'static str> {
    let mut menu = TtyMenu::default();
    // Create all the necessary panes and their items.
    let items = interp.menu.menu_items;
    let used = interp.menu.menu_items_used;
    let mut maxwidth = 0usize;
    let mut lpane = TTYM_FAILURE;
    let mut i = 0usize;
    while i < used {
        if crate::lisp::types::eq(aref(items, i), Value::T) {
            // Create a new pane.
            let pane_name = aref(items, i + MENU_ITEMS_PANE_NAME);
            let prefix = aref(items, i + MENU_ITEMS_PANE_PREFIX);
            let mut pane_string = if pane_name.is_nil() {
                Vec::new()
            } else {
                super::sdata_c_string(&pane_name)
            };
            if menuflags & MENU_KEYMAPS != 0 && !prefix.is_nil() && !pane_string.is_empty() {
                pane_string.remove(0);
            }
            lpane = tty_menu_add_pane(interp, &mut menu, pane_string);
            i += MENU_ITEMS_PANE_LENGTH;
            // Find the width of the widest item in this pane.
            let mut j = i;
            while j < used {
                let item = aref(items, j);
                if crate::lisp::types::eq(item, Value::T) {
                    break;
                }
                if item.is_nil() {
                    j += 1;
                    continue;
                }
                maxwidth = maxwidth.max(sbytes(&item));
                j += MENU_ITEMS_ITEM_LENGTH;
            }
        } else if crate::lisp::types::eq(aref(items, i), sym(cached_symbol!("quote"))) {
            // Ignore a nil in the item list.  It's meaningful only for
            // dialog boxes.
            i += 1;
        } else {
            // Create a new item within current pane.
            let item_name = aref(items, i + MENU_ITEMS_ITEM_NAME);
            let enable = aref(items, i + MENU_ITEMS_ITEM_ENABLE);
            let descrip = aref(items, i + MENU_ITEMS_ITEM_EQUIV_KEY);
            let help = aref(items, i + MENU_ITEMS_ITEM_HELP);
            let help_string = help.is_string().then(|| super::sdata_c_string(&help));
            let name_bytes = super::sdata_c_string(&item_name);
            let item_data = if descrip.is_nil() {
                name_bytes
            } else {
                // The name, blanks from its character count up to the
                // widest name's byte count, and the description.
                let descrip = super::sdata_c_string(&descrip);
                let mut data = vec![0u8; maxwidth + descrip.len()];
                data[..name_bytes.len()].copy_from_slice(&name_bytes);
                let mut j = schars(&item_name);
                while j < maxwidth {
                    data[j] = b' ';
                    j += 1;
                }
                data.truncate(j);
                data.extend_from_slice(&descrip);
                // A C string ends at its first NUL.
                if let Some(end) = data.iter().position(|&byte| byte == 0) {
                    data.truncate(end);
                }
                data
            };
            if lpane == TTYM_FAILURE
                || !tty_menu_add_selection(
                    interp,
                    &mut menu,
                    lpane,
                    item_data,
                    !enable.is_nil(),
                    help_string,
                )
            {
                return Err("Can't add selection to menu");
            }
            i += MENU_ITEMS_ITEM_LENGTH;
        }
    }

    Ok(menu)
}

/// term.c:tty_menu_show: menu_items as a tty_menu on frame F at X, Y,
/// activated; the selection, and the error to signal, if any.
fn tty_menu_show(
    interp: &mut Interpreter,
    frame: Value,
    x: i64,
    y: i64,
    menuflags: u32,
    _title: Value,
    env: &mut Env,
) -> Result<(Value, Option<&'static str>), LispError> {
    if interp.menu.menu_items_n_panes == 0 {
        return Ok((Value::Nil, None));
    }
    if interp.menu.menu_items_used <= MENU_ITEMS_PANE_LENGTH {
        return Ok((Value::Nil, Some("Empty menu")));
    }
    // Don't GC while we prepare and show the menu, because we give the
    // menu functions pointers to the contents of strings (alloc.c's
    // inhibit_garbage_collection, undone by the SAFE_FREE_UNBIND_TO that
    // returns the entry, or by the unwind of a quit or a throw to top
    // level).
    interp.inhibit_garbage_collection();
    let shown = tty_menu_show_1(interp, frame, x, y, menuflags, env);
    interp.allow_garbage_collection();
    shown
}

/// tty_menu_show from its inhibit_garbage_collection on.
fn tty_menu_show_1(
    interp: &mut Interpreter,
    frame: Value,
    x: i64,
    y: i64,
    menuflags: u32,
    env: &mut Env,
) -> Result<(Value, Option<&'static str>), LispError> {
    let Kind::Frame(f) = frame.kind() else {
        return Ok((Value::Nil, None));
    };
    // Adjust coordinates to be root-window-relative.
    let (left_pos, top_pos, text_cols, text_lines) = {
        let state = f.borrow();
        (state.left, state.top, state.width, state.text_height)
    };
    let mut x = x + left_pos;
    let mut y = y + top_pos;
    let mut item_x = x;
    let item_y = y;
    let items = interp.menu.menu_items;
    let used = interp.menu.menu_items_used;
    let mut menu = match build_tty_menu(interp, menuflags) {
        Ok(menu) => menu,
        Err(error_name) => return Ok((Value::Nil, Some(error_name))),
    };

    // All set and ready to fly.
    let dispwidth = text_cols;
    let dispheight = text_lines;
    x = x.min(dispwidth).max(1);
    y = y.min(dispheight).max(1);
    // tty_menu_locate.
    let (width, height) = menu.calc_size();
    let width = width as i64 + 2;
    let height = height as i64;
    let mut ulx = x + 1;
    let mut uly = y;
    if ulx + width > dispwidth {
        x -= (ulx + width) - dispwidth;
        ulx = dispwidth - width;
    }
    if uly + height > dispheight {
        y -= (uly + height) - dispheight;
        uly = dispheight - height;
    }
    if frame_has_minibuffer(interp, f) && uly + height > dispheight - 2 {
        // Move the menu away of the echo area, to avoid overwriting the
        // menu with help echo messages or vice versa.
        if super::dispatch::display::echo_area_message_value().is_some() {
            let mini = super::call(interp, "minibuffer-window", &[], env)?;
            let lines = super::call(interp, "window-total-height", &[mini], env)?.as_integer()? + 1;
            y -= lines;
            uly -= lines;
        } else {
            y -= 2;
            uly -= 2;
        }
    }
    if ulx < 0 {
        x -= ulx;
    }
    if uly < 0 {
        y -= uly;
    }

    // tty_pop_down_menu restores the current buffer, which
    // tty_menu_activate's redisplay switches at will.
    let buffer = interp.current_buffer_id();
    let navigation = interp
        .lookup_var_key(cached_symbol!("tty-menu-navigation-map"), env)
        .unwrap_or(Value::Nil);
    let count = interp.specpdl_index();
    interp.specbind_symbol(
        &cached_symbol!("overriding-terminal-local-map").with(|symbol| *symbol),
        navigation,
        env,
    )?;
    let activation = run_tty_menu_activate(
        interp,
        env,
        &mut menu,
        x,
        y,
        menuflags & MENU_KBD_NAVIGATION != 0,
    );
    let unbound = match &activation {
        Err(error) => interp.with_lisp_stack_roots(error, |interp| interp.unbind_to(count, env)),
        Ok(_) => interp.unbind_to(count, env),
    };
    if interp.has_buffer_id(buffer) {
        interp.set_current_buffer_id(buffer)?;
    }
    unbound?;
    let activation = activation?;
    let mut entry = Value::Nil;
    let mut error_name = None;
    match activation.status {
        TTYM_SUCCESS => {
            // Find the item number SELIDX in pane number PANE.
            let mut pane = activation.pane;
            let mut selidx = activation.selidx;
            let mut pane_prefix = Value::Nil;
            let mut i = 0usize;
            while i < used {
                if crate::lisp::types::eq(aref(items, i), Value::T) {
                    if pane == 0 {
                        pane_prefix = aref(items, i + MENU_ITEMS_PANE_PREFIX);
                    }
                    pane -= 1;
                    i += MENU_ITEMS_PANE_LENGTH;
                } else {
                    if pane == -1 {
                        if selidx == 0 {
                            entry = aref(items, i + MENU_ITEMS_ITEM_VALUE);
                            if menuflags & MENU_KEYMAPS != 0 {
                                entry = Value::list([entry]);
                                if !pane_prefix.is_nil() {
                                    entry = Value::cons(pane_prefix, entry);
                                }
                            }
                            break;
                        }
                        selidx -= 1;
                    }
                    i += MENU_ITEMS_ITEM_LENGTH;
                }
            }
        }
        TTYM_NEXT | TTYM_PREV => {
            tty_menu_new_item_coords(interp, frame, activation.status, &mut item_x);
            entry = Value::cons(Value::Integer(item_x), Value::Integer(item_y));
        }
        TTYM_FAILURE => error_name = Some("Can't activate menu"),
        TTYM_NO_SELECT => {
            // If the selected frame was changed while we displayed a menu,
            // throw to top level in order to undo any temporary settings
            // made by TTY menu code.
            if !crate::lisp::types::eq(frame, interp.selected_frame_value()) {
                super::call(interp, "top-level", &[], env)?;
            }
            // Make "Cancel" equivalent to C-g unless FOR_CLICK (which
            // means the menu was invoked with a mouse event as POSITION).
            if menuflags & MENU_FOR_CLICK == 0 {
                return Err(LispError::SignalValue(Value::list([Value::symbol("quit")])));
            }
        }
        _ => {}
    }
    Ok((entry, error_name))
}

// ---------------------------------------------------------------------
// The frame's menu bar: xdisp.c's update_menu_bar and display_menu_bar,
// menu.c's menu-bar-menu-at-x-y.

/// xdisp.c:update_menu_bar for FRAME, when its menu bar must be
/// recomputed (CHANGED): `activate-menubar-hook' and
/// `menu-bar-update-hook' unless HOOKS_RUN, then FRAME_MENU_BAR_ITEMS
/// from menu_bar_items, with the selected window's buffer current and,
/// without `overriding-local-map-menu-flag', the overriding maps bound
/// to nil.  Whether the hooks have run.
pub(crate) fn update_menu_bar(
    interp: &mut Interpreter,
    frame: crate::lisp::types::FrameRef,
    save_match_data: bool,
    hooks_run: bool,
    changed: bool,
    env: &mut Env,
) -> Result<bool, LispError> {
    // If called recursively during a menu update, do nothing.  This can
    // happen when, for instance, an activate-menubar-hook causes a
    // redisplay.
    if interp
        .lookup_var_key(cached_symbol!("inhibit-menubar-update"), env)
        .is_some_and(|value| value.is_truthy())
    {
        return Ok(hooks_run);
    }
    if frame.borrow().menu_bar_lines <= 0 || !changed {
        return Ok(hooks_run);
    }
    let window = frame.borrow().selected_window_id();
    let window_value = interp.record_value(window);
    let buffer = super::window_buffer_id(interp, &window_value);
    let prev = interp.current_buffer_id();
    // record_unwind_save_match_data: here when SAVE_MATCH_DATA, around
    // the frame loop in prepare_menu_bars otherwise.
    let _ = save_match_data;
    let saved_match_data = super::call(interp, "match-data", &[], env)?;
    let count = interp.specpdl_index();
    let result = (|| {
        interp.specbind_symbol(
            &cached_symbol!("inhibit-menubar-update").with(|symbol| *symbol),
            Value::T,
            env,
        )?;
        if let Some(buffer) = buffer {
            interp.set_current_buffer_id(buffer)?;
        }
        if !interp
            .lookup_var_key(cached_symbol!("overriding-local-map-menu-flag"), env)
            .is_some_and(|value| value.is_truthy())
        {
            interp.specbind_symbol(
                &cached_symbol!("overriding-terminal-local-map").with(|symbol| *symbol),
                Value::Nil,
                env,
            )?;
            interp.specbind_symbol(
                &cached_symbol!("overriding-local-map").with(|symbol| *symbol),
                Value::Nil,
                env,
            )?;
        }
        let mut hooks_run = hooks_run;
        if !hooks_run {
            // Run the Lucid hook.
            super::safe_run_hooks(
                interp,
                &cached_symbol!("activate-menubar-hook").with(|symbol| *symbol),
                env,
            )?;
            // If it has changed current-menubar from previous value,
            // really recompute the menu-bar from the value.
            super::safe_run_hooks(
                interp,
                &cached_symbol!("menu-bar-update-hook").with(|symbol| *symbol),
                env,
            )?;
            hooks_run = true;
        }
        interp.set_c_variable("menu-updating-frame", Value::Frame(frame));
        let old = frame.borrow().menu_bar_items;
        let items = menu_bar_items(interp, old, env)?;
        frame.borrow_mut().menu_bar_items = items;
        Ok(hooks_run)
    })();
    let unbound = match &result {
        Err(error) => interp.with_lisp_stack_roots(error, |interp| interp.unbind_to(count, env)),
        Ok(_) => interp.unbind_to(count, env),
    };
    let restored = super::call(interp, "set-match-data", &[saved_match_data, Value::T], env);
    if interp.has_buffer_id(prev) {
        interp.set_current_buffer_id(prev)?;
    }
    unbound?;
    restored?;
    result
}

/// xdisp.c:display_menu_bar's walk of FRAME_MENU_BAR_ITEMS: each item's
/// column recorded where it is displayed (its name padded with one
/// space, up to the frame's width); the names to show, in order, with
/// their columns.
pub(crate) fn display_menu_bar_items(
    interp: &Interpreter,
    frame: crate::lisp::types::FrameRef,
) -> Vec<(Value, usize)> {
    let items = frame.borrow().menu_bar_items;
    let cols = frame.borrow().width.max(0) as usize;
    let env = Env::new();
    let mut shown = Vec::new();
    let mut hpos = 0usize;
    let mut i = 0usize;
    while i + 3 < asize(items) {
        let string = aref(items, i + 1);
        // Stop at nil string.
        if string.is_nil() {
            break;
        }
        // Remember where item was displayed.
        aset(items, i + 3, Value::Integer(hpos as i64));
        // Display the item, pad with one space.
        if hpos < cols {
            shown.push((string, hpos));
            let columns: usize = super::string_like(&string)
                .map(|text| {
                    text.character_codes()
                        .into_iter()
                        .map(|c| super::character_width(interp, c as u32, &env))
                        .sum()
                })
                .unwrap_or(0);
            hpos = (hpos + columns.max(schars(&string) + 1)).min(cols);
        }
        i += 4;
    }
    shown
}

/// menu.c:Fmenu_bar_menu_at_x_y: the menu-bar menu of FRAME at X, Y.
pub(crate) fn menu_bar_menu_at_x_y(
    interp: &mut Interpreter,
    x: i64,
    y: i64,
    frame: Value,
) -> Result<Value, LispError> {
    let f = match frame.kind() {
        Kind::Frame(f) => f,
        _ => interp.selected_frame_id,
    };
    if !interp.frame_is_live(f) {
        return Ok(Value::Nil);
    }
    // pixel_to_glyph_coords on a text frame: the cell is the pixel.
    let (col, row) = (x, y);
    if 0 <= row && row < f.borrow().menu_bar_lines {
        let items = f.borrow().menu_bar_items;
        // This loop assumes a single menu-bar line, and will fail to find
        // an item if it is not in the first line.  Note that
        // make_lispy_event in keyboard.c makes the same assumption.
        let mut i = 0usize;
        while i < asize(items) {
            let string = aref(items, i + 1);
            let pos = aref(items, i + 3);
            if string.is_nil() {
                return Ok(Value::Nil);
            }
            let pos = pos.as_integer().unwrap_or(0);
            let width = menu_item_width(interp, &super::sdata_c_string(&string)) as i64;
            // We use <= so the blank between 2 items on a TTY is
            // considered part of the previous item.
            if pos <= col && col <= pos + width {
                return Ok(aref(items, i));
            }
            i += 4;
        }
    }
    Ok(Value::Nil)
}

/// menu.c:Fx_popup_dialog: POSITION's frame, redisplayed, and CONTENTS
/// shown as a dialog -- on a text terminal, which has no
/// popup_dialog_hook, emulated with a menu.
pub(crate) fn x_popup_dialog(
    interp: &mut Interpreter,
    position: Value,
    contents: Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    // Decode the first argument: find the window or frame to use.
    let head = position.car_if_list().unwrap_or(Value::Nil);
    let window = if crate::lisp::types::eq(position, Value::T)
        || (position.is_cons()
            && (crate::lisp::types::eq(head, sym(cached_symbol!("menu-bar")))
                || crate::lisp::types::eq(head, sym(cached_symbol!("tab-bar")))
                || crate::lisp::types::eq(head, sym(cached_symbol!("tool-bar")))))
    {
        interp.selected_window_value()
    } else if let Some((tem, rest)) = position.cons_values() {
        if tem.is_cons() {
            car_of(interp, rest, env)?
        } else {
            // EVENT_START (position), then POSN_WINDOW.
            let start = car_of(interp, rest, env)?;
            car_of(interp, start, env)?
        }
    } else if matches!(position.kind(), Kind::Frame(_))
        || super::call(interp, "windowp", &[position], env)?.is_truthy()
    {
        position
    } else {
        Value::Nil
    };
    // Decode where to put the menu.
    let frame = if matches!(window.kind(), Kind::Frame(_)) {
        window
    } else if super::call(interp, "windowp", &[window], env)?.is_truthy() {
        if super::call(interp, "window-live-p", &[window], env)?.is_nil() {
            return Err(wrong_type_argument("window-live-p", window));
        }
        super::call(interp, "window-frame", &[window], env)?
    } else {
        return Err(wrong_type_argument("windowp", window));
    };
    // Note that xw_popup_dialog can call menu code, so
    // Vmenu_updating_frame should be set (Bug#17891).
    interp.set_c_variable("menu-updating-frame", frame);
    // Force a redisplay before showing the dialog.
    super::call(interp, "redisplay", &[Value::T], env)?;
    // ... or emulate it with a menu.
    emulate_dialog_with_menu(interp, frame, contents, env)
}

// ---------------------------------------------------------------------
// keyboard.c: menus read_char shows for a keymap with a prompt.

/// EVENT_HAS_PARAMETERS.
fn event_has_parameters(event: Value) -> bool {
    event.cons_values().is_some_and(|(_, rest)| rest.is_cons())
}

fn menu_prompting(interp: &Interpreter, env: &Env) -> bool {
    interp
        .lookup_var_key(cached_symbol!("menu-prompting"), env)
        .is_some_and(|value| value.is_truthy())
}

/// keyboard.c:record_menu_key: C, a key a mouse menu gave, recorded for
/// echoing, for this-command-keys, and so on.
fn record_menu_key(interp: &mut Interpreter, c: Value, env: &mut Env) -> Result<(), LispError> {
    // Wipe the echo area.
    super::dispatch::display::clear_message(interp, env, true, false)?;
    super::record_external_input_event(interp, &c, env);
    // Once we reread a character, echoing can happen the next time we
    // pause to read a new one.
    interp.keyboard_input.ok_to_echo_at_next_pause = false;
    // Record this character as part of the current key.
    interp.keyboard_input.last_event_reread = false;
    super::add_read_event_to_command_keys(interp, c, env)?;
    super::echo_update(interp, env)?;
    // Re-reading in the middle of a command.
    interp.set_variable_key(cached_symbol!("last-input-event"), c, env);
    Ok(())
}

/// keyboard.c:read_char_x_menu_prompt: after the mouse click PREV_EVENT,
/// MAP's menu popped up and its first event returned (the others queued
/// on `unread-command-events'), t for a menu rejected; nil without
/// `menu-prompting' or a click.  *USED_MOUSE_MENU whether a menu showed.
pub(crate) fn read_char_x_menu_prompt(
    interp: &mut Interpreter,
    map: Value,
    prev_event: Value,
    used_mouse_menu: &mut bool,
    env: &mut Env,
) -> Result<Value, LispError> {
    *used_mouse_menu = false;
    // Use local over global Menu maps.
    if !menu_prompting(interp, env) {
        return Ok(Value::Nil);
    }
    // If we got to this point via a mouse click, use a real menu for
    // mouse selection.
    let head = prev_event.car_if_list().unwrap_or(Value::Nil);
    if event_has_parameters(prev_event)
        && !crate::lisp::types::eq(head, sym(cached_symbol!("menu-bar")))
        && !crate::lisp::types::eq(head, sym(cached_symbol!("tab-bar")))
        && !crate::lisp::types::eq(head, sym(cached_symbol!("tool-bar")))
    {
        // Display the menu and get the selection.
        let keymap = super::get_keymap(interp, map, false, true, env)?;
        let mut value = x_popup_menu_1(interp, prev_event, keymap, env)?;
        if let Some((first, rest)) = value.cons_values() {
            record_menu_key(interp, first, env)?;
            // If we got multiple events, unread all but the first.  There
            // is no way to prevent those unread events from showing up
            // later in last_nonmenu_event.  So turn symbol and integer
            // events into lists, to indicate that they came from a mouse
            // menu, so that when present in last_nonmenu_event they won't
            // confuse things.
            let mut tem = rest;
            while let Some((event, next)) = tem.cons_values() {
                record_menu_key(interp, event, env)?;
                if event.is_symbol() || event.is_integer() {
                    tem.set_car(Value::cons(event, sym(cached_symbol!("disabled"))))?;
                }
                tem = next;
            }
            // If we got more than one event, put all but the first onto
            // this list to be read later.  Return just the first event now.
            let unread = interp
                .lookup_var_key(cached_symbol!("unread-command-events"), env)
                .unwrap_or(Value::Nil);
            let combined = super::call(interp, "nconc", &[rest, unread], env)?;
            interp.set_variable_key(cached_symbol!("unread-command-events"), combined, env);
            value = first;
        } else if value.is_nil() {
            value = Value::T;
        }
        *used_mouse_menu = true;
        return Ok(value);
    }
    Ok(Value::Nil)
}

/// keyboard.c:read_char_minibuf_menu_prompt: MAP's prompt and its
/// documented bindings shown in the echo area a line at a time, and a
/// character read (another line after `menu-prompt-more-char'); nil when
/// MAP has no prompt or without `menu-prompting'.
pub(crate) fn read_char_minibuf_menu_prompt(
    interp: &mut Interpreter,
    commandflag: i32,
    quit_char_is_event: bool,
    map: Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    // FIXME: Use the minibuffer's frame width.
    let width = interp.frame_width() - 4;
    let mut idx: Option<usize> = None;
    let mut nobindings = true;
    let mut vector = Value::Nil;
    if !menu_prompting(interp, env) {
        return Ok(Value::Nil);
    }
    let map = super::get_keymap(interp, map, false, true, env)?;
    let name = super::keymap_prompt(interp, map, env)?;
    // If we don't have any menus, just read a character normally.
    if !name.is_string() {
        return Ok(Value::Nil);
    }
    // Prompt string always starts with map's prompt, and a space.
    // A list built backwards, as GNU's: (": " NAME).
    let prompt_strings = Value::cons(Value::string(": "), Value::list([name]));
    let nlength = schars(&name) as i64 + 2;
    let mut rest = map;
    // Present the documented bindings, a line at a time.
    loop {
        let mut notfirst = false;
        let mut menu_strings = prompt_strings;
        let mut i = nlength;
        // Loop over elements of map.
        while i < width {
            // At end of map, wrap around if just starting, or end this
            // line if already have something on it.
            if rest.is_nil() {
                if notfirst || nobindings {
                    break;
                }
                rest = map;
            }
            // Look at the next element of the map.
            let elt = match idx {
                Some(index) => aref(vector, index),
                None => rest.car_if_list().unwrap_or(Value::Nil),
            };
            if idx.is_none() && matches!(elt.kind(), Kind::Vector(_)) {
                // If we found a dense table in the keymap, advanced past
                // it, but start scanning its contents.
                rest = rest.cdr_if_list().unwrap_or(Value::Nil);
                vector = elt;
                idx = Some(0);
                continue;
            }
            // An ordinary element.
            let (event, elt) = match idx {
                None => (
                    elt.car_if_list().unwrap_or(Value::Nil),
                    elt.cdr_if_list().unwrap_or(Value::Nil),
                ),
                Some(index) => (Value::Integer(index as i64), elt),
            };
            // Ignore the element if it has no prompt string.
            if event.is_integer() && parse_menu_item(interp, elt, -1, env)? {
                // True if the char to type matches the string.
                let mut s = item_prop(interp, ITEM_PROPERTY_NAME);
                let upcased = super::call(interp, "upcase", &[event], env)?.as_integer()?;
                let downcased = super::call(interp, "downcase", &[event], env)?.as_integer()?;
                let first = i64::from(sref0(&s));
                let char_matches = upcased == first || downcased == first;
                let desc = if char_matches {
                    Value::Nil
                } else {
                    super::call(interp, "single-key-description", &[event, Value::Nil], env)?
                };
                let type_ = item_prop(interp, ITEM_PROPERTY_TYPE);
                if crate::lisp::types::eq(type_, sym(cached_symbol!(":radio")))
                    || crate::lisp::types::eq(type_, sym(cached_symbol!(":toggle")))
                {
                    // Insert button prefix.
                    let selected = item_prop(interp, ITEM_PROPERTY_SELECTED);
                    let prefix = if crate::lisp::types::eq(type_, sym(cached_symbol!(":radio"))) {
                        if selected.is_nil() { "(*) " } else { "( ) " }
                    } else if selected.is_nil() {
                        "[X] "
                    } else {
                        "[ ] "
                    };
                    s = concat2(interp, Value::string(prefix), s, env)?;
                }
                // If we have room for the prompt string, add it to this
                // line.  If this is the first on the line, always add it.
                let desc_width = if char_matches {
                    0
                } else {
                    schars(&desc) as i64 + 3
                };
                if schars(&s) as i64 + i + 2 + desc_width < width || !notfirst {
                    // Punctuate between strings.
                    if notfirst {
                        menu_strings = Value::cons(Value::string(", "), menu_strings);
                        i += 2;
                    }
                    notfirst = true;
                    nobindings = false;
                    // If the char to type doesn't match the string's first
                    // char, explicitly show what char to type.
                    if !char_matches {
                        // Add as much of string as fits.
                        let thiswidth = (schars(&desc) as i64).min(width - i);
                        menu_strings = Value::cons(
                            super::call(
                                interp,
                                "substring",
                                &[desc, Value::Integer(0), Value::Integer(thiswidth)],
                                env,
                            )?,
                            menu_strings,
                        );
                        i += thiswidth;
                        menu_strings = Value::cons(Value::string(" = "), menu_strings);
                        i += 3;
                    }
                    // Add as much of string as fits.
                    let thiswidth = (schars(&s) as i64).min(width - i);
                    menu_strings = Value::cons(
                        super::call(
                            interp,
                            "substring",
                            &[s, Value::Integer(0), Value::Integer(thiswidth)],
                            env,
                        )?,
                        menu_strings,
                    );
                    i += thiswidth;
                } else {
                    // If this element does not fit, end the line now, and
                    // save the element for the next line.
                    menu_strings = Value::cons(Value::string("..."), menu_strings);
                    break;
                }
            }
            // Move past this element.
            match idx {
                Some(index) if index + 1 >= asize(vector) => {
                    // Handle reaching end of dense table.
                    idx = None;
                    rest = rest.cdr_if_list().unwrap_or(Value::Nil);
                }
                Some(index) => idx = Some(index + 1),
                None => rest = rest.cdr_if_list().unwrap_or(Value::Nil),
            }
        }
        // Prompt with that and read response: apply1 (Qconcat, Fnreverse
        // (menu_strings)), whose reversal also relinks PROMPT_STRINGS' own
        // conses, so a continuation line starts from the last line's
        // strings after ": ", as GNU's does.
        let reversed = super::call(interp, "nreverse", &[menu_strings], env)?;
        let message = super::call(interp, "apply", &[Value::symbol("concat"), reversed], env)?;
        super::dispatch::display::message3_nolog(interp, message, env)?;
        // Make believe it's not a keyboard macro in case the help char is
        // pressed.  Help characters are not recorded because menu
        // prompting is not used on replay.
        let orig_defn_macro = interp
            .lookup_var_key(cached_symbol!("defining-kbd-macro"), env)
            .unwrap_or(Value::Nil);
        interp.set_variable_key(cached_symbol!("defining-kbd-macro"), Value::Nil, env);
        // That read_char records the key (record_char, with no macro being
        // defined) and adds it to this-command-keys (add_command_key and
        // echo_update), where the read_char that prompts adds nothing.
        let obj = super::read_menu_prompt_char(interp, env, commandflag, quit_char_is_event)
            .and_then(|obj| {
                super::record_unrecorded_event(interp, &obj, env);
                super::add_read_event_to_command_keys(interp, obj, env).map(|()| obj)
            });
        interp.set_variable_key(cached_symbol!("defining-kbd-macro"), orig_defn_macro, env);
        let obj = obj?;
        let more = interp
            .lookup_var_key(cached_symbol!("menu-prompt-more-char"), env)
            .unwrap_or(Value::Nil);
        let is_more = crate::lisp::types::eq(obj, more)
            || more
                .as_integer()
                .is_ok_and(|more| obj.as_integer().is_ok_and(|obj| obj == more & 0o37));
        if !obj.is_integer() || obj.as_integer().is_ok_and(|obj| obj == -2) || !is_more {
            if !orig_defn_macro.is_nil() {
                super::call(interp, "store-kbd-macro-event", &[obj], env)?;
            }
            return Ok(obj);
        }
        // Help char - go round again.
    }
}

/// x_popup_menu_1's parse of MENU into menu_items and tty_menu_show's
/// tty_menu of it, for the unit tests.
#[cfg(test)]
pub(crate) fn tty_menu_of_menu(
    interp: &mut Interpreter,
    menu: Value,
    env: &mut Env,
) -> Result<TtyMenu, LispError> {
    let mut menuflags = 0;
    let parsed = parse_popup_menu(interp, menu, &mut menuflags, env);
    unuse_menu_items(interp);
    parsed?;
    build_tty_menu(interp, menuflags).map_err(|error| LispError::Signal(error.into()))
}

/// The names of menu_bar_items' items, in order, for the unit tests.
#[cfg(test)]
pub(crate) fn menu_bar_item_names(
    interp: &mut Interpreter,
    env: &mut Env,
) -> Result<Vec<String>, LispError> {
    let items = menu_bar_items(interp, Value::Nil, env)?;
    let mut names = Vec::new();
    let mut i = 0;
    while i + 1 < asize(items) && !aref(items, i + 1).is_nil() {
        names.push(super::string_text(&aref(items, i + 1))?);
        i += 4;
    }
    Ok(names)
}
