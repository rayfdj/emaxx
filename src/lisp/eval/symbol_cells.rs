//! Per-interpreter symbol cells (V02, V03 stage 1).
//!
//! GNU keeps a variable's value, its redirect (`SYMBOL_VARALIAS',
//! `SYMBOL_LOCALIZED') and `declared_special' in the `Lisp_Symbol' object
//! and reads them through the object, never through its name.  Emaxx's
//! symbol objects are shared by every interpreter on a thread (the test
//! image template is cloned), so the cells live in the interpreter, indexed
//! by the symbol's dense id: one bounds-checked index per read instead of
//! three name-keyed hash probes.  Name-keyed callers reach the same cell
//! through the interned table, so a name and its symbol can never address
//! different cells.
//!
//! The bound-value enumeration keeps first-binding order, exactly as the
//! insertion-ordered map it replaces did: a removed name that is bound
//! again enumerates last.

use super::super::types::{SymbolName, UNINTERNED_SYMBOL_ID_BIT, Value};
use crate::lisp::primitives::FnvBuildHasher;
use std::cell::Cell;
use std::collections::HashMap;

/// data.c: `SYMBOL_LOCALIZED' -- the value cell can forward through a
/// buffer-local binding.
pub(crate) const LOCALIZED: u8 = 1;
/// `declared_special'.
pub(crate) const SPECIAL: u8 = 2;
/// `blv->local_if_set': setting the variable makes it local
/// (`make-variable-buffer-local').
pub(crate) const LOCAL_IF_SET: u8 = 4;
/// A DEFVAR_PER_BUFFER slot (`BUFFER_OBJFWDP') with a positive
/// buffer_local_flags index: inherits the default until assigned locally.
pub(crate) const PER_BUFFER: u8 = 8;
/// A DEFVAR_PER_BUFFER slot whose index is -1: local in every buffer.
pub(crate) const ALWAYS_LOCAL: u8 = 16;
/// `SYMBOL_FORWARDED': a DEFVAR_* slot of the contracted oracle build,
/// until `makunbound' detaches it (data.c:set_internal makes it plain).
pub(crate) const FORWARDED: u8 = 32;
/// `Lisp_Fwd_Bool': a store keeps `!NILP (newval)'.
pub(crate) const FWD_BOOL: u8 = 64;
/// `Lisp_Fwd_Int': a store is CHECK_INTEGER plus an intmax_t range check.
pub(crate) const FWD_INT: u8 = 128;

#[derive(Clone, Default)]
struct SymbolCell {
    /// The symbol this cell belongs to, held once the cell is populated so
    /// enumeration can name it.
    symbol: Option<SymbolName>,
    value: Option<Value>,
    /// `SYMBOL_VARALIAS': the alias target.
    alias: Option<SymbolName>,
    flags: u8,
    /// Position in `order' while the value is bound.
    position: Option<u32>,
    /// `SYMBOL_VAL' as generated code sees it: the value's native word,
    /// stamped with the heap and collection generation that produced it
    /// (stamp 0 = none).  Every value, alias or flag write clears it, so the
    /// word is only ever the current plain value's.
    native: Cell<(u64, usize)>,
}

/// A cell's value, alias target and flags, copied out for the image writer.
#[derive(Clone, Debug)]
pub(crate) struct SymbolCellSnapshot {
    pub(crate) value: Option<Value>,
    pub(crate) alias: Option<SymbolName>,
    pub(crate) flags: u8,
}

#[derive(Clone, Default)]
pub(crate) struct SymbolCells {
    /// Interned symbols, indexed by id.
    cells: Vec<SymbolCell>,
    /// Uninterned symbols that acquired a cell, by id: rare, and their ids
    /// are sparse.
    uninterned: HashMap<u32, SymbolCell, FnvBuildHasher>,
    /// Ids in first-binding order; stale entries are skipped by comparing
    /// the cell's recorded position.
    order: Vec<u32>,
    bound: usize,
    /// Number of cells with a redirect, so alias-free interpreters skip
    /// resolution entirely.
    aliases: usize,
}

impl SymbolCells {
    pub(crate) fn from_bindings(entries: impl IntoIterator<Item = (String, Value)>) -> Self {
        let mut cells = Self::default();
        for (name, value) in entries {
            cells.insert(&SymbolName::intern_str(&name), value);
        }
        cells
    }

    fn cell(&self, id: u32) -> Option<&SymbolCell> {
        if id & UNINTERNED_SYMBOL_ID_BIT != 0 {
            self.uninterned.get(&id)
        } else {
            self.cells.get(id as usize)
        }
    }

    fn existing_cell_mut(&mut self, id: u32) -> Option<&mut SymbolCell> {
        if id & UNINTERNED_SYMBOL_ID_BIT != 0 {
            self.uninterned.get_mut(&id)
        } else {
            self.cells.get_mut(id as usize)
        }
    }

    fn cell_mut(&mut self, symbol: &SymbolName) -> &mut SymbolCell {
        let id = symbol.id();
        let cell = if id & UNINTERNED_SYMBOL_ID_BIT != 0 {
            self.uninterned.entry(id).or_default()
        } else {
            let index = id as usize;
            if index >= self.cells.len() {
                self.cells.resize_with(index + 1, SymbolCell::default);
            }
            &mut self.cells[index]
        };
        if cell.symbol.is_none() {
            cell.symbol = Some(symbol.clone());
        }
        cell
    }

    // --- value cell -----------------------------------------------------

    pub(crate) fn value(&self, symbol: &SymbolName) -> Option<&Value> {
        self.cell(symbol.id()).and_then(|cell| cell.value.as_ref())
    }

    pub(crate) fn value_by_name(&self, name: &str) -> Option<&Value> {
        let id = SymbolName::id_of(name)?;
        self.cell(id).and_then(|cell| cell.value.as_ref())
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn value_by_name_mut(&mut self, name: &str) -> Option<&mut Value> {
        let id = SymbolName::id_of(name)?;
        let cell = self.existing_cell_mut(id)?;
        cell.native.set((0, 0));
        cell.value.as_mut()
    }

    pub(crate) fn value_mut(&mut self, symbol: &SymbolName) -> Option<&mut Value> {
        let cell = self.existing_cell_mut(symbol.id())?;
        cell.native.set((0, 0));
        cell.value.as_mut()
    }

    // --- the value's native word ------------------------------------------

    /// The cached native word of SYMBOL's plain value, if it was produced
    /// under STAMP (a heap id and collection generation).
    pub(crate) fn native_word(&self, symbol: &SymbolName, stamp: u64) -> Option<usize> {
        let (cached_stamp, word) = self.cell(symbol.id())?.native.get();
        (cached_stamp == stamp && stamp != 0).then_some(word)
    }

    /// Record WORD as the native word of SYMBOL's current value under STAMP.
    /// A symbol without a cell has no value to cache for.
    pub(crate) fn set_native_word(&self, symbol: &SymbolName, stamp: u64, word: usize) {
        if let Some(cell) = self.cell(symbol.id())
            && cell.value.is_some()
        {
            cell.native.set((stamp, word));
        }
    }

    /// Number of cells holding a word produced under STAMP.
    #[cfg(test)]
    pub(crate) fn native_words_under(&self, stamp: u64) -> usize {
        self.cells
            .iter()
            .chain(self.uninterned.values())
            .filter(|cell| cell.native.get().0 == stamp)
            .count()
    }

    pub(crate) fn is_bound_name(&self, name: &str) -> bool {
        self.value_by_name(name).is_some()
    }

    /// Store VALUE; a first binding enumerates after every existing one.
    pub(crate) fn insert(&mut self, symbol: &SymbolName, value: Value) -> Option<Value> {
        let next_position = u32::try_from(self.order.len()).expect("symbol order index");
        let cell = self.cell_mut(symbol);
        cell.native.set((0, 0));
        let previous = cell.value.replace(value);
        if previous.is_none() {
            cell.position = Some(next_position);
            self.order.push(symbol.id());
            self.bound += 1;
            self.compact_if_sparse();
        }
        previous
    }

    pub(crate) fn insert_by_name(&mut self, name: &str, value: Value) -> Option<Value> {
        self.insert(&SymbolName::intern_str(name), value)
    }

    pub(crate) fn remove(&mut self, symbol: &SymbolName) -> Option<Value> {
        let cell = self.existing_cell_mut(symbol.id())?;
        cell.native.set((0, 0));
        let previous = cell.value.take();
        if previous.is_some() {
            cell.position = None;
            self.bound -= 1;
        }
        previous
    }

    pub(crate) fn remove_by_name(&mut self, name: &str) -> Option<Value> {
        let id = SymbolName::id_of(name)?;
        let cell = self.existing_cell_mut(id)?;
        cell.native.set((0, 0));
        let previous = cell.value.take();
        if previous.is_some() {
            cell.position = None;
            self.bound -= 1;
        }
        previous
    }

    fn compact_if_sparse(&mut self) {
        if self.order.len() < 1024 || self.order.len() < self.bound * 2 {
            return;
        }
        let mut order = Vec::with_capacity(self.bound);
        let stale = std::mem::take(&mut self.order);
        for (position, id) in stale.into_iter().enumerate() {
            let Some(cell) = self.existing_cell_mut(id) else {
                continue;
            };
            if cell.position == Some(position as u32) && cell.value.is_some() {
                cell.position = Some(u32::try_from(order.len()).expect("symbol order index"));
                order.push(id);
            }
        }
        self.order = order;
    }

    /// Bound (symbol, value) pairs in first-binding order.
    pub(crate) fn iter(&self) -> impl Iterator<Item = (&SymbolName, &Value)> {
        self.order
            .iter()
            .enumerate()
            .filter_map(move |(position, id)| {
                let cell = self.cell(*id)?;
                if cell.position != Some(position as u32) {
                    return None;
                }
                Some((cell.symbol.as_ref()?, cell.value.as_ref()?))
            })
    }

    pub(crate) fn values_mut(&mut self) -> impl Iterator<Item = &mut Value> {
        self.cells
            .iter_mut()
            .chain(self.uninterned.values_mut())
            .filter_map(|cell| cell.value.as_mut())
    }

    // --- redirect: alias -------------------------------------------------

    pub(crate) fn alias(&self, symbol: &SymbolName) -> Option<&SymbolName> {
        self.cell(symbol.id()).and_then(|cell| cell.alias.as_ref())
    }

    pub(crate) fn alias_by_name(&self, name: &str) -> Option<&SymbolName> {
        let id = SymbolName::id_of(name)?;
        self.cell(id).and_then(|cell| cell.alias.as_ref())
    }

    pub(crate) fn set_alias(&mut self, symbol: &SymbolName, target: SymbolName) {
        let cell = self.cell_mut(symbol);
        cell.native.set((0, 0));
        if cell.alias.replace(target).is_none() {
            self.aliases += 1;
        }
    }

    pub(crate) fn clear_alias_by_name(&mut self, name: &str) -> bool {
        let Some(id) = SymbolName::id_of(name) else {
            return false;
        };
        let Some(cell) = self.existing_cell_mut(id) else {
            return false;
        };
        cell.native.set((0, 0));
        let cleared = cell.alias.take().is_some();
        if cleared {
            self.aliases -= 1;
        }
        cleared
    }

    pub(crate) fn has_aliases(&self) -> bool {
        self.aliases != 0
    }

    // --- flags -------------------------------------------------------------

    /// The cell as the image writer records it (pdumper.c:dump_symbol
    /// reads the Lisp_Symbol fields).
    pub(crate) fn snapshot(&self, symbol: &SymbolName) -> SymbolCellSnapshot {
        match self.cell(symbol.id()) {
            Some(cell) => SymbolCellSnapshot {
                value: cell.value.clone(),
                alias: cell.alias.clone(),
                flags: cell.flags,
            },
            None => SymbolCellSnapshot {
                value: None,
                alias: None,
                flags: 0,
            },
        }
    }

    /// Install a cell as the image writer recorded it (the inverse of
    /// `snapshot'): the value keeps first-binding order, the alias and
    /// flags replace whatever the cell had.
    pub(crate) fn install_cell(&mut self, symbol: &SymbolName, snapshot: SymbolCellSnapshot) {
        match snapshot.value {
            Some(value) => {
                self.insert(symbol, value);
            }
            None => {
                self.remove_by_name(symbol.as_str());
            }
        }
        let had_alias = self.alias(symbol).is_some();
        let cell = self.cell_mut(symbol);
        cell.native.set((0, 0));
        cell.flags = snapshot.flags;
        let has_alias = snapshot.alias.is_some();
        cell.alias = snapshot.alias;
        match (had_alias, has_alias) {
            (false, true) => self.aliases += 1,
            (true, false) => self.aliases -= 1,
            _ => {}
        }
    }

    pub(crate) fn has_flag(&self, symbol: &SymbolName, flag: u8) -> bool {
        self.cell(symbol.id())
            .is_some_and(|cell| cell.flags & flag != 0)
    }

    pub(crate) fn has_flag_by_name(&self, name: &str, flag: u8) -> bool {
        SymbolName::id_of(name)
            .and_then(|id| self.cell(id))
            .is_some_and(|cell| cell.flags & flag != 0)
    }

    /// Set FLAG; true when it was not set before.
    pub(crate) fn set_flag_by_name(&mut self, name: &str, flag: u8) -> bool {
        let cell = self.cell_mut(&SymbolName::intern_str(name));
        cell.native.set((0, 0));
        let was_clear = cell.flags & flag == 0;
        cell.flags |= flag;
        was_clear
    }

    pub(crate) fn clear_flag_by_name(&mut self, name: &str, flag: u8) {
        if let Some(id) = SymbolName::id_of(name)
            && let Some(cell) = self.existing_cell_mut(id)
        {
            cell.native.set((0, 0));
            cell.flags &= !flag;
        }
    }
}

impl<'a> IntoIterator for &'a SymbolCells {
    type Item = (&'a SymbolName, &'a Value);
    type IntoIter = Box<dyn Iterator<Item = (&'a SymbolName, &'a Value)> + 'a>;

    fn into_iter(self) -> Self::IntoIter {
        Box::new(self.iter())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names(cells: &SymbolCells) -> Vec<String> {
        cells
            .iter()
            .map(|(name, _)| name.as_str().to_owned())
            .collect()
    }

    #[test]
    fn a_symbol_and_its_name_address_one_cell() {
        let mut cells = SymbolCells::default();
        let symbol = SymbolName::intern_str("symbol-cells-test-a");
        cells.insert_by_name("symbol-cells-test-a", Value::Integer(1));
        assert_eq!(cells.value(&symbol), Some(&Value::Integer(1)));
        cells.insert(&symbol, Value::Integer(2));
        assert_eq!(
            cells.value_by_name("symbol-cells-test-a"),
            Some(&Value::Integer(2))
        );
        assert!(cells.remove_by_name("symbol-cells-test-a").is_some());
        assert!(cells.value(&symbol).is_none());
        assert!(!cells.is_bound_name("symbol-cells-test-never-mentioned"));
    }

    #[test]
    fn a_live_uninterned_symbol_is_reached_through_its_private_name() {
        let mut cells = SymbolCells::default();
        let first = SymbolName::make_uninterned(Value::string("cell"), "cell", 41);
        let second = SymbolName::make_uninterned(Value::string("cell"), "cell", 42);
        cells.insert(&first, Value::Integer(1));
        assert_eq!(
            cells.value_by_name(first.as_str()),
            Some(&Value::Integer(1))
        );
        assert!(cells.value(&second).is_none());
        // `make-symbol' twice with one name makes two variables.
        assert_ne!(first.id(), second.id());
        assert!(SymbolName::intern_str(first.as_str()).id() == first.id());
    }

    #[test]
    fn bound_values_enumerate_in_first_binding_order_and_a_rebinding_moves_last() {
        let mut cells = SymbolCells::default();
        for name in [
            "symbol-cells-order-a",
            "symbol-cells-order-b",
            "symbol-cells-order-c",
        ] {
            cells.insert_by_name(name, Value::Nil);
        }
        cells.insert_by_name("symbol-cells-order-a", Value::T);
        assert_eq!(
            names(&cells),
            [
                "symbol-cells-order-a",
                "symbol-cells-order-b",
                "symbol-cells-order-c"
            ]
        );
        cells.remove_by_name("symbol-cells-order-b");
        cells.insert_by_name("symbol-cells-order-b", Value::T);
        assert_eq!(
            names(&cells),
            [
                "symbol-cells-order-a",
                "symbol-cells-order-c",
                "symbol-cells-order-b"
            ]
        );
        // Compaction after many removals keeps the same order.
        for index in 0..3000 {
            let name = format!("symbol-cells-order-fill-{index}");
            cells.insert_by_name(&name, Value::Nil);
            cells.remove_by_name(&name);
        }
        cells.insert_by_name("symbol-cells-order-d", Value::Nil);
        assert_eq!(
            names(&cells),
            [
                "symbol-cells-order-a",
                "symbol-cells-order-c",
                "symbol-cells-order-b",
                "symbol-cells-order-d"
            ]
        );
        assert!(cells.order.len() <= 1024);
    }

    #[test]
    fn a_cells_native_word_is_cleared_by_every_data_c_write_transition() {
        let mut cells = SymbolCells::default();
        let symbol = SymbolName::intern_str("symbol-cells-word");
        let stamp = (5u64 << 32) | 1;
        // No cell, no value: nothing to cache for.
        cells.set_native_word(&symbol, stamp, 0x10);
        assert_eq!(cells.native_word(&symbol, stamp), None);
        cells.insert(&symbol, Value::Integer(1));
        cells.set_native_word(&symbol, stamp, 0x10);
        assert_eq!(cells.native_word(&symbol, stamp), Some(0x10));
        assert_eq!(
            cells.native_word(&symbol, stamp + 1),
            None,
            "another generation"
        );
        assert_eq!(cells.native_word(&symbol, 0), None, "stamp 0 never matches");
        // set_internal: the word belongs to the previous value.
        cells.insert(&symbol, Value::Integer(2));
        assert_eq!(cells.native_word(&symbol, stamp), None);
        cells.set_native_word(&symbol, stamp, 0x20);
        *cells.value_by_name_mut("symbol-cells-word").expect("bound") = Value::Integer(3);
        assert_eq!(cells.native_word(&symbol, stamp), None);
        // A redirect or localization changes what a read means.
        for transition in [0u8, 1, 2, 3] {
            cells.set_native_word(&symbol, stamp, 0x30);
            assert_eq!(cells.native_word(&symbol, stamp), Some(0x30));
            match transition {
                0 => cells.set_alias(&symbol, SymbolName::intern_str("symbol-cells-word-base")),
                1 => {
                    cells.clear_alias_by_name("symbol-cells-word");
                }
                2 => {
                    cells.set_flag_by_name("symbol-cells-word", LOCALIZED);
                }
                _ => cells.clear_flag_by_name("symbol-cells-word", LOCALIZED),
            }
            assert_eq!(
                cells.native_word(&symbol, stamp),
                None,
                "transition {transition}"
            );
        }
        assert_eq!(cells.native_words_under(stamp), 0);
        cells.set_native_word(&symbol, stamp, 0x40);
        assert_eq!(cells.native_words_under(stamp), 1);
        // makunbound.
        cells.remove_by_name("symbol-cells-word");
        assert_eq!(cells.native_words_under(stamp), 0);
    }

    #[test]
    fn a_cloned_table_does_not_share_cells_and_flags_follow_the_symbol() {
        let mut cells = SymbolCells::default();
        let symbol = SymbolName::intern_str("symbol-cells-clone");
        cells.insert(&symbol, Value::Integer(1));
        assert!(cells.set_flag_by_name("symbol-cells-clone", SPECIAL));
        assert!(!cells.set_flag_by_name("symbol-cells-clone", SPECIAL));
        let mut clone = cells.clone();
        clone.insert(&symbol, Value::Integer(2));
        clone.clear_flag_by_name("symbol-cells-clone", SPECIAL);
        clone.set_alias(&symbol, SymbolName::intern_str("symbol-cells-clone-target"));
        assert_eq!(cells.value(&symbol), Some(&Value::Integer(1)));
        assert!(cells.has_flag(&symbol, SPECIAL));
        assert!(cells.alias(&symbol).is_none());
        assert!(!cells.has_aliases());
        assert!(!clone.has_flag(&symbol, LOCALIZED));
        assert!(clone.has_aliases());
        assert!(clone.clear_alias_by_name("symbol-cells-clone"));
        assert!(!clone.has_aliases());
    }
}
