//! Direct access to the fields of this editor's allocated symbols.
//!
//! GNU stores value/alias, function, plist and flags in Lisp_Symbol. The
//! obarray owns enumeration independently of field contents (lread.c:
//! map_obarray); a field store neither changes membership nor maintains a
//! parallel list. There is no dense-id metadata table or weak side-cell sweep.
//! Localized/forwarded storage and remaining name/id callers are still adapters.

use super::super::types::{SymbolName, Value};

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

/// A symbol's value, alias target and flags, copied for the image writer.
#[derive(Clone, Debug)]
pub(crate) struct SymbolCellSnapshot {
    pub(crate) value: Option<Value>,
    pub(crate) alias: Option<SymbolName>,
    pub(crate) flags: u8,
}

#[derive(Clone)]
pub(crate) struct SymbolCells {
    /// Canonical objects retained by this editor's obarray, including while parked.
    symbols: crate::lisp::types::symbols::SymbolOwner,
    /// This editor's Qsymbol reference for the debugger flag probe.
    edebug_entered: SymbolName,
}

impl Default for SymbolCells {
    fn default() -> Self {
        Self {
            symbols: Default::default(),
            edebug_entered: SymbolName::intern_str("edebug-entered"),
        }
    }
}

impl SymbolCells {
    pub(crate) fn from_bindings(entries: impl IntoIterator<Item = (String, Value)>) -> Self {
        let mut cells = Self::default();
        for (name, value) in entries {
            cells.insert(&SymbolName::intern_str(&name), value);
        }
        cells
    }

    pub(crate) fn mark_symbol_roots(&self, mark: &mut dyn FnMut(&Value)) {
        self.symbols.mark_roots(mark);
        mark(&Value::Symbol(self.edebug_entered));
    }

    pub(crate) fn names(&self) -> Vec<SymbolName> {
        self.symbols.names()
    }

    pub(crate) fn names_len(&self) -> usize {
        self.symbols.len()
    }

    pub(crate) fn find_name(&self, name: &str) -> Option<SymbolName> {
        self.symbols.find(name)
    }

    pub(crate) fn remove_name(&self, name: &str) -> Option<SymbolName> {
        self.symbols.remove(name)
    }

    #[cfg(test)]
    pub(crate) fn symbol_owner(&self) -> crate::lisp::types::symbols::SymbolOwner {
        self.symbols.clone()
    }

    /// The graph copier relocates the actual fields. Retain the new owner
    /// and its Qsymbol reference; no field indexes need reconstruction.
    #[cfg(test)]
    pub(crate) fn map_symbols(
        &mut self,
        owner: crate::lisp::types::symbols::SymbolOwner,
        mut copy: impl FnMut(SymbolName) -> SymbolName,
    ) {
        self.edebug_entered = copy(self.edebug_entered);
        self.symbols = owner;
    }

    pub(crate) fn value(&self, symbol: &SymbolName) -> Option<Value> {
        symbol.cell().value()
    }

    pub(crate) fn value_by_name(&self, name: &str) -> Option<Value> {
        SymbolName::find(name)?.cell().value()
    }

    /// A copied word and an explicit store avoid aliased Rust payload references.
    #[inline]
    pub(crate) fn replace_bound_value(&mut self, symbol: &SymbolName, value: Value) -> bool {
        symbol.cell().replace_bound_value(value)
    }

    #[cfg(test)]
    pub(crate) fn is_bound_name(&self, name: &str) -> bool {
        self.value_by_name(name).is_some()
    }

    pub(crate) fn insert(&mut self, symbol: &SymbolName, value: Value) -> Option<Value> {
        let previous = symbol.cell().value();
        symbol.cell().set_value(value);
        previous
    }

    pub(crate) fn insert_by_name(&mut self, name: &str, value: Value) -> Option<Value> {
        self.insert(&SymbolName::intern_str(name), value)
    }

    pub(crate) fn remove(&mut self, symbol: &SymbolName) -> Option<Value> {
        symbol.cell().take_value()
    }

    pub(crate) fn remove_by_name(&mut self, name: &str) -> Option<Value> {
        self.remove(&SymbolName::find(name)?)
    }

    #[cfg(test)]
    pub(crate) fn bound_len(&self) -> usize {
        self.iter().count()
    }

    /// Bound canonical symbols, in their owner's obarray order.
    pub(crate) fn iter(&self) -> impl Iterator<Item = (SymbolName, Value)> {
        self.names()
            .into_iter()
            .filter_map(|symbol| symbol.cell().value().map(|value| (symbol, value)))
    }

    pub(crate) fn function(&self, symbol: &SymbolName) -> Option<Value> {
        let function = symbol.cell().function.get();
        (!function.is_nil()).then_some(function)
    }

    pub(crate) fn set_function(&mut self, symbol: &SymbolName, function: Option<Value>) {
        let value = function.unwrap_or(Value::Nil);
        symbol.cell().function.set(value);
        if value.is_nil() {
            symbol.cell().set_function_definition(false);
        }
    }

    /// Preserve the startup adapter's distinction between defsubr and a Lisp
    /// installation in the existing state byte, without an id-table probe.
    pub(crate) fn set_function_definition(&mut self, symbol: &SymbolName, function: Option<Value>) {
        self.set_function(symbol, function);
        symbol
            .cell()
            .set_function_definition(self.function(symbol).is_some());
    }

    pub(crate) fn has_function_definition(&self, symbol: &SymbolName) -> bool {
        symbol.cell().has_function_definition()
    }

    pub(crate) fn function_definition_by_name(&self, name: &str) -> Option<Value> {
        let symbol = SymbolName::find(name)?;
        self.has_function_definition(&symbol)
            .then(|| self.function(&symbol))
            .flatten()
    }

    #[cfg(test)]
    pub(crate) fn function_definitions_len(&self) -> usize {
        self.function_definitions().count()
    }

    #[cfg(test)]
    pub(crate) fn function_definitions(&self) -> impl Iterator<Item = (SymbolName, Value)> + '_ {
        self.names().into_iter().filter_map(|symbol| {
            self.has_function_definition(&symbol)
                .then(|| self.function(&symbol).map(|function| (symbol, function)))
                .flatten()
        })
    }

    #[cfg(test)]
    pub(crate) fn function_values(&self) -> impl Iterator<Item = Value> + '_ {
        self.names()
            .into_iter()
            .filter_map(|symbol| self.function(&symbol))
    }

    pub(crate) fn plist(&self, symbol: &SymbolName) -> Value {
        symbol.cell().plist.get()
    }

    pub(crate) fn plist_by_name(&self, name: &str) -> Value {
        SymbolName::find(name).map_or(Value::Nil, |symbol| symbol.cell().plist.get())
    }

    pub(crate) fn set_plist(&mut self, symbol: &SymbolName, plist: Value) {
        symbol.cell().plist.set(plist);
    }

    #[cfg(test)]
    pub(crate) fn plists(&self) -> impl Iterator<Item = (SymbolName, Value)> {
        self.names().into_iter().filter_map(|symbol| {
            let plist = symbol.cell().plist.get();
            (!plist.is_nil()).then_some((symbol, plist))
        })
    }

    pub(crate) fn alias(&self, symbol: &SymbolName) -> Option<SymbolName> {
        symbol.cell().alias()
    }

    pub(crate) fn alias_by_name(&self, name: &str) -> Option<SymbolName> {
        SymbolName::find(name)?.cell().alias()
    }

    #[inline]
    pub(crate) fn plain_store(&self, symbol: &SymbolName) -> bool {
        symbol.cell().plain_store()
    }

    pub(crate) fn set_plain_store(&mut self, symbol: &SymbolName, plain: bool) {
        symbol.cell().set_plain_store(plain);
    }

    pub(crate) fn set_alias(&mut self, symbol: &SymbolName, target: SymbolName) {
        // eval.c:Fdefvaralias transferred any previous value to an unbound
        // target already. SET_SYMBOL_ALIAS replaces that value's only edge.
        symbol.cell().set_alias(target);
    }

    #[cfg(test)]
    pub(crate) fn clear_alias_by_name(&mut self, name: &str) -> bool {
        SymbolName::find(name).is_some_and(|symbol| self.clear_alias(&symbol))
    }

    fn clear_alias(&mut self, symbol: &SymbolName) -> bool {
        symbol.cell().clear_alias()
    }

    #[cfg(test)]
    pub(crate) fn aliases_len(&self) -> usize {
        self.aliases().count()
    }

    #[cfg(test)]
    pub(crate) fn aliases(&self) -> impl Iterator<Item = (SymbolName, SymbolName)> {
        self.names()
            .into_iter()
            .filter_map(|symbol| symbol.cell().alias().map(|target| (symbol, target)))
    }

    pub(crate) fn snapshot(&self, symbol: &SymbolName) -> SymbolCellSnapshot {
        SymbolCellSnapshot {
            value: symbol.cell().value(),
            alias: symbol.cell().alias(),
            flags: symbol.cell().flags.get(),
        }
    }

    /// Restore the allocated payload and redirect, independent of enumeration.
    pub(crate) fn install_cell(&mut self, symbol: &SymbolName, snapshot: SymbolCellSnapshot) {
        self.clear_alias(symbol);
        match snapshot.value {
            Some(value) => {
                self.insert(symbol, value);
            }
            None => {
                self.remove(symbol);
            }
        }
        symbol.cell().flags.set(snapshot.flags);
        symbol.cell().set_plain_store(false);
        if let Some(target) = snapshot.alias {
            self.set_alias(symbol, target);
        }
    }

    pub(crate) fn has_flag(&self, symbol: &SymbolName, flag: u8) -> bool {
        symbol.cell().flags.get() & flag != 0
    }

    pub(crate) fn edebug_is_special(&self) -> bool {
        self.has_flag(&self.edebug_entered, SPECIAL)
    }

    pub(crate) fn has_flag_by_name(&self, name: &str, flag: u8) -> bool {
        SymbolName::find(name).is_some_and(|symbol| self.has_flag(&symbol, flag))
    }

    pub(crate) fn set_flag(&mut self, symbol: &SymbolName, flag: u8) -> bool {
        symbol.cell().set_plain_store(false);
        let old = symbol.cell().flags.get();
        symbol.cell().flags.set(old | flag);
        old & flag == 0
    }

    pub(crate) fn set_flag_by_name(&mut self, name: &str, flag: u8) -> bool {
        self.set_flag(&SymbolName::intern_str(name), flag)
    }

    pub(crate) fn clear_flag_by_name(&mut self, name: &str, flag: u8) {
        if let Some(symbol) = SymbolName::find(name) {
            self.clear_flag(&symbol, flag);
        }
    }

    pub(crate) fn clear_flag(&mut self, symbol: &SymbolName, flag: u8) {
        symbol.cell().flags.set(symbol.cell().flags.get() & !flag);
    }
}

impl<'a> IntoIterator for &'a SymbolCells {
    type Item = (SymbolName, Value);
    type IntoIter = Box<dyn Iterator<Item = (SymbolName, Value)> + 'a>;

    fn into_iter(self) -> Self::IntoIter {
        Box::new(self.iter())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fresh_cells() -> SymbolCells {
        crate::lisp::types::symbols::SymbolOwner::fresh().select();
        SymbolCells::default()
    }

    fn names(cells: &SymbolCells) -> Vec<String> {
        cells
            .iter()
            .map(|(name, _)| name.as_str().to_owned())
            .collect()
    }

    #[test]
    fn alias_payload_replaces_the_value_and_traces_only_its_current_target() {
        let interp = crate::lisp::eval::Interpreter::new();
        let mut cells = fresh_cells();
        let symbol = SymbolName::intern_str("redirect-union-owner");
        let first = SymbolName::intern_str("redirect-union-first");
        let second = SymbolName::intern_str("redirect-union-second");
        let old_value = Value::vector([Value::Integer(19)]);
        let function = Value::vector([Value::Integer(23)]);
        let plist = Value::list([Value::symbol("payload"), Value::Integer(29)]);
        cells.insert(&symbol, old_value);
        cells.set_function_definition(&symbol, Some(function));
        assert!(cells.has_function_definition(&symbol));
        symbol.cell().set_c_slot(false);
        cells.set_plist(&symbol, plist);
        cells.set_plain_store(&symbol, true);
        cells.set_alias(&symbol, first);
        assert!(!cells.plain_store(&symbol));
        assert!(cells.has_function_definition(&symbol));
        assert!(!symbol.cell().has_c_slot());
        assert!(cells.snapshot(&symbol).value.is_none());
        assert_eq!(cells.snapshot(&symbol).alias, Some(first));
        assert_eq!(cells.bound_len(), 0);
        assert_eq!(cells.aliases_len(), 1);
        // Inspect the real traversal so neither an obsolete alias target
        // nor its old value remains an edge of this symbol.
        let (_, reached) = crate::lisp::eval::LispReachability::trace_order_for_test(
            &interp,
            Value::Symbol(symbol),
        );
        assert!(!reached.contains(&old_value));
        assert!(reached.contains(&Value::Symbol(first)));
        assert!(reached.contains(&function) && reached.contains(&plist));
        cells.set_alias(&symbol, second);
        assert_eq!(cells.aliases_len(), 1);
        assert_eq!(cells.aliases().next(), Some((symbol, second)));
        let (_, reached) = crate::lisp::eval::LispReachability::trace_order_for_test(
            &interp,
            Value::Symbol(symbol),
        );
        assert!(!reached.contains(&old_value) && !reached.contains(&Value::Symbol(first)));
        assert!(reached.contains(&Value::Symbol(second)));
        assert!(reached.contains(&function) && reached.contains(&plist));
        cells.install_cell(
            &symbol,
            SymbolCellSnapshot {
                value: Some(Value::Integer(31)),
                alias: None,
                flags: SPECIAL,
            },
        );
        assert_eq!(cells.value(&symbol), Some(Value::Integer(31)));
        assert!(cells.alias(&symbol).is_none());
        assert_eq!(cells.bound_len(), 1);
        assert_eq!(cells.aliases_len(), 0);
        assert_eq!(cells.function(&symbol), Some(function));
        assert!(cells.has_function_definition(&symbol));
        assert!(!symbol.cell().has_c_slot());
        assert_eq!(cells.plist(&symbol), plist);
        assert_eq!(
            std::mem::size_of_val(&symbol.cell().payload),
            std::mem::size_of::<Value>()
        );
    }

    #[test]
    fn compact_fields_preserve_nil_values_void_defaults_and_image_replacement() {
        // No per-symbol side metadata remains. The owner/Qsymbol handle is
        // per interpreter; the allocated header still includes its host-name adapter.
        assert_eq!(std::mem::size_of::<SymbolCells>(), 16);
        assert_eq!(std::mem::size_of::<crate::lisp::alloc::SymbolCell>(), 64);
        let mut cells = fresh_cells();
        let symbol = SymbolName::intern_str("compact-field-symbol");
        let target = SymbolName::intern_str("compact-field-target");
        assert!(cells.value(&symbol).is_none());
        cells.insert(&symbol, Value::Nil);
        assert_eq!(cells.value(&symbol), Some(Value::Nil));
        assert_eq!(cells.bound_len(), 1);
        cells.set_function_definition(&symbol, Some(Value::T));
        assert_eq!(cells.function(&symbol), Some(Value::T));
        cells.set_function_definition(&symbol, Some(Value::Nil));
        assert!(cells.function(&symbol).is_none());
        assert_eq!(cells.function_definitions_len(), 0);
        // The current localized-value adapter can install an explicitly
        // void default; keep it distinct from an absent binding until
        // that adapter is removed, including through the image snapshot.
        cells.insert(&symbol, Value::Unbound);
        let snapshot = cells.snapshot(&symbol);
        assert_eq!(snapshot.value, Some(Value::Unbound));
        let mut restored = SymbolCells::default();
        restored.install_cell(&symbol, snapshot);
        assert_eq!(restored.value(&symbol), Some(Value::Unbound));
        assert_eq!(restored.bound_len(), 1);
        restored.install_cell(
            &symbol,
            SymbolCellSnapshot {
                value: None,
                alias: Some(target),
                flags: SPECIAL,
            },
        );
        assert!(restored.value(&symbol).is_none());
        assert_eq!(restored.bound_len(), 0);
        assert_eq!(restored.alias(&symbol), Some(target));
        assert_eq!(restored.aliases_len(), 1);
        restored.install_cell(
            &symbol,
            SymbolCellSnapshot {
                value: Some(Value::Integer(71)),
                alias: None,
                flags: 0,
            },
        );
        assert_eq!(restored.value(&symbol), Some(Value::Integer(71)));
        assert_eq!(restored.bound_len(), 1);
        assert_eq!(restored.aliases_len(), 0);
        assert_eq!(restored.aliases().count(), 0);
        assert!(!restored.has_flag(&symbol, SPECIAL));
    }

    #[test]
    fn alias_enumeration_follows_one_target_through_retargeting_and_compaction() {
        let mut cells = fresh_cells();
        let symbols = [
            "alias-order-first",
            "alias-order-second",
            "alias-order-third",
        ]
        .map(SymbolName::intern_str);
        let first_target = SymbolName::intern_str("alias-order-target-a");
        let next_target = SymbolName::intern_str("alias-order-target-b");
        for symbol in &symbols {
            cells.set_alias(symbol, first_target);
        }
        cells.set_alias(&symbols[0], next_target);
        assert!(cells.clear_alias_by_name(symbols[1].as_str()));
        assert!(!cells.clear_alias_by_name(symbols[1].as_str()));
        cells.set_alias(&symbols[1], next_target);
        let transient = SymbolName::intern_str("alias-order-transient");
        let owner_count = cells.names_len();
        let order = cells
            .aliases()
            .map(|(symbol, _)| symbol)
            .collect::<Vec<_>>();
        for _ in 0..3000 {
            cells.set_alias(&transient, first_target);
            assert!(cells.clear_alias_by_name(transient.as_str()));
        }
        assert_eq!(cells.aliases_len(), 3);
        assert_eq!(cells.names_len(), owner_count);
        assert_eq!(
            cells
                .aliases()
                .map(|(symbol, _)| symbol)
                .collect::<Vec<_>>(),
            order
        );
        for (symbol, target) in [
            (symbols[0], next_target),
            (symbols[2], first_target),
            (symbols[1], next_target),
        ] {
            assert!(cells.aliases().any(|entry| entry == (symbol, target)));
        }
    }

    #[test]
    fn function_enumeration_survives_redefinition_voiding_and_compaction() {
        let mut cells = fresh_cells();
        let symbols = [
            "function-order-first",
            "function-order-second",
            "function-order-third",
        ]
        .map(SymbolName::intern_str);
        let direct = SymbolName::intern_str("function-order-direct");
        cells.set_function(&direct, Some(Value::Integer(71)));
        assert_eq!(cells.function_definitions_len(), 0);
        assert_eq!(cells.function(&direct), Some(Value::Integer(71)));
        for (index, symbol) in symbols.iter().enumerate() {
            cells.set_function_definition(symbol, Some(Value::Integer(index as i64)));
        }
        cells.set_function_definition(&symbols[0], Some(Value::Integer(17)));
        cells.set_function_definition(&symbols[1], Some(Value::Nil));
        assert!(!cells.has_function_definition(&symbols[1]));
        assert!(cells.function(&symbols[1]).is_none());
        cells.set_function_definition(&symbols[1], Some(Value::Integer(43)));
        let transient = SymbolName::intern_str("function-order-transient");
        let owner_count = cells.names_len();
        let order = cells
            .function_definitions()
            .map(|(symbol, _)| symbol)
            .collect::<Vec<_>>();
        for index in 0..3000 {
            cells.set_function_definition(&transient, Some(Value::Integer(index)));
            cells.set_function_definition(&transient, None);
        }
        assert_eq!(cells.function_definitions_len(), 3);
        assert_eq!(cells.names_len(), owner_count);
        assert_eq!(
            cells
                .function_definitions()
                .map(|(symbol, _)| symbol)
                .collect::<Vec<_>>(),
            order
        );
        for (symbol, value) in [
            (symbols[0], Value::Integer(17)),
            (symbols[2], Value::Integer(2)),
            (symbols[1], Value::Integer(43)),
        ] {
            assert!(
                cells
                    .function_definitions()
                    .any(|entry| entry == (symbol, value))
            );
        }
        // A direct void store also retires an enumeration entry. Repeating
        // it must not decrement the live count or revive an older slot.
        cells.set_function(&symbols[0], None);
        cells.set_function(&symbols[0], None);
        assert_eq!(cells.function_definitions_len(), 2);
        assert_eq!(cells.function_values().count(), 3);
        assert!(
            cells
                .function_definition_by_name(symbols[0].as_str())
                .is_none()
        );
    }

    #[test]
    fn a_symbol_and_its_name_address_one_cell() {
        let mut cells = fresh_cells();
        let symbol = SymbolName::intern_str("symbol-cells-test-a");
        cells.insert_by_name("symbol-cells-test-a", Value::Integer(1));
        assert_eq!(cells.value(&symbol), Some(Value::Integer(1)));
        cells.insert(&symbol, Value::Integer(2));
        assert_eq!(
            cells.value_by_name("symbol-cells-test-a"),
            Some(Value::Integer(2))
        );
        assert!(cells.remove_by_name("symbol-cells-test-a").is_some());
        assert!(cells.value(&symbol).is_none());
        assert!(!cells.is_bound_name("symbol-cells-test-never-mentioned"));
    }

    #[test]
    fn a_live_uninterned_symbol_is_reached_through_its_private_name() {
        let mut cells = fresh_cells();
        let first = SymbolName::make_uninterned(Value::string("cell"), "cell", 41);
        let second = SymbolName::make_uninterned(Value::string("cell"), "cell", 42);
        cells.insert(&first, Value::Integer(1));
        assert_eq!(cells.value_by_name(first.as_str()), Some(Value::Integer(1)));
        assert!(cells.value(&second).is_none());
        // `make-symbol' twice with one name makes two variables.
        assert_ne!(first, second);
        assert_eq!(SymbolName::intern_str(first.as_str()), first);
    }

    #[test]
    fn bound_values_enumerate_in_first_binding_order_and_a_rebinding_moves_last() {
        let mut cells = fresh_cells();
        for name in [
            "symbol-cells-order-a",
            "symbol-cells-order-b",
            "symbol-cells-order-c",
        ] {
            cells.insert_by_name(name, Value::Nil);
        }
        // Historical selector: GNU map_obarray order belongs to the obarray,
        // not to the first/most recent binding. No symbols are added here.
        let before = names(&cells);
        cells.insert_by_name("symbol-cells-order-a", Value::T);
        assert_eq!(names(&cells), before);
        cells.remove_by_name("symbol-cells-order-b");
        assert_eq!(cells.bound_len(), 2);
        cells.insert_by_name("symbol-cells-order-b", Value::T);
        assert_eq!(names(&cells), before);
        for index in 0..3000 {
            let name = format!("symbol-cells-order-fill-{index}");
            cells.insert_by_name(&name, Value::Nil);
            cells.remove_by_name(&name);
        }
        cells.insert_by_name("symbol-cells-order-d", Value::Nil);
        let mut members = names(&cells);
        members.sort();
        assert_eq!(
            members,
            [
                "symbol-cells-order-a",
                "symbol-cells-order-b",
                "symbol-cells-order-c",
                "symbol-cells-order-d",
            ]
        );
        assert_eq!(cells.value_by_name("symbol-cells-order-a"), Some(Value::T));
        assert_eq!(cells.value_by_name("symbol-cells-order-b"), Some(Value::T));
        assert_eq!(
            cells.value_by_name("symbol-cells-order-c"),
            Some(Value::Nil)
        );
        assert_eq!(
            cells.value_by_name("symbol-cells-order-d"),
            Some(Value::Nil)
        );
        assert_eq!(cells.bound_len(), 4);
    }

    #[test]
    fn a_cells_native_word_is_cleared_by_every_data_c_write_transition() {
        // Historical selector: no native-word cache remains. Every reader
        // sees the cell's one value through all data.c transitions.
        let mut cells = fresh_cells();
        let symbol = SymbolName::intern_str("symbol-cells-word");
        let target = SymbolName::intern_str("symbol-cells-word-base");
        assert!(cells.value(&symbol).is_none());
        cells.insert(&symbol, Value::Integer(1));
        assert_eq!(
            cells.value(&symbol).map(Value::word),
            Some(Value::Integer(1).word())
        );
        cells.insert(&symbol, Value::Integer(2));
        assert_eq!(
            cells.value(&symbol).map(Value::word),
            Some(Value::Integer(2).word())
        );
        assert_eq!(
            cells.insert_by_name("symbol-cells-word", Value::Integer(3)),
            Some(Value::Integer(2))
        );
        assert_eq!(cells.value(&symbol), Some(Value::Integer(3)));
        cells.set_alias(&symbol, target);
        assert_eq!(cells.alias(&symbol), Some(target));
        assert!(cells.value(&symbol).is_none());
        assert_eq!(cells.bound_len(), 0);
        cells.clear_alias_by_name("symbol-cells-word");
        assert!(cells.alias(&symbol).is_none());
        // GNU's SET_SYMBOL_ALIAS overwrites the previous value. The test-only
        // redirect removal leaves an unbound plain cell, never that old value.
        assert!(cells.value(&symbol).is_none());
        cells.insert(&symbol, Value::Integer(3));
        cells.set_flag_by_name("symbol-cells-word", LOCALIZED);
        assert!(cells.has_flag(&symbol, LOCALIZED));
        cells.clear_flag_by_name("symbol-cells-word", LOCALIZED);
        assert!(!cells.has_flag(&symbol, LOCALIZED));
        assert_eq!(cells.value(&symbol), Some(Value::Integer(3)));
        cells.remove_by_name("symbol-cells-word");
        assert!(cells.value(&symbol).is_none());
    }

    #[test]
    fn a_cloned_table_does_not_share_cells_and_flags_follow_the_symbol() {
        let mut source = crate::lisp::eval::Interpreter::new();
        let symbol = SymbolName::intern_str("symbol-cells-clone");
        source.globals.insert(&symbol, Value::Integer(1));
        assert!(
            source
                .globals
                .set_flag_by_name("symbol-cells-clone", SPECIAL)
        );
        assert!(
            !source
                .globals
                .set_flag_by_name("symbol-cells-clone", SPECIAL)
        );
        // Copy the owning image, including its symbol objects. Cloning only
        // the enumeration adapter cannot create independent Lisp fields.
        let mut image = source.deep_clone_image();
        image.select_symbol_owner();
        let copied = SymbolName::intern_str("symbol-cells-clone");
        assert_ne!(symbol, copied);
        let cells = &source.globals;
        let clone = &mut image.globals;
        clone.insert(&copied, Value::Integer(2));
        clone.clear_flag_by_name("symbol-cells-clone", SPECIAL);
        clone.set_alias(&copied, SymbolName::intern_str("symbol-cells-clone-target"));
        assert_eq!(cells.value(&symbol), Some(Value::Integer(1)));
        assert!(cells.has_flag(&symbol, SPECIAL));
        assert!(cells.alias(&symbol).is_none());
        assert_eq!(cells.aliases_len(), 0);
        assert!(!clone.has_flag(&copied, LOCALIZED));
        assert_eq!(clone.aliases_len(), 1);
        assert!(clone.clear_alias_by_name("symbol-cells-clone"));
        assert_eq!(clone.aliases_len(), 0);
    }
}
