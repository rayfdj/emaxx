//! One buffer's local variable bindings (V04 stage 1).
//!
//! GNU keeps them as the buffer's `local_var_alist': a list of
//! `(symbol . value)' conses that `find_symbol_value' searches by the
//! symbol object (`assq_no_quit'), never by name, and a cell whose value is
//! `Qunbound' is still a binding (`local-variable-p' answers t and
//! `buffer-local-variables' lists the bare symbol).  Emaxx keys the table by
//! the actual symbol, keeps first-binding order, and stores `Value::Unbound'
//! for a void local.

use super::super::types::{SymbolName, Value};
use crate::lisp::primitives::FnvBuildHasher;
use crate::lisp::types::Kind;
use hashlink::LinkedHashMap;

#[derive(Clone, Default)]
pub(crate) struct LocalCells {
    cells: LinkedHashMap<SymbolName, Value, FnvBuildHasher>,
}

impl LocalCells {
    /// A local binding retains its actual symbol, as GNU's local_var_alist does.
    pub(crate) fn symbols(&self) -> impl Iterator<Item = &SymbolName> {
        self.cells.keys()
    }

    /// The binding for SYMBOL: `Some(None)' is a void local, `None' no local.
    pub(crate) fn binding(&self, symbol: &SymbolName) -> Option<Option<&Value>> {
        self.cells
            .get(symbol)
            .map(|value| (!matches!(value.kind(), Kind::Unbound)).then_some(value))
    }

    pub(crate) fn binding_by_name(&self, name: &str) -> Option<Option<&Value>> {
        let symbol = SymbolName::find(name)?;
        self.cells
            .get(&symbol)
            .map(|value| (!matches!(value.kind(), Kind::Unbound)).then_some(value))
    }

    /// Bind SYMBOL; a new binding enumerates after every existing one.
    pub(crate) fn insert(&mut self, symbol: &SymbolName, value: Value) {
        match self.cells.get_mut(symbol) {
            Some(existing) => *existing = value,
            None => {
                self.cells.insert(*symbol, value);
            }
        }
    }

    pub(crate) fn remove(&mut self, name: &str) -> Option<Value> {
        let symbol = SymbolName::find(name)?;
        self.cells.remove(&symbol)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.cells.is_empty()
    }

    /// Bindings in first-binding order, a void local as `Value::Unbound'.
    pub(crate) fn iter(&self) -> impl Iterator<Item = (&SymbolName, &Value)> {
        self.cells.iter()
    }

    #[cfg(test)]
    pub(crate) fn values_mut(&mut self) -> impl Iterator<Item = &mut Value> {
        self.cells.values_mut()
    }

    #[cfg(test)]
    pub(crate) fn map_symbols(&mut self, mut copy: impl FnMut(SymbolName) -> SymbolName) {
        self.cells = std::mem::take(&mut self.cells)
            .into_iter()
            .map(|(symbol, value)| (copy(symbol), value))
            .collect();
    }
}
