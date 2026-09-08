//! One buffer's local variable bindings (V04 stage 1).
//!
//! GNU keeps them as the buffer's `local_var_alist': a list of
//! `(symbol . value)' conses that `find_symbol_value' searches by the
//! symbol object (`assq_no_quit'), never by name, and a cell whose value is
//! `Qunbound' is still a binding (`local-variable-p' answers t and
//! `buffer-local-variables' lists the bare symbol).  Emaxx keys the table by
//! the symbol's id, keeps first-binding order, and stores `Value::Unbound'
//! for a void local.

use super::super::types::{SymbolName, Value};
use crate::lisp::primitives::FnvBuildHasher;
use hashlink::LinkedHashMap;

#[derive(Clone, Default)]
pub(crate) struct LocalCells {
    cells: LinkedHashMap<u32, (SymbolName, Value), FnvBuildHasher>,
}

impl LocalCells {
    /// The binding for SYMBOL: `Some(None)' is a void local, `None' no local.
    pub(crate) fn binding(&self, symbol: &SymbolName) -> Option<Option<&Value>> {
        self.cells
            .get(&symbol.id())
            .map(|(_, value)| (!matches!(value, Value::Unbound)).then_some(value))
    }

    pub(crate) fn binding_by_name(&self, name: &str) -> Option<Option<&Value>> {
        let id = SymbolName::id_of(name)?;
        self.cells
            .get(&id)
            .map(|(_, value)| (!matches!(value, Value::Unbound)).then_some(value))
    }

    /// Bind SYMBOL; a new binding enumerates after every existing one.
    pub(crate) fn insert(&mut self, symbol: &SymbolName, value: Value) {
        match self.cells.get_mut(&symbol.id()) {
            Some((_, existing)) => *existing = value,
            None => {
                self.cells.insert(symbol.id(), (symbol.clone(), value));
            }
        }
    }

    pub(crate) fn remove(&mut self, name: &str) -> Option<Value> {
        let id = SymbolName::id_of(name)?;
        self.cells.remove(&id).map(|(_, value)| value)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.cells.is_empty()
    }

    /// Bindings in first-binding order, a void local as `Value::Unbound'.
    pub(crate) fn iter(&self) -> impl Iterator<Item = (&SymbolName, &Value)> {
        self.cells.values().map(|(symbol, value)| (symbol, value))
    }

    pub(crate) fn values_mut(&mut self) -> impl Iterator<Item = &mut Value> {
        self.cells.values_mut().map(|(_, value)| value)
    }
}
