//! One buffer's local variable bindings (V04 stage 1).
//!
//! GNU keeps them as the buffer's `local_var_alist': a list of
//! `(symbol . value)' conses that `find_symbol_value' searches by the
//! symbol object (`assq_no_quit'), never by name, and a cell whose value is
//! `Qunbound' is still a binding (`local-variable-p' answers t and
//! `buffer-local-variables' lists the bare symbol).  Emaxx keys the table by
//! the actual symbol, keeps first-binding order, and stores `Value::Unbound'
//! for a void local.
//!
//! Each binding's value lives in its own node, at an address that holds
//! while the binding does (the alist cons of GNU's binding), so a symbol's
//! loaded binding (`LoadedBindings') can name it.  Making or killing one
//! binding unloads its symbol, as data.c swaps a variable's binding out
//! when its loaded buffer gains or loses one; dropping or copying a
//! buffer's bindings wholesale advances the bindings' shape, which retires
//! every binding loaded before it.

use super::super::types::{SymbolName, Value};
use crate::lisp::types::IdentityBuildHasher;
use crate::lisp::types::Kind;
use hashlink::LinkedHashMap;
use std::cell::Cell;
use std::sync::atomic::{AtomicU64, Ordering};

/// The shape of the buffers' local bindings: advanced whenever a buffer's
/// bindings are dropped or copied wholesale, so that a binding loaded
/// under an older shape names no node.
static SHAPE: AtomicU64 = AtomicU64::new(1);

#[inline(always)]
fn shape() -> u64 {
    SHAPE.load(Ordering::Relaxed)
}

fn reshape() {
    SHAPE.fetch_add(1, Ordering::Relaxed);
}

/// The bindings a buffer has room for once it gains any beyond the
/// built-in ones.
const MODE_BINDINGS: usize = 56;

#[derive(Default)]
pub(crate) struct LocalCells {
    cells: LinkedHashMap<SymbolName, Cell<Value>, IdentityBuildHasher>,
    /// How many of the bindings are of the variables whose evaluator cells
    /// hold the current buffer's value (`is_forwarded_eval_cell'), so that
    /// a buffer switch need not look each of them up.
    forwarded_eval: usize,
}

impl Clone for LocalCells {
    fn clone(&self) -> Self {
        reshape();
        Self {
            cells: self.cells.clone(),
            forwarded_eval: self.forwarded_eval,
        }
    }
}

impl Drop for LocalCells {
    fn drop(&mut self) {
        if !self.cells.is_empty() {
            reshape();
        }
    }
}

impl LocalCells {
    /// The bindings a new buffer is made with, room for CAPACITY of them.
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            cells: LinkedHashMap::with_capacity_and_hasher(capacity, Default::default()),
            forwarded_eval: 0,
        }
    }

    /// Bind SYMBOL in a buffer just made: no binding of the buffer can be
    /// loaded yet, SYMBOL has none here, and it has no evaluator cell.
    pub(crate) fn insert_fresh(&mut self, symbol: &SymbolName, value: Value) {
        debug_assert!(self.cells.get(symbol).is_none());
        debug_assert!(!super::variables::is_forwarded_eval_cell(symbol));
        self.cells.insert(*symbol, Cell::new(value));
    }

    /// A local binding retains its actual symbol, as GNU's local_var_alist does.
    pub(crate) fn symbols(&self) -> impl Iterator<Item = &SymbolName> {
        self.cells.keys()
    }

    /// The binding for SYMBOL: `Some(None)' is a void local, `None' no local.
    pub(crate) fn binding(&self, symbol: &SymbolName) -> Option<Option<Value>> {
        self.cells.get(symbol).map(|value| live(value.get()))
    }

    /// SYMBOL's binding here, void or not, to store into.
    pub(crate) fn binding_cell(&self, symbol: &SymbolName) -> Option<&Cell<Value>> {
        self.cells.get(symbol)
    }

    pub(crate) fn binding_by_name(&self, name: &str) -> Option<Option<Value>> {
        let symbol = SymbolName::find(name)?;
        self.binding(&symbol)
    }

    /// Bind SYMBOL; a new binding enumerates after every existing one, and
    /// SYMBOL's binding loaded in LOADED (this buffer's default, perhaps)
    /// is unloaded.
    pub(crate) fn insert(&mut self, symbol: &SymbolName, value: Value, loaded: &LoadedBindings) {
        match self.cells.get(symbol) {
            Some(existing) => existing.set(value),
            None => {
                if super::variables::is_forwarded_eval_cell(symbol) {
                    self.forwarded_eval += 1;
                }
                loaded.unload(symbol);
                // A buffer is made with room for its built-in bindings
                // alone; one gaining others (a major mode sets dozens)
                // grows at once to room for MODE_BINDINGS, not by
                // doubling.
                if self.cells.len() == self.cells.capacity() {
                    self.cells.reserve(
                        MODE_BINDINGS
                            .saturating_sub(self.cells.len())
                            .max(self.cells.len()),
                    );
                }
                self.cells.insert(*symbol, Cell::new(value));
            }
        }
    }

    /// Kill NAME's binding, unloading its symbol's binding in LOADED (the
    /// node it names is reused).
    pub(crate) fn remove(&mut self, name: &str, loaded: &LoadedBindings) -> Option<Value> {
        let symbol = SymbolName::find(name)?;
        let removed = self.cells.remove(&symbol)?;
        loaded.unload(&symbol);
        if super::variables::is_forwarded_eval_cell(&symbol) {
            self.forwarded_eval -= 1;
        }
        Some(removed.get())
    }

    /// Whether a variable whose evaluator cell holds the current buffer's
    /// value has a binding here, void or not.
    pub(crate) fn has_forwarded_eval_binding(&self) -> bool {
        self.forwarded_eval != 0
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.cells.is_empty()
    }

    /// Bindings in first-binding order, a void local as `Value::Unbound'.
    pub(crate) fn iter(&self) -> impl Iterator<Item = (&SymbolName, Value)> {
        self.cells
            .iter()
            .map(|(symbol, value)| (symbol, value.get()))
    }

    #[cfg(test)]
    pub(crate) fn values_mut(&mut self) -> impl Iterator<Item = &mut Value> {
        self.cells.values_mut().map(Cell::get_mut)
    }

    #[cfg(test)]
    pub(crate) fn map_symbols(&mut self, mut copy: impl FnMut(SymbolName) -> SymbolName) {
        reshape();
        self.cells = std::mem::take(&mut self.cells)
            .into_iter()
            .map(|(symbol, value)| (copy(symbol), value))
            .collect();
        self.forwarded_eval = self
            .cells
            .keys()
            .filter(|symbol| super::variables::is_forwarded_eval_cell(symbol))
            .count();
    }
}

/// A binding's value, none when it is void.
pub(crate) fn live(value: Value) -> Option<Value> {
    (!matches!(value.kind(), Kind::Unbound)).then_some(value)
}

/// How many symbols' bindings stay loaded at once.
const LOADED_BINDINGS: usize = 256;

/// data.c's `struct Lisp_Buffer_Local_Value' `where' and `valcell': the
/// binding of SYMBOL (its cell's address) loaded for BUFFER, under SHAPE.
/// VALUE names the binding's value, or is null when the buffer has none
/// and sees the default (`found' false).
#[derive(Clone, Copy)]
struct LoadedBinding {
    symbol: usize,
    buffer: u64,
    shape: u64,
    value: *const Cell<Value>,
}

const UNLOADED: LoadedBinding = LoadedBinding {
    symbol: 0,
    buffer: 0,
    shape: 0,
    value: std::ptr::null(),
};

/// The loaded bindings of an interpreter's buffer-local variables, kept
/// beside the symbols (which the interpreter states of a process share)
/// rather than in them: one per symbol slot, so that reading or storing
/// the binding a symbol last used in a buffer is a compare and a load, as
/// GNU's is while `where' is the current buffer.  Only plain buffer-local
/// variables whose bindings live in the buffers' cells load here (those
/// whose value a buffer or the evaluator holds take the full path), and
/// only a plain one's store is the binding's alone.
pub(crate) struct LoadedBindings(Box<[Cell<LoadedBinding>; LOADED_BINDINGS]>);

impl Default for LoadedBindings {
    fn default() -> Self {
        Self(Box::new([const { Cell::new(UNLOADED) }; LOADED_BINDINGS]))
    }
}

/// A copy of an interpreter loads nothing: its bindings are its own.
impl Clone for LoadedBindings {
    fn clone(&self) -> Self {
        Self::default()
    }
}

/// A loaded binding: the binding's value cell, or none when the buffer
/// sees the default.
pub(crate) type Loaded<'a> = Option<&'a Cell<Value>>;

impl LoadedBindings {
    #[inline(always)]
    fn slot(&self, symbol: &SymbolName) -> &Cell<LoadedBinding> {
        // Symbol cells are 64 bytes apart.
        &self.0[(symbol.identity_ptr() >> 6) % LOADED_BINDINGS]
    }

    /// SYMBOL's binding loaded for BUFFER, if it is still the one there.
    #[inline(always)]
    pub(crate) fn get(&self, symbol: &SymbolName, buffer: u64) -> Option<Loaded<'_>> {
        let loaded = self.slot(symbol).get();
        if loaded.symbol != symbol.identity_ptr()
            || loaded.buffer != buffer
            || loaded.shape != shape()
        {
            return None;
        }
        // SAFETY: no binding has been made, killed or copied, nor any
        // buffer's bindings dropped, since VALUE was loaded (the shape is
        // unchanged), so its node still holds SYMBOL's binding in BUFFER.
        Some(unsafe { loaded.value.as_ref() })
    }

    /// Forget SYMBOL's loaded binding, whatever its buffer.
    #[inline]
    pub(crate) fn unload(&self, symbol: &SymbolName) {
        self.slot(symbol).set(UNLOADED);
    }

    /// Load SYMBOL's binding in BUFFER: VALUE, or the default when none.
    #[inline]
    pub(crate) fn load(&self, symbol: &SymbolName, buffer: u64, value: Loaded<'_>) {
        self.slot(symbol).set(LoadedBinding {
            symbol: symbol.identity_ptr(),
            buffer,
            shape: shape(),
            value: value.map_or(std::ptr::null(), std::ptr::from_ref),
        });
    }
}
