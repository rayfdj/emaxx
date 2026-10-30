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
//! loaded binding (`LoadedBindings') can name it; a buffer's always-local
//! slots, struct buffer's own fields in GNU, are made with the buffer in
//! one block (`FixedSlots').  Making or killing one binding unloads its
//! symbol, as data.c swaps a variable's binding out when its loaded buffer
//! gains or loses one, and so does discarding a buffer's bindings, for
//! each of them (swap_out_buffer_local_variables); copying them, or
//! dropping them otherwise, advances the bindings' shape, which retires
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

/// How many always-local slots a buffer keeps with its bindings: the
/// buffer_local_flags index -1 fields of struct buffer that are not on the
/// buffer record itself.
pub(crate) const FIXED_SLOTS: usize = 14;

/// The always-local slots' bindings, struct buffer's fields: each value at
/// a place that holds while the buffer's bindings do, found by its symbol
/// among the slots' own (the variables flagged ALWAYS_LOCAL, a few).
struct FixedSlots {
    symbols: [SymbolName; FIXED_SLOTS],
    values: [Cell<Value>; FIXED_SLOTS],
}

#[derive(Default)]
pub(crate) struct LocalCells {
    /// A buffer's always-local slots, made with it (`with_fixed_slots');
    /// the bindings of a buffer made otherwise keep such a variable as any
    /// other.
    fixed: Option<Box<FixedSlots>>,
    /// Which fixed slots are bindings, by bit.
    present: u16,
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
            fixed: self.fixed.as_ref().map(|fixed| {
                Box::new(FixedSlots {
                    symbols: fixed.symbols,
                    values: fixed.values.clone(),
                })
            }),
            present: self.present,
            cells: self.cells.clone(),
            forwarded_eval: self.forwarded_eval,
        }
    }
}

impl Drop for LocalCells {
    fn drop(&mut self) {
        if !self.is_empty() {
            reshape();
        }
    }
}

impl LocalCells {
    /// The bindings a buffer is made with: its always-local slots, each
    /// SYMBOLS[i] bound to VALUES[i], none with an evaluator cell.
    pub(crate) fn with_fixed_slots(
        symbols: [SymbolName; FIXED_SLOTS],
        values: [Value; FIXED_SLOTS],
    ) -> Self {
        debug_assert!(
            symbols
                .iter()
                .all(|symbol| !super::variables::is_forwarded_eval_cell(symbol))
        );
        Self {
            fixed: Some(Box::new(FixedSlots {
                symbols,
                values: values.map(Cell::new),
            })),
            present: (1 << FIXED_SLOTS) - 1,
            cells: LinkedHashMap::default(),
            forwarded_eval: 0,
        }
    }

    /// SYMBOL's fixed slot, bound or not.
    #[inline]
    fn fixed_slot(&self, symbol: &SymbolName) -> Option<usize> {
        if symbol.cell().flags.get() & super::symbol_cell_flags::ALWAYS_LOCAL == 0 {
            return None;
        }
        let fixed = self.fixed.as_ref()?;
        let wanted = symbol.identity_ptr();
        fixed
            .symbols
            .iter()
            .position(|slot| slot.identity_ptr() == wanted)
    }

    /// The fixed slots that are bindings, in their order.
    /// Each fixed slot's value, None where it is not a binding, as a
    /// killed buffer keeps them.
    pub(crate) fn fixed_values(&self) -> [Option<Value>; FIXED_SLOTS] {
        let mut values = [None; FIXED_SLOTS];
        if let Some(fixed) = &self.fixed {
            for (slot, value) in fixed.values.iter().enumerate() {
                if self.present & (1 << slot) != 0 {
                    values[slot] = Some(value.get());
                }
            }
        }
        values
    }

    fn fixed_bindings(&self) -> impl Iterator<Item = (&SymbolName, &Cell<Value>)> {
        let present = self.present;
        self.fixed.iter().flat_map(move |fixed| {
            fixed
                .symbols
                .iter()
                .zip(&fixed.values)
                .enumerate()
                .filter(move |(slot, _)| present & (1 << slot) != 0)
                .map(|(_, binding)| binding)
        })
    }

    /// A local binding retains its actual symbol, as GNU's local_var_alist does.
    pub(crate) fn symbols(&self) -> impl Iterator<Item = &SymbolName> {
        self.fixed_bindings()
            .map(|(symbol, _)| symbol)
            .chain(self.cells.keys())
    }

    /// The binding for SYMBOL: `Some(None)' is a void local, `None' no local.
    pub(crate) fn binding(&self, symbol: &SymbolName) -> Option<Option<Value>> {
        self.binding_cell(symbol).map(|value| live(value.get()))
    }

    /// SYMBOL's binding here, void or not, to store into.
    pub(crate) fn binding_cell(&self, symbol: &SymbolName) -> Option<&Cell<Value>> {
        match self.fixed_slot(symbol) {
            Some(slot) => (self.present & (1 << slot) != 0)
                .then(|| &self.fixed.as_ref().expect("fixed slots").values[slot]),
            None => self.cells.get(symbol),
        }
    }

    pub(crate) fn binding_by_name(&self, name: &str) -> Option<Option<Value>> {
        let symbol = SymbolName::find(name)?;
        self.binding(&symbol)
    }

    /// Bind SYMBOL; a new binding enumerates after every existing one, and
    /// SYMBOL's binding loaded in LOADED (this buffer's default, perhaps)
    /// is unloaded.
    pub(crate) fn insert(&mut self, symbol: &SymbolName, value: Value, loaded: &LoadedBindings) {
        if let Some(slot) = self.fixed_slot(symbol) {
            if self.present & (1 << slot) == 0 {
                if super::variables::is_forwarded_eval_cell(symbol) {
                    self.forwarded_eval += 1;
                }
                loaded.unload(symbol);
                self.present |= 1 << slot;
            }
            self.fixed.as_ref().expect("fixed slots").values[slot].set(value);
            return;
        }
        match self.cells.get(symbol) {
            Some(existing) => existing.set(value),
            None => {
                if super::variables::is_forwarded_eval_cell(symbol) {
                    self.forwarded_eval += 1;
                }
                loaded.unload(symbol);
                // A buffer is made with its built-in bindings alone; one
                // gaining others (a major mode sets dozens) grows at once to
                // room for MODE_BINDINGS, not by doubling.
                if self.cells.len() == self.cells.capacity() {
                    let bindings = self.len();
                    self.cells.reserve(
                        MODE_BINDINGS
                            .saturating_sub(bindings)
                            .max(self.cells.len())
                            .max(1),
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
        let removed = match self.fixed_slot(&symbol) {
            Some(slot) => {
                if self.present & (1 << slot) == 0 {
                    return None;
                }
                self.present &= !(1 << slot);
                self.fixed.as_ref().expect("fixed slots").values[slot].get()
            }
            None => self.cells.remove(&symbol)?.get(),
        };
        loaded.unload(&symbol);
        if super::variables::is_forwarded_eval_cell(&symbol) {
            self.forwarded_eval -= 1;
        }
        Some(removed)
    }

    /// Drop these bindings, unloading each one's symbol in LOADED instead
    /// of retiring every loaded binding: a binding loaded for their buffer
    /// names one of them, or none (the buffer saw a default, which a later
    /// binding there unloads in turn), and a buffer's id is never another's.
    pub(crate) fn discard(mut self, loaded: &LoadedBindings) {
        for symbol in self.symbols() {
            loaded.unload(symbol);
        }
        self.present = 0;
        self.cells.clear();
    }

    /// Drop the bindings but the fixed slots (the local_var_alist that
    /// reset_buffer_local_variables resets: struct buffer's fields stay),
    /// unloading their symbols in LOADED as `discard' does.
    pub(crate) fn discard_cells(&mut self, loaded: &LoadedBindings) {
        for symbol in self.cells.keys() {
            loaded.unload(symbol);
        }
        self.cells.clear();
        self.forwarded_eval = self
            .fixed_bindings()
            .filter(|(symbol, _)| super::variables::is_forwarded_eval_cell(symbol))
            .count();
    }

    /// Whether a variable whose evaluator cell holds the current buffer's
    /// value has a binding here, void or not.
    pub(crate) fn has_forwarded_eval_binding(&self) -> bool {
        self.forwarded_eval != 0
    }

    fn len(&self) -> usize {
        self.present.count_ones() as usize + self.cells.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.present == 0 && self.cells.is_empty()
    }

    /// Bindings in first-binding order, a void local as `Value::Unbound'.
    pub(crate) fn iter(&self) -> impl Iterator<Item = (&SymbolName, Value)> {
        self.fixed_bindings()
            .chain(self.cells.iter())
            .map(|(symbol, value)| (symbol, value.get()))
    }

    #[cfg(test)]
    pub(crate) fn values_mut(&mut self) -> impl Iterator<Item = &mut Value> {
        let present = self.present;
        self.fixed
            .iter_mut()
            .flat_map(move |fixed| {
                fixed
                    .values
                    .iter_mut()
                    .enumerate()
                    .filter(move |(slot, _)| present & (1 << slot) != 0)
                    .map(|(_, value)| value)
            })
            .chain(self.cells.values_mut())
            .map(Cell::get_mut)
    }

    #[cfg(test)]
    pub(crate) fn map_symbols(&mut self, mut copy: impl FnMut(SymbolName) -> SymbolName) {
        reshape();
        if let Some(fixed) = self.fixed.as_mut() {
            fixed.symbols = fixed.symbols.map(&mut copy);
        }
        self.cells = std::mem::take(&mut self.cells)
            .into_iter()
            .map(|(symbol, value)| (copy(symbol), value))
            .collect();
        self.forwarded_eval = self
            .symbols()
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
