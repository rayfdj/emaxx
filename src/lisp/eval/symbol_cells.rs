//! Per-interpreter symbol cells (V02, V03 stage 1).
//!
//! GNU keeps a variable's value, its redirect (`SYMBOL_VARALIAS',
//! `SYMBOL_LOCALIZED') and `declared_special' in the `Lisp_Symbol' object
//! and reads them through the object, never through its name.  Emaxx's
//! symbol objects are shared by every interpreter in the process (the test
//! image template is cloned), so the cells live in the interpreter, indexed
//! by the symbol's dense id: one bounds-checked index per read instead of
//! three name-keyed hash probes.  Name-keyed callers reach the same cell
//! through the interned table, so a name and its symbol can never address
//! different cells.
//! Property lists use that same payload, without a second name-keyed store or
//! position cache. Uninterned cells are weak side entries: GC follows their
//! fields when the symbol is reached and retires entries before symbol sweep.
//! Per-instance allocated symbols remain necessary to remove this side table
//! and its GC-only fixed-point walk while preserving interpreter isolation.
//!
//! The bound-value enumeration keeps first-binding order, exactly as the
//! insertion-ordered map it replaces did: a removed name that is bound
//! again enumerates last.

use super::super::types::{SymbolName, UNINTERNED_SYMBOL_ID_BIT, Value};
use crate::lisp::primitives::FnvBuildHasher;
use std::collections::HashMap;
use std::num::NonZeroU32;

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

#[derive(Clone)]
struct SymbolCell {
    /// The symbol this cell belongs to, held once the cell is populated so
    /// enumeration can name it.
    symbol: Option<SymbolName>,
    /// lisp.h's `val': one word holds either the value or the alias target.
    /// `alias_position' selects the alias member; otherwise the value member
    /// is initialized, including Qunbound in an absent slot. `position'
    /// distinguishes an explicitly void default in the localized adapter.
    payload: SymbolPayload,
    /// lisp.h's `u.s.function': the function cell, read by id as
    /// eval_sub reads `XSYMBOL (fun)->u.s.function'. Lookup, tracing and
    /// image copying all read this one payload.
    function: Value,
    /// data.c: the same symbol cell owns the live property list.
    plist: Value,
    plist_position: Option<NonZeroU32>,
    /// One-based position in `function_order' for a Lisp-installed
    /// definition. Static defsubr installation has no such entry.
    function_position: Option<NonZeroU32>,
    /// Alias enumeration order and the redirect discriminator. The target
    /// exists solely in `payload', replacing the former value as in GNU.
    alias_position: Option<NonZeroU32>,
    flags: u8,
    /// Position in `order' while the value is bound.
    position: Option<NonZeroU32>,
    /// data.c's SYMBOL_PLAINVAL with `trapped_write == SYMBOL_UNTRAPPED_WRITE'
    /// and no dedicated store behind the name: an assignment or a dynamic
    /// binding is a store into `value' and nothing else.  Learned by the
    /// first full assignment; cleared by whatever could change the answer
    /// (an alias, a flag, a watcher).
    plain_store: bool,
}

/// The two Copy members of Lisp_Symbol's value/redirect union. Access is
/// confined to SymbolCell: a present alias_position means `alias'; every
/// other state initializes `value'. A value position never coexists with
/// an alias position. No borrowed reference survives a mutable cell access.
#[derive(Clone, Copy)]
union SymbolPayload {
    value: Value,
    alias: SymbolName,
}

impl Default for SymbolCell {
    fn default() -> Self {
        Self {
            symbol: None,
            payload: SymbolPayload {
                value: Value::Unbound,
            },
            function: Value::Nil,
            plist: Value::Nil,
            plist_position: None,
            function_position: None,
            alias_position: None,
            flags: 0,
            position: None,
            plain_store: false,
        }
    }
}

impl SymbolCell {
    fn value(&self) -> Option<&Value> {
        // SAFETY: a value position is installed only for the value member;
        // set_alias retires it before replacing the payload with a target.
        self.position.map(|_| unsafe { &self.payload.value })
    }

    fn value_mut(&mut self) -> Option<&mut Value> {
        // SAFETY: as in value; the exclusive cell borrow prevents a redirect
        // transition while this reference is in use.
        self.position.map(|_| unsafe { &mut self.payload.value })
    }

    fn take_value(&mut self) -> Option<Value> {
        self.position.take().map(|_| {
            // SAFETY: the former value position selects this union member.
            unsafe { std::mem::replace(&mut self.payload.value, Value::Unbound) }
        })
    }

    fn alias(&self) -> Option<&SymbolName> {
        // SAFETY: only set_alias installs an alias position, together with
        // the target. clear_alias initializes the value member again.
        self.alias_position.map(|_| unsafe { &self.payload.alias })
    }

    fn function(&self) -> Option<&Value> {
        (self.function.word() != Value::Nil.word()).then_some(&self.function)
    }

    #[cfg(test)]
    fn function_mut(&mut self) -> Option<&mut Value> {
        (self.function.word() != Value::Nil.word()).then_some(&mut self.function)
    }

    fn roots(&self) -> impl Iterator<Item = Value> + '_ {
        self.value()
            .copied()
            .into_iter()
            .chain(self.function().copied())
            .chain([self.plist])
            .chain(self.alias().copied().map(Value::Symbol))
    }
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
    /// Enumeration metadata only: Lisp-installed functions keep the
    /// previous first-definition order without copying names or values.
    /// Remove this adapter when the obarray owns enumeration directly.
    function_order: Vec<u32>,
    defined_functions: usize,
    /// Enumeration metadata only; no duplicate plist or symbol-name payload.
    plist_order: Vec<u32>,
    plists: usize,
    /// Number of cells with a redirect, so alias-free interpreters skip
    /// resolution entirely.
    aliases: usize,
    /// Temporary ordered enumeration, like values/functions/plists. No
    /// name or target copies; remove when the obarray owns enumeration.
    alias_order: Vec<u32>,
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
            cell.symbol = Some(*symbol);
        }
        cell
    }

    // --- value cell -----------------------------------------------------

    pub(crate) fn value(&self, symbol: &SymbolName) -> Option<&Value> {
        self.cell(symbol.id()).and_then(SymbolCell::value)
    }

    pub(crate) fn value_by_name(&self, name: &str) -> Option<&Value> {
        let id = SymbolName::id_of(name)?;
        self.cell(id).and_then(SymbolCell::value)
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn value_by_name_mut(&mut self, name: &str) -> Option<&mut Value> {
        let id = SymbolName::id_of(name)?;
        let cell = self.existing_cell_mut(id)?;
        cell.value_mut()
    }

    pub(crate) fn value_mut(&mut self, symbol: &SymbolName) -> Option<&mut Value> {
        let cell = self.existing_cell_mut(symbol.id())?;
        cell.value_mut()
    }

    pub(crate) fn is_bound_name(&self, name: &str) -> bool {
        self.value_by_name(name).is_some()
    }

    /// Store VALUE; a first binding enumerates after every existing one.
    pub(crate) fn insert(&mut self, symbol: &SymbolName, value: Value) -> Option<Value> {
        let next_position =
            NonZeroU32::new(u32::try_from(self.order.len() + 1).expect("symbol order index"));
        let cell = self.cell_mut(symbol);
        // Callers resolve aliases before a plain store. The image installer
        // clears the old redirect explicitly before restoring a value.
        assert!(
            cell.alias_position.is_none(),
            "plain store into a symbol alias"
        );
        let previous = cell.value().copied();
        cell.payload = SymbolPayload { value };
        if previous.is_none() {
            cell.position = next_position;
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
        let previous = cell.take_value();
        if previous.is_some() {
            self.bound -= 1;
        }
        previous
    }

    pub(crate) fn remove_by_name(&mut self, name: &str) -> Option<Value> {
        let id = SymbolName::id_of(name)?;
        let cell = self.existing_cell_mut(id)?;
        let previous = cell.take_value();
        if previous.is_some() {
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
            if cell
                .position
                .is_some_and(|slot| slot.get() as usize == position + 1)
            {
                cell.position =
                    NonZeroU32::new(u32::try_from(order.len() + 1).expect("symbol order index"));
                order.push(id);
            }
        }
        self.order = order;
    }

    /// How many cells hold a value.
    pub(crate) fn bound_len(&self) -> usize {
        self.bound
    }

    /// Bound (symbol, value) pairs in first-binding order.
    pub(crate) fn iter(&self) -> impl Iterator<Item = (&SymbolName, &Value)> {
        self.order
            .iter()
            .enumerate()
            .filter_map(move |(position, id)| {
                let cell = self.cell(*id)?;
                if cell.position?.get() as usize != position + 1 {
                    return None;
                }
                Some((cell.symbol.as_ref()?, cell.value()?))
            })
    }

    /// Interned symbols are rooted by the process obarray. Uninterned cells
    /// are edges from their symbol, never independent roots of that symbol.
    pub(crate) fn permanent_roots(&self) -> impl Iterator<Item = Value> + '_ {
        self.cells
            .iter()
            .filter(|cell| cell.symbol.is_some())
            .flat_map(SymbolCell::roots)
    }

    pub(crate) fn reached_symbol_roots(&self, epoch: u32) -> impl Iterator<Item = Value> + '_ {
        self.uninterned
            .values()
            .filter(move |cell| {
                cell.symbol
                    .is_some_and(|symbol| symbol.mark_bit().is_marked(epoch))
            })
            .flat_map(SymbolCell::roots)
    }

    /// Remove weak side cells before the symbol allocator releases their
    /// addresses. Neither their fields nor a self-cycle can retain the owner.
    pub(crate) fn sweep_uninterned(&mut self, epoch: u32) -> bool {
        let before = self.uninterned.len();
        self.uninterned.retain(|_, cell| {
            if cell
                .symbol
                .is_some_and(|symbol| symbol.mark_bit().is_marked(epoch))
            {
                return true;
            }
            self.bound -= usize::from(cell.position.is_some());
            self.defined_functions -= usize::from(cell.function_position.is_some());
            self.plists -= usize::from(cell.plist_position.is_some());
            self.aliases -= usize::from(cell.alias_position.is_some());
            false
        });
        before != self.uninterned.len()
    }

    #[cfg(test)]
    pub(crate) fn values_mut(&mut self) -> impl Iterator<Item = &mut Value> {
        self.cells
            .iter_mut()
            .chain(self.uninterned.values_mut())
            .filter_map(SymbolCell::value_mut)
    }

    // --- the function cell --------------------------------------------

    /// The symbol's function cell, by id.
    pub(crate) fn function(&self, symbol: &SymbolName) -> Option<&Value> {
        self.cell(symbol.id()).and_then(SymbolCell::function)
    }

    pub(crate) fn function_by_name(&self, name: &str) -> Option<&Value> {
        let id = SymbolName::id_of(name)?;
        self.cell(id)?.function()
    }

    /// Write (or void) the symbol's function cell.
    pub(crate) fn set_function(&mut self, symbol: &SymbolName, function: Option<Value>) {
        match function.filter(|value| !value.is_nil()) {
            Some(function) => self.cell_mut(symbol).function = function,
            None => {
                if let Some(cell) = self.existing_cell_mut(symbol.id()) {
                    cell.function = Value::Nil;
                    if cell.function_position.take().is_some() {
                        self.defined_functions -= 1;
                    }
                }
            }
        }
    }

    /// A Lisp definition uses the same cell as defsubr. The only extra
    /// state records enumeration order, not another function payload.
    pub(crate) fn set_function_definition(&mut self, symbol: &SymbolName, function: Option<Value>) {
        self.set_function(symbol, function);
        let next = self.function_order.len() + 1;
        let Some(cell) = self.existing_cell_mut(symbol.id()) else {
            return;
        };
        if cell.function().is_some() && cell.function_position.is_none() {
            cell.function_position =
                NonZeroU32::new(u32::try_from(next).expect("function order index"));
            self.function_order.push(symbol.id());
            self.defined_functions += 1;
            self.compact_function_order_if_sparse();
        }
    }

    pub(crate) fn has_function_definition(&self, symbol: &SymbolName) -> bool {
        self.cell(symbol.id())
            .is_some_and(|cell| cell.function_position.is_some())
    }

    pub(crate) fn function_definition_by_name(&self, name: &str) -> Option<&Value> {
        let cell = self.cell(SymbolName::id_of(name)?)?;
        cell.function_position?;
        cell.function()
    }

    pub(crate) fn function_definitions_len(&self) -> usize {
        self.defined_functions
    }

    pub(crate) fn function_definitions(&self) -> impl Iterator<Item = (&SymbolName, &Value)> {
        self.function_order
            .iter()
            .enumerate()
            .filter_map(move |(position, id)| {
                let cell = self.cell(*id)?;
                if cell.function_position?.get() as usize != position + 1 {
                    return None;
                }
                Some((cell.symbol.as_ref()?, cell.function()?))
            })
    }

    fn compact_function_order_if_sparse(&mut self) {
        if self.function_order.len() < 1024
            || self.function_order.len() < self.defined_functions * 2
        {
            return;
        }
        let mut order = Vec::with_capacity(self.defined_functions);
        for (position, id) in std::mem::take(&mut self.function_order)
            .into_iter()
            .enumerate()
        {
            let Some(cell) = self.existing_cell_mut(id) else {
                continue;
            };
            if cell
                .function_position
                .is_some_and(|slot| slot.get() as usize == position + 1)
            {
                cell.function_position =
                    NonZeroU32::new(u32::try_from(order.len() + 1).expect("function order index"));
                order.push(id);
            }
        }
        self.function_order = order;
    }

    /// Trace all actual function cells, including direct defsubr/image
    /// stores that do not participate in Lisp-definition enumeration.
    #[cfg(test)]
    pub(crate) fn function_values(&self) -> impl Iterator<Item = &Value> {
        self.cells
            .iter()
            .chain(self.uninterned.values())
            .filter_map(SymbolCell::function)
    }

    /// Every function cell, for the image copier.
    #[cfg(test)]
    pub(crate) fn functions_mut(&mut self) -> impl Iterator<Item = &mut Value> {
        self.cells
            .iter_mut()
            .chain(self.uninterned.values_mut())
            .filter_map(SymbolCell::function_mut)
    }

    // --- property list ---------------------------------------------------

    pub(crate) fn plist(&self, symbol: &SymbolName) -> Value {
        self.cell(symbol.id()).map_or(Value::Nil, |cell| cell.plist)
    }

    pub(crate) fn plist_by_name(&self, name: &str) -> Value {
        SymbolName::id_of(name)
            .and_then(|id| self.cell(id))
            .map_or(Value::Nil, |cell| cell.plist)
    }

    pub(crate) fn set_plist(&mut self, symbol: &SymbolName, plist: Value) {
        if plist.is_nil() {
            if let Some(cell) = self.existing_cell_mut(symbol.id()) {
                cell.plist = plist;
                if cell.plist_position.take().is_some() {
                    self.plists -= 1;
                }
            }
            return;
        }
        let next = self.plist_order.len() + 1;
        let cell = self.cell_mut(symbol);
        cell.plist = plist;
        if cell.plist_position.is_none() {
            cell.plist_position = NonZeroU32::new(u32::try_from(next).expect("plist order index"));
            self.plist_order.push(symbol.id());
            self.plists += 1;
            self.compact_plist_order_if_sparse();
        }
    }

    pub(crate) fn plists_len(&self) -> usize {
        self.plists
    }

    pub(crate) fn plists(&self) -> impl Iterator<Item = (&SymbolName, &Value)> {
        self.plist_order
            .iter()
            .enumerate()
            .filter_map(move |(position, id)| {
                let cell = self.cell(*id)?;
                if cell.plist_position?.get() as usize != position + 1 {
                    return None;
                }
                Some((cell.symbol.as_ref()?, &cell.plist))
            })
    }

    fn compact_plist_order_if_sparse(&mut self) {
        if self.plist_order.len() < 1024 || self.plist_order.len() < self.plists * 2 {
            return;
        }
        let mut order = Vec::with_capacity(self.plists);
        for (position, id) in std::mem::take(&mut self.plist_order)
            .into_iter()
            .enumerate()
        {
            if let Some(cell) = self.existing_cell_mut(id)
                && cell
                    .plist_position
                    .is_some_and(|slot| slot.get() as usize == position + 1)
            {
                cell.plist_position =
                    NonZeroU32::new(u32::try_from(order.len() + 1).expect("plist order index"));
                order.push(id);
            }
        }
        self.plist_order = order;
    }

    #[cfg(test)]
    pub(crate) fn plists_mut(&mut self) -> impl Iterator<Item = &mut Value> {
        self.cells
            .iter_mut()
            .chain(self.uninterned.values_mut())
            .filter(|cell| cell.plist_position.is_some())
            .map(|cell| &mut cell.plist)
    }

    // --- redirect: alias -------------------------------------------------

    pub(crate) fn alias(&self, symbol: &SymbolName) -> Option<&SymbolName> {
        self.cell(symbol.id()).and_then(SymbolCell::alias)
    }

    pub(crate) fn alias_by_name(&self, name: &str) -> Option<&SymbolName> {
        let id = SymbolName::id_of(name)?;
        self.cell(id).and_then(SymbolCell::alias)
    }

    // --- the plain-store bit -------------------------------------------------

    #[inline]
    pub(crate) fn plain_store(&self, symbol: &SymbolName) -> bool {
        self.cell(symbol.id()).is_some_and(|cell| cell.plain_store)
    }

    pub(crate) fn set_plain_store(&mut self, symbol: &SymbolName, plain: bool) {
        if let Some(cell) = self.existing_cell_mut(symbol.id()) {
            cell.plain_store = plain;
        }
    }

    pub(crate) fn set_alias(&mut self, symbol: &SymbolName, target: SymbolName) {
        // eval.c:Fdefvaralias has already handed a previous value to an
        // unbound target when required. SET_SYMBOL_ALIAS overwrites the old
        // value; it is not a hidden binding or an extra GC edge thereafter.
        let next = self.alias_order.len() + 1;
        let cell = self.cell_mut(symbol);
        let had_value = cell.take_value().is_some();
        let first_alias = cell.alias_position.is_none();
        cell.plain_store = false;
        cell.payload = SymbolPayload { alias: target };
        if first_alias {
            cell.alias_position = NonZeroU32::new(u32::try_from(next).expect("alias order index"));
        }
        self.bound -= usize::from(had_value);
        if first_alias {
            self.alias_order.push(symbol.id());
            self.aliases += 1;
            self.compact_alias_order_if_sparse();
        }
    }

    #[cfg(test)]
    pub(crate) fn clear_alias_by_name(&mut self, name: &str) -> bool {
        let Some(id) = SymbolName::id_of(name) else {
            return false;
        };
        self.clear_alias(id)
    }

    fn clear_alias(&mut self, id: u32) -> bool {
        let Some(cell) = self.existing_cell_mut(id) else {
            return false;
        };
        let cleared = cell.alias_position.take().is_some();
        if cleared {
            cell.payload = SymbolPayload {
                value: Value::Unbound,
            };
            self.aliases -= 1;
        }
        cleared
    }

    pub(crate) fn aliases_len(&self) -> usize {
        self.aliases
    }

    pub(crate) fn aliases(&self) -> impl Iterator<Item = (&SymbolName, &SymbolName)> {
        self.alias_order
            .iter()
            .enumerate()
            .filter_map(move |(position, id)| {
                let cell = self.cell(*id)?;
                if cell.alias_position?.get() as usize != position + 1 {
                    return None;
                }
                Some((cell.symbol.as_ref()?, cell.alias()?))
            })
    }

    fn compact_alias_order_if_sparse(&mut self) {
        if self.alias_order.len() < 1024 || self.alias_order.len() < self.aliases * 2 {
            return;
        }
        let mut order = Vec::with_capacity(self.aliases);
        for (position, id) in std::mem::take(&mut self.alias_order)
            .into_iter()
            .enumerate()
        {
            if let Some(cell) = self.existing_cell_mut(id)
                && cell
                    .alias_position
                    .is_some_and(|slot| slot.get() as usize == position + 1)
            {
                cell.alias_position =
                    NonZeroU32::new(u32::try_from(order.len() + 1).expect("alias order index"));
                order.push(id);
            }
        }
        self.alias_order = order;
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
                value: cell.value().copied(),
                alias: cell.alias().copied(),
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
        self.clear_alias(symbol.id());
        match snapshot.value {
            Some(value) => {
                self.insert(symbol, value);
            }
            None => {
                self.remove_by_name(symbol.as_str());
            }
        }
        let cell = self.cell_mut(symbol);
        cell.flags = snapshot.flags;
        cell.plain_store = false;
        match snapshot.alias {
            Some(target) => self.set_alias(symbol, target),
            None => {
                self.clear_alias(symbol.id());
            }
        }
    }

    pub(crate) fn has_flag(&self, symbol: &SymbolName, flag: u8) -> bool {
        self.cell(symbol.id())
            .is_some_and(|cell| cell.flags & flag != 0)
    }

    /// `has_flag' by a symbol's id.
    pub(crate) fn has_flag_id(&self, id: u32, flag: u8) -> bool {
        self.cell(id).is_some_and(|cell| cell.flags & flag != 0)
    }

    pub(crate) fn has_flag_by_name(&self, name: &str, flag: u8) -> bool {
        SymbolName::id_of(name)
            .and_then(|id| self.cell(id))
            .is_some_and(|cell| cell.flags & flag != 0)
    }

    /// `set_flag_by_name' for the symbol in hand.
    pub(crate) fn set_flag(&mut self, symbol: &SymbolName, flag: u8) -> bool {
        let cell = self.cell_mut(symbol);
        cell.plain_store = false;
        let was_clear = cell.flags & flag == 0;
        cell.flags |= flag;
        was_clear
    }

    /// Set FLAG; true when it was not set before.
    pub(crate) fn set_flag_by_name(&mut self, name: &str, flag: u8) -> bool {
        let cell = self.cell_mut(&SymbolName::intern_str(name));
        cell.plain_store = false;
        let was_clear = cell.flags & flag == 0;
        cell.flags |= flag;
        was_clear
    }

    pub(crate) fn clear_flag_by_name(&mut self, name: &str, flag: u8) {
        if let Some(id) = SymbolName::id_of(name)
            && let Some(cell) = self.existing_cell_mut(id)
        {
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
    fn alias_payload_replaces_the_value_and_traces_only_its_current_target() {
        let mut cells = SymbolCells::default();
        let symbol = SymbolName::intern_str("redirect-union-owner");
        let first = SymbolName::intern_str("redirect-union-first");
        let second = SymbolName::intern_str("redirect-union-second");
        let old_value = Value::vector([Value::Integer(19)]);
        let function = Value::vector([Value::Integer(23)]);
        let plist = Value::list([Value::symbol("payload"), Value::Integer(29)]);
        cells.insert(&symbol, old_value);
        cells.set_function(&symbol, Some(function));
        cells.set_plist(&symbol, plist);
        cells.set_plain_store(&symbol, true);
        cells.set_alias(&symbol, first);
        assert!(!cells.plain_store(&symbol));
        assert!(cells.snapshot(&symbol).value.is_none());
        assert_eq!(cells.snapshot(&symbol).alias, Some(first));
        assert_eq!(cells.bound_len(), 0);
        assert_eq!(cells.aliases_len(), 1);
        let roots = cells.permanent_roots().collect::<Vec<_>>();
        assert!(!roots.contains(&old_value));
        assert!(roots.contains(&Value::Symbol(first)));
        assert!(roots.contains(&function) && roots.contains(&plist));
        cells.set_alias(&symbol, second);
        assert_eq!(cells.aliases_len(), 1);
        assert_eq!(cells.aliases().next(), Some((&symbol, &second)));
        let roots = cells.permanent_roots().collect::<Vec<_>>();
        assert!(!roots.contains(&old_value) && !roots.contains(&Value::Symbol(first)));
        assert!(roots.contains(&Value::Symbol(second)));
        assert!(roots.contains(&function) && roots.contains(&plist));
        cells.install_cell(
            &symbol,
            SymbolCellSnapshot {
                value: Some(Value::Integer(31)),
                alias: None,
                flags: SPECIAL,
            },
        );
        assert_eq!(cells.value(&symbol), Some(&Value::Integer(31)));
        assert!(cells.alias(&symbol).is_none());
        assert_eq!(cells.bound_len(), 1);
        assert_eq!(cells.aliases_len(), 0);
        assert_eq!(cells.function(&symbol), Some(&function));
        assert_eq!(cells.plist(&symbol), plist);
        assert_eq!(
            std::mem::size_of::<SymbolPayload>(),
            std::mem::size_of::<Value>()
        );
    }

    #[test]
    fn compact_fields_preserve_nil_values_void_defaults_and_image_replacement() {
        assert_eq!(std::mem::size_of::<SymbolCell>(), 56);
        let mut cells = SymbolCells::default();
        let symbol = SymbolName::intern_str("compact-field-symbol");
        let target = SymbolName::intern_str("compact-field-target");
        assert!(cells.value(&symbol).is_none());
        cells.insert(&symbol, Value::Nil);
        assert_eq!(cells.value(&symbol), Some(&Value::Nil));
        assert_eq!(cells.bound_len(), 1);
        cells.set_function_definition(&symbol, Some(Value::T));
        assert_eq!(cells.function(&symbol), Some(&Value::T));
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
        assert_eq!(restored.value(&symbol), Some(&Value::Unbound));
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
        assert_eq!(restored.alias(&symbol), Some(&target));
        assert_eq!(restored.aliases_len(), 1);
        restored.install_cell(
            &symbol,
            SymbolCellSnapshot {
                value: Some(Value::Integer(71)),
                alias: None,
                flags: 0,
            },
        );
        assert_eq!(restored.value(&symbol), Some(&Value::Integer(71)));
        assert_eq!(restored.bound_len(), 1);
        assert_eq!(restored.aliases_len(), 0);
        assert_eq!(restored.aliases().count(), 0);
        assert!(!restored.has_flag(&symbol, SPECIAL));
    }

    #[test]
    fn alias_enumeration_follows_one_target_through_retargeting_and_compaction() {
        let mut cells = SymbolCells::default();
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
        for _ in 0..3000 {
            cells.set_alias(&transient, first_target);
            assert!(cells.clear_alias_by_name(transient.as_str()));
        }
        assert_eq!(cells.aliases_len(), 3);
        assert!(cells.alias_order.len() <= 1024);
        assert_eq!(
            cells
                .aliases()
                .map(|(symbol, target)| (*symbol, *target))
                .collect::<Vec<_>>(),
            vec![
                (symbols[0], next_target),
                (symbols[2], first_target),
                (symbols[1], next_target)
            ]
        );
    }

    #[test]
    fn function_enumeration_survives_redefinition_voiding_and_compaction() {
        let mut cells = SymbolCells::default();
        let symbols = [
            "function-order-first",
            "function-order-second",
            "function-order-third",
        ]
        .map(SymbolName::intern_str);
        let direct = SymbolName::intern_str("function-order-direct");
        cells.set_function(&direct, Some(Value::Integer(71)));
        assert_eq!(cells.function_definitions_len(), 0);
        assert_eq!(
            cells.function_by_name(direct.as_str()),
            Some(&Value::Integer(71))
        );
        for (index, symbol) in symbols.iter().enumerate() {
            cells.set_function_definition(symbol, Some(Value::Integer(index as i64)));
        }
        cells.set_function_definition(&symbols[0], Some(Value::Integer(17)));
        cells.set_function_definition(&symbols[1], Some(Value::Nil));
        assert!(!cells.has_function_definition(&symbols[1]));
        assert!(cells.function_by_name(symbols[1].as_str()).is_none());
        cells.set_function_definition(&symbols[1], Some(Value::Integer(43)));
        let transient = SymbolName::intern_str("function-order-transient");
        for index in 0..3000 {
            cells.set_function_definition(&transient, Some(Value::Integer(index)));
            cells.set_function_definition(&transient, None);
        }
        assert_eq!(cells.function_definitions_len(), 3);
        assert!(cells.function_order.len() <= 1024);
        assert_eq!(
            cells
                .function_definitions()
                .map(|(symbol, value)| (*symbol, *value))
                .collect::<Vec<_>>(),
            vec![
                (symbols[0], Value::Integer(17)),
                (symbols[2], Value::Integer(2)),
                (symbols[1], Value::Integer(43))
            ]
        );
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
        // Historical selector: no native-word cache remains. Every reader
        // sees the cell's one value through all data.c transitions.
        let mut cells = SymbolCells::default();
        let symbol = SymbolName::intern_str("symbol-cells-word");
        let target = SymbolName::intern_str("symbol-cells-word-base");
        assert!(cells.value(&symbol).is_none());
        cells.insert(&symbol, Value::Integer(1));
        assert_eq!(
            cells.value(&symbol).copied().map(Value::word),
            Some(Value::Integer(1).word())
        );
        cells.insert(&symbol, Value::Integer(2));
        assert_eq!(
            cells.value(&symbol).copied().map(Value::word),
            Some(Value::Integer(2).word())
        );
        *cells.value_by_name_mut("symbol-cells-word").expect("bound") = Value::Integer(3);
        assert_eq!(cells.value(&symbol), Some(&Value::Integer(3)));
        cells.set_alias(&symbol, target);
        assert_eq!(cells.alias(&symbol), Some(&target));
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
        assert_eq!(cells.value(&symbol), Some(&Value::Integer(3)));
        cells.remove_by_name("symbol-cells-word");
        assert!(cells.value(&symbol).is_none());
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
