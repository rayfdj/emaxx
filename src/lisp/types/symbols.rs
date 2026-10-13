//! Canonical symbols owned by an editor entry.
//!
//! GNU's Vobarray and Qsymbol references belong to one Emacs process. An
//! embedded process may run successive editors, so their canonical objects
//! must have separate owners even while the allocator uses one serialized
//! heap. The active owner is selected at the editor boundary, not per VM
//! instruction. Interpreter states retain their owner for GC and parked calls.

use super::{ProcessTable, SymbolName, Value};
use crate::lisp::alloc::vectors::hash_tables::DescriptorPool;
use crate::lisp::primitives::FnvBuildHasher;
use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

type Names = HashSet<NameEntry, FnvBuildHasher>;
type AddressNames = HashMap<(usize, usize), SymbolName, FnvBuildHasher>;

/// Lookup compares immutable host text; Lisp symbol equality compares the
/// allocated object. Keep the lookup key's equality separate so two editor
/// objects with identical names cannot compare equal as Lisp values.
#[derive(Clone, Copy)]
pub(super) struct NameEntry(pub(super) SymbolName);

impl std::borrow::Borrow<str> for NameEntry {
    fn borrow(&self) -> &str {
        self.0.as_str()
    }
}

impl std::ops::Deref for NameEntry {
    type Target = SymbolName;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl PartialEq for NameEntry {
    fn eq(&self, other: &Self) -> bool {
        self.0.as_str() == other.0.as_str()
    }
}

impl Eq for NameEntry {}

impl std::hash::Hash for NameEntry {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::hash::Hash::hash(self.0.as_str(), state);
    }
}

const KEY_NAMES: &[&str] = &[
    "parse-sexp-lookup-properties",
    "char-property-alias-alist",
    "search-spaces-regexp",
    "symbol",
    "integer",
    "string",
    "cons",
    "float",
    "operations",
    "lexical-binding",
    "macroexp--dynvars",
    "inhibit-modification-hooks",
    "buffer-file-name",
    "buffer-file-truename",
    "combine-after-change-calls",
    "before-change-functions",
    "after-change-functions",
    "inhibit-read-only",
    "buffer-read-only",
    "char-code-property-alist",
    "category-table",
    "case-fold-search",
    "undo-auto--undoable-change",
];

struct Symbols {
    names: RefCell<Names>,
    by_address: RefCell<AddressNames>,
    keys: [Cell<Option<SymbolName>>; KEY_NAMES.len()],
    hash_tests: RefCell<DescriptorPool>,
}

impl Default for Symbols {
    fn default() -> Self {
        Self {
            names: RefCell::new(Names::default()),
            by_address: RefCell::new(AddressNames::default()),
            keys: std::array::from_fn(|_| Cell::new(None)),
            hash_tests: RefCell::new(DescriptorPool::default()),
        }
    }
}

// Internal heap operations that do not create an editor retain their existing
// process owner. Editor entry points always install a separate owner below.
static HOST_SYMBOLS: ProcessTable<Rc<Symbols>> = ProcessTable::new();

thread_local! {
    static ACTIVE: RefCell<Option<Rc<Symbols>>> = const { RefCell::new(None) };
}

fn with_current<R>(body: impl FnOnce(&Symbols) -> R) -> R {
    ACTIVE.with_borrow(|active| match active {
        Some(symbols) => body(symbols),
        None => {
            // Release ProcessTable's initialization borrow before BODY can
            // resolve another symbol through the same host owner.
            let symbols = HOST_SYMBOLS.with_borrow(Rc::clone);
            body(&symbols)
        }
    })
}

/// The interpreter retains its actual obarray owner, including when another
/// owned editor entry is nested or the interpreter is parked for collection.
#[derive(Clone)]
pub(crate) struct SymbolOwner(Rc<Symbols>);

impl Default for SymbolOwner {
    fn default() -> Self {
        ACTIVE.with_borrow(|active| {
            Self(match active {
                Some(symbols) => Rc::clone(symbols),
                None => HOST_SYMBOLS.with_borrow(Rc::clone),
            })
        })
    }
}

impl SymbolOwner {
    #[cfg(test)]
    pub(crate) fn fresh() -> Self {
        Self(Rc::default())
    }

    #[cfg(test)]
    pub(crate) fn enter(&self) -> EditorSymbolScope {
        EditorSymbolScope(ACTIVE.with_borrow_mut(|active| active.replace(Rc::clone(&self.0))))
    }

    /// Private host-side image fixtures retain the selected editor while
    /// inspecting returned Lisp objects. Public entries have their own
    /// enclosing scope, which restores the caller on exit.
    #[cfg(test)]
    pub(crate) fn select(&self) {
        ACTIVE.with_borrow_mut(|active| *active = Some(Rc::clone(&self.0)));
    }

    #[cfg(test)]
    pub(crate) fn names(&self) -> Vec<SymbolName> {
        self.0.names.borrow().iter().map(|entry| entry.0).collect()
    }

    pub(crate) fn mark_roots(&self, mark: &mut dyn FnMut(&Value)) {
        mark_names(&self.0, mark);
    }
}

/// Select an editor's objects for a complete owned host entry. Holding an Rc
/// rather than a pointer into Interpreter avoids aliasing its mutable state.
/// Drop restores a nested caller's owner and releases this scope's ownership.
pub(crate) struct EditorSymbolScope(Option<Rc<Symbols>>);

impl EditorSymbolScope {
    pub(crate) fn enter() -> Self {
        Self(ACTIVE.with_borrow_mut(|active| active.replace(Rc::default())))
    }
}

impl Drop for EditorSymbolScope {
    fn drop(&mut self) {
        ACTIVE.with_borrow_mut(|active| *active = self.0.take());
    }
}

pub(super) struct InternedNames;

impl InternedNames {
    pub(super) fn with_borrow<R>(&self, body: impl FnOnce(&Names) -> R) -> R {
        with_current(|symbols| body(&symbols.names.borrow()))
    }

    pub(super) fn with_borrow_mut<R>(&self, body: impl FnOnce(&mut Names) -> R) -> R {
        with_current(|symbols| body(&mut symbols.names.borrow_mut()))
    }
}

pub(super) fn with_address_names<R>(body: impl FnOnce(&AddressNames) -> R) -> R {
    with_current(|symbols| body(&symbols.by_address.borrow()))
}

pub(super) fn with_address_names_mut<R>(body: impl FnOnce(&mut AddressNames) -> R) -> R {
    with_current(|symbols| body(&mut symbols.by_address.borrow_mut()))
}

pub(crate) fn with_hash_tests<R>(body: impl FnOnce(&mut DescriptorPool) -> R) -> R {
    with_current(|symbols| body(&mut symbols.hash_tests.borrow_mut()))
}

/// A Qsymbol-style reference in the current editor, with a fixed slot. The
/// slot contains the actual canonical symbol, never a second Lisp payload.
#[derive(Clone, Copy)]
pub(crate) struct SymbolKey(usize);

impl SymbolKey {
    pub(crate) const fn new(name: &str) -> Self {
        let wanted = name.as_bytes();
        let mut index = 0;
        while index < KEY_NAMES.len() {
            let candidate = KEY_NAMES[index].as_bytes();
            if candidate.len() == wanted.len() {
                let mut byte = 0;
                while byte < wanted.len() && candidate[byte] == wanted[byte] {
                    byte += 1;
                }
                if byte == wanted.len() {
                    return Self(index);
                }
            }
            index += 1;
        }
        panic!("undeclared editor symbol key");
    }

    pub(crate) fn with<R>(self, body: impl FnOnce(&SymbolName) -> R) -> R {
        let symbol = with_current(|symbols| {
            symbols.keys[self.0].get().unwrap_or_else(|| {
                let symbol = SymbolName::intern_str(KEY_NAMES[self.0]);
                symbols.keys[self.0].set(Some(symbol));
                symbol
            })
        });
        body(&symbol)
    }
}

fn mark_names(symbols: &Symbols, mark: &mut dyn FnMut(&Value)) {
    for name in symbols.names.borrow().iter() {
        let value = Value::Symbol(name.0);
        if matches!(value.kind(), super::Kind::Nil | super::Kind::T) {
            // Fixed nil/t ABI words still use their allocated name cells.
            name.mark_bit().mark(super::current_mark_epoch());
            mark(&name.lisp_name());
        } else {
            mark(&value);
        }
    }
    symbols.hash_tests.borrow().mark_roots(mark);
}

pub(super) fn mark_scope_roots(mark: &mut dyn FnMut(&Value)) {
    let host = HOST_SYMBOLS.with_borrow(Rc::clone);
    mark_names(&host, mark);
    ACTIVE.with_borrow(|active| {
        if let Some(symbols) = active {
            mark_names(symbols, mark);
        }
    });
}

#[cfg(test)]
mod tests {
    use super::{EditorSymbolScope, SymbolName};
    use std::collections::{BTreeSet, HashMap};

    #[test]
    fn nested_editors_keep_same_named_symbols_distinct_and_restore_the_caller() {
        let _outer = EditorSymbolScope::enter();
        for text in ["owner-field", "owner-hook", "owner-λ"] {
            let outer = SymbolName::intern_str(text);
            let mut values = HashMap::from([(outer, 17)]);
            let mut ordered = BTreeSet::from([outer]);
            {
                let _inner = EditorSymbolScope::enter();
                let inner = SymbolName::intern_str(text);
                assert_ne!(outer, inner);
                assert_eq!(inner, SymbolName::intern_str(text));
                assert!(values.insert(inner, 43).is_none());
                assert_eq!(values.get(&outer), Some(&17));
                assert_eq!(values.get(&inner), Some(&43));
                assert!(ordered.insert(inner));
                assert_eq!(ordered.len(), 2);
            }
            assert_eq!(SymbolName::intern_str(text), outer);
            assert_eq!(values.get(&SymbolName::intern_str(text)), Some(&17));
        }
    }
}
