//! The records by id (`find_record'), a registry and not a root: a
//! record the sweep frees leaves it (`purge_freed_records').  Ids are
//! sparse (an image's records keep the ids of the dumping process), so
//! the registry holds only the records.
//!
//! GNU code holds a window, a frame or a process as a Lisp_Object and
//! reads its slots through the pointer (XWINDOW (w)->buffer); the side
//! tables here hold ids, so each read finds the record again.  The ids
//! looked up last are kept beside the tree, one slot an id modulo
//! RECENT, and a slot is cleared whenever its id's record is put in,
//! replaced or taken out, so that it never names a record the tree does
//! not.

use crate::lisp::types::RecordRef;
use std::cell::Cell;
use std::collections::BTreeMap;

/// How many recent lookups the registry keeps.
const RECENT: usize = 32;

pub(crate) struct RecordRegistry {
    records: BTreeMap<u64, RecordRef>,
    recent: [Cell<Option<(u64, RecordRef)>>; RECENT],
}

impl Default for RecordRegistry {
    fn default() -> Self {
        Self {
            records: BTreeMap::new(),
            recent: std::array::from_fn(|_| Cell::new(None)),
        }
    }
}

/// A copy keeps the records and none of the lookups (a copied state's
/// records are replaced in place by its copier).
impl Clone for RecordRegistry {
    fn clone(&self) -> Self {
        Self {
            records: self.records.clone(),
            recent: std::array::from_fn(|_| Cell::new(None)),
        }
    }
}

impl<const N: usize> From<[(u64, RecordRef); N]> for RecordRegistry {
    fn from(records: [(u64, RecordRef); N]) -> Self {
        Self {
            records: BTreeMap::from(records),
            ..Self::default()
        }
    }
}

impl RecordRegistry {
    #[inline(always)]
    fn slot(&self, id: u64) -> &Cell<Option<(u64, RecordRef)>> {
        &self.recent[id as usize % RECENT]
    }

    /// The record ID names.
    #[inline]
    pub(crate) fn get(&self, id: u64) -> Option<RecordRef> {
        let slot = self.slot(id);
        if let Some((held, record)) = slot.get()
            && held == id
        {
            return Some(record);
        }
        let record = self.records.get(&id).copied()?;
        slot.set(Some((id, record)));
        Some(record)
    }

    /// RECORD under ID, in place of any record that had it.
    pub(crate) fn insert(&mut self, id: u64, record: RecordRef) {
        self.slot(id).set(None);
        self.records.insert(id, record);
    }

    /// ID's record out of the registry.
    pub(crate) fn remove(&mut self, id: u64) {
        self.slot(id).set(None);
        self.records.remove(&id);
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.records.len()
    }

    pub(crate) fn values(&self) -> impl Iterator<Item = &RecordRef> {
        self.records.values()
    }

    #[cfg(test)]
    pub(crate) fn values_mut(&mut self) -> impl Iterator<Item = &mut RecordRef> {
        for slot in &self.recent {
            slot.set(None);
        }
        self.records.values_mut()
    }
}

#[cfg(test)]
mod tests {
    use super::{RECENT, RecordRegistry};
    use crate::lisp::eval::{Interpreter, RecordKind, RecordState, next_record_owner};
    use crate::lisp::types::{RecordRef, Value};

    fn record(id: u64) -> RecordRef {
        RecordRef::allocate(RecordState {
            id,
            owner: next_record_owner(),
            type_tag: Value::symbol("thread"),
            slots: Vec::new(),
            kind: RecordKind::Thread,
        })
    }

    #[test]
    fn a_remembered_lookup_is_forgotten_when_its_record_leaves_or_is_replaced() {
        let _interp = Interpreter::new();
        let id = 7;
        let twin = id + RECENT as u64;
        let (first, second, other) = (record(id), record(id), record(twin));
        let mut registry = RecordRegistry::from([(id, first)]);
        assert!(registry.get(id).is_some_and(|found| found.ptr_eq(&first)));
        assert!(registry.get(twin).is_none());
        registry.insert(twin, other);
        assert!(registry.get(twin).is_some_and(|found| found.ptr_eq(&other)));
        assert!(registry.get(id).is_some_and(|found| found.ptr_eq(&first)));
        registry.insert(id, second);
        assert!(registry.get(id).is_some_and(|found| found.ptr_eq(&second)));
        registry.remove(id);
        assert!(registry.get(id).is_none());
        assert!(registry.get(twin).is_some_and(|found| found.ptr_eq(&other)));
        assert_eq!(registry.len(), 1);
        let copy = registry.clone();
        registry.remove(twin);
        assert!(registry.get(twin).is_none());
        assert!(copy.get(twin).is_some_and(|found| found.ptr_eq(&other)));
    }
}
