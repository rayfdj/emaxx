//! Exact validation of the graph embedded by a derived regexp cache.
//!
//! GNU's regexp engine reads its tables directly. Our backend embeds their
//! contents, so native stores as well as Lisp setters must invalidate a cache.
//! Capture the traversal on a miss; a hit compares each payload in place,
//! without rebuilding the graph, a visited set or a signature vector.
//!
//! Char-table and sub-char-table slots change only through their Rust
//! setters, which count every store and every new table or subtable
//! (`char_table_store_epoch'), so a dead table's address reused by a new
//! one cannot match by its raw word. While that
//! count is unchanged since the nodes last matched in full, a hit skips the
//! table nodes and compares only the other nodes (conses, vectors, records,
//! strings), which native code can store into directly. Builds with debug
//! assertions compare the full graph as well and require the same answer.
//!
//! A capture of a table whose recent capture still matches is that capture:
//! the traversal of an unchanged graph records the same nodes in the same
//! order. The nodes are shared, and a digest taken at capture time stands
//! for them in hashing.

use super::{CharTableRef, HashSet, Kind, Value};
use crate::lisp::alloc::vectors::char_tables::char_table_store_epoch;
use std::cell::{Cell, RefCell};
use std::hash::{BuildHasher, Hash, Hasher};
use std::rc::Rc;

#[derive(Clone, Debug, Default)]
pub(crate) struct CharTableChainSignature {
    root: Option<usize>,
    nodes: Rc<[Node]>,
    /// A hash of ROOT and NODES, taken once at capture.
    digest: u64,
    /// The char-table store count when the nodes last matched in full.
    verified_at: Cell<u64>,
}

/// Equality is the captured graph's; when it was last verified is not part
/// of the signature. Equal graphs have equal digests, so hashing the digest
/// agrees with equality.
impl PartialEq for CharTableChainSignature {
    fn eq(&self, other: &Self) -> bool {
        self.root == other.root
            && self.digest == other.digest
            && (Rc::ptr_eq(&self.nodes, &other.nodes) || self.nodes == other.nodes)
    }
}

impl Eq for CharTableChainSignature {}

impl Hash for CharTableChainSignature {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.root.hash(state);
        self.digest.hash(state);
    }
}

/// The most recent captures, by root. Raw words, like every cached
/// signature: they do not root the graph, and `matches' rejects any change.
const RECENT_CAPTURES: usize = 8;

thread_local! {
    static RECENT: RefCell<Vec<CharTableChainSignature>> = const { RefCell::new(Vec::new()) };
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct Node {
    word: usize,
    payload: Payload,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum Payload {
    CharTable(Vec<usize>),
    SubCharTable(usize, u32, Vec<usize>),
    Cons(usize, usize),
    Vector(Vec<usize>),
    Record(Vec<usize>),
    LispRecord(Vec<usize>),
    StringObject(String),
}

fn capture_slots(slots: impl Iterator<Item = Value>, pending: &mut Vec<Value>) -> Vec<usize> {
    slots
        .map(|value| {
            pending.push(value);
            value.word()
        })
        .collect()
}

fn same_slots(slots: impl Iterator<Item = Value>, expected: &[usize]) -> bool {
    slots.map(|value| value.word()).eq(expected.iter().copied())
}

impl CharTableChainSignature {
    pub(crate) fn capture(table: CharTableRef) -> Self {
        let root = Some(Value::CharTable(table).word());
        let recent = RECENT.with_borrow(|recent| {
            recent
                .iter()
                .find(|signature| signature.root == root && signature.matches(Some(table)))
                .cloned()
        });
        if let Some(recent) = recent {
            debug_assert!(
                recent == Self::capture_fresh(table),
                "a matching recent capture differs from a fresh one"
            );
            return recent;
        }
        let fresh = Self::capture_fresh(table);
        RECENT.with_borrow_mut(|recent| {
            recent.retain(|signature| signature.root != root);
            if recent.len() == RECENT_CAPTURES {
                recent.remove(0);
            }
            recent.push(fresh.clone());
        });
        fresh
    }

    fn capture_fresh(table: CharTableRef) -> Self {
        let root = Value::CharTable(table).word();
        let verified_at = char_table_store_epoch();
        let mut nodes = Vec::new();
        let mut pending = vec![Value::CharTable(table)];
        let mut seen = HashSet::with_hasher(crate::lisp::primitives::FnvBuildHasher::default());
        while let Some(value) = pending.pop() {
            let payload = match value.kind() {
                Kind::CharTable(table) if seen.insert(value.word()) => {
                    Payload::CharTable(capture_slots(table.slots(), &mut pending))
                }
                Kind::SubCharTable(table) if seen.insert(value.word()) => Payload::SubCharTable(
                    table.depth(),
                    table.min_char(),
                    capture_slots(table.slots(), &mut pending),
                ),
                Kind::Cons(cell) if seen.insert(value.word()) => {
                    let car = cell.car.get();
                    let cdr = cell.cdr.get();
                    pending.extend([car, cdr]);
                    Payload::Cons(car.word(), cdr.word())
                }
                Kind::Vector(vector) if seen.insert(value.word()) => {
                    Payload::Vector(capture_slots(vector.slots(), &mut pending))
                }
                Kind::Record(record) if seen.insert(value.word()) => {
                    Payload::Record(capture_slots(record.slots.iter().copied(), &mut pending))
                }
                Kind::LispRecord(record) if seen.insert(value.word()) => {
                    Payload::LispRecord(capture_slots(record.slots(), &mut pending))
                }
                Kind::StringObject(string) if seen.insert(value.word()) => {
                    Payload::StringObject(string.borrow().text())
                }
                _ => continue,
            };
            // A node is recorded before its children are taken from PENDING.
            nodes.push(Node {
                word: value.word(),
                payload,
            });
        }
        let mut hasher = crate::lisp::primitives::FnvBuildHasher::default().build_hasher();
        root.hash(&mut hasher);
        nodes.hash(&mut hasher);
        Self {
            root: Some(root),
            nodes: nodes.into(),
            digest: hasher.finish(),
            verified_at: Cell::new(verified_at),
        }
    }

    pub(crate) fn matches(&self, table: Option<CharTableRef>) -> bool {
        if self.root != table.map(|table| Value::CharTable(table).word()) {
            return false;
        }
        let epoch = char_table_store_epoch();
        if self.verified_at.get() == epoch {
            // No table slot changed, so every edge out of a table node is the
            // captured one and each other node is still reached through it.
            let matched = self
                .nodes
                .iter()
                .all(|node| node.is_table() || node.matches_current());
            debug_assert_eq!(
                matched,
                self.nodes.iter().all(Node::matches_current),
                "char-table store count missed a table change"
            );
            return matched;
        }
        let matched = self.nodes.iter().all(Node::matches_current);
        if matched {
            self.verified_at.set(epoch);
        }
        matched
    }
}

impl Node {
    fn is_table(&self) -> bool {
        matches!(
            self.payload,
            Payload::CharTable(_) | Payload::SubCharTable(..)
        )
    }

    /// Nodes are compared in capture order, so a node is read only after
    /// the node it was reached through has matched (or, on the fast path,
    /// is a table node whose slots the unchanged store count vouches for).
    fn matches_current(&self) -> bool {
        let node = self;
        {
            // SAFETY: the current root is live. Every other node was reached
            // through a preceding node whose complete current slots have
            // already matched, or through a table node no store has changed
            // since the graph last matched in full. A replaced edge returns
            // false before its old referent is read. This check neither calls Lisp nor collects;
            // runtime ownership excludes a concurrent native mutation.
            // Raw words do not root the cached graph across collections.
            let current = unsafe { Value::from_word(node.word) };
            match (&node.payload, current.kind()) {
                (Payload::CharTable(slots), Kind::CharTable(table)) => {
                    same_slots(table.slots(), slots)
                }
                (Payload::SubCharTable(depth, min, slots), Kind::SubCharTable(table)) => {
                    *depth == table.depth()
                        && *min == table.min_char()
                        && same_slots(table.slots(), slots)
                }
                (Payload::Cons(car, cdr), Kind::Cons(cell)) => {
                    *car == cell.car.get().word() && *cdr == cell.cdr.get().word()
                }
                (Payload::Vector(slots), Kind::Vector(vector)) => same_slots(vector.slots(), slots),
                (Payload::Record(slots), Kind::Record(record)) => {
                    same_slots(record.slots.iter().copied(), slots)
                }
                (Payload::LispRecord(slots), Kind::LispRecord(record)) => {
                    same_slots(record.slots(), slots)
                }
                (Payload::StringObject(text), Kind::StringObject(string)) => {
                    *text == string.borrow().text()
                }
                _ => false,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lisp::types::SubCharTableRef;

    #[test]
    fn making_or_storing_into_a_table_changes_the_store_count() {
        // A new table may take a dead one's address: its creation must
        // invalidate fast hits as any slot store does.
        let before = char_table_store_epoch();
        let table = CharTableRef::new(Value::Nil, Value::Nil, 0);
        assert_ne!(char_table_store_epoch(), before, "char table creation");
        let before = char_table_store_epoch();
        let _sub = SubCharTableRef::new(1, 0, Value::Nil);
        assert_ne!(char_table_store_epoch(), before, "subtable creation");
        let before = char_table_store_epoch();
        table.set(97, Value::T);
        assert_ne!(char_table_store_epoch(), before, "slot store");
        let before = char_table_store_epoch();
        let signature = CharTableChainSignature::capture(table);
        assert!(signature.matches(Some(table)));
        assert_eq!(char_table_store_epoch(), before, "reads store nothing");
        // A store the signature does not cover by value still invalidates.
        table.set(98, Value::T);
        assert!(!signature.matches(Some(table)));
    }
}
