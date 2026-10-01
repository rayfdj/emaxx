//! Exact validation of the graph embedded by a derived regexp cache.
//!
//! GNU's regexp engine reads its tables directly. Our backend embeds their
//! contents, so native stores as well as Lisp setters must invalidate a cache.
//! Capture the traversal on a miss; a hit compares each payload in place,
//! without rebuilding the graph, a visited set or a signature vector.

use super::{CharTableRef, HashSet, Kind, Value};

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq)]
pub(crate) struct CharTableChainSignature {
    root: Option<usize>,
    nodes: Vec<Node>,
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
        let root = Value::CharTable(table).word();
        let mut result = Self {
            root: Some(root),
            nodes: Vec::new(),
        };
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
            result.nodes.push(Node {
                word: value.word(),
                payload,
            });
        }
        result
    }

    pub(crate) fn matches(&self, table: Option<CharTableRef>) -> bool {
        if self.root != table.map(|table| Value::CharTable(table).word()) {
            return false;
        }
        self.nodes.iter().all(|node| {
            // SAFETY: the current root is live. Every other node was reached
            // through a preceding node, whose complete current slots have
            // already matched. A replaced edge returns false before its old
            // referent is read. This check neither calls Lisp nor collects;
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
        })
    }
}
