#![allow(dead_code)]

use crate::lisp::types::Kind;
use crate::lisp::types::Value;

/// An overlay on a buffer region, with properties.
///
/// Overlays are identified by a unique `id` for Lisp-level `eq` comparison.
/// They track a [beg, end) region in a buffer and carry a property list.
#[derive(Clone, Debug)]
pub struct Overlay {
    pub id: u64,
    pub beg: usize,
    pub end: usize,
    pub front_advance: bool,
    pub rear_advance: bool,
    /// Buffer ID this overlay belongs to, or None if deleted/detached.
    pub buffer_id: Option<u64>,
    /// buffer.c's OVERLAY_PLIST: the actual Lisp list, traced and relocated
    /// as one object graph instead of reconstructed from a Rust pair vector.
    pub plist: Value,
}

impl Overlay {
    pub fn new(
        id: u64,
        beg: usize,
        end: usize,
        buffer_id: u64,
        front_advance: bool,
        rear_advance: bool,
    ) -> Self {
        let (beg, end) = if beg > end { (end, beg) } else { (beg, end) };
        Overlay {
            id,
            beg,
            end,
            front_advance,
            rear_advance,
            buffer_id: Some(buffer_id),
            plist: Value::Nil,
        }
    }

    pub fn get_prop(&self, key: &Value) -> Option<Value> {
        self.find_prop(|property| property.word() == key.word())
    }

    /// Existing display callers name their properties with Rust strings.
    /// Compare those names in the list without interning a symbol on each read.
    pub(crate) fn get_symbol_prop(&self, key: &str) -> Option<Value> {
        self.find_prop(|property| matches!(property.kind(), Kind::Symbol(name) if name == key))
    }

    fn find_prop(&self, matches: impl Fn(Value) -> bool) -> Option<Value> {
        let mut tail = self.plist;
        while let Kind::Cons(property) = tail.kind() {
            let Kind::Cons(value) = property.cdr.get().kind() else {
                return None;
            };
            if matches(property.car.get()) {
                return Some(value.car.get());
            }
            tail = value.cdr.get();
        }
        None
    }

    pub fn put_prop(&mut self, key: Value, value: Value) {
        // buffer.c:Foverlay_put compares keys with EQ, replaces the existing
        // value in place, or prepends the two new conses. In particular, equal
        // but distinct floats are different property keys.
        let mut tail = self.plist;
        while let Kind::Cons(property) = tail.kind() {
            let Kind::Cons(value_cell) = property.cdr.get().kind() else {
                break;
            };
            if property.car.get().word() == key.word() {
                value_cell.car.set(value);
                return;
            }
            tail = value_cell.cdr.get();
        }
        self.plist = Value::cons(key, Value::cons(value, self.plist));
    }

    pub fn is_dead(&self) -> bool {
        self.buffer_id.is_none()
    }

    /// Priority for sorting (higher = more important). Defaults to 0.
    pub fn priority(&self) -> i64 {
        match self
            .get_prop(&Value::Symbol("priority".into()))
            .map(|v| v.kind())
        {
            Some(Kind::Integer(n)) => n,
            _ => 0,
        }
    }
}

/// Adjust overlay positions after inserting `nchars` at position `pos` (1-based).
pub fn adjust_for_insert(overlays: &mut [Overlay], pos: usize, nchars: usize) {
    for ov in overlays.iter_mut() {
        if ov.beg == ov.end && ov.beg == pos {
            if ov.rear_advance {
                ov.end += nchars;
                if ov.front_advance {
                    ov.beg += nchars;
                }
            }
            continue;
        }
        if ov.beg > pos || (ov.beg == pos && ov.front_advance) {
            ov.beg += nchars;
        }
        if ov.end > pos || (ov.end == pos && ov.rear_advance) {
            ov.end += nchars;
        }
    }
}

/// Adjust overlay positions after deleting the range [from, to) (1-based).
pub fn adjust_for_delete(overlays: &mut [Overlay], from: usize, to: usize) {
    let nchars = to.saturating_sub(from);
    for ov in overlays.iter_mut() {
        if ov.beg >= to {
            ov.beg -= nchars;
        } else if ov.beg > from {
            ov.beg = from;
        }
        if ov.end >= to {
            ov.end -= nchars;
        } else if ov.end > from {
            ov.end = from;
        }
    }
}

/// Delete overlays that have the `evaporate` property and are now
/// empty.  Like GNU's Fdelete_overlay the object keeps its properties:
/// `move-overlay` revives it (rfn-eshadow's shadow overlay cycles
/// through exactly this).
pub fn evaporate(overlays: &mut [Overlay]) {
    for ov in overlays.iter_mut() {
        if ov.beg == ov.end
            && !ov.is_dead()
            && ov
                .get_prop(&Value::Symbol("evaporate".into()))
                .is_some_and(|value| value.is_truthy())
        {
            ov.buffer_id = None;
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn adjust_insert_basic() {
        let mut ovs = vec![Overlay::new(1, 5, 10, 0, false, false)];
        adjust_for_insert(&mut ovs, 3, 2);
        assert_eq!(ovs[0].beg, 7);
        assert_eq!(ovs[0].end, 12);
    }

    #[test]
    fn adjust_insert_at_beg_no_advance() {
        let mut ovs = vec![Overlay::new(1, 5, 10, 0, false, false)];
        adjust_for_insert(&mut ovs, 5, 2);
        // front_advance=false, so beg stays
        assert_eq!(ovs[0].beg, 5);
        assert_eq!(ovs[0].end, 12);
    }

    #[test]
    fn adjust_insert_at_beg_with_advance() {
        let mut ovs = vec![Overlay::new(1, 5, 10, 0, true, false)];
        adjust_for_insert(&mut ovs, 5, 2);
        assert_eq!(ovs[0].beg, 7);
        assert_eq!(ovs[0].end, 12);
    }

    #[test]
    fn adjust_insert_empty_overlay_front_advance_only_stays_put() {
        let mut ovs = vec![Overlay::new(1, 5, 5, 0, true, false)];
        adjust_for_insert(&mut ovs, 5, 2);
        assert_eq!(ovs[0].beg, 5);
        assert_eq!(ovs[0].end, 5);
    }

    #[test]
    fn adjust_insert_empty_overlay_with_both_advances_moves() {
        let mut ovs = vec![Overlay::new(1, 5, 5, 0, true, true)];
        adjust_for_insert(&mut ovs, 5, 2);
        assert_eq!(ovs[0].beg, 7);
        assert_eq!(ovs[0].end, 7);
    }

    #[test]
    fn adjust_delete_basic() {
        let mut ovs = vec![Overlay::new(1, 5, 10, 0, false, false)];
        adjust_for_delete(&mut ovs, 3, 6);
        assert_eq!(ovs[0].beg, 3);
        assert_eq!(ovs[0].end, 7);
    }

    #[test]
    fn adjust_delete_encompassing() {
        let mut ovs = vec![Overlay::new(1, 5, 10, 0, false, false)];
        adjust_for_delete(&mut ovs, 3, 12);
        assert_eq!(ovs[0].beg, 3);
        assert_eq!(ovs[0].end, 3);
    }
}
