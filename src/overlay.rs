//! Canonical GNU overlay objects and their buffer-owned interval index.

use crate::lisp::types::{BufferRef, Kind, Value};
use std::cell::Cell;

mod itree;
pub(crate) use itree::Traversal;

/// lisp.h:Lisp_Overlay after its one-word pseudovector header. The plist
/// is its only strong Lisp field. The buffer link is weak; the overlay owns
/// its interval node, and the buffer's index links that node in place.
#[repr(C)]
pub struct OverlayValue {
    plist: Cell<Value>,
    buffer: Cell<Option<BufferRef>>,
    interval: itree::NodeRef,
}

pub type OverlayRef = crate::lisp::alloc::VectorlikeRef<OverlayValue>;

const _: () = {
    assert!(std::mem::size_of::<OverlayValue>() == 24);
    assert!(std::mem::offset_of!(OverlayValue, plist) == 0);
    assert!(std::mem::offset_of!(OverlayValue, buffer) == 8);
    assert!(std::mem::offset_of!(OverlayValue, interval) == 16);
};

impl OverlayRef {
    pub(crate) fn new(front_advance: bool, rear_advance: bool) -> Self {
        // Include the separately allocated interval node, not just the Lisp
        // pseudovector. Both are freed when this object is swept.
        crate::lisp::native_comp::note_lisp_allocation(32 + 80);
        let interval = Box::into_raw(Box::new(itree::Node::new(0, front_advance, rear_advance)));
        // SAFETY: this overlay owns the pinned node until its vector cleanup.
        // Buffers unlink nodes before either endpoint's storage is swept.
        let interval = unsafe { itree::NodeRef::from_raw(interval) };
        let object = Self::allocate(OverlayValue {
            plist: Cell::new(Value::Nil),
            buffer: Cell::new(None),
            interval,
        });
        interval.set_data(Value::Overlay(object).word());
        object
    }

    pub(crate) fn move_to(&self, buffer: BufferRef, beg: usize, end: usize) {
        if self.buffer().is_some_and(|old| old.ptr_eq(&buffer)) {
            buffer
                .borrow_mut()
                .overlays
                .0
                .set_region(self.interval, beg as isize, end as isize);
        } else {
            self.detach();
            buffer
                .borrow_mut()
                .overlays
                .0
                .insert(self.interval, beg as isize, end as isize);
            self.buffer.set(Some(buffer));
        }
    }

    pub(crate) fn detach(&self) {
        if let Some(buffer) = self.buffer() {
            buffer.borrow_mut().overlays.remove(*self);
        }
    }
}

impl OverlayValue {
    #[inline]
    pub fn plist(&self) -> Value {
        self.plist.get()
    }
    #[inline]
    pub(crate) fn set_plist(&self, plist: Value) {
        self.plist.set(plist);
    }
    #[inline]
    pub fn buffer(&self) -> Option<BufferRef> {
        self.buffer.get()
    }
    #[inline]
    pub fn is_dead(&self) -> bool {
        self.buffer.get().is_none()
    }
    #[inline]
    pub fn front_advance(&self) -> bool {
        self.interval.front_advance()
    }
    #[inline]
    pub fn rear_advance(&self) -> bool {
        self.interval.rear_advance()
    }

    pub(crate) fn bounds(&self) -> (isize, isize) {
        if let Some(buffer) = self.buffer() {
            buffer.borrow().overlays.0.bounds(self.interval)
        } else {
            self.interval.bounds()
        }
    }
    pub fn beg(&self) -> usize {
        self.bounds().0.max(0) as usize
    }
    pub fn end(&self) -> usize {
        self.bounds().1.max(0) as usize
    }

    /// Image restoration initializes detached positions before linking the
    /// object into its restored buffer, if any.
    pub(crate) fn restore_bounds(&self, beg: isize, end: isize) {
        assert!(self.is_dead());
        self.interval.set_detached_bounds(beg, end);
    }

    pub fn get_prop(&self, key: &Value) -> Option<Value> {
        self.find_prop(|property| property.word() == key.word())
    }

    pub(crate) fn get_symbol_prop(&self, key: &str) -> Option<Value> {
        self.find_prop(|property| matches!(property.kind(), Kind::Symbol(name) if name == key))
    }

    fn find_prop(&self, matches: impl Fn(Value) -> bool) -> Option<Value> {
        let mut tail = self.plist();
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

    pub fn put_prop(&self, key: Value, value: Value) {
        // buffer.c:Foverlay_put: EQ keys, replace the existing value car,
        // otherwise prepend two conses to the authoritative Lisp plist.
        let mut tail = self.plist();
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
        self.plist
            .set(Value::cons(key, Value::cons(value, self.plist())));
    }
}

impl Drop for OverlayValue {
    fn drop(&mut self) {
        debug_assert!(self.is_dead(), "buffer sweep unlinks overlays first");
        // SAFETY: new allocated this node once; the overlay is its sole owner.
        // GC's buffer pass has removed every link before vector cleanup.
        unsafe {
            drop(Box::from_raw(self.interval.as_ptr()));
        }
    }
}

impl PartialEq for OverlayRef {
    fn eq(&self, other: &Self) -> bool {
        self.ptr_eq(other)
    }
}
impl Eq for OverlayRef {}
impl std::fmt::Display for OverlayRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:x}", self.identity())
    }
}
impl std::fmt::Debug for OverlayValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // No buffer borrow: diagnostics can run while editing that buffer.
        f.debug_struct("OverlayValue")
            .field("buffer", &self.buffer())
            .finish_non_exhaustive()
    }
}

/// buffer.c's overlay index. It contains links to the canonical objects,
/// owns no overlay payloads, and holds no detached-object registry.
#[derive(Default)]
pub struct OverlayTree(itree::Tree);

impl OverlayTree {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
    pub fn len(&self) -> usize {
        self.0.len()
    }
    pub(crate) fn iter(&self) -> OverlayIter<'_> {
        self.intersecting(isize::MIN, isize::MAX, Traversal::Ascending)
    }
    pub(crate) fn intersecting(&self, beg: isize, end: isize, order: Traversal) -> OverlayIter<'_> {
        OverlayIter(self.0.iter(beg, end, order))
    }
    pub(crate) fn visit_lisp_values(&self, visit: &mut impl FnMut(&Value)) {
        self.0.visit_data(&mut |word| {
            // SAFETY: only an allocated overlay's Lisp word is inserted.
            visit(&unsafe { Value::from_word(word) });
        });
    }
    pub(crate) fn remove(&mut self, overlay: OverlayRef) {
        self.0.remove(overlay.interval);
        overlay.buffer.set(None);
    }
    pub(crate) fn clear(&mut self) {
        // The postorder iterator computes its successor before yielding; GNU
        // delete_all_overlays relies on this to clear each node's links.
        for node in self.0.iter(isize::MIN, isize::MAX, Traversal::PostOrder) {
            overlay_of(node).buffer.set(None);
            node.clear_links();
        }
        self.0.clear();
    }
    pub(crate) fn insert_gap(&mut self, pos: usize, length: usize, before_markers: bool) {
        self.0
            .insert_gap(pos as isize, length as isize, before_markers);
    }
    pub(crate) fn delete_gap(&mut self, pos: usize, length: usize) {
        self.0.delete_gap(pos as isize, length as isize);
        // Only overlays collapsed at POS can newly evaporate. Empty overlays
        // with evaporate already set are removed by overlay-put/move-overlay.
        let evaporated: Vec<_> = self
            .0
            .iter(pos as isize, pos as isize + 1, Traversal::PreOrder)
            .filter(|node| {
                node.bounds() == (pos as isize, pos as isize)
                    && overlay_of(*node)
                        .get_symbol_prop("evaporate")
                        .is_some_and(|v| v.is_truthy())
            })
            .map(overlay_of)
            .collect();
        for overlay in evaporated {
            self.remove(overlay);
        }
    }
    pub(crate) fn set_owner(&self, owner: BufferRef) {
        self.0.visit_data(&mut |word| {
            // SAFETY: every indexed node retains its allocated overlay word.
            let Kind::Overlay(overlay) = unsafe { Value::from_word(word) }.kind() else {
                unreachable!()
            };
            overlay.buffer.set(Some(owner));
        });
    }
}

impl Drop for OverlayTree {
    fn drop(&mut self) {
        self.clear();
    }
}

pub struct OverlayIter<'a>(itree::Iter<'a>);
impl OverlayIter<'_> {
    pub(crate) fn narrow(&mut self, beg: isize, end: isize) {
        self.0.narrow(beg, end);
    }
}
impl Iterator for OverlayIter<'_> {
    type Item = OverlayRef;
    fn next(&mut self) -> Option<OverlayRef> {
        self.0.next().map(overlay_of)
    }
}
impl<'a> IntoIterator for &'a OverlayTree {
    type Item = OverlayRef;
    type IntoIter = OverlayIter<'a>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

fn overlay_of(node: itree::NodeRef) -> OverlayRef {
    // SAFETY: insertions use only the owning overlay's canonical tagged word;
    // a buffer's index roots every overlay until it is unlinked.
    unsafe { OverlayRef::from_raw((node.data() & !7) as *mut _) }
}

#[cfg(test)]
mod tests {
    use super::itree::{Node, NodeRef, Tree};

    fn edited(
        beg: isize,
        end: isize,
        front: bool,
        rear: bool,
        pos: isize,
        length: isize,
        insert: bool,
    ) -> (isize, isize) {
        let mut owned = Box::new(Node::new(0, front, rear));
        // SAFETY: owned outlives the tree and its node is unlinked before drop.
        let node = unsafe { NodeRef::from_raw(&mut *owned) };
        let mut tree = Tree::default();
        tree.insert(node, beg, end);
        if insert {
            tree.insert_gap(pos, length, false);
        } else {
            tree.delete_gap(pos, length);
        }
        let bounds = tree.bounds(node);
        tree.remove(node);
        bounds
    }
    #[test]
    fn adjust_insert_basic() {
        assert_eq!(edited(5, 10, false, false, 3, 2, true), (7, 12));
    }
    #[test]
    fn adjust_insert_at_beg_no_advance() {
        assert_eq!(edited(5, 10, false, false, 5, 2, true), (5, 12));
    }
    #[test]
    fn adjust_insert_at_beg_with_advance() {
        assert_eq!(edited(5, 10, true, false, 5, 2, true), (7, 12));
    }
    #[test]
    fn adjust_insert_empty_overlay_front_advance_only_stays_put() {
        assert_eq!(edited(5, 5, true, false, 5, 2, true), (5, 5));
    }
    #[test]
    fn adjust_insert_empty_overlay_with_both_advances_moves() {
        assert_eq!(edited(5, 5, true, true, 5, 2, true), (7, 7));
    }
    #[test]
    fn adjust_delete_basic() {
        assert_eq!(edited(5, 10, false, false, 3, 3, false), (3, 7));
    }
    #[test]
    fn adjust_delete_encompassing() {
        assert_eq!(edited(5, 10, false, false, 3, 9, false), (3, 3));
    }
}
