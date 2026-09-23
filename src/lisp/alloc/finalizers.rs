//! alloc.c's finalizer objects and its two intrusive lists. The Lisp
//! payload is exactly `Lisp_Finalizer`: one header, the function word,
//! and two untraced list links. Ordinary access never resolves an id.

use super::{VectorHeader, VectorTag, VectorlikeRef};
use crate::lisp::types::Value;
use std::cell::Cell;
use std::marker::PhantomData;
use std::rc::Rc;

#[repr(C)]
#[derive(Debug)]
pub struct FinalizerState {
    function: Cell<Value>,
    prev: Cell<*mut VectorHeader>,
    next: Cell<*mut VectorHeader>,
}

pub type FinalizerRef = VectorlikeRef<FinalizerState>;

const _: () = assert!(std::mem::size_of::<FinalizerState>() == 3 * size_of::<usize>());

impl super::vectors::Vectorlike for FinalizerState {
    const TAG: VectorTag = VectorTag::Finalizer;
    const LISP_SLOTS: usize = 1;
}

impl FinalizerState {
    fn new(function: Value) -> Self {
        Self {
            function: Cell::new(function),
            prev: Cell::new(std::ptr::null_mut()),
            next: Cell::new(std::ptr::null_mut()),
        }
    }

    pub(crate) fn function(&self) -> Value {
        self.function.get()
    }

    pub(crate) fn set_function(&self, function: Value) {
        self.function.set(function);
    }

    /// alloc.c:unchain_finalizer. Links name allocated finalizers or a
    /// stable list sentinel. A sweep unlinks each object before freeing it.
    fn unlink(&self) {
        let prev = self.prev.replace(std::ptr::null_mut());
        let next = self.next.replace(std::ptr::null_mut());
        if !prev.is_null() {
            assert!(!next.is_null());
            // SAFETY: the list owns its sentinel; its members are unlinked
            // before reclamation. Only the link Cells are changed.
            unsafe {
                fields(prev).next.set(next);
                fields(next).prev.set(prev);
            }
        }
    }
}

impl Drop for FinalizerState {
    fn drop(&mut self) {
        self.unlink();
    }
}

/// # Safety
/// HEADER names an allocated finalizer or a live list sentinel, and the
/// caller keeps that allocation alive for the returned borrow.
unsafe fn fields<'a>(header: *mut VectorHeader) -> &'a FinalizerState {
    // SAFETY: both layouts put these fields immediately after one word.
    unsafe {
        &*header
            .cast::<u8>()
            .add(size_of::<usize>())
            .cast::<FinalizerState>()
    }
}

impl FinalizerRef {
    pub(crate) fn new(function: Value) -> Self {
        crate::lisp::native_comp::note_lisp_allocation(4 * size_of::<usize>());
        Self::allocate(FinalizerState::new(function))
    }

    /// Dump the actual links. GNU's static sentinels have a zero header;
    /// they are process pointers rather than Lisp objects in the image.
    pub(crate) fn neighbors(&self) -> (Option<Self>, Option<Self>) {
        let object = |pointer: *mut VectorHeader| {
            if pointer.is_null() {
                return None;
            }
            // SAFETY: links name live objects or the owning list's sentinel.
            unsafe { (pointer.cast::<usize>().read() != 0).then(|| Self::from_raw(pointer)) }
        };
        (object(self.prev.get()), object(self.next.get()))
    }
}

impl PartialEq for FinalizerRef {
    fn eq(&self, other: &Self) -> bool {
        self.ptr_eq(other)
    }
}

impl Eq for FinalizerRef {}

#[repr(C)]
struct FinalizerListHead {
    header: usize,
    fields: FinalizerState,
}

impl Drop for FinalizerListHead {
    fn drop(&mut self) {
        let head = std::ptr::from_mut(self).cast::<VectorHeader>();
        let mut next = self.fields.next.get();
        while next != head {
            // SAFETY: every member remains allocated until its sweep
            // unlinks it; dropping the last list owner detaches survivors.
            // Clear members directly, without accessing this exclusively
            // borrowed sentinel through one of their older raw links.
            let object = unsafe { fields(next) };
            next = object.next.replace(std::ptr::null_mut());
            object.prev.set(std::ptr::null_mut());
        }
        self.fields.prev.set(std::ptr::null_mut());
        self.fields.next.set(std::ptr::null_mut());
    }
}

/// Interpreter shells can move and shallow clones share their Lisp graph.
/// A stable shared sentinel keeps that graph on one list; the test image's
/// deep copier creates fresh lists and objects. The Rc is per list, never
/// per Lisp value, field read, or native transition.
#[derive(Clone)]
pub(crate) struct FinalizerList(Rc<FinalizerListHead>);

impl Default for FinalizerList {
    fn default() -> Self {
        let head = Rc::new(FinalizerListHead {
            header: 0,
            fields: FinalizerState::new(Value::Nil),
        });
        let pointer = Rc::as_ptr(&head).cast_mut().cast::<VectorHeader>();
        head.fields.prev.set(pointer);
        head.fields.next.set(pointer);
        Self(head)
    }
}

impl FinalizerList {
    fn head(&self) -> *mut VectorHeader {
        Rc::as_ptr(&self.0).cast_mut().cast()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.0.fields.next.get() == self.head()
    }

    /// alloc.c:finalizer_insert, including moving an object from the
    /// active list to the doomed list without copying its callback.
    pub(crate) fn append(&self, object: FinalizerRef) {
        object.unlink();
        let head = self.head();
        let prev = self.0.fields.prev.get();
        let pointer = object.identity() as *mut VectorHeader;
        object.prev.set(prev);
        object.next.set(head);
        // SAFETY: PREV is the live sentinel or its current final member.
        unsafe { fields(prev).next.set(pointer) };
        self.0.fields.prev.set(pointer);
    }

    pub(crate) fn pop_front(&self) -> Option<FinalizerRef> {
        let pointer = self.0.fields.next.get();
        if pointer == self.head() {
            return None;
        }
        // SAFETY: a non-sentinel link names an allocated finalizer.
        let object = unsafe { FinalizerRef::from_raw(pointer) };
        object.unlink();
        Some(object)
    }

    /// # Safety
    /// Until the iterator is dropped, no collection may sweep its members,
    /// and only the most recently yielded member may be moved or unlinked.
    /// In particular, the caller must not invoke Lisp while traversing it.
    pub(crate) unsafe fn iter(&self) -> Finalizers<'_> {
        Finalizers {
            next: self.0.fields.next.get(),
            head: self.head(),
            owner: PhantomData,
        }
    }
}

pub(crate) struct Finalizers<'a> {
    next: *mut VectorHeader,
    head: *mut VectorHeader,
    owner: PhantomData<&'a FinalizerList>,
}

impl Iterator for Finalizers<'_> {
    type Item = FinalizerRef;

    fn next(&mut self) -> Option<Self::Item> {
        if self.next == self.head {
            return None;
        }
        // SAFETY: the owner keeps the sentinel alive; collection visits
        // this list before sweeping any member. Save the next link so the
        // caller may move the yielded object to the doomed list.
        let object = unsafe { FinalizerRef::from_raw(self.next) };
        self.next = object.next.get();
        Some(object)
    }
}
