//! alloc.c:allocate_record: a header followed by the type and data slots.
//! These are the fields used by Lisp, native code, tracing and image restore.

use super::*;

/// lisp.h:PSEUDOVECTOR_SIZE_MASK includes the record's type slot.
pub(crate) const MAX_RECORD_SLOTS: usize = PSEUDOVECTOR_SIZE_MASK;

#[repr(transparent)]
#[derive(Clone, Copy)]
pub struct LispRecordRef(NonNull<VectorHeader>);

impl LispRecordRef {
    pub(crate) fn new(type_tag: Value, slots: &[Value]) -> Self {
        Self::allocate(type_tag, slots.len(), |index| slots[index])
    }

    pub(crate) fn filled(type_tag: Value, data_slots: usize, initial: Value) -> Self {
        Self::allocate(type_tag, data_slots, |_| initial)
    }

    pub(crate) fn shallow_copy(&self) -> Self {
        Self::allocate(self.type_tag(), self.len() - 1, |index| {
            self.fields()[index + 1].get()
        })
    }

    fn allocate(type_tag: Value, data_slots: usize, field: impl Fn(usize) -> Value) -> Self {
        assert!(data_slots < MAX_RECORD_SLOTS);
        let count = data_slots + 1;
        let logical_bytes = HEADER_SIZE + count * WORD_SIZE;
        crate::lisp::native_comp::note_lisp_allocation(logical_bytes);
        let header = allocate_vectorlike(logical_bytes);
        // SAFETY: fresh aligned storage for the header and COUNT Lisp cells.
        unsafe {
            (*header).size =
                VectorHeader::pseudovector_slots_word(VectorTag::Record, logical_bytes, count);
            (*header)
                .mark_bit()
                .mark(super::super::super::types::current_mark_epoch());
            let fields = payload(header).cast::<Cell<Value>>();
            fields.write(Cell::new(type_tag));
            for index in 1..count {
                fields.add(index).write(Cell::new(field(index - 1)));
            }
            census_on_allocate(header);
            Self(NonNull::new_unchecked(header))
        }
    }

    /// # Safety
    /// HEADER names a live PVEC_RECORD with at least its inline type slot.
    pub(crate) unsafe fn from_raw(header: *mut VectorHeader) -> Self {
        Self(unsafe { NonNull::new_unchecked(header) })
    }

    /// Includes the type in slot zero, as GNU ASIZE does for records.
    #[inline]
    pub(crate) fn len(&self) -> usize {
        // SAFETY: the handle names a live record; its size never changes.
        unsafe { self.0.as_ref() }.size & PSEUDOVECTOR_SIZE_MASK
    }

    #[inline]
    fn fields(&self) -> &[Cell<Value>] {
        // SAFETY: LEN cells immediately follow this allocation's header.
        // Cell permits native stores without an outstanding Rust &Value.
        unsafe { std::slice::from_raw_parts(payload(self.0.as_ptr()).cast(), self.len()) }
    }

    #[inline]
    pub(crate) fn get(&self, index: usize) -> Option<Value> {
        self.fields().get(index).map(Cell::get)
    }

    #[inline]
    pub(crate) fn set(&self, index: usize, value: Value) -> bool {
        if let Some(field) = self.fields().get(index) {
            field.set(value);
            true
        } else {
            false
        }
    }

    #[inline]
    pub(crate) fn type_tag(&self) -> Value {
        self.fields()[0].get()
    }

    pub(crate) fn slots(&self) -> impl ExactSizeIterator<Item = Value> + DoubleEndedIterator + '_ {
        self.fields().iter().map(Cell::get)
    }

    #[inline]
    pub(crate) fn identity(&self) -> usize {
        self.0.as_ptr() as usize
    }

    #[inline]
    pub(crate) fn ptr_eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }

    #[inline]
    pub(crate) fn mark_bit(&self) -> VectorMark<'_> {
        // SAFETY: the object is live for the handle's use.
        unsafe { self.0.as_ref() }.mark_bit()
    }
}

impl std::fmt::Debug for LispRecordRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // A slot can lead back here; do not recursively inspect its fields.
        f.debug_tuple("LispRecord").field(&self.identity()).finish()
    }
}

/// The remaining host pseudovector adapter has no inline Lisp fields. Every
/// actual record has at least one. Remove this distinction when those host
/// kinds have their own GNU layouts and tags.
///
/// # Safety
/// HEADER is a live vector header already identified as PVEC_RECORD.
pub(crate) unsafe fn record_has_inline_slots(header: *mut VectorHeader) -> bool {
    unsafe { (*header).size & PSEUDOVECTOR_SIZE_MASK != 0 }
}
