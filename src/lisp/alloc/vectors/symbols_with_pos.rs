//! lisp.h:Lisp_Symbol_With_Pos and alloc.c:build_symbol_with_pos.
//! The header and two Lisp fields are the allocation used by every runtime.

use super::*;

#[repr(transparent)]
#[derive(Clone, Copy)]
pub struct SymbolWithPosRef(NonNull<VectorHeader>);

impl SymbolWithPosRef {
    pub(crate) fn allocate(symbol: Value, position: Value) -> Self {
        let logical_bytes = HEADER_SIZE + 2 * WORD_SIZE;
        // GNU allocate_vector(2) charges the three Lisp words. Word-aligned
        // storage needs no additional padding for this header and payload.
        crate::lisp::native_comp::note_lisp_allocation(logical_bytes);
        let header = allocate_vectorlike(logical_bytes);
        // SAFETY: fresh storage for the header and both aligned Cell<Value>s.
        unsafe {
            (*header).size =
                VectorHeader::pseudovector_slots_word(VectorTag::SymbolWithPos, logical_bytes, 2);
            (*header)
                .mark_bit()
                .mark(super::super::super::types::current_mark_epoch());
            let fields = payload(header).cast::<Cell<Value>>();
            fields.write(Cell::new(symbol));
            fields.add(1).write(Cell::new(position));
            census_on_allocate(header);
            Self(NonNull::new_unchecked(header))
        }
    }

    /// # Safety
    /// HEADER names a live PVEC_SYMBOL_WITH_POS allocation.
    pub(crate) unsafe fn from_raw(header: *mut VectorHeader) -> Self {
        Self(unsafe { NonNull::new_unchecked(header) })
    }

    #[inline]
    fn fields(&self) -> &[Cell<Value>; 2] {
        // SAFETY: each handle names a live object with exactly two fields.
        unsafe { &*payload(self.0.as_ptr()).cast() }
    }

    #[inline]
    pub(crate) fn symbol(&self) -> Value {
        self.fields()[0].get()
    }

    #[inline]
    pub(crate) fn position(&self) -> Value {
        self.fields()[1].get()
    }

    /// Fill a graph copy after publishing its identity, preserving cycles.
    #[cfg(test)]
    pub(crate) fn initialize(&self, symbol: Value, position: Value) {
        self.fields()[0].set(symbol);
        self.fields()[1].set(position);
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

impl std::fmt::Debug for SymbolWithPosRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SymbolWithPos")
            .field("symbol", &self.symbol())
            .field("position", &self.position())
            .finish()
    }
}
