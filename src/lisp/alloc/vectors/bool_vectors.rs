//! lisp.h's `struct Lisp_Bool_Vector': a header, the length in bits, then
//! the bits, BOOL_VECTOR_BITS_PER_CHAR to a byte from the low bit up, the
//! last word's bits past the length zero (alloc.c:make_uninit_bool_vector).

use super::*;

/// lisp.h's `bits_word', BITS_PER_BITS_WORD of them a word.
pub(crate) type BitsWord = usize;
pub(crate) const BITS_PER_BITS_WORD: usize = BitsWord::BITS as usize;

/// lisp.h's `bool_header_size': the header and the length.
const BOOL_HEADER_SIZE: usize = HEADER_SIZE + WORD_SIZE;

/// lisp.h's `bool_vector_words'.
pub(crate) const fn bool_vector_words(nbits: usize) -> usize {
    nbits.div_ceil(BITS_PER_BITS_WORD)
}

/// alloc.c's `vector_nbytes' for a bool vector of NBITS.
pub(super) const fn bool_vector_nbytes(nbits: usize) -> usize {
    vroundup(BOOL_HEADER_SIZE + bool_vector_words(nbits) * WORD_SIZE)
}

#[repr(transparent)]
#[derive(Clone, Copy)]
pub struct BoolVectorRef(NonNull<VectorHeader>);

impl BoolVectorRef {
    /// alloc.c's `make_uninit_bool_vector' with every bit clear.
    pub(crate) fn new(nbits: usize) -> Self {
        let nbytes = BOOL_HEADER_SIZE + bool_vector_words(nbits) * WORD_SIZE;
        crate::lisp::native_comp::note_lisp_allocation(nbytes);
        let header = allocate_vectorlike(nbytes);
        // SAFETY: fresh storage for the header, the length and the words;
        // the length, which gives the footprint the mark's place depends
        // on, is written before the mark.
        unsafe {
            (*header).size =
                PSEUDOVECTOR_FLAG | ((VectorTag::BoolVector as usize) << PSEUDOVECTOR_AREA_BITS);
            let body = payload(header).cast::<usize>();
            body.write(nbits);
            std::ptr::write_bytes(body.add(1), 0, bool_vector_words(nbits));
            (*header)
                .mark_bit()
                .mark(super::super::super::types::current_mark_epoch());
            census_on_allocate(header);
            Self(NonNull::new_unchecked(header))
        }
    }

    /// Fmake_bool_vector: NBITS bits, each INIT.
    pub(crate) fn filled(nbits: usize, init: bool) -> Self {
        let vector = Self::new(nbits);
        vector.fill(init);
        vector
    }

    /// Fbool_vector: a bit for each of BITS.
    pub(crate) fn from_bits(bits: impl ExactSizeIterator<Item = bool>) -> Self {
        let vector = Self::new(bits.len());
        for (index, bit) in bits.enumerate() {
            vector.set(index, bit);
        }
        vector
    }

    /// fns.c:Fcopy_sequence's copy: a new bool vector of VECTOR's bits.
    pub(crate) fn copy(vector: Self) -> Self {
        let copy = Self::new(vector.len());
        for (to, from) in copy.words().iter().zip(vector.words()) {
            to.set(from.get());
        }
        copy
    }

    /// # Safety
    /// HEADER names a live PVEC_BOOL_VECTOR.
    pub(crate) unsafe fn from_raw(header: *mut VectorHeader) -> Self {
        Self(unsafe { NonNull::new_unchecked(header) })
    }

    /// lisp.h's `bool_vector_size'.
    #[inline]
    pub(crate) fn len(&self) -> usize {
        // SAFETY: the length word follows the header of a live bool vector.
        unsafe { payload(self.0.as_ptr()).cast::<usize>().read() }
    }

    /// lisp.h's `bool_vector_data': the words of the bits.
    #[inline]
    pub(crate) fn words(&self) -> &[Cell<BitsWord>] {
        // SAFETY: bool_vector_words (len) words follow the length word.
        unsafe {
            std::slice::from_raw_parts(
                payload(self.0.as_ptr()).add(WORD_SIZE).cast(),
                bool_vector_words(self.len()),
            )
        }
    }

    /// lisp.h's `bool_vector_bitref'; None past the end.
    #[inline]
    pub(crate) fn get(&self, index: usize) -> Option<bool> {
        (index < self.len()).then(|| {
            self.words()[index / BITS_PER_BITS_WORD].get() >> (index % BITS_PER_BITS_WORD) & 1 != 0
        })
    }

    /// lisp.h's `bool_vector_set'; INDEX is below the length.
    #[inline]
    pub(crate) fn set(&self, index: usize, bit: bool) {
        assert!(index < self.len(), "a bool vector's bit past its end");
        let word = &self.words()[index / BITS_PER_BITS_WORD];
        let mask = 1 << (index % BITS_PER_BITS_WORD);
        word.set(if bit {
            word.get() | mask
        } else {
            word.get() & !mask
        });
    }

    /// alloc.c's `bool_vector_fill'.
    pub(crate) fn fill(&self, init: bool) {
        let nbits = self.len();
        let words = self.words();
        for word in words {
            word.set(if init { BitsWord::MAX } else { 0 });
        }
        if init
            && let Some(last) = words.last()
            && !nbits.is_multiple_of(BITS_PER_BITS_WORD)
        {
            last.set(last.get() & ((1 << (nbits % BITS_PER_BITS_WORD)) - 1));
        }
    }

    /// The bits in order.
    pub(crate) fn bits(&self) -> impl ExactSizeIterator<Item = bool> + '_ {
        (0..self.len()).map(|index| self.get(index).expect("a bit below the length"))
    }

    #[inline]
    pub(crate) fn identity(&self) -> usize {
        self.0.as_ptr() as usize
    }

    #[inline]
    pub(crate) fn mark_bit(&self) -> VectorMark<'_> {
        // SAFETY: the object is live for the handle's use.
        unsafe { self.0.as_ref() }.mark_bit()
    }
}

impl std::fmt::Debug for BoolVectorRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("BoolVector").field(&self.identity()).finish()
    }
}
