use super::BufferRef;
use std::cell::Cell;

/// lisp.h's Lisp_Marker payload, following the one-word vector header.
/// Both links are weak: buffer killing and the buffer sweep unlink markers
/// before the vector allocator frees either endpoint. Cells permit native
/// stores without creating aliased Rust mutable references.
#[repr(C)]
pub struct MarkerValue {
    buffer: Cell<Option<BufferRef>>,
    flags: Cell<u8>,
    padding: [u8; 7],
    next: Cell<Option<MarkerRef>>,
    charpos: Cell<usize>,
    bytepos: Cell<usize>,
}

pub type MarkerRef = crate::lisp::alloc::VectorlikeRef<MarkerValue>;

const _: () = {
    assert!(std::mem::size_of::<MarkerValue>() == 40);
    assert!(std::mem::offset_of!(MarkerValue, buffer) == 0);
    assert!(std::mem::offset_of!(MarkerValue, flags) == 8);
    assert!(std::mem::offset_of!(MarkerValue, next) == 16);
    assert!(std::mem::offset_of!(MarkerValue, charpos) == 24);
    assert!(std::mem::offset_of!(MarkerValue, bytepos) == 32);
};

impl MarkerRef {
    pub(crate) fn new() -> Self {
        crate::lisp::native_comp::note_lisp_allocation(48);
        Self::allocate(MarkerValue {
            buffer: Cell::new(None),
            flags: Cell::new(0),
            padding: [0; 7],
            next: Cell::new(None),
            charpos: Cell::new(0),
            bytepos: Cell::new(0),
        })
    }

    #[inline]
    pub fn buffer(&self) -> Option<BufferRef> {
        self.buffer.get()
    }

    #[inline]
    pub fn position(&self) -> Option<usize> {
        self.buffer.get().map(|_| self.charpos.get())
    }

    /// marker-last-position reads charpos even after unchain_marker.
    #[inline]
    pub fn last_position(&self) -> usize {
        self.charpos.get()
    }

    #[inline]
    pub fn byte_position(&self) -> Option<usize> {
        self.buffer.get().map(|_| self.bytepos.get())
    }

    /// The initialized bytepos field; a detached marker retains its last value.
    #[inline]
    pub(crate) fn bytepos(&self) -> usize {
        self.bytepos.get()
    }

    #[inline]
    pub fn insertion_type(&self) -> bool {
        self.flags.get() & 2 != 0
    }

    #[inline]
    pub fn set_insertion_type(&self, insertion_type: bool) {
        self.flags
            .set((self.flags.get() & !2) | (u8::from(insertion_type) << 1));
    }

    /// lisp.h's `need_adjustment', the bit before the insertion type:
    /// coding.c's region conversions mark the markers they put back.
    #[inline]
    pub(crate) fn need_adjustment(&self) -> bool {
        self.flags.get() & 1 != 0
    }

    #[inline]
    pub(crate) fn set_need_adjustment(&self, need_adjustment: bool) {
        self.flags
            .set((self.flags.get() & !1) | u8::from(need_adjustment));
    }

    pub(crate) fn set_positions(&self, charpos: usize, bytepos: usize) {
        self.charpos.set(charpos);
        self.bytepos.set(bytepos);
    }

    pub(crate) fn attach(&self, buffer: BufferRef, charpos: usize, bytepos: usize) {
        self.set_positions(charpos, bytepos);
        if self.buffer.get().is_some_and(|old| old.ptr_eq(&buffer)) {
            return;
        }
        self.detach();
        self.buffer.set(Some(buffer));
        let chain = buffer.chain_owner();
        self.next.set(chain.markers.get());
        chain.markers.set(Some(*self));
    }

    pub(crate) fn detach(&self) {
        let Some(buffer) = self.buffer.replace(None) else {
            return;
        };
        let buffer = buffer.chain_owner();
        let mut previous: Option<MarkerRef> = None;
        let mut current = buffer.markers.get();
        while let Some(marker) = current {
            let next = marker.next.get();
            if marker.ptr_eq(self) {
                if let Some(previous) = previous {
                    previous.next.set(next);
                } else {
                    buffer.markers.set(next);
                }
                self.next.set(None);
                return;
            }
            previous = Some(marker);
            current = next;
        }
        unreachable!("an attached marker is in its buffer's chain");
    }
}

impl PartialEq for MarkerRef {
    fn eq(&self, other: &Self) -> bool {
        self.ptr_eq(other)
    }
}

impl Eq for MarkerRef {}

impl std::fmt::Display for MarkerRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:x}", self.identity())
    }
}

pub(crate) struct Markers(Option<MarkerRef>);

impl Iterator for Markers {
    type Item = MarkerRef;
    fn next(&mut self) -> Option<Self::Item> {
        let marker = self.0?;
        self.0 = marker.next.get();
        Some(marker)
    }
}

impl BufferRef {
    /// The buffer whose chain holds this buffer's markers: its base while
    /// it shares the base's text (BUF_MARKERS of the one buffer_text).
    #[inline]
    pub(crate) fn chain_owner(&self) -> BufferRef {
        self.base.get().unwrap_or(*self)
    }

    /// BUF_MARKERS: every marker of the buffer's text, those of the rest
    /// of its indirect family among them.
    pub(crate) fn markers(&self) -> Markers {
        Markers(self.chain_owner().markers.get())
    }

    /// Unchain the buffer's markers: all of its text's for a buffer with
    /// its own text, only its own for an indirect buffer (Fkill_buffer).
    pub(crate) fn detach_markers(&self) {
        if let Some(base) = self.base.get() {
            let mut previous: Option<MarkerRef> = None;
            let mut current = base.markers.get();
            while let Some(marker) = current {
                let next = marker.next.get();
                if marker.buffer.get().is_some_and(|owner| owner.ptr_eq(self)) {
                    if let Some(previous) = previous {
                        previous.next.set(next);
                    } else {
                        base.markers.set(next);
                    }
                    marker.buffer.set(None);
                    marker.next.set(None);
                } else {
                    previous = Some(marker);
                }
                current = next;
            }
            return;
        }
        for marker in self.markers() {
            marker.buffer.set(None);
            marker.next.set(None);
        }
        self.markers.set(None);
    }

    /// Fmake_indirect_buffer's sharing of BASE's text: the buffer, which
    /// has no markers yet, takes its markers on BASE's chain.
    pub(crate) fn share_marker_chain(&self, base: BufferRef) {
        debug_assert!(self.markers.get().is_none());
        self.base.set(Some(base));
    }

    /// The buffer no longer shares a base's text (it was killed).
    pub(crate) fn leave_marker_chain(&self) {
        self.base.set(None);
    }

    /// buffer.c:Fbuffer_swap_text moves the weak chains with buffer_text and
    /// changes each marker's buffer pointer without changing its positions.
    pub(crate) fn swap_marker_chains(&self, other: BufferRef) {
        let chain = self.markers.replace(other.markers.get());
        other.markers.set(chain);
        for marker in self.markers() {
            marker.buffer.set(Some(*self));
        }
        for marker in other.markers() {
            marker.buffer.set(Some(other));
        }
    }

    /// Restore the order recorded by pdumper after relocating all endpoints.
    pub(crate) fn restore_marker_order(&self, chain: &[MarkerRef]) -> bool {
        let mut seen = std::collections::HashSet::with_capacity(chain.len());
        if chain.len() != self.markers().count()
            || chain.iter().any(|marker| {
                !marker
                    .buffer()
                    .is_some_and(|owner| owner.chain_owner().ptr_eq(self))
                    || !seen.insert(marker.identity())
            })
        {
            return false;
        }
        self.markers.set(chain.first().copied());
        for (index, marker) in chain.iter().enumerate() {
            marker.next.set(chain.get(index + 1).copied());
        }
        true
    }

    /// alloc.c:sweep_buffers unlinks weak chain entries before sweep_vectors.
    pub(crate) fn sweep_markers(&self, epoch: u32) {
        if !self.mark_bit().is_marked(epoch) {
            self.detach_markers();
            return;
        }
        if self.base.get().is_some() {
            // The base's sweep walks the one chain of their text.
            return;
        }
        let mut previous: Option<MarkerRef> = None;
        for marker in self.markers() {
            if marker.mark_bit().is_marked(epoch) {
                previous = Some(marker);
            } else {
                if let Some(previous) = previous {
                    previous.next.set(marker.next.get());
                } else {
                    self.markers.set(marker.next.get());
                }
                marker.buffer.set(None);
                marker.next.set(None);
            }
        }
    }

    pub(crate) fn mark_object(&self) -> Option<MarkerRef> {
        self.borrow().mark_object()
    }
}

impl MarkerValue {
    pub(crate) fn is_detached(&self) -> bool {
        self.buffer.get().is_none()
    }
}

impl std::fmt::Debug for MarkerValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MarkerValue")
            .field("buffer", &self.buffer.get())
            .field("charpos", &self.charpos.get())
            .field("bytepos", &self.bytepos.get())
            .field("flags", &self.flags.get())
            .finish()
    }
}
