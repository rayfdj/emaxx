//! pdumper.c's dump_public (where each mapped dump is) and dump_private's
//! mark_bits and last_mark_bits, for the image's mapped objects, which no
//! sweep visits.
//!
//! GNU never sweeps its dump: a dumped object that dies stays where it is
//! and is not counted.  Its mark is a bit of mark_bits, one for each 8-byte
//! word of the dump (DUMP_ALIGNMENT), never in the object, and
//! pdumper_clear_marks, the end of gc_sweep, makes mark_bits the last ones
//! and clears the next.  The conservative scan takes a word for a dumped
//! object only if the last collection marked it
//! (pdumper_find_object_type_impl's test of last_mark_bits, which the load
//! sets all ones), so a dumped object that died is never reached again
//! and the heap objects it held may be freed.
//!
//! GNU has one dump in a process.  Here every image load maps one, and an
//! image once loaded stays for the life of the process (the loader makes
//! it permanent before it adopts a block), so the regions are a list,
//! sorted by address and only ever added to: the test binary loads an
//! image for each fresh interpreter.  A region is entered before any of
//! its objects is a heap object and never leaves, so no later mapping can
//! share an address with one listed or read another's bits.
//!
//! The image's rebuilt cells' counts (`ImageCells') are kept here too.
//!
//! Nothing here takes a lock: a mark, the conservative scan, a sweep and
//! a load all run on the OS thread that holds the runtime (alloc.c's
//! objects are global_lock's; `register' checks it), so no two of them
//! overlap, and only that thread reads or writes a region's bits.

use super::ImageCells;
use std::cell::Cell;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};

/// The dump's word, DUMP_ALIGNMENT: a mapped object starts on one.
const ALIGNMENT: usize = 8;

/// One mapped image's hot section and its two bitsets.
struct Region {
    start: usize,
    end: usize,
    /// Which of `bits' is mark_bits; the other is last_mark_bits.
    current: Cell<usize>,
    bits: [Box<[Cell<usize>]>; 2],
    /// Whether a collection has ended since the load: until then the last
    /// marks are all set (dump_bitsets_init's last_mark_bits), which this
    /// stands for, so that neither bitset is written before a mark is.
    collected: Cell<bool>,
}

impl Region {
    #[inline(always)]
    fn bit(&self, address: usize) -> (usize, usize) {
        let bit = (address - self.start) / ALIGNMENT;
        (
            bit / usize::BITS as usize,
            1 << (bit % usize::BITS as usize),
        )
    }

    #[inline(always)]
    fn words(&self, last: bool) -> &[Cell<usize>] {
        &self.bits[self.current.get() ^ usize::from(last)]
    }
}

/// pdumper.c's dump_public and dump_private, for each mapped image.
pub(crate) struct DumpPrivate {
    /// The regions, a leaked sorted list replaced whole when one is added
    /// (null before the first).
    regions: AtomicPtr<Vec<Region>>,
    /// The lowest start and the highest end of the regions, dump_public's
    /// start and end: the test of an address no region holds.
    low: AtomicUsize,
    high: AtomicUsize,
    /// The index of the region last found, tried first: a collection
    /// marks one image's objects in runs.
    hint: AtomicUsize,
    /// The only region's start, length and current mark_bits, which the
    /// mark path of a process with one image reads alone (a length of 0
    /// while there is none, or more than one).
    only_start: AtomicUsize,
    only_len: AtomicUsize,
    only_marks: AtomicPtr<Cell<usize>>,
    /// The rebuilt strings and vectors of the images.
    pub(crate) strings: ImageCells,
    pub(crate) vectors: ImageCells,
}

pub(crate) static DUMP: DumpPrivate = DumpPrivate {
    regions: AtomicPtr::new(std::ptr::null_mut()),
    low: AtomicUsize::new(usize::MAX),
    high: AtomicUsize::new(0),
    hint: AtomicUsize::new(0),
    only_start: AtomicUsize::new(0),
    only_len: AtomicUsize::new(0),
    only_marks: AtomicPtr::new(std::ptr::null_mut()),
    strings: ImageCells::new(),
    vectors: ImageCells::new(),
};

impl DumpPrivate {
    #[inline(always)]
    fn regions(&self) -> &[Region] {
        let list = self.regions.load(Ordering::Relaxed);
        if list.is_null() {
            return &[];
        }
        // SAFETY: a list `register' leaked; it frees a list only once it
        // has made the successor, on the thread that holds the runtime, as
        // every reader is.
        unsafe { &*list }
    }

    /// The region holding ADDRESS, if any.
    #[inline(always)]
    fn region(&self, address: usize) -> Option<&Region> {
        if address < self.low.load(Ordering::Relaxed)
            || address >= self.high.load(Ordering::Relaxed)
        {
            return None;
        }
        let regions = self.regions();
        if let [only] = regions {
            return Some(only);
        }
        if let Some(region) = regions.get(self.hint.load(Ordering::Relaxed))
            && region.start <= address
            && address < region.end
        {
            return Some(region);
        }
        let index = regions
            .partition_point(|region| region.start <= address)
            .checked_sub(1)?;
        let region = regions.get(index).filter(|region| address < region.end)?;
        self.hint.store(index, Ordering::Relaxed);
        Some(region)
    }

    /// pdumper_object_p: whether ADDRESS is in a mapped image's hot section.
    #[inline(always)]
    pub(crate) fn contains(&self, address: usize) -> bool {
        self.region(address).is_some()
    }

    /// The offset of ADDRESS in the only region, when there is one region
    /// and it holds ADDRESS.
    #[inline(always)]
    fn only_offset(&self, address: usize) -> Option<usize> {
        let offset = address.wrapping_sub(self.only_start.load(Ordering::Relaxed));
        (offset < self.only_len.load(Ordering::Relaxed)).then_some(offset)
    }

    /// The only region's mark_bits word and mask for OFFSET.
    ///
    /// # Safety
    /// OFFSET is one `only_offset' returned.
    #[inline(always)]
    unsafe fn only_bit(&self, offset: usize) -> (&Cell<usize>, usize) {
        let bit = offset / ALIGNMENT;
        // SAFETY: `only_marks' is the only region's current mark_bits, a
        // slice of `only_len' / ALIGNMENT bits that lives as long as the
        // process; BIT is below that.
        let word = unsafe {
            &*self
                .only_marks
                .load(Ordering::Relaxed)
                .add(bit / usize::BITS as usize)
        };
        (word, 1 << (bit % usize::BITS as usize))
    }

    /// pdumper_set_marked of the mapped object at ADDRESS: true when it was
    /// not yet marked in this collection.
    #[inline(always)]
    pub(crate) fn mark(&self, address: usize) -> bool {
        if let Some(offset) = self.only_offset(address) {
            // SAFETY: an offset `only_offset' returned.
            let (word, mask) = unsafe { self.only_bit(offset) };
            let old = word.get();
            word.set(old | mask);
            return old & mask == 0;
        }
        self.mark_in_regions(address)
    }

    #[inline(never)]
    fn mark_in_regions(&self, address: usize) -> bool {
        let region = self.region(address).expect("a mapped object");
        let (word, mask) = region.bit(address);
        debug_assert!(
            !region.collected.get() || region.words(true)[word].get() & mask != 0,
            "marking a mapped object the last collection did not mark"
        );
        let cell = &region.words(false)[word];
        let old = cell.get();
        cell.set(old | mask);
        old & mask == 0
    }

    /// pdumper_marked_p of the mapped object at ADDRESS.
    #[inline(always)]
    pub(crate) fn is_marked(&self, address: usize) -> bool {
        if let Some(offset) = self.only_offset(address) {
            // SAFETY: an offset `only_offset' returned.
            let (word, mask) = unsafe { self.only_bit(offset) };
            return word.get() & mask != 0;
        }
        self.is_marked_in_regions(address)
    }

    #[inline(never)]
    fn is_marked_in_regions(&self, address: usize) -> bool {
        let region = self.region(address).expect("a mapped object");
        let (word, mask) = region.bit(address);
        region.words(false)[word].get() & mask != 0
    }

    /// pdumper_find_object_type_impl's test: whether the last collection
    /// marked the mapped object at ADDRESS (every object, before one has).
    #[inline]
    pub(crate) fn marked_last(&self, address: usize) -> bool {
        let region = self.region(address).expect("a mapped object");
        if !region.collected.get() {
            return true;
        }
        let (word, mask) = region.bit(address);
        region.words(true)[word].get() & mask != 0
    }

    /// pdumper_clear_marks: this collection's marks become the last ones,
    /// and the next collection's start clear.
    pub(crate) fn clear_marks(&self) {
        for region in self.regions() {
            region.current.set(region.current.get() ^ 1);
            for word in region.words(false) {
                if word.get() != 0 {
                    word.set(0);
                }
            }
            region.collected.set(true);
        }
        self.cache_first();
    }

    /// The only region's start, length and current mark_bits, for
    /// `only_offset' and `only_bit'.
    fn cache_first(&self) {
        match self.regions() {
            [only] => {
                self.only_start.store(only.start, Ordering::Relaxed);
                self.only_marks
                    .store(only.words(false).as_ptr().cast_mut(), Ordering::Relaxed);
                self.only_len
                    .store(only.end - only.start, Ordering::Relaxed);
            }
            _ => self.only_len.store(0, Ordering::Relaxed),
        }
    }

    /// Enter the hot section from START to END of an image just mapped,
    /// before any of its objects is adopted: no mark yet, and every object
    /// marked last (dump_bitsets_init), both bitsets zeroed memory that
    /// nothing writes before a collection marks.
    pub(crate) fn register(&self, start: usize, end: usize) {
        crate::lisp::runtime::ensure_held();
        assert!(
            start.is_multiple_of(ALIGNMENT) && start < end,
            "a mapped image's hot section"
        );
        let words = (end - start).div_ceil(ALIGNMENT * usize::BITS as usize);
        let bitset = || {
            // SAFETY: a zero word is a valid `Cell<usize>'.
            unsafe { Box::<[Cell<usize>]>::new_zeroed_slice(words).assume_init() }
        };
        let region = Region {
            start,
            end,
            current: Cell::new(0),
            bits: [bitset(), bitset()],
            collected: Cell::new(false),
        };
        let old = self.regions.load(Ordering::Relaxed);
        let mut list = if old.is_null() {
            Vec::new()
        } else {
            // SAFETY: the current list, which nothing else reads while
            // this thread holds the runtime; its successor replaces it.
            *unsafe { Box::from_raw(old) }
        };
        assert!(
            list.iter()
                .all(|other| end <= other.start || other.end <= start),
            "a mapped image overlaps one already listed"
        );
        let index = list.partition_point(|other| other.start < start);
        list.insert(index, region);
        self.regions
            .store(Box::into_raw(Box::new(list)), Ordering::Relaxed);
        self.low.fetch_min(start, Ordering::Relaxed);
        self.high.fetch_max(end, Ordering::Relaxed);
        self.cache_first();
    }

    /// For tests: how many regions are listed.
    #[cfg(test)]
    pub(crate) fn regions_listed(&self) -> usize {
        crate::lisp::runtime::ensure_held();
        self.regions().len()
    }
}
