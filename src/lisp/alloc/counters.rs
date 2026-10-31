//! alloc.c's cumulative allocation counters: `cons_cells_consed',
//! `floats_consed', `vector_cells_consed', `symbols_consed',
//! `string_chars_consed', `intervals_consed' and `strings_consed'.
//!
//! Each is one process-wide integer, raised at the allocation point GNU
//! raises it (Fcons, make_float, allocate_vectorlike, Fmake_symbol,
//! make_clear_multibyte_string/allocate_string, make_interval).
//! Collection never lowers them. Their DEFVAR_INT symbols forward to these
//! words (see `SymbolCell::forward_to_counter'), so Lisp reads, stores and
//! dynamic bindings act on the counters themselves, as lread.c:defvar_int
//! arranges for the C variables.
//!
//! The allocator's other books use the same relaxed load/store pairs: the
//! mutator is serialized, as GNU's is.
//!
//! An object's intervals are a sequence (see intervals.rs), not one
//! allocation each, so `intervals-consed' is raised where intervals.c
//! calls make_interval: create_root_interval, split_interval_left and
//! split_interval_right, and copy_intervals and reproduce_tree for every
//! interval they copy.

use std::sync::atomic::{AtomicI64, Ordering};

pub(crate) static CONS_CELLS_CONSED: AtomicI64 = AtomicI64::new(0);
pub(crate) static FLOATS_CONSED: AtomicI64 = AtomicI64::new(0);
pub(crate) static VECTOR_CELLS_CONSED: AtomicI64 = AtomicI64::new(0);
pub(crate) static SYMBOLS_CONSED: AtomicI64 = AtomicI64::new(0);
pub(crate) static STRING_CHARS_CONSED: AtomicI64 = AtomicI64::new(0);
pub(crate) static INTERVALS_CONSED: AtomicI64 = AtomicI64::new(0);
pub(crate) static STRINGS_CONSED: AtomicI64 = AtomicI64::new(0);

/// The DEFVAR_INT names, in `memory-use-counts' order, with their words.
pub(crate) static FORWARDED_COUNTERS: [(&str, &AtomicI64); 7] = [
    ("cons-cells-consed", &CONS_CELLS_CONSED),
    ("floats-consed", &FLOATS_CONSED),
    ("vector-cells-consed", &VECTOR_CELLS_CONSED),
    ("symbols-consed", &SYMBOLS_CONSED),
    ("string-chars-consed", &STRING_CHARS_CONSED),
    ("intervals-consed", &INTERVALS_CONSED),
    ("strings-consed", &STRINGS_CONSED),
];

/// `counter += amount' on a C intmax_t.
#[inline(always)]
pub(crate) fn tally(counter: &AtomicI64, amount: i64) {
    counter.store(
        counter.load(Ordering::Relaxed).wrapping_add(amount),
        Ordering::Relaxed,
    );
}

/// alloc.c:make_interval's books for COUNT new intervals:
/// `intervals_consed' and tally_consing of `sizeof (struct interval)'.
pub(crate) fn note_intervals(count: usize) {
    tally(&INTERVALS_CONSED, count as i64);
    crate::lisp::native_comp::note_lisp_allocation(
        count.saturating_mul(crate::lisp::eval::GNU_INTERVAL_SIZE),
    );
}

/// alloc.c:Fmemory_use_counts: the seven counters as they stand.
pub(crate) fn memory_use_counts() -> [i64; 7] {
    FORWARDED_COUNTERS.map(|(_, counter)| counter.load(Ordering::Relaxed))
}
