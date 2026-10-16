//! sort.c: GNU's Timsort, CPython's listsort with the "powersort" merge
//! strategy (https://github.com/python/cpython/blob/main/Objects/listsort.txt),
//! ported so a sort predicate sees exactly GNU's comparisons, in GNU's
//! order.  `inorder' is the only comparison; a signal from it leaves the
//! sort at once, the entries as sort.c leaves a vector's slots (see
//! `MergeState::cleanup').  The entries are the caller's rooted storage, an item and
//! its key (the item itself without a key function), moved together as
//! sort.c's sortslice moves keys and values; the merges' temporary array
//! is rooted too (sort.c's merge_markmem).

use crate::lisp::alloc::RootedVec;
use crate::lisp::types::{LispError, Value};

/// An item and its sort key.
pub(crate) type Entry = (Value, Value);

/// sort.c's MAX_MERGE_PENDING: floor(log2(n)) + 1 stretches at most.
const MAX_MERGE_PENDING: usize = usize::BITS as usize;

/// Once galloping, stay there while both runs win at least this many
/// consecutive times.
const GALLOP_WIN_MIN: usize = 7;

/// sort.c's MERGESTATE_TEMP_SIZE: the merge room on the C stack.
const MERGESTATE_TEMP_SIZE: usize = 256;

#[derive(Clone, Copy, Default)]
struct Stretch {
    base: usize,
    len: usize,
    power: u32,
}

/// sort.c's merge_state.
struct MergeState<F> {
    inorder: F,
    listlen: usize,
    pending: [Stretch; MAX_MERGE_PENDING],
    n: usize,
    min_gallop: usize,
    /// sort.c's `a': room for the shorter run of a merge.
    temp: RootedVec<Entry>,
    /// sort.c's `alloced', the entries its merge room holds: its stack
    /// array's share, until a merge needs more.
    alloced: usize,
    /// Whether sort.c's cleanup_mem is registered: from the start when
    /// the keys were allocated, else from the first merge that outgrows
    /// the stack array.  Only then does a signal put a merge's temporary
    /// run back; before, the run is lost and the stale copies it left
    /// stay (the C stack array is simply abandoned).
    cleanup: bool,
}

/// Sort ENTRIES by their keys, where INORDER answers whether A < B
/// (sort.c:tim_sort after the keys are computed and the input reversed
/// if asked).  KEYED says a key function computed the keys, which sort.c
/// keeps apart from the items.
pub(crate) fn tim_sort(
    entries: &mut [Entry],
    keyed: bool,
    inorder: impl FnMut(Value, Value) -> Result<bool, LispError>,
) -> Result<(), LispError> {
    let length = entries.len();
    if length < 2 {
        return Ok(());
    }
    // sort.c:merge_init.  With keys, the stack array holds them when
    // they fit in half of it, and the merge room is what the rest allows.
    let (alloced, cleanup) = if keyed {
        (
            length.div_ceil(2).min(MERGESTATE_TEMP_SIZE / 2),
            length >= MERGESTATE_TEMP_SIZE / 2,
        )
    } else {
        (MERGESTATE_TEMP_SIZE, false)
    };
    let mut ms = MergeState {
        inorder,
        listlen: length,
        pending: [Stretch::default(); MAX_MERGE_PENDING],
        n: 0,
        min_gallop: GALLOP_WIN_MIN,
        temp: RootedVec::new(),
        alloced,
        cleanup,
    };
    // March over the array once, left to right, finding natural runs,
    // and extending short natural runs to minrun elements.
    let minrun = merge_compute_minrun(length);
    let mut lo = 0;
    let mut nremaining = length;
    while nremaining > 0 {
        let (mut n, descending) = ms.count_run(entries, lo, lo + nremaining)?;
        if descending {
            entries[lo..lo + n].reverse();
        }
        // If the run is short, extend it to min(minrun, nremaining).
        if n < minrun {
            let force = nremaining.min(minrun);
            ms.binarysort(entries, lo, lo + force, lo + n)?;
            n = force;
        }
        ms.found_new_run(entries, n)?;
        debug_assert!(ms.n < MAX_MERGE_PENDING);
        ms.pending[ms.n] = Stretch {
            base: lo,
            len: n,
            power: 0,
        };
        ms.n += 1;
        lo += n;
        nremaining -= n;
    }
    ms.merge_force_collapse(entries)?;
    debug_assert!(ms.n == 1 && ms.pending[0].len == length);
    Ok(())
}

/// A good minimum run length: N below 64; 32 for a power of two; else a
/// K in [32, 64] such that N/K is close to, but strictly less than, a
/// power of two.
fn merge_compute_minrun(mut n: usize) -> usize {
    let mut r = 0;
    while n >= 64 {
        r |= n & 1;
        n >>= 1;
    }
    n + r
}

/// The "power" of the first of two adjacent runs beginning at S1, of
/// lengths N1 and N2, in a list of N: its depth in the merge tree.
fn powerloop(s1: usize, n1: usize, n2: usize, n: usize) -> u32 {
    debug_assert!(n1 > 0 && n2 > 0 && s1 + n1 + n2 <= n);
    // The midpoints doubled: a = 2*s1 + n1, b = a + n1 + n2.
    let mut a = 2 * s1 + n1;
    let mut b = a + n1 + n2;
    let mut result = 0;
    // Emulate a/n and b/n one bit a time, until their bits differ.
    loop {
        result += 1;
        if a >= n {
            // Both quotient bits are 1.
            a -= n;
            b -= n;
        } else if b >= n {
            // The a/n bit is 0 and the b/n bit 1.
            break;
        }
        a <<= 1;
        b <<= 1;
    }
    result
}

impl<F: FnMut(Value, Value) -> Result<bool, LispError>> MergeState<F> {
    /// Whether A < B by the order predicate.
    #[inline]
    fn lt(&mut self, a: Value, b: Value) -> Result<bool, LispError> {
        (self.inorder)(a, b)
    }

    /// Stable binary insertion sort of [LO, HI), whose [LO, START) is
    /// already sorted.
    fn binarysort(
        &mut self,
        list: &mut [Entry],
        lo: usize,
        hi: usize,
        mut start: usize,
    ) -> Result<(), LispError> {
        debug_assert!(lo <= start && start <= hi);
        if lo == start {
            start += 1;
        }
        while start < hi {
            let mut l = lo;
            let mut r = start;
            let pivot = list[r];
            loop {
                let p = l + ((r - l) >> 1);
                if self.lt(pivot.1, list[p].1)? {
                    r = p;
                } else {
                    l = p + 1;
                }
                if l >= r {
                    break;
                }
            }
            list.copy_within(l..start, l + 1);
            list[l] = pivot;
            start += 1;
        }
        Ok(())
    }

    /// The length of the run starting at LO in [LO, HI): the longest
    /// non-decreasing sequence, or the longest strictly decreasing one
    /// (DESCENDING), which can be reversed without breaking stability.
    fn count_run(
        &mut self,
        list: &[Entry],
        lo: usize,
        hi: usize,
    ) -> Result<(usize, bool), LispError> {
        debug_assert!(lo < hi);
        let mut i = lo + 1;
        if i == hi {
            return Ok((1, false));
        }
        let mut n = 2;
        let descending = self.lt(list[i].1, list[i - 1].1)?;
        i += 1;
        while i < hi {
            if self.lt(list[i].1, list[i - 1].1)? != descending {
                break;
            }
            i += 1;
            n += 1;
        }
        Ok((n, descending))
    }

    /// The insertion point of KEY in the sorted A, left of any equal
    /// elements, searching out from HINT: the K with A[K-1] < KEY <= A[K].
    fn gallop_left(&mut self, key: Value, a: &[Entry], hint: usize) -> Result<usize, LispError> {
        let n = a.len() as isize;
        let hint = hint as isize;
        debug_assert!(n > 0 && hint < n);
        let at = |index: isize| a[index as usize].1;
        let mut lastofs: isize = 0;
        let mut ofs: isize = 1;
        if self.lt(at(hint), key)? {
            // a[hint] < key: gallop right until
            // a[hint + lastofs] < key <= a[hint + ofs].
            let maxofs = n - hint;
            while ofs < maxofs {
                if self.lt(at(hint + ofs), key)? {
                    lastofs = ofs;
                    ofs = (ofs << 1) + 1;
                } else {
                    break;
                }
            }
            ofs = ofs.min(maxofs);
            lastofs += hint;
            ofs += hint;
        } else {
            // key <= a[hint]: gallop left until
            // a[hint - ofs] < key <= a[hint - lastofs].
            let maxofs = hint + 1;
            while ofs < maxofs {
                if self.lt(at(hint - ofs), key)? {
                    break;
                }
                lastofs = ofs;
                ofs = (ofs << 1) + 1;
            }
            ofs = ofs.min(maxofs);
            let k = lastofs;
            lastofs = hint - ofs;
            ofs = hint - k;
        }
        // a[lastofs] < key <= a[ofs]: binary search, with the invariant
        // a[lastofs - 1] < key <= a[ofs].
        lastofs += 1;
        while lastofs < ofs {
            let m = lastofs + ((ofs - lastofs) >> 1);
            if self.lt(at(m), key)? {
                lastofs = m + 1;
            } else {
                ofs = m;
            }
        }
        Ok(ofs as usize)
    }

    /// `gallop_left', but right of any elements equal to KEY: the K with
    /// A[K-1] <= KEY < A[K].
    fn gallop_right(&mut self, key: Value, a: &[Entry], hint: usize) -> Result<usize, LispError> {
        let n = a.len() as isize;
        let hint = hint as isize;
        debug_assert!(n > 0 && hint < n);
        let at = |index: isize| a[index as usize].1;
        let mut lastofs: isize = 0;
        let mut ofs: isize = 1;
        if self.lt(key, at(hint))? {
            // key < a[hint]: gallop left until
            // a[hint - ofs] <= key < a[hint - lastofs].
            let maxofs = hint + 1;
            while ofs < maxofs {
                if self.lt(key, at(hint - ofs))? {
                    lastofs = ofs;
                    ofs = (ofs << 1) + 1;
                } else {
                    break;
                }
            }
            ofs = ofs.min(maxofs);
            let k = lastofs;
            lastofs = hint - ofs;
            ofs = hint - k;
        } else {
            // a[hint] <= key: gallop right until
            // a[hint + lastofs] <= key < a[hint + ofs].
            let maxofs = n - hint;
            while ofs < maxofs {
                if self.lt(key, at(hint + ofs))? {
                    break;
                }
                lastofs = ofs;
                ofs = (ofs << 1) + 1;
            }
            ofs = ofs.min(maxofs);
            lastofs += hint;
            ofs += hint;
        }
        // a[lastofs] <= key < a[ofs]: binary search, with the invariant
        // a[lastofs - 1] <= key < a[ofs].
        lastofs += 1;
        while lastofs < ofs {
            let m = lastofs + ((ofs - lastofs) >> 1);
            if self.lt(key, at(m))? {
                ofs = m;
            } else {
                lastofs = m + 1;
            }
        }
        Ok(ofs as usize)
    }

    /// Room for NEED entries in the temporary array.
    fn needmem(&mut self, need: usize) {
        // sort.c:merge_getmem registers the cleanup with its first heap
        // allocation.
        if need > self.alloced {
            self.alloced = need;
            self.cleanup = true;
        }
        if self.temp.len() < need {
            // The longest merge needs half the list, rounded up.
            let size = need.max(self.listlen.div_ceil(2));
            self.temp.clear();
            self.temp
                .extend(std::iter::repeat_n((Value::Nil, Value::Nil), size));
        }
    }

    /// Stably merge, in place, the NA entries at SSA with the NB at
    /// SSB = SSA + NA, NA <= NB, where LIST[SSA + NA - 1] belongs at the
    /// end of the merge.
    fn merge_lo(
        &mut self,
        list: &mut [Entry],
        ssa: usize,
        mut na: usize,
        mut ssb: usize,
        mut nb: usize,
    ) -> Result<(), LispError> {
        debug_assert!(na > 0 && nb > 0 && ssa + na == ssb);
        self.needmem(na);
        let mut temp = std::mem::take(&mut self.temp);
        let result = self.merge_lo_with(list, &mut temp, ssa, &mut na, &mut ssb, &mut nb);
        self.temp = temp;
        result
    }

    fn merge_lo_with(
        &mut self,
        list: &mut [Entry],
        temp: &mut [Entry],
        mut dest: usize,
        na: &mut usize,
        ssb: &mut usize,
        nb: &mut usize,
    ) -> Result<(), LispError> {
        enum Exit {
            Succeed,
            CopyB,
        }
        temp[..*na].copy_from_slice(&list[dest..dest + *na]);
        let mut ssa = 0;
        list[dest] = list[*ssb];
        dest += 1;
        *ssb += 1;
        *nb -= 1;
        // sort.c's reloc: a signal leaves A's unmerged entries, still in
        // TEMP, where the merge would put them, so the list holds every
        // entry once (cleanup_mem).
        macro_rules! relocating {
            ($result:expr) => {
                match $result {
                    Ok(value) => value,
                    Err(error) => {
                        if self.cleanup {
                            list[dest..dest + *na].copy_from_slice(&temp[ssa..ssa + *na]);
                        }
                        return Err(error);
                    }
                }
            };
        }
        let exit = 'merge: {
            if *nb == 0 {
                break 'merge Exit::Succeed;
            }
            if *na == 1 {
                break 'merge Exit::CopyB;
            }
            let mut min_gallop = self.min_gallop;
            loop {
                // The number of consecutive times A won, and B won.
                let mut acount = 0;
                let mut bcount = 0;
                loop {
                    debug_assert!(*na > 1 && *nb > 0);
                    if relocating!(self.lt(list[*ssb].1, temp[ssa].1)) {
                        list[dest] = list[*ssb];
                        dest += 1;
                        *ssb += 1;
                        bcount += 1;
                        acount = 0;
                        *nb -= 1;
                        if *nb == 0 {
                            break 'merge Exit::Succeed;
                        }
                        if bcount >= min_gallop {
                            break;
                        }
                    } else {
                        list[dest] = temp[ssa];
                        dest += 1;
                        ssa += 1;
                        acount += 1;
                        bcount = 0;
                        *na -= 1;
                        if *na == 1 {
                            break 'merge Exit::CopyB;
                        }
                        if acount >= min_gallop {
                            break;
                        }
                    }
                }
                // One run is winning so consistently that galloping may
                // be a huge speedup: gallop until neither run appears to
                // be winning consistently anymore.
                min_gallop += 1;
                loop {
                    debug_assert!(*na > 1 && *nb > 0);
                    min_gallop -= usize::from(min_gallop > 1);
                    self.min_gallop = min_gallop;
                    let k = relocating!(self.gallop_right(list[*ssb].1, &temp[ssa..ssa + *na], 0));
                    acount = k;
                    if k > 0 {
                        list[dest..dest + k].copy_from_slice(&temp[ssa..ssa + k]);
                        dest += k;
                        ssa += k;
                        *na -= k;
                        if *na == 1 {
                            break 'merge Exit::CopyB;
                        }
                        // Impossible for a consistent comparison
                        // function, which is not assumed.
                        if *na == 0 {
                            break 'merge Exit::Succeed;
                        }
                    }
                    list[dest] = list[*ssb];
                    dest += 1;
                    *ssb += 1;
                    *nb -= 1;
                    if *nb == 0 {
                        break 'merge Exit::Succeed;
                    }
                    let k = relocating!(self.gallop_left(temp[ssa].1, &list[*ssb..*ssb + *nb], 0));
                    bcount = k;
                    if k > 0 {
                        list.copy_within(*ssb..*ssb + k, dest);
                        dest += k;
                        *ssb += k;
                        *nb -= k;
                        if *nb == 0 {
                            break 'merge Exit::Succeed;
                        }
                    }
                    list[dest] = temp[ssa];
                    dest += 1;
                    ssa += 1;
                    *na -= 1;
                    if *na == 1 {
                        break 'merge Exit::CopyB;
                    }
                    if acount < GALLOP_WIN_MIN && bcount < GALLOP_WIN_MIN {
                        break;
                    }
                }
                // A penalty for leaving galloping mode.
                min_gallop += 1;
                self.min_gallop = min_gallop;
            }
        };
        match exit {
            Exit::Succeed => {
                list[dest..dest + *na].copy_from_slice(&temp[ssa..ssa + *na]);
            }
            Exit::CopyB => {
                debug_assert!(*na == 1 && *nb > 0);
                // The last entry of A belongs at the end of the merge.
                list.copy_within(*ssb..*ssb + *nb, dest);
                list[dest + *nb] = temp[ssa];
            }
        }
        Ok(())
    }

    /// Stably merge, in place, the NA entries at SSA with the NB at
    /// SSB = SSA + NA, NA >= NB, where LIST[SSA + NA - 1] belongs at the
    /// end of the merge.
    fn merge_hi(
        &mut self,
        list: &mut [Entry],
        ssa: usize,
        na: usize,
        ssb: usize,
        nb: usize,
    ) -> Result<(), LispError> {
        debug_assert!(na > 0 && nb > 0 && ssa + na == ssb);
        self.needmem(nb);
        let mut temp = std::mem::take(&mut self.temp);
        let result = self.merge_hi_with(list, &mut temp, ssa, na, ssb, nb);
        self.temp = temp;
        result
    }

    fn merge_hi_with(
        &mut self,
        list: &mut [Entry],
        temp: &mut [Entry],
        basea: usize,
        na: usize,
        ssb: usize,
        nb: usize,
    ) -> Result<(), LispError> {
        enum Exit {
            Succeed,
            CopyA,
        }
        // Indices run down past the start of a run, so they are signed:
        // DEST and SSA into LIST, SSB into TEMP, which holds B.
        let get = |slice: &[Entry], index: isize| slice[index as usize];
        let (mut na, mut nb) = (na as isize, nb as isize);
        let mut dest = (ssb as isize) + nb - 1;
        temp[..nb as usize].copy_from_slice(&list[ssb..ssb + nb as usize]);
        let mut ssb: isize = nb - 1;
        let mut ssa = (basea as isize) + na - 1;
        list[dest as usize] = get(list, ssa);
        dest -= 1;
        ssa -= 1;
        na -= 1;
        // sort.c's reloc: a signal leaves B's unmerged entries, still in
        // TEMP, where the merge would put them, so the list holds every
        // entry once (cleanup_mem).
        macro_rules! relocating {
            ($result:expr) => {
                match $result {
                    Ok(value) => value,
                    Err(error) => {
                        if self.cleanup {
                            let start = (dest - (nb - 1)) as usize;
                            list[start..start + nb as usize].copy_from_slice(&temp[..nb as usize]);
                        }
                        return Err(error);
                    }
                }
            };
        }
        let exit = 'merge: {
            if na == 0 {
                break 'merge Exit::Succeed;
            }
            if nb == 1 {
                break 'merge Exit::CopyA;
            }
            let mut min_gallop = self.min_gallop;
            loop {
                // The number of consecutive times A won, and B won.
                let mut acount = 0;
                let mut bcount = 0;
                loop {
                    debug_assert!(na > 0 && nb > 1);
                    if relocating!(self.lt(get(temp, ssb).1, get(list, ssa).1)) {
                        list[dest as usize] = get(list, ssa);
                        dest -= 1;
                        ssa -= 1;
                        acount += 1;
                        bcount = 0;
                        na -= 1;
                        if na == 0 {
                            break 'merge Exit::Succeed;
                        }
                        if acount >= min_gallop {
                            break;
                        }
                    } else {
                        list[dest as usize] = get(temp, ssb);
                        dest -= 1;
                        ssb -= 1;
                        bcount += 1;
                        acount = 0;
                        nb -= 1;
                        if nb == 1 {
                            break 'merge Exit::CopyA;
                        }
                        if bcount >= min_gallop {
                            break;
                        }
                    }
                }
                // One run is winning so consistently that galloping may
                // be a huge speedup: gallop until neither run appears to
                // be winning consistently anymore.
                min_gallop += 1;
                loop {
                    debug_assert!(na > 0 && nb > 1);
                    min_gallop -= usize::from(min_gallop > 1);
                    self.min_gallop = min_gallop;
                    let a = &list[basea..basea + na as usize];
                    let k = na
                        - relocating!(self.gallop_right(get(temp, ssb).1, a, na as usize - 1))
                            as isize;
                    acount = k as usize;
                    if k > 0 {
                        dest -= k;
                        ssa -= k;
                        list.copy_within(
                            (ssa + 1) as usize..(ssa + 1 + k) as usize,
                            (dest + 1) as usize,
                        );
                        na -= k;
                        if na == 0 {
                            break 'merge Exit::Succeed;
                        }
                    }
                    list[dest as usize] = get(temp, ssb);
                    dest -= 1;
                    ssb -= 1;
                    nb -= 1;
                    if nb == 1 {
                        break 'merge Exit::CopyA;
                    }
                    let b = &temp[..nb as usize];
                    let k = nb
                        - relocating!(self.gallop_left(get(list, ssa).1, b, nb as usize - 1))
                            as isize;
                    bcount = k as usize;
                    if k > 0 {
                        dest -= k;
                        ssb -= k;
                        list[(dest + 1) as usize..(dest + 1 + k) as usize]
                            .copy_from_slice(&temp[(ssb + 1) as usize..(ssb + 1 + k) as usize]);
                        nb -= k;
                        if nb == 1 {
                            break 'merge Exit::CopyA;
                        }
                        // Impossible for a consistent comparison
                        // function, which is not assumed.
                        if nb == 0 {
                            break 'merge Exit::Succeed;
                        }
                    }
                    list[dest as usize] = get(list, ssa);
                    dest -= 1;
                    ssa -= 1;
                    na -= 1;
                    if na == 0 {
                        break 'merge Exit::Succeed;
                    }
                    if acount < GALLOP_WIN_MIN && bcount < GALLOP_WIN_MIN {
                        break;
                    }
                }
                // A penalty for leaving galloping mode.
                min_gallop += 1;
                self.min_gallop = min_gallop;
            }
        };
        match exit {
            Exit::Succeed => {
                if nb > 0 {
                    let start = (dest - (nb - 1)) as usize;
                    list[start..start + nb as usize].copy_from_slice(&temp[..nb as usize]);
                }
            }
            Exit::CopyA => {
                debug_assert!(nb == 1 && na > 0);
                // The first entry of B belongs at the front of the merge.
                list.copy_within(
                    (ssa + 1 - na) as usize..(ssa + 1) as usize,
                    (dest + 1 - na) as usize,
                );
                dest -= na;
                list[dest as usize] = get(temp, ssb);
            }
        }
        Ok(())
    }

    /// Merge the stretches at stack indices I and I + 1.
    fn merge_at(&mut self, list: &mut [Entry], i: usize) -> Result<(), LispError> {
        debug_assert!(self.n >= 2 && (i + 2 == self.n || i + 3 == self.n));
        let mut ssa = self.pending[i].base;
        let mut na = self.pending[i].len;
        let ssb = self.pending[i + 1].base;
        let nb = self.pending[i + 1].len;
        debug_assert!(na > 0 && nb > 0 && ssa + na == ssb);
        // The combined length; the stretch at I + 1 goes, and a third
        // from the top slides down over it.
        self.pending[i].len = na + nb;
        if i + 3 == self.n {
            self.pending[i + 1] = self.pending[i + 2];
        }
        self.n -= 1;
        // Where B starts in A: the entries before it are in place.
        let k = self.gallop_right(list[ssb].1, &list[ssa..ssa + na], 0)?;
        ssa += k;
        na -= k;
        if na == 0 {
            return Ok(());
        }
        // Where A ends in B: the entries after it are in place.
        let nb = self.gallop_left(list[ssa + na - 1].1, &list[ssb..ssb + nb], nb - 1)?;
        if nb == 0 {
            return Ok(());
        }
        // Merge what remains of the runs, with temporary room for the
        // shorter.
        if na <= nb {
            self.merge_lo(list, ssa, na, ssb, nb)
        } else {
            self.merge_hi(list, ssa, na, ssb, nb)
        }
    }

    /// A run of N2 entries was found after the stretches on the stack:
    /// merge the stretches whose power exceeds the top stretch's.
    fn found_new_run(&mut self, list: &mut [Entry], n2: usize) -> Result<(), LispError> {
        if self.n > 0 {
            let s1 = self.pending[self.n - 1].base;
            let n1 = self.pending[self.n - 1].len;
            let power = powerloop(s1, n1, n2, self.listlen);
            while self.n > 1 && self.pending[self.n - 2].power > power {
                self.merge_at(list, self.n - 2)?;
            }
            debug_assert!(self.n < 2 || self.pending[self.n - 2].power < power);
            self.pending[self.n - 1].power = power;
        }
        Ok(())
    }

    /// Merge every stretch on the stack until one remains.
    fn merge_force_collapse(&mut self, list: &mut [Entry]) -> Result<(), LispError> {
        while self.n > 1 {
            let mut n = self.n - 2;
            if n > 0 && self.pending[n - 1].len < self.pending[n + 1].len {
                n -= 1;
            }
            self.merge_at(list, n)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sorted_with_comparisons(keys: &[i64]) -> (Vec<i64>, Vec<(i64, i64)>) {
        let mut entries: Vec<Entry> = keys
            .iter()
            .enumerate()
            .map(|(index, key)| (Value::Integer(index as i64), Value::Integer(*key)))
            .collect();
        let mut comparisons = Vec::new();
        tim_sort(&mut entries, false, |a, b| {
            let (a, b) = (
                a.integer_value().expect("key"),
                b.integer_value().expect("key"),
            );
            comparisons.push((a, b));
            Ok(a < b)
        })
        .expect("integer comparisons never signal");
        let order = entries
            .iter()
            .map(|(item, _)| item.integer_value().expect("item"))
            .collect();
        (order, comparisons)
    }

    /// A deterministic scramble of 0..N with many equal keys.
    fn scrambled(n: usize, modulus: i64) -> Vec<i64> {
        let mut state = 0x2545_f491_4f6c_dd1du64;
        (0..n)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                (state % modulus as u64) as i64
            })
            .collect()
    }

    #[test]
    fn sorts_stably_at_every_size_and_shape() {
        for n in [0, 1, 2, 3, 31, 63, 64, 65, 127, 200, 1000, 3000] {
            for modulus in [2, 7, 1 << 30] {
                let mut shapes = vec![scrambled(n, modulus)];
                shapes.push((0..n as i64).collect());
                shapes.push((0..n as i64).rev().collect());
                // Ascending runs with a descending tail, to gallop.
                shapes.push(
                    (0..n as i64)
                        .map(|i| if i < n as i64 / 2 { i } else { n as i64 - i })
                        .collect(),
                );
                for keys in shapes {
                    let (order, _) = sorted_with_comparisons(&keys);
                    let mut expected: Vec<i64> = (0..n as i64).collect();
                    expected.sort_by_key(|&index| keys[index as usize]);
                    assert_eq!(order, expected, "n={n} modulus={modulus}");
                }
            }
        }
    }

    #[test]
    fn signals_from_the_predicate_end_the_sort() {
        let mut entries: Vec<Entry> = (0..500)
            .map(|index| (Value::Integer(index), Value::Integer((index * 7919) % 500)))
            .collect();
        let mut calls = 0;
        let result = tim_sort(&mut entries, false, |a, b| {
            calls += 1;
            if calls == 300 {
                return Err(LispError::Signal("stop".into()));
            }
            Ok(a.integer_value() < b.integer_value())
        });
        assert!(result.is_err());
        assert_eq!(calls, 300, "no comparison runs after the signal");
    }
}
