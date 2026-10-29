//! GNU char-table payloads and chartab.c's radix operations.
//!
//! The header and inline fields are the authoritative Lisp/native storage.
//! Root tables contain 68 standard Lisp slots and their fixed extra slots.
//! Subtables contain one packed depth/min_char word followed by Lisp slots;
//! the collector skips that first, non-Lisp word (alloc.c:mark_char_table).

use super::*;
use crate::lisp::types::Kind;

/// Raised by every store into a char-table or sub-char-table slot. GNU's
/// regexp engine reads the tables directly; Emaxx's backend compiles their
/// contents into its patterns, so its caches must notice any table change.
/// An unchanged count proves no table slot changed and no table was made:
/// every store passes through `set_slot' or `set_extra' (the cells are
/// private), every new table or subtable through `allocate_words', and
/// native code cannot store into a char table inline (comp.c emits a call
/// for `aset'; only `setcar' and `setcdr' are inlined).
static CHAR_TABLE_STORES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// The current count of char-table stores (see `CHAR_TABLE_STORES').
pub(crate) fn char_table_store_epoch() -> u64 {
    CHAR_TABLE_STORES.load(Ordering::Relaxed)
}

#[inline]
pub(super) fn note_char_table_store() {
    CHAR_TABLE_STORES.store(
        CHAR_TABLE_STORES.load(Ordering::Relaxed).wrapping_add(1),
        Ordering::Relaxed,
    );
}

pub(crate) const CHAR_TABLE_STANDARD_SLOTS: usize = 68;
pub(crate) const CHAR_TABLE_MAX_SLOTS: usize = PSEUDOVECTOR_SIZE_MASK;
pub(crate) const CHAR_TABLE_MAX_CHAR: u32 = 0x3f_ffff;
const SIZES: [usize; 4] = [64, 16, 32, 128];
/// chartab.c's `chartab_size': the slots of a subtable of each depth.
pub(super) const SUB_CHAR_TABLE_SIZES: [usize; 4] = SIZES;
const SHIFTS: [u32; 4] = [16, 12, 7, 0];
const DEFAULT: usize = 0;
const PARENT: usize = 1;
const PURPOSE: usize = 2;
const ASCII: usize = 3;
const CONTENTS: usize = 4;

/// chartab.c:char_table_ref_and_range shrinks these bounds while examining
/// adjacent slots. Examining a compressed Unicode slot expands it in place,
/// which is observable even when the Lisp caller only uses the returned value.
struct RangeQuery {
    from: u32,
    to: u32,
    default: Value,
    uniprop: bool,
}

impl RangeQuery {
    fn value(&mut self, value: Value, character: u32) -> Value {
        if let Some(sub) = subtable(value) {
            sub.ref_and_range(character, self)
        } else if value.is_nil() {
            self.default
        } else {
            value
        }
    }
}

#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct CharTableRef(NonNull<VectorHeader>);

#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct SubCharTableRef(NonNull<VectorHeader>);

/// Allocate the inline words used by chartab.c's make_vector/XSETPVECTYPE.
/// Char-table headers retain the public slot count, with no synthetic rest
/// field for allocator padding. vector_nbytes rounds these kinds separately.
fn allocate_words(tag: VectorTag, slots: usize) -> NonNull<VectorHeader> {
    let nbytes = vroundup(HEADER_SIZE + slots * WORD_SIZE);
    crate::lisp::native_comp::note_lisp_allocation(nbytes);
    // A new table or subtable may occupy a dead one's address, and its
    // slots are initialized here rather than through `set_slot': count it,
    // so a cached graph's raw table words never vouch for a new object.
    note_char_table_store();
    let header = allocate_vectorlike(nbytes);
    // SAFETY: freshly allocated, aligned storage; allocation does not collect.
    // The caller initializes all fields before publishing the object.
    unsafe {
        (*header).size = PSEUDOVECTOR_FLAG | ((tag as usize) << PSEUDOVECTOR_AREA_BITS) | slots;
        (*header)
            .mark_bit()
            .mark(crate::lisp::types::current_mark_epoch());
        NonNull::new_unchecked(header)
    }
}

impl CharTableRef {
    pub(crate) fn new(purpose: Value, initial: Value, extras: usize) -> Self {
        assert!(extras <= CHAR_TABLE_MAX_SLOTS - CHAR_TABLE_STANDARD_SLOTS);
        let slots = CHAR_TABLE_STANDARD_SLOTS + extras;
        let header = allocate_words(VectorTag::CharTable, slots);
        // SAFETY: all SLOTS cells fit in this new allocation.
        unsafe {
            let cells = payload(header.as_ptr()).cast::<Cell<Value>>();
            for index in 0..slots {
                cells.add(index).write(Cell::new(initial));
            }
            (*cells.add(PARENT)).set(Value::Nil);
            (*cells.add(PURPOSE)).set(purpose);
            census_on_allocate(header.as_ptr());
        }
        Self(header)
    }

    /// # Safety
    /// HEADER is a live PVEC_CHAR_TABLE allocation with its declared slots.
    pub(crate) unsafe fn from_raw(header: *mut VectorHeader) -> Self {
        Self(unsafe { NonNull::new_unchecked(header) })
    }

    #[inline]
    pub(crate) fn identity(self) -> usize {
        self.0.as_ptr() as usize
    }

    #[inline]
    pub(crate) fn value(self) -> Value {
        // SAFETY: the word points to this canonical initialized pseudovector.
        unsafe { Value::from_word(self.identity() | 5) }
    }

    #[inline]
    fn header(&self) -> &VectorHeader {
        // SAFETY: a reached table keeps its allocation live.
        unsafe { self.0.as_ref() }
    }

    #[inline]
    pub(crate) fn mark_bit(&self) -> VectorMark<'_> {
        self.header().mark_bit()
    }

    #[inline]
    pub(crate) fn slot_count(&self) -> usize {
        self.header().size & PSEUDOVECTOR_SIZE_MASK
    }

    #[inline]
    fn cells(&self) -> &[Cell<Value>] {
        // SAFETY: every public slot is initialized, including fixed extras.
        unsafe { std::slice::from_raw_parts(payload(self.0.as_ptr()).cast(), self.slot_count()) }
    }

    pub(crate) fn slots(&self) -> impl DoubleEndedIterator<Item = Value> + ExactSizeIterator + '_ {
        self.cells().iter().map(Cell::get)
    }

    #[inline]
    pub(crate) fn slot(&self, index: usize) -> Value {
        self.cells()[index].get()
    }

    #[inline]
    pub(crate) fn set_slot(&self, index: usize, value: Value) {
        self.cells()[index].set(value);
        note_char_table_store();
    }

    /// One of the slots below the extras, which every char-table has (the
    /// dump loader and `new' admit no fewer), read in place as
    /// XCHAR_TABLE's fields are, with no count to check it against.
    #[inline(always)]
    fn standard_slot(&self, index: usize) -> Value {
        debug_assert!(index < CHAR_TABLE_STANDARD_SLOTS);
        // SAFETY: INDEX is one of the table's initialized standard slots.
        unsafe { (*payload(self.0.as_ptr()).cast::<Cell<Value>>().add(index)).get() }
    }

    #[inline]
    pub(crate) fn default(&self) -> Value {
        self.standard_slot(DEFAULT)
    }

    #[inline]
    pub(crate) fn set_default(&self, value: Value) {
        self.set_slot(DEFAULT, value);
    }

    #[inline]
    pub(crate) fn parent(&self) -> Option<Self> {
        self.standard_slot(PARENT).char_table_ref()
    }

    #[inline]
    pub(crate) fn set_parent(&self, parent: Option<Self>) {
        self.set_slot(PARENT, parent.map_or(Value::Nil, Self::value));
    }

    #[inline]
    pub(crate) fn purpose(&self) -> Value {
        self.standard_slot(PURPOSE)
    }

    pub(crate) fn has_purpose(&self, name: &str) -> bool {
        self.purpose().eq_value(Value::symbol(name))
    }

    #[inline]
    pub(crate) fn extra_count(&self) -> usize {
        self.slot_count() - CHAR_TABLE_STANDARD_SLOTS
    }

    pub(crate) fn extra(&self, index: usize) -> Option<Value> {
        self.cells()
            .get(CHAR_TABLE_STANDARD_SLOTS.checked_add(index)?)
            .map(Cell::get)
    }

    pub(crate) fn set_extra(&self, index: usize, value: Value) -> bool {
        let Some(slot) = index
            .checked_add(CHAR_TABLE_STANDARD_SLOTS)
            .and_then(|i| self.cells().get(i))
        else {
            return false;
        };
        slot.set(value);
        note_char_table_store();
        true
    }

    /// chartab.c's UNIPROP_TABLE_P: the purpose compared with the symbol
    /// itself, as EQ with Qchar_code_property_table.
    pub(crate) fn is_uniprop(&self) -> bool {
        self.extra_count() == 5
            && cached_symbol!("char-code-property-table")
                .with(|symbol| self.purpose().word() == Value::Symbol(*symbol).word())
    }

    /// chartab.c:char_table_ascii. ASCII aliases the actual bottom subtable;
    /// it is not a separately maintained array of decoded values or indices.
    pub(crate) fn refresh_ascii(&self) {
        let mut value = self.slot(CONTENTS);
        if let Some(sub) = subtable(value) {
            value = sub.slot(0);
            if let Some(sub) = subtable(value) {
                value = sub.child(0, self.is_uniprop());
            }
        }
        self.set_slot(ASCII, value);
    }

    /// lisp.h:CHAR_TABLE_REF: CHAR_TABLE_REF_ASCII for an ASCII
    /// character, else char_table_ref.  Nil falls through this table's
    /// default first, then its parent. INIT is also stored in every
    /// initial content slot, so replacing the default preserves INIT values.
    #[inline]
    pub(crate) fn get(&self, character: u32) -> Value {
        if character < 128 {
            return self.ascii_get(character as usize);
        }
        self.get_non_ascii(character)
    }

    /// CHAR_TABLE_REF of a character past ASCII: down this tree, its
    /// default for nil, up the parents while nil.
    #[inline(never)]
    fn get_non_ascii(&self, character: u32) -> Value {
        assert!(character <= CHAR_TABLE_MAX_CHAR);
        let mut table = *self;
        loop {
            let mut value = table.contents_get(character);
            if value.is_nil() {
                value = table.default();
            }
            if !value.is_nil() {
                return value;
            }
            let Some(parent) = table.parent() else {
                return Value::Nil;
            };
            table = parent;
        }
    }

    /// lisp.h:CHAR_TABLE_REF_ASCII: each table's ASCII slot, its default
    /// for nil, up the parents while nil.
    #[inline]
    fn ascii_get(&self, index: usize) -> Value {
        let mut table = *self;
        loop {
            let mut value = table.ascii_explicit_get(index);
            if value.is_nil() {
                value = table.default();
            }
            if !value.is_nil() {
                return value;
            }
            let Some(parent) = table.parent() else {
                return value;
            };
            table = parent;
        }
    }

    /// CHAR_TABLE_REF_ASCII of every ASCII character, character C's value
    /// at index C, read in one walk up the parents: each table answers the
    /// characters its children left nil.
    pub(crate) fn ascii_get_all(&self) -> [Value; 128] {
        let mut values = [Value::Nil; 128];
        let mut unanswered = u128::MAX;
        let mut table = *self;
        loop {
            let ascii = table.standard_slot(ASCII);
            let bottom = subtable(ascii);
            let default = table.default();
            let mut rest = unanswered;
            while rest != 0 {
                let index = rest.trailing_zeros() as usize;
                rest &= rest - 1;
                let mut value = bottom.map_or(ascii, |sub| sub.bottom_slot(index));
                if value.is_nil() {
                    value = default;
                }
                if !value.is_nil() {
                    values[index] = value;
                    unanswered &= !(1 << index);
                }
            }
            if unanswered == 0 {
                return values;
            }
            let Some(parent) = table.parent() else {
                return values;
            };
            table = parent;
        }
    }

    pub(crate) fn explicit_get(&self, character: u32) -> Value {
        if character < 128 {
            return self.ascii_explicit_get(character as usize);
        }
        assert!(character <= CHAR_TABLE_MAX_CHAR);
        self.contents_get(character)
    }

    /// The ASCII slot's value for INDEX, below 128: its element when it
    /// holds the bottom subtable (char_table_ascii stores no other).
    #[inline(always)]
    fn ascii_explicit_get(&self, index: usize) -> Value {
        let ascii = self.standard_slot(ASCII);
        match subtable(ascii) {
            Some(sub) => sub.bottom_slot(index),
            None => ascii,
        }
    }

    fn contents_get(&self, character: u32) -> Value {
        let value = self.standard_slot(CONTENTS + (character >> SHIFTS[0]) as usize);
        subtable(value).map_or(value, |sub| sub.get(character, self.is_uniprop()))
    }

    /// chartab.c:char_table_ref_and_range reads this tree and its default,
    /// without following the parent or consulting the ASCII cache.
    pub(crate) fn range_value(&self, from: u32, to: u32) -> Value {
        self.ref_and_range(from, from, to).0
    }

    /// chartab.c:char_table_ref_and_range: the value for CHARACTER, read
    /// from this tree and its default (not the parent), with the run of
    /// characters around it within [FROM, TO] whose value is the same
    /// object.
    pub(crate) fn ref_and_range(&self, character: u32, from: u32, to: u32) -> (Value, u32, u32) {
        let mut query = RangeQuery {
            from,
            to,
            default: self.default(),
            uniprop: self.is_uniprop(),
        };
        let index = (character >> SHIFTS[0]) as usize;
        let value = query.value(self.slot(CONTENTS + index), character);
        let mut left = index;
        while query.from < (left as u32) << SHIFTS[0] {
            let character = ((left as u32) << SHIFTS[0]) - 1;
            left -= 1;
            let previous = query.value(self.slot(CONTENTS + left), character);
            if !previous.eq_value(value) {
                query.from = character + 1;
                break;
            }
        }
        let mut right = index;
        while query.to >= ((right + 1) as u32) << SHIFTS[0] {
            right += 1;
            let character = (right as u32) << SHIFTS[0];
            let next = query.value(self.slot(CONTENTS + right), character);
            if !next.eq_value(value) {
                query.to = character - 1;
                break;
            }
        }
        (value, query.from, query.to)
    }

    pub(crate) fn set(&self, character: u32, value: Value) {
        assert!(character <= CHAR_TABLE_MAX_CHAR);
        if character < 128
            && let Some(ascii) = subtable(self.slot(ASCII))
        {
            ascii.set_slot(character as usize, value);
            return;
        }
        let index = (character >> SHIFTS[0]) as usize;
        let current = self.slot(CONTENTS + index);
        let sub = subtable(current).unwrap_or_else(|| {
            let sub = SubCharTableRef::new(1, (index as u32) << SHIFTS[0], current);
            self.set_slot(CONTENTS + index, sub.value());
            sub
        });
        sub.set(character, value, self.is_uniprop());
        if character < 128 {
            self.refresh_ascii();
        }
    }

    /// The RANGE=t operation changes contents, not the default or extras.
    pub(crate) fn set_all(&self, value: Value) {
        for index in ASCII..CHAR_TABLE_STANDARD_SLOTS {
            self.set_slot(index, value);
        }
    }

    pub(crate) fn set_range(&self, from: u32, to: u32, value: Value) {
        assert!(from <= CHAR_TABLE_MAX_CHAR && to <= CHAR_TABLE_MAX_CHAR);
        if from > to {
            return;
        }
        if from == to {
            self.set(from, value);
            return;
        }
        for index in (from >> SHIFTS[0])..=(to >> SHIFTS[0]) {
            let begin = index << SHIFTS[0];
            if begin > to {
                break;
            }
            if from <= begin && begin + (1 << SHIFTS[0]) - 1 <= to {
                self.set_slot(CONTENTS + index as usize, value);
            } else {
                let current = self.slot(CONTENTS + index as usize);
                let sub = subtable(current).unwrap_or_else(|| {
                    let sub = SubCharTableRef::new(1, begin, current);
                    self.set_slot(CONTENTS + index as usize, sub.value());
                    sub
                });
                sub.set_range(from, to, value, self.is_uniprop());
            }
        }
        if from < 128 {
            self.refresh_ascii();
        }
    }

    pub(crate) fn copy(&self) -> Self {
        let copy = Self::new(self.purpose(), Value::Nil, self.extra_count());
        copy.set_default(self.default());
        copy.set_slot(PARENT, self.slot(PARENT));
        for index in CONTENTS..self.slot_count() {
            let value = self.slot(index);
            copy.set_slot(
                index,
                if index < CHAR_TABLE_STANDARD_SLOTS {
                    subtable(value).map_or(value, |sub| sub.copy().value())
                } else {
                    value
                },
            );
        }
        copy.refresh_ascii();
        copy
    }

    pub(crate) fn ranges(&self) -> Vec<crate::lisp::eval::CharTableEntry> {
        let mut ranges: Vec<crate::lisp::eval::CharTableEntry> = Vec::new();
        self.visit_ranges(&mut |start, end, value| {
            if let Some(previous) = ranges.last_mut()
                && previous.end + 1 == start
                && previous.value.eq_value(value)
            {
                previous.end = end;
            } else {
                ranges.push(crate::lisp::eval::CharTableEntry { start, end, value });
            }
        });
        ranges
    }

    pub(crate) fn append_change_boundaries(&self, from: u32, to: u32, boundaries: &mut Vec<u32>) {
        self.visit_ranges_between(from, to, &mut |start, end, _| {
            if start >= from && start <= to {
                boundaries.push(start);
            }
            if end >= from && end < to {
                boundaries.push(end + 1);
            }
        });
    }

    pub(crate) fn effective_ranges(&self) -> Vec<crate::lisp::eval::CharTableEntry> {
        let mut boundaries = vec![0, CHAR_TABLE_MAX_CHAR + 1];
        let mut table = Some(*self);
        while let Some(current) = table {
            current.append_change_boundaries(0, CHAR_TABLE_MAX_CHAR, &mut boundaries);
            table = current.parent();
        }
        boundaries.sort_unstable();
        boundaries.dedup();
        let mut result: Vec<crate::lisp::eval::CharTableEntry> = Vec::new();
        for pair in boundaries.windows(2) {
            let start = pair[0];
            let end = pair[1] - 1;
            let value = self.get(start);
            if value.is_nil() {
                continue;
            }
            if let Some(previous) = result.last_mut()
                && previous.end + 1 == start
                && previous.value.eq_value(value)
            {
                previous.end = end;
            } else {
                result.push(crate::lisp::eval::CharTableEntry { start, end, value });
            }
        }
        result
    }

    /// A walk of current leaf ranges, including nil. This owns no persistent
    /// range cache or write history; callers that need resolved inheritance
    /// combine the returned ranges with the actual default/parent fields.
    pub(crate) fn visit_ranges(&self, visit: &mut impl FnMut(u32, u32, Value)) {
        self.visit_ranges_between(0, CHAR_TABLE_MAX_CHAR, visit);
    }

    fn visit_ranges_between(&self, from: u32, to: u32, visit: &mut impl FnMut(u32, u32, Value)) {
        let uniprop = self.is_uniprop();
        for index in (from >> SHIFTS[0]) as usize..=(to >> SHIFTS[0]) as usize {
            let begin = (index as u32) << SHIFTS[0];
            let value = self.slot(CONTENTS + index);
            if let Some(sub) = subtable(value) {
                sub.visit_ranges_between(from, to, uniprop, visit);
            } else {
                visit(begin, begin + (1 << SHIFTS[0]) - 1, value);
            }
        }
    }
}

impl SubCharTableRef {
    pub(crate) fn new(depth: usize, min_char: u32, initial: Value) -> Self {
        assert!((1..=3).contains(&depth));
        let header = allocate_words(VectorTag::SubCharTable, 1 + SIZES[depth]);
        // SAFETY: the packed non-Lisp word and all content slots fit the
        // fresh allocation. Cell has its contents' C layout/alignment.
        unsafe {
            let body = payload(header.as_ptr());
            body.cast::<Cell<i32>>().write(Cell::new(depth as i32));
            body.add(4)
                .cast::<Cell<i32>>()
                .write(Cell::new(min_char as i32));
            let contents = body.add(8).cast::<Cell<Value>>();
            for index in 0..SIZES[depth] {
                contents.add(index).write(Cell::new(initial));
            }
            census_on_allocate(header.as_ptr());
        }
        Self(header)
    }

    /// # Safety
    /// HEADER is a live PVEC_SUB_CHAR_TABLE with valid depth and contents.
    pub(crate) unsafe fn from_raw(header: *mut VectorHeader) -> Self {
        Self(unsafe { NonNull::new_unchecked(header) })
    }

    #[inline]
    pub(crate) fn identity(self) -> usize {
        self.0.as_ptr() as usize
    }

    #[inline]
    pub(crate) fn value(self) -> Value {
        // SAFETY: the canonical word of this initialized subtable.
        unsafe { Value::from_word(self.identity() | 5) }
    }

    #[inline]
    pub(crate) fn mark_bit(&self) -> VectorMark<'_> {
        // SAFETY: a reached subtable keeps the header alive.
        unsafe { self.0.as_ref().mark_bit() }
    }

    #[inline]
    pub(crate) fn depth(&self) -> usize {
        // SAFETY: the initialized packed depth field.
        unsafe { (*payload(self.0.as_ptr()).cast::<Cell<i32>>()).get() as usize }
    }

    #[inline]
    pub(crate) fn min_char(&self) -> u32 {
        // SAFETY: the initialized packed minimum-character field.
        unsafe { (*payload(self.0.as_ptr()).add(4).cast::<Cell<i32>>()).get() as u32 }
    }

    #[inline]
    fn cells(&self) -> &[Cell<Value>] {
        // SAFETY: exactly SIZES[depth] initialized Lisp cells follow the
        // packed word, which must not be interpreted or marked as Lisp.
        unsafe {
            std::slice::from_raw_parts(payload(self.0.as_ptr()).add(8).cast(), SIZES[self.depth()])
        }
    }

    pub(crate) fn slots(&self) -> impl DoubleEndedIterator<Item = Value> + ExactSizeIterator + '_ {
        self.cells().iter().map(Cell::get)
    }

    #[inline]
    pub(crate) fn slot(&self, index: usize) -> Value {
        self.cells()[index].get()
    }

    /// Element INDEX of a bottom (depth 3) subtable's 128, read in place
    /// as XSUB_CHAR_TABLE's contents are; another depth's checked slot.
    #[inline(always)]
    fn bottom_slot(&self, index: usize) -> Value {
        if self.depth() == 3 && index < SIZES[3] {
            // SAFETY: a depth-3 subtable's SIZES[3] initialized cells
            // follow its packed word, INDEX among them.
            unsafe {
                (*payload(self.0.as_ptr())
                    .add(8)
                    .cast::<Cell<Value>>()
                    .add(index))
                .get()
            }
        } else {
            self.slot(index)
        }
    }

    #[inline]
    pub(crate) fn set_slot(&self, index: usize, value: Value) {
        self.cells()[index].set(value);
        note_char_table_store();
    }

    pub(crate) fn child(&self, index: usize, uniprop: bool) -> Value {
        let value = self.slot(index);
        if uniprop && compressed(value) {
            self.uncompress(index, value).value()
        } else {
            value
        }
    }

    fn get(&self, character: u32, uniprop: bool) -> Value {
        let index = ((character - self.min_char()) >> SHIFTS[self.depth()]) as usize;
        let value = self.child(index, uniprop);
        subtable(value).map_or(value, |sub| sub.get(character, uniprop))
    }

    fn ref_and_range(&self, character: u32, query: &mut RangeQuery) -> Value {
        let depth = self.depth();
        let minimum = self.min_char();
        let shift = SHIFTS[depth];
        let index = ((character - minimum) >> shift) as usize;
        let value = query.value(self.child(index, query.uniprop), character);
        let mut left = index;
        while left > 0 && query.from < minimum + ((left as u32) << shift) {
            let character = minimum + ((left as u32) << shift) - 1;
            left -= 1;
            let previous = query.value(self.child(left, query.uniprop), character);
            if !previous.eq_value(value) {
                query.from = character + 1;
                break;
            }
        }
        let mut right = index;
        while right + 1 < SIZES[depth] && minimum + (((right + 1) as u32) << shift) <= query.to {
            right += 1;
            let character = minimum + ((right as u32) << shift);
            let next = query.value(self.child(right, query.uniprop), character);
            if !next.eq_value(value) {
                query.to = character - 1;
                break;
            }
        }
        value
    }

    fn set(&self, character: u32, value: Value, uniprop: bool) {
        let depth = self.depth();
        let index = ((character - self.min_char()) >> SHIFTS[depth]) as usize;
        if depth == 3 {
            self.set_slot(index, value);
            return;
        }
        let current = self.child(index, uniprop);
        let sub = subtable(current).unwrap_or_else(|| {
            let sub = Self::new(
                depth + 1,
                self.min_char() + ((index as u32) << SHIFTS[depth]),
                current,
            );
            self.set_slot(index, sub.value());
            sub
        });
        sub.set(character, value, uniprop);
    }

    fn set_range(&self, from: u32, to: u32, value: Value, uniprop: bool) {
        let depth = self.depth();
        let min_char = self.min_char();
        let from = from.max(min_char);
        for index in ((from - min_char) >> SHIFTS[depth]) as usize..SIZES[depth] {
            let begin = min_char + ((index as u32) << SHIFTS[depth]);
            if begin > to {
                break;
            }
            if from <= begin && begin + (1 << SHIFTS[depth]) - 1 <= to {
                self.set_slot(index, value);
            } else {
                let current = self.child(index, uniprop);
                let sub = subtable(current).unwrap_or_else(|| {
                    let sub = Self::new(depth + 1, begin, current);
                    self.set_slot(index, sub.value());
                    sub
                });
                sub.set_range(from, to, value, uniprop);
            }
        }
    }

    pub(crate) fn copy(&self) -> Self {
        let copy = Self::new(self.depth(), self.min_char(), Value::Nil);
        for (index, value) in self.slots().enumerate() {
            copy.set_slot(
                index,
                subtable(value).map_or(value, |sub| sub.copy().value()),
            );
        }
        copy
    }

    fn visit_ranges_between(
        &self,
        from: u32,
        to: u32,
        uniprop: bool,
        visit: &mut impl FnMut(u32, u32, Value),
    ) {
        let depth = self.depth();
        let minimum = self.min_char();
        let first = (from.saturating_sub(minimum) >> SHIFTS[depth]) as usize;
        let last = ((to.saturating_sub(minimum) >> SHIFTS[depth]) as usize).min(SIZES[depth] - 1);
        for index in first..=last {
            let begin = minimum + ((index as u32) << SHIFTS[depth]);
            let value = self.child(index, uniprop);
            if let Some(sub) = subtable(value) {
                sub.visit_ranges_between(from, to, uniprop, visit);
            } else {
                visit(begin, begin + (1 << SHIFTS[depth]) - 1, value);
            }
        }
    }

    /// chartab.c:uniprop_table_uncompress. Decoding happens once into the
    /// actual leaf and updates its parent's slot, without a second payload.
    fn uncompress(&self, index: usize, value: Value) -> Self {
        let codes = crate::lisp::primitives::string_like(&value)
            .expect("compressed Unicode-property string")
            .character_codes();
        let sub = Self::new(3, self.min_char() + (index as u32) * 128, Value::Nil);
        self.set_slot(index, sub.value());
        match codes[0] {
            1 => {
                let start = codes.get(1).copied().unwrap_or(0) as usize;
                for (index, value) in codes
                    .get(2..)
                    .unwrap_or_default()
                    .iter()
                    .enumerate()
                    .take(128usize.saturating_sub(start))
                {
                    sub.set_slot(
                        start + index,
                        if *value > 0 {
                            Value::Integer(*value)
                        } else {
                            Value::Nil
                        },
                    );
                }
            }
            2 => {
                let mut source = 1;
                let mut destination = 0;
                while source < codes.len() {
                    let value = codes[source];
                    source += 1;
                    let count = if source < codes.len() && codes[source] >= 128 {
                        let count = codes[source] as usize - 128;
                        source += 1;
                        count
                    } else {
                        1
                    };
                    // Only the 128 allocated leaf slots may be written, even
                    // for a malformed compressed string read from Lisp.
                    for _ in 0..count.min(128usize.saturating_sub(destination)) {
                        sub.set_slot(destination, Value::Integer(value));
                        destination += 1;
                    }
                }
            }
            _ => unreachable!("compressed property format was checked"),
        }
        sub
    }
}

#[inline(always)]
fn subtable(value: Value) -> Option<SubCharTableRef> {
    value.sub_char_table_ref()
}

fn compressed(value: Value) -> bool {
    match value.kind() {
        Kind::StringObject(string) => {
            matches!(string.borrow().bytes().first(), Some(1 | 2))
        }
        _ => false,
    }
}

impl std::fmt::Debug for CharTableRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CharTable({:x})", self.identity())
    }
}
impl std::fmt::Display for CharTableRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:x}", self.identity())
    }
}
impl std::fmt::Debug for SubCharTableRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SubCharTable({:x})", self.identity())
    }
}
