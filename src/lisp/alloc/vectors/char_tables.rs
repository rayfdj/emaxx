//! GNU char-table payloads and chartab.c's radix operations.
//!
//! The header and inline fields are the authoritative Lisp/native storage.
//! Root tables contain 68 standard Lisp slots and their fixed extra slots.
//! Subtables contain one packed depth/min_char word followed by Lisp slots;
//! the collector skips that first, non-Lisp word (alloc.c:mark_char_table).

use super::*;
use crate::lisp::types::Kind;

pub(crate) const CHAR_TABLE_STANDARD_SLOTS: usize = 68;
pub(crate) const CHAR_TABLE_MAX_SLOTS: usize = PSEUDOVECTOR_SIZE_MASK;
pub(crate) const CHAR_TABLE_MAX_CHAR: u32 = 0x3f_ffff;
const SIZES: [usize; 4] = [64, 16, 32, 128];
const SHIFTS: [u32; 4] = [16, 12, 7, 0];
const DEFAULT: usize = 0;
const PARENT: usize = 1;
const PURPOSE: usize = 2;
const ASCII: usize = 3;
const CONTENTS: usize = 4;

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
    }

    #[inline]
    pub(crate) fn default(&self) -> Value {
        self.slot(DEFAULT)
    }

    #[inline]
    pub(crate) fn set_default(&self, value: Value) {
        self.set_slot(DEFAULT, value);
    }

    #[inline]
    pub(crate) fn parent(&self) -> Option<Self> {
        match self.slot(PARENT).kind() {
            Kind::CharTable(parent) => Some(parent),
            _ => None,
        }
    }

    #[inline]
    pub(crate) fn set_parent(&self, parent: Option<Self>) {
        self.set_slot(PARENT, parent.map_or(Value::Nil, Self::value));
    }

    #[inline]
    pub(crate) fn purpose(&self) -> Value {
        self.slot(PURPOSE)
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
        true
    }

    pub(crate) fn is_uniprop(&self) -> bool {
        self.extra_count() == 5 && self.has_purpose("char-code-property-table")
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

    /// CHAR_TABLE_REF_ASCII and char_table_ref: nil falls through this
    /// table's default first, then its parent. INIT is also stored in every
    /// initial content slot, so replacing the default preserves INIT values.
    pub(crate) fn get(&self, character: u32) -> Value {
        assert!(character <= CHAR_TABLE_MAX_CHAR);
        let mut table = *self;
        loop {
            let mut value = table.explicit_get(character);
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

    pub(crate) fn explicit_get(&self, character: u32) -> Value {
        assert!(character <= CHAR_TABLE_MAX_CHAR);
        if character < 128 {
            let ascii = self.slot(ASCII);
            return subtable(ascii).map_or(ascii, |sub| sub.slot(character as usize));
        }
        self.contents_get(character)
    }

    fn contents_get(&self, character: u32) -> Value {
        let value = self.slot(CONTENTS + (character >> SHIFTS[0]) as usize);
        subtable(value).map_or(value, |sub| sub.get(character, self.is_uniprop()))
    }

    /// chartab.c:char_table_ref_and_range reads this tree and its default,
    /// without following the parent or consulting the ASCII cache.
    pub(crate) fn range_value(&self, character: u32) -> Value {
        let value = self.contents_get(character);
        if value.is_nil() {
            self.default()
        } else {
            value
        }
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

    #[inline]
    pub(crate) fn set_slot(&self, index: usize, value: Value) {
        self.cells()[index].set(value);
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

fn subtable(value: Value) -> Option<SubCharTableRef> {
    match value.kind() {
        Kind::SubCharTable(sub) => Some(sub),
        _ => None,
    }
}

fn compressed(value: Value) -> bool {
    match value.kind() {
        Kind::String(text) => matches!(text.as_bytes().first(), Some(1 | 2)),
        Kind::StringObject(string) => {
            matches!(string.borrow().text.as_bytes().first(), Some(1 | 2))
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
