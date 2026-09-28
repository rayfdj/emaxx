//! Unintegrated draft for alloc/vectors/hash_tables.rs. No validation claim.
//! lisp.h:Lisp_Hash_Table and fns.c's four directly owned arrays.

use super::*;

const UNUSED_KEY: usize = 7; // lisp.h:INVALID_LISP_VALUE, not Lisp nil.
const MUTABLE: u8 = 1 << 6;
static EMPTY_INDEX: i32 = -1;

#[repr(C)]
pub(crate) struct HashTableTest {
    pub(crate) hash: extern "C" fn(usize, *mut LispHashTable) -> u32,
    pub(crate) compare: Option<extern "C" fn(usize, usize, *mut LispHashTable) -> usize>,
    pub(crate) user_hash: Value,
    pub(crate) user_compare: Value,
    pub(crate) name: Value,
}

/// Cells permit stores through the native ABI without outstanding references
/// to immutable fields. No array reference may span a Lisp callback or resize.
#[repr(C)]
pub(crate) struct LispHashTable {
    header: VectorHeader,
    index: Cell<*mut Cell<i32>>,
    hash: Cell<*mut Cell<u32>>,
    key_and_value: Cell<*mut Cell<usize>>,
    test: Cell<*const HashTableTest>,
    next: Cell<*mut Cell<i32>>,
    count: Cell<i32>,
    next_free: Cell<i32>,
    table_size: Cell<i32>,
    index_bits: Cell<u8>,
    flags: Cell<u8>,
    padding: [u8; 2],
    next_weak: Cell<*mut LispHashTable>,
}

const _: () = {
    assert!(std::mem::size_of::<LispHashTable>() == 72);
    assert!(std::mem::offset_of!(LispHashTable, key_and_value) == 24);
    assert!(std::mem::offset_of!(LispHashTable, count) == 48);
    assert!(std::mem::offset_of!(LispHashTable, table_size) == 56);
    assert!(std::mem::offset_of!(LispHashTable, flags) == 61);
    assert!(std::mem::size_of::<HashTableTest>() == 40);
    assert!(std::mem::offset_of!(HashTableTest, name) == 32);
};

/// Allocation staging owns all arrays until all four allocations succeed.
/// Once installed their only owner is LispHashTable; no host entry mirror.
struct Arrays {
    index: Box<[Cell<i32>]>,
    hash: Box<[Cell<u32>]>,
    entries: Box<[Cell<usize>]>,
    next: Box<[Cell<i32>]>,
}

impl Arrays {
    fn new(size: usize) -> Self {
        assert!(size > 0 && size <= i32::MAX as usize / 2);
        let indices = crate::lisp::eval::gnu_hash_table_index_slots(size);
        Self {
            index: (0..indices).map(|_| Cell::new(-1)).collect(),
            hash: (0..size).map(|_| Cell::new(0)).collect(),
            entries: (0..2 * size).map(|_| Cell::new(UNUSED_KEY)).collect(),
            next: (0..size)
                .map(|i| Cell::new(if i + 1 == size { -1 } else { (i + 1) as i32 }))
                .collect(),
        }
    }
}

#[repr(transparent)]
#[derive(Clone, Copy)]
pub struct HashTableRef(NonNull<LispHashTable>);

impl HashTableRef {
    /// TEST belongs to the descriptor owner, which must outlive every table
    /// sharing it. Wiring the GNU descriptor pool and its roots is required
    /// before this draft can be used by ordinary constructors.
    pub(crate) unsafe fn new(test: *const HashTableTest, size: usize, flags: u8) -> Self {
        let arrays = (size > 0).then(|| Arrays::new(size));
        let bytes = std::mem::size_of::<LispHashTable>();
        let raw = allocate_vectorlike(bytes).cast::<LispHashTable>();
        // SAFETY: fresh suitably aligned vector allocation of BYTES.
        unsafe {
            raw.write(LispHashTable {
                header: VectorHeader {
                    size: VectorHeader::pseudovector_slots_word(VectorTag::HashTable, bytes, 0),
                },
                index: Cell::new(std::ptr::addr_of!(EMPTY_INDEX).cast_mut().cast()),
                hash: Cell::new(std::ptr::null_mut()),
                key_and_value: Cell::new(std::ptr::null_mut()),
                test: Cell::new(test),
                next: Cell::new(std::ptr::null_mut()),
                count: Cell::new(0),
                next_free: Cell::new(-1),
                table_size: Cell::new(0),
                index_bits: Cell::new(0),
                flags: Cell::new(flags | MUTABLE),
                padding: [0; 2],
                next_weak: Cell::new(std::ptr::null_mut()),
            });
            (*raw).header.mark_bit().mark(crate::lisp::types::current_mark_epoch());
        }
        let result = Self(unsafe { NonNull::new_unchecked(raw) });
        if let Some(arrays) = arrays {
            result.install(arrays);
            result.object().next_free.set(0);
        }
        crate::lisp::native_comp::note_lisp_allocation(
            bytes + crate::lisp::eval::gnu_hash_table_storage_bytes(size),
        );
        unsafe { census_on_allocate(raw.cast()) };
        result
    }

    /// # Safety
    /// RAW is a live PVEC_HASH_TABLE allocation with this layout.
    pub(crate) unsafe fn from_raw(raw: *mut VectorHeader) -> Self {
        Self(unsafe { NonNull::new_unchecked(raw.cast()) })
    }

    fn object(&self) -> &LispHashTable {
        // SAFETY: the collector keeps this allocation live while the handle
        // is rooted. No shared reference escapes the borrowed handle.
        unsafe { self.0.as_ref() }
    }

    pub(crate) fn identity(&self) -> usize { self.0.as_ptr() as usize }
    pub(crate) fn capacity(&self) -> usize { self.object().table_size.get() as usize }
    pub(crate) fn count(&self) -> usize { self.object().count.get() as usize }
    pub(crate) fn is_mutable(&self) -> bool { self.object().flags.get() & MUTABLE != 0 }
    pub(crate) fn set_mutable(&self, mutable: bool) {
        let flags = self.object().flags.get() & !MUTABLE;
        self.object().flags.set(flags | if mutable { MUTABLE } else { 0 });
    }

    fn index_size(&self) -> usize { 1usize << self.object().index_bits.get() }
    fn bucket(&self, hash: u32) -> usize {
        let product = u64::from(hash.wrapping_mul(2_654_435_769));
        (product >> (32 - self.object().index_bits.get())) as usize
    }

    fn install(&self, arrays: Arrays) {
        let size = arrays.hash.len();
        let bits = arrays.index.len().trailing_zeros() as u8;
        let object = self.object();
        object.index.set(Box::into_raw(arrays.index).cast());
        object.hash.set(Box::into_raw(arrays.hash).cast());
        object.key_and_value.set(Box::into_raw(arrays.entries).cast());
        object.next.set(Box::into_raw(arrays.next).cast());
        object.table_size.set(size as i32);
        object.index_bits.set(bits);
    }

    /// Copy out one entry; no Rust reference to array storage escapes.
    pub(crate) fn entry(&self, slot: usize) -> Option<(Value, Value)> {
        if slot >= self.capacity() { return None; }
        let data = self.object().key_and_value.get();
        // SAFETY: two initialized words per allocated slot. The unused
        // sentinel is checked before any Value is decoded.
        unsafe {
            let key = (*data.add(2 * slot)).get();
            if key == UNUSED_KEY { return None; }
            Some((Value::from_word(key), Value::from_word((*data.add(2 * slot + 1)).get())))
        }
    }

    pub(crate) fn entry_at_or_after(&self, minimum: usize) -> Option<(usize, Value, Value)> {
        (minimum..self.capacity()).find_map(|slot| self.entry(slot).map(|(k,v)| (slot,k,v)))
    }

    pub(crate) fn set_value(&self, slot: usize, value: Value) {
        assert!(slot < self.capacity());
        unsafe { (*self.object().key_and_value.get().add(2 * slot + 1)).set(value.word()); }
    }

    pub(crate) fn stored_hash(&self, slot: usize) -> u32 {
        assert!(slot < self.capacity());
        unsafe { (*self.object().hash.get().add(slot)).get() }
    }

    /// A caller compares candidates one at a time and must use the table's
    /// immutable callback guard. It need not clone the candidate bucket.
    pub(crate) fn first_in_bucket(&self, hash: u32) -> i32 {
        unsafe { (*self.object().index.get().add(self.bucket(hash))).get() }
    }
    pub(crate) fn next_in_bucket(&self, slot: usize) -> i32 {
        assert!(slot < self.capacity());
        unsafe { (*self.object().next.get().add(slot)).get() }
    }

    fn grow(&self) {
        let old_size = self.capacity();
        let new_size = crate::lisp::eval::gnu_hash_grown_capacity(old_size, old_size + 1);
        let arrays = Arrays::new(new_size);
        for slot in 0..old_size {
            unsafe {
                arrays.entries[2 * slot].set((*self.object().key_and_value.get().add(2 * slot)).get());
                arrays.entries[2 * slot + 1].set((*self.object().key_and_value.get().add(2 * slot + 1)).get());
                arrays.hash[slot].set((*self.object().hash.get().add(slot)).get());
                arrays.next[slot].set((*self.object().next.get().add(slot)).get());
            }
        }
        let old = unsafe { self.detach_arrays() };
        self.install(arrays);
        self.object().next_free.set(old_size as i32);
        self.rebuild_index();
        drop(old);
        crate::lisp::native_comp::note_lisp_allocation(
            crate::lisp::eval::gnu_hash_table_storage_bytes(new_size)
                - crate::lisp::eval::gnu_hash_table_storage_bytes(old_size),
        );
    }

    /// The stored hashes survive mutation, copy and growth. Only thaw or a
    /// relocation of identity keys is allowed to recompute them.
    pub(crate) fn rebuild_index(&self) {
        let o = self.object();
        for i in 0..self.index_size() {
            // Empty tables share an immutable static index; it is already -1.
            if self.capacity() > 0 { unsafe { (*o.index.get().add(i)).set(-1); } }
        }
        for slot in 0..self.capacity() {
            if self.entry(slot).is_none() { continue; }
            let bucket = self.bucket(self.stored_hash(slot));
            unsafe {
                (*o.next.get().add(slot)).set((*o.index.get().add(bucket)).get());
                (*o.index.get().add(bucket)).set(slot as i32);
            }
        }
    }

    pub(crate) fn insert(&self, hash: u32, key: Value, value: Value) -> usize {
        if self.object().next_free.get() < 0 { self.grow(); }
        let o = self.object();
        let slot = o.next_free.get() as usize;
        let bucket = self.bucket(hash);
        unsafe {
            o.next_free.set((*o.next.get().add(slot)).get());
            (*o.key_and_value.get().add(2 * slot)).set(key.word());
            (*o.key_and_value.get().add(2 * slot + 1)).set(value.word());
            (*o.hash.get().add(slot)).set(hash);
            (*o.next.get().add(slot)).set((*o.index.get().add(bucket)).get());
            (*o.index.get().add(bucket)).set(slot as i32);
        }
        o.count.set(o.count.get() + 1);
        slot
    }

    pub(crate) fn remove(&self, slot: usize) {
        assert!(self.entry(slot).is_some());
        let o = self.object();
        let bucket = self.bucket(self.stored_hash(slot));
        let mut link = unsafe { &*o.index.get().add(bucket) };
        while link.get() != slot as i32 {
            assert!(link.get() >= 0);
            link = unsafe { &*o.next.get().add(link.get() as usize) };
        }
        unsafe {
            link.set((*o.next.get().add(slot)).get());
            (*o.key_and_value.get().add(2 * slot)).set(UNUSED_KEY);
            (*o.key_and_value.get().add(2 * slot + 1)).set(UNUSED_KEY);
            (*o.next.get().add(slot)).set(o.next_free.get());
        }
        o.next_free.set(slot as i32);
        o.count.set(o.count.get() - 1);
    }

    pub(crate) fn clear(&self) {
        let o = self.object();
        let size = self.capacity();
        for slot in 0..size {
            unsafe {
                (*o.key_and_value.get().add(2 * slot)).set(UNUSED_KEY);
                (*o.key_and_value.get().add(2 * slot + 1)).set(UNUSED_KEY);
                (*o.next.get().add(slot)).set(if slot + 1 == size { -1 } else { (slot + 1) as i32 });
            }
        }
        o.count.set(0);
        o.next_free.set(if size == 0 { -1 } else { 0 });
        self.rebuild_index();
    }

    /// # Safety
    /// The caller immediately replaces or destroys the header; its former
    /// pointers must not be read again while detached arrays are owned here.
    unsafe fn detach_arrays(&self) -> Option<Arrays> {
        let n = self.capacity();
        if n == 0 { return None; }
        let o = self.object();
        unsafe {
            Some(Arrays {
                index: Box::from_raw(std::ptr::slice_from_raw_parts_mut(o.index.get(), self.index_size())),
                hash: Box::from_raw(std::ptr::slice_from_raw_parts_mut(o.hash.get(), n)),
                entries: Box::from_raw(std::ptr::slice_from_raw_parts_mut(o.key_and_value.get(), 2 * n)),
                next: Box::from_raw(std::ptr::slice_from_raw_parts_mut(o.next.get(), n)),
            })
        }
    }
}

/// # Safety
/// HEADER is an unreachable live hash table that has not been cleaned up.
pub(crate) unsafe fn cleanup(header: *mut VectorHeader) {
    let table = unsafe { HashTableRef::from_raw(header) };
    drop(unsafe { table.detach_arrays() });
}
