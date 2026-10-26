//! pdumper.c:pdumper_load's validation and object reconstruction (the
//! part the round-trip controls need; the process-level restore is
//! D12/D13).
//!
//! The loader validates the file exactly as `pdumper_load' does (size,
//! magic, the incomplete marker, the fingerprint), then rebuilds one Rust
//! object per object-start entry and applies the relocation tables to
//! its fields, so sharing and cycles come back as they were written.
//! Character tables and their internal nodes use the same graph relocation
//! as vectors; no numeric object identity or range history is reconstructed.

use super::super::*;
use super::context::*;
use super::image::*;
use crate::lisp::eval::RecordKind;
use crate::lisp::types::{CharTableRef, Kind, SubCharTableRef, SymbolName};

/// pdumper.c:pdumper_load_result.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum LoadError {
    /// PDUMPER_LOAD_FILE_NOT_FOUND belongs to the process-level loader.
    BadFileType,
    /// The incomplete marker is still set.
    FailedDump,
    /// The fingerprint is another binary's.
    VersionMismatch,
    Error(String),
}

/// One symbol record, as the image describes the symbol.
#[derive(Clone, Debug)]
pub(crate) struct LoadedSymbol {
    pub(crate) symbol: SymbolName,
    pub(crate) flags: u64,
    /// `None' is the unbound marker.
    pub(crate) value: Option<Value>,
    pub(crate) alias: Option<SymbolName>,
    pub(crate) function: Value,
    pub(crate) plist: Value,
    pub(crate) watchers: Vec<Value>,
}

pub(crate) struct LoadedImage {
    /// The validated header; the tests check its section layout.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) header: DumpHeader,
    pub(crate) roots: Vec<(RootSlot, Value)>,
    pub(crate) symbols: Vec<LoadedSymbol>,
    /// The initial obarray record's symbols in image order.
    pub(crate) obarray: Vec<SymbolName>,
    /// The cells of `nil' and `t'.
    pub(crate) builtin_cells: Vec<LoadedSymbol>,
    /// RELOC_NATIVE_COMP_UNIT: the native compilation unit records, in
    /// image order, for the late phase that reopens each unit.
    pub(crate) native_units: Vec<crate::lisp::types::NativeUnitRef>,
    /// RELOC_NATIVE_SUBR: the native function records with their names,
    /// for the very late phase that resolves each in its unit.
    pub(crate) native_functions: Vec<crate::lisp::native_comp::DumpedNativeFunction>,
}

/// The image file's bytes, mapped as pdumper.c:pdumper_load maps the file
/// (dump_mmap_contiguous): private and writable, the pages the page
/// cache's until one is written, nothing copied or zeroed for them, where
/// reading the file into a fresh vector cost the copy and the first touch
/// of every page on each boot.  The mapping starts at a cons block's
/// alignment, so that the image's cons blocks are blocks where they lie.
/// A file the host cannot map is read into memory of the same alignment.
pub(crate) struct ImageBytes {
    address: std::ptr::NonNull<u8>,
    len: usize,
    mapped: bool,
}

/// An image whose bytes stay for the life of the process, as the mapped
/// dump does: the loaded strings' data is where the image holds it.
#[derive(Clone, Copy)]
pub(crate) struct PermanentImage {
    address: std::ptr::NonNull<u8>,
    len: usize,
}

const IMAGE_ALIGNMENT: usize = crate::lisp::alloc::CONS_BLOCK_BYTES;

/// pdumper.c's dump_mmap_contiguous at `IMAGE_ALIGNMENT': address space
/// that much larger than the file is reserved, the file mapped private and
/// writable over its first aligned address, and the rest given back.
///
/// # Safety
/// FD is an open file of at least LEN bytes, LEN nonzero.
#[cfg(unix)]
unsafe fn map_aligned(fd: std::ffi::c_int, len: usize) -> *mut std::ffi::c_void {
    // SAFETY: a fresh reservation at an address of the kernel's choosing,
    // then the file over part of it (MAP_FIXED replaces only reserved
    // pages), then the reservation's unused ends unmapped.
    unsafe {
        let reserved = len + IMAGE_ALIGNMENT;
        let base = libc::mmap(
            std::ptr::null_mut(),
            reserved,
            libc::PROT_NONE,
            libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
            -1,
            0,
        );
        if base == libc::MAP_FAILED {
            return base;
        }
        let start = (base as usize).next_multiple_of(IMAGE_ALIGNMENT);
        let address = libc::mmap(
            start as *mut std::ffi::c_void,
            len,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_PRIVATE | libc::MAP_FIXED,
            fd,
            0,
        );
        if address == libc::MAP_FAILED {
            libc::munmap(base, reserved);
            return address;
        }
        let page = usize::try_from(libc::sysconf(libc::_SC_PAGESIZE)).unwrap_or(4096);
        let end = start + len.next_multiple_of(page);
        if start > base as usize {
            libc::munmap(base, start - base as usize);
        }
        if base as usize + reserved > end {
            libc::munmap(end as *mut std::ffi::c_void, base as usize + reserved - end);
        }
        address
    }
}

impl ImageBytes {
    pub(crate) fn open(path: &std::path::Path) -> std::io::Result<Self> {
        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd;
            // pdumper.c pdumper_load: an open error stays what it was
            // (ENOENT and ENOTDIR are FILE_NOT_FOUND, the rest strerror),
            // a failed fstat is FILE_NOT_FOUND, and from the size check on
            // everything that goes wrong is BAD_FILE_TYPE ("not a dump
            // file"): a directory opens and cannot be read.
            let file = crate::file_system::File::open(path)?;
            let len = usize::try_from(
                crate::file_system::file_metadata(&file)
                    .map_err(|error| std::io::Error::new(std::io::ErrorKind::NotFound, error))?
                    .len(),
            )
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "too large"))?;
            if len == 0 {
                return Ok(Self::copy(&[]));
            }
            // GNU's file mapping does not request anonymous-memory
            // prefaulting flags such as MAP_POPULATE.
            // SAFETY: a private mapping of the open file's `len' bytes at
            // the first aligned address of address space reserved for it
            // (`map_aligned'), unmapped by Drop unless it is made
            // permanent.
            let address = unsafe { map_aligned(file.as_raw_fd(), len) };
            if address == libc::MAP_FAILED {
                // A file the host cannot map (a directory, a filesystem
                // without mmap): read it, and report what the read does.
                return crate::file_system::read(path)
                    .map(|bytes| Self::copy(&bytes))
                    .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error));
            }
            let address = std::ptr::NonNull::new(address.cast::<u8>()).ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "mmap returned a null address",
                )
            })?;
            Ok(Self {
                address,
                len,
                mapped: true,
            })
        }
        #[cfg(not(unix))]
        {
            std::fs::read(path).map(|bytes| Self::copy(&bytes))
        }
    }

    /// BYTES copied into aligned memory of their own.
    pub(crate) fn copy(bytes: &[u8]) -> Self {
        if bytes.is_empty() {
            return Self {
                address: std::ptr::NonNull::<u64>::dangling().cast(),
                len: 0,
                mapped: false,
            };
        }
        let layout = std::alloc::Layout::from_size_align(bytes.len(), IMAGE_ALIGNMENT)
            .expect("image layout");
        // SAFETY: a nonzero layout; the copy fills every byte.
        let address = unsafe {
            let address = std::alloc::alloc(layout);
            if address.is_null() {
                std::alloc::handle_alloc_error(layout);
            }
            std::ptr::copy_nonoverlapping(bytes.as_ptr(), address, bytes.len());
            std::ptr::NonNull::new_unchecked(address)
        };
        Self {
            address,
            len: bytes.len(),
            mapped: false,
        }
    }

    /// pdumper.c never unmaps a loaded dump: its objects point into it.
    pub(crate) fn into_permanent(self) -> PermanentImage {
        let image = PermanentImage {
            address: self.address,
            len: self.len,
        };
        std::mem::forget(self);
        image
    }
}

impl std::ops::Deref for ImageBytes {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        // SAFETY: the mapping or copy covers `len' readable bytes for the
        // life of this value.
        unsafe { std::slice::from_raw_parts(self.address.as_ptr(), self.len) }
    }
}

impl Drop for ImageBytes {
    fn drop(&mut self) {
        // SAFETY: the mapping or the allocation this value made, released
        // once.
        unsafe {
            if self.mapped {
                #[cfg(unix)]
                libc::munmap(self.address.as_ptr().cast(), self.len);
            } else if self.len != 0 {
                std::alloc::dealloc(
                    self.address.as_ptr(),
                    std::alloc::Layout::from_size_align(self.len, IMAGE_ALIGNMENT)
                        .expect("image layout"),
                );
            }
        }
    }
}

impl PermanentImage {
    fn bytes(&self) -> &'static [u8] {
        // SAFETY: the bytes are never released. They are written only
        // through string data after the load, never read through this
        // slice then.
        unsafe { std::slice::from_raw_parts(self.address.as_ptr(), self.len) }
    }

    /// The address of the byte at OFFSET, which is in the image.
    fn at(&self, offset: usize) -> std::ptr::NonNull<u8> {
        assert!(offset < self.len, "offset {offset} is outside the image");
        // SAFETY: inside the allocation, by the assertion.
        unsafe { self.address.add(offset) }
    }
}

struct Reader<'a> {
    bytes: &'a [u8],
}

impl Reader<'_> {
    #[inline]
    fn word(&self, offset: u32) -> Result<u64, LoadError> {
        let [word] = self.words(offset)?;
        Ok(word)
    }

    /// The N consecutive words at OFFSET.
    #[inline]
    fn words<const N: usize>(&self, offset: u32) -> Result<[u64; N], LoadError> {
        let start = offset as usize;
        let Some(bytes) = self.bytes.get(start..start + 8 * N) else {
            return Err(outside_image(offset));
        };
        let mut words = [0; N];
        for (word, bytes) in words.iter_mut().zip(bytes.as_chunks::<8>().0) {
            *word = u64::from_le_bytes(*bytes);
        }
        Ok(words)
    }

    fn u32(&self, offset: u32) -> Result<u32, LoadError> {
        let start = offset as usize;
        self.bytes
            .get(start..start + 4)
            .map(|slice| u32::from_le_bytes(slice.try_into().expect("four bytes")))
            .ok_or_else(|| {
                LoadError::Error(format!("table entry at {offset} is outside the image"))
            })
    }
}

#[cold]
fn outside_image(offset: u32) -> LoadError {
    LoadError::Error(format!("word at {offset} is outside the image"))
}

/// The validation pdumper_load performs before the point of no return.
pub(crate) fn validate_header(bytes: &[u8]) -> Result<DumpHeader, LoadError> {
    let Some(mut header) = DumpHeader::from_bytes(bytes) else {
        return Err(LoadError::BadFileType);
    };
    if header.magic != DUMP_MAGIC {
        if header.magic[0] == INCOMPLETE_MAGIC_BYTE && {
            header.magic[0] = DUMP_MAGIC[0];
            header.magic == DUMP_MAGIC
        } {
            return Err(LoadError::FailedDump);
        }
        return Err(LoadError::BadFileType);
    }
    let desired = executable_fingerprint();
    if header.fingerprint != *desired {
        if !super::dump_messages_suppressed() {
            eprintln!("desired fingerprint: {}", hex(desired));
            eprintln!("found fingerprint: {}", hex(&header.fingerprint));
        }
        return Err(LoadError::VersionMismatch);
    }
    Ok(header)
}

/// Load an image from BYTES, copied for the life of the process.
#[cfg(test)]
pub(crate) fn load_image(bytes: &[u8], interp: &mut Interpreter) -> Result<LoadedImage, LoadError> {
    let header = validate_header(bytes)?;
    load_permanent_image(ImageBytes::copy(bytes).into_permanent(), header, interp)
}

/// Load IMAGE, whose HEADER `validate_header' accepted.
pub(crate) fn load_permanent_image(
    image: PermanentImage,
    header: DumpHeader,
    interp: &mut Interpreter,
) -> Result<LoadedImage, LoadError> {
    let reader = Reader {
        bytes: image.bytes(),
    };
    let starts = StartTable::read(&reader, header.object_starts, "object start")?;
    let relocs = |phase: usize| RelocTable::read(&reader, header.dump_relocs[phase], "relocation");
    let mut loader = Loader {
        objects: object_slots(starts.len()),
        starts,
        relocs: relocs(EARLY_RELOCS)?,
        reloc_cursor: 0,
        reloc_last: 0,
        reloc_next: 0,
        late_relocs: relocs(LATE_RELOCS)?,
        very_late_relocs: relocs(VERY_LATE_RELOCS)?,
        conses: ImageConses::default(),
        subrs: HashMap::default(),
        reader,
        image,
        interp,
    };
    loader.load(header)
}

/// N objects' slots, each `nil' until its object is installed: zero words,
/// the kernel's pages until written, as an unread page of the dump is.
/// The image records `nil', an immediate, as no object.
fn object_slots(n: usize) -> Vec<Value> {
    let mut words = std::mem::ManuallyDrop::new(vec![0_usize; n]);
    // SAFETY: a Value is a transparent word, and the zero word is `nil'.
    unsafe {
        Vec::from_raw_parts(
            words.as_mut_ptr().cast::<Value>(),
            words.len(),
            words.capacity(),
        )
    }
}

/// One of the image's tables of LEN-byte entries sorted by offset -- the
/// object starts, a phase's dump relocations -- read where the image maps
/// it, as pdumper.c reads its tables.  An entry is found by searching the
/// offsets, from a known position outward when there is one: the loader
/// reads fields in ascending order, so the entry it wants next is the
/// cursor's or a few after it.
struct ImageTable<'a, const LEN: usize> {
    entries: &'a [[u8; LEN]],
}

/// The object-start table: an object's offset and type code.
type StartTable<'a> = ImageTable<'a, TABLE_ENTRY_LEN>;
/// A relocation table: the field's offset, the relocation's code and the
/// number of the object it names.
type RelocTable<'a> = ImageTable<'a, RELOC_ENTRY_LEN>;

impl<'a, const LEN: usize> ImageTable<'a, LEN> {
    fn read(reader: &Reader<'a>, locator: TableLocator, what: &str) -> Result<Self, LoadError> {
        let len = locator.nr_entries as usize;
        let start = locator.offset as usize;
        let entries = start
            .checked_add(len * LEN)
            .and_then(|end| reader.bytes.get(start..end))
            .ok_or_else(|| LoadError::Error(format!("the {what} table is outside the image")))?
            .as_chunks::<LEN>()
            .0;
        let table = Self { entries };
        // Each key is an aligned image word, once: a second entry for the
        // same word would be lost.  One pass checks every entry, and a
        // failure is located after.
        let mut previous = -1_i64;
        let mut misplaced = false;
        for entry in entries {
            let offset = entry_word(entry, 0);
            misplaced |=
                !(offset as usize).is_multiple_of(DUMP_ALIGNMENT) || i64::from(offset) <= previous;
            previous = i64::from(offset);
        }
        if misplaced {
            let index = (0..len)
                .find(|&index| {
                    let offset = table.offset(index);
                    !(offset as usize).is_multiple_of(DUMP_ALIGNMENT)
                        || index > 0 && table.offset(index - 1) >= offset
                })
                .unwrap_or_default();
            return Err(LoadError::Error(format!(
                "{what} table entry {index} at {} is unaligned or out of order",
                table.offset(index)
            )));
        }
        Ok(table)
    }

    #[inline(always)]
    fn len(&self) -> usize {
        self.entries.len()
    }

    /// The offset of entry INDEX, which is below `len'.
    #[inline(always)]
    fn offset(&self, index: usize) -> u32 {
        entry_word(&self.entries[index], 0)
    }

    /// The offset of entry INDEX, u32::MAX past the last.
    #[inline(always)]
    fn offset_or_end(&self, index: usize) -> u32 {
        self.entries
            .get(index)
            .map_or(u32::MAX, |entry| entry_word(entry, 0))
    }

    /// The code of entry INDEX, which is below `len'.
    #[inline(always)]
    fn code(&self, index: usize) -> u32 {
        entry_word(&self.entries[index], 4)
    }

    /// The relocation at OFFSET, for a pass that asks in ascending order:
    /// the search starts at HINT, where the pass's last answer left it.
    fn reloc_at_from(&self, hint: &mut usize, offset: u32) -> Option<DumpRelocKind> {
        let index = self.rank_from(*hint, offset);
        *hint = index;
        (self.offset_or_end(index) == offset)
            .then(|| DumpRelocKind::from_u32(self.code(index)))
            .flatten()
    }

    /// The number of entries before OFFSET, searched outward from entry
    /// HINT in steps that double, then within the last step.
    fn rank_from(&self, hint: usize, offset: u32) -> usize {
        let len = self.len();
        let hint = hint.min(len);
        let (mut low, mut high) = (0, len);
        if hint < len && self.offset(hint) < offset {
            low = hint + 1;
            let mut step = 1;
            while hint + step < len {
                if self.offset(hint + step) >= offset {
                    high = hint + step;
                    break;
                }
                low = hint + step + 1;
                step *= 2;
            }
        } else {
            high = hint;
            let mut step = 1;
            while step <= hint {
                if self.offset(hint - step) < offset {
                    low = hint - step + 1;
                    break;
                }
                high = hint - step;
                step *= 2;
            }
        }
        low + self.entries[low..high].partition_point(|entry| entry_word(entry, 0) < offset)
    }

    /// The index of the entry at OFFSET.
    fn find(&self, offset: u32) -> Option<usize> {
        let index = self
            .entries
            .partition_point(|entry| entry_word(entry, 0) < offset);
        (index < self.len() && self.offset(index) == offset).then_some(index)
    }
}

impl RelocTable<'_> {
    /// The object number of relocation INDEX, which is below `len'.
    #[inline(always)]
    fn target(&self, index: usize) -> u32 {
        entry_word(&self.entries[index], 8)
    }
}

/// The little-endian word AT bytes into ENTRY.
#[inline(always)]
fn entry_word<const LEN: usize>(entry: &[u8; LEN], at: usize) -> u32 {
    let word: [u8; 4] = entry[at..at + 4].try_into().expect("a word of the entry");
    u32::from_le_bytes(word)
}

struct Loader<'a> {
    reader: Reader<'a>,
    image: PermanentImage,
    /// The object-start table: an object's number is its entry's index.
    starts: StartTable<'a>,
    /// The reconstructed objects by number; `nil' for an entry that stands
    /// for no Lisp object (text properties, built-in cells).
    objects: Vec<Value>,
    relocs: RelocTable<'a>,
    /// The relocations' cursor: the number of entries at or before the
    /// field read last (`reloc_last') and the offset of the next one
    /// (`reloc_next', u32::MAX past the end).  Fields are read in
    /// ascending order within a pass, so the next one's entry is looked
    /// for from here on, as pdumper.c applies its sorted relocations in
    /// order; a read elsewhere seeks through the index.
    reloc_cursor: usize,
    reloc_last: u32,
    reloc_next: u32,
    late_relocs: RelocTable<'a>,
    very_late_relocs: RelocTable<'a>,
    /// The image's cons blocks, once adopted.
    conses: ImageConses,
    /// The subr each copied record of the Emacs image names.
    subrs: HashMap<
        u32,
        crate::lisp::native_comp::abi::BuiltinRef,
        crate::lisp::types::IdentityBuildHasher,
    >,
    interp: &'a mut Interpreter,
}

/// The code of RELOC_DUMP_TO_DUMP_LV naming a cons.
pub(super) const CONS_LV: u32 = 0x100 | DumpType::Cons as u32;

/// The image's cons blocks where the image is mapped: BASE the image's
/// address, START the first block's offset, COUNT the conses.
#[derive(Default)]
struct ImageConses {
    base: usize,
    start: usize,
    count: usize,
    /// Past the last cell, from START.
    end: usize,
}

impl ImageConses {
    /// The offsets of the first cell and just past the last.
    fn cells(&self) -> (u32, u32) {
        let first = self.start + crate::lisp::alloc::image_cons_offset(0);
        (first as u32, (self.start + self.end) as u32)
    }

    /// True when the word at OFFSET is in one of the cells: a block's
    /// cells, not its metadata, and no later than the last cell.
    #[inline(always)]
    fn holds(&self, offset: u32) -> bool {
        use crate::lisp::alloc::{CONS_BLOCK_BYTES, CONS_BLOCK_CELLS};
        let at = (offset as usize).wrapping_sub(self.start);
        at < self.end && at % CONS_BLOCK_BYTES >= CONS_BLOCK_CELLS
    }

    /// The cons whose cell is at OFFSET, which must be a cell of the
    /// blocks.
    #[inline(always)]
    fn cons_at(&self, offset: u32) -> Result<Value, LoadError> {
        if !self.holds(offset) || !offset.is_multiple_of(16) {
            return Err(LoadError::Error(format!(
                "cons at {offset} is not a cell of the cons blocks"
            )));
        }
        // SAFETY: an allocated cell of the adopted blocks, by the check.
        Ok(Value::Cons(unsafe {
            crate::lisp::alloc::ConsRef::from_raw(
                (self.base + offset as usize) as *const crate::lisp::types::ConsCell,
            )
        }))
    }

    /// The word at OFFSET, which must be a car or cdr of the cells.
    #[inline(always)]
    fn field(&self, offset: u32) -> Result<*mut Value, LoadError> {
        if !self.holds(offset) || !offset.is_multiple_of(8) {
            return Err(LoadError::Error(format!(
                "relocation at {offset} is not a word of a cons"
            )));
        }
        Ok((self.base + offset as usize) as *mut Value)
    }
}

impl Loader<'_> {
    fn load(&mut self, header: DumpHeader) -> Result<LoadedImage, LoadError> {
        // pdumper's mapped objects: the image's cons blocks are the heap's
        // where they lie, before anything names one of their conses.
        self.conses = self.adopt_cons_blocks(&header)?;
        // Each object's type, and the objects by the phases that build
        // them, each list in image order: the objects that need only their
        // own bytes, and the rest; a cons is where its block holds it.
        let mut kinds = Vec::with_capacity(self.starts.len());
        let mut own_bytes_objects = Vec::new();
        let mut other_objects = Vec::new();
        for number in 0..self.starts.len() {
            let code = self.starts.code(number);
            let kind = DumpType::from_u32(code).ok_or_else(|| {
                LoadError::Error(format!("unknown object type {code} of object {number}"))
            })?;
            match kind {
                DumpType::Cons => {
                    self.objects[number] = self.conses.cons_at(self.starts.offset(number))?;
                }
                DumpType::String
                | DumpType::StringObject
                | DumpType::EmptyString
                | DumpType::Float
                | DumpType::Bignum
                | DumpType::BoolVector => own_bytes_objects.push(number as u32),
                _ => other_objects.push(number as u32),
            }
            kinds.push(kind);
        }
        // Phase 1: objects that need nothing but their own bytes.
        let mut string_props: Vec<(usize, u32)> = Vec::new();
        for &number in &own_bytes_objects {
            let number = number as usize;
            let kind = kinds[number];
            let offset = self.starts.offset(number);
            match kind {
                DumpType::String | DumpType::StringObject | DumpType::EmptyString => {
                    let (value, props) = self.load_string(offset, kind)?;
                    if let Some(props) = props {
                        string_props.push((number, props));
                    }
                    self.objects[number] = value;
                }
                DumpType::Float => {
                    let value = Value::float(f64::from_bits(self.reader.word(offset)?));
                    self.objects[number] = value;
                }
                DumpType::Bignum => {
                    let value = self.load_bignum(offset)?;
                    self.objects[number] = value;
                }
                DumpType::BoolVector => {
                    let value = self.load_bool_vector(offset)?;
                    self.objects[number] = value;
                }
                _ => {}
            }
        }

        // Phase 2: objects whose construction needs a name, and the
        // containers as placeholders.
        let mut symbol_records = Vec::new();
        let mut obarray_records = Vec::new();
        let mut hash_table_records = Vec::new();
        let mut char_tables = Vec::new();
        let mut sub_char_tables = Vec::new();
        let mut buffers = Vec::new();
        let mut markers = Vec::new();
        let mut overlays = Vec::new();
        let mut finalizers = Vec::new();
        let mut native_units = Vec::new();
        let mut native_function_records = Vec::new();
        // The native records' relocations, found in object order.
        let (mut native_subr_hint, mut native_unit_hint) = (0, 0);
        for &number in &other_objects {
            let number = number as usize;
            let kind = kinds[number];
            let offset = self.starts.offset(number);
            match kind {
                DumpType::Symbol => {
                    let flags = self.reader.word(offset)?;
                    let name = self.value_at(offset + 8)?;
                    let symbol = self.symbol_of_record(offset, flags, name)?;
                    self.objects[number] = Value::Symbol(symbol);
                    symbol_records.push((offset, symbol, flags));
                }
                DumpType::Closure => {
                    let size = self.reader.word(offset)? as usize;
                    if !(3..=0xfff).contains(&size) {
                        return Err(LoadError::Error(format!(
                            "invalid closure size at {offset}"
                        )));
                    }
                    // Publish every identity before relocating any field.
                    // Any closure slot can participate in an object cycle.
                    self.objects[number] =
                        Value::Closure(crate::lisp::types::ClosureRef::filled(size, Value::Nil));
                }
                DumpType::Vector => {
                    let size = self.reader.word(offset)? as usize;
                    if size != 0 {
                        // Value::vector's C footprint: the header word and
                        // a word a slot.
                        crate::lisp::native_comp::note_lisp_allocation((size + 1) * 8);
                    }
                    self.objects[number] =
                        Value::Vector(crate::lisp::types::VectorRef::filled(size, Value::Nil));
                }
                DumpType::LispRecord => {
                    let size = self.reader.word(offset)? as usize;
                    if !(1..=crate::lisp::alloc::vectors::generic_records::MAX_RECORD_SLOTS)
                        .contains(&size)
                    {
                        return Err(LoadError::Error(format!("invalid record size at {offset}")));
                    }
                    let record =
                        crate::lisp::types::LispRecordRef::filled(Value::Nil, size - 1, Value::Nil);
                    self.objects[number] = Value::LispRecord(record);
                }
                DumpType::HashTable => {
                    let nslots = self.reader.word(offset + 24)? as usize;
                    if nslots != 7 || self.reader.word(offset + 8)? != 7 {
                        return Err(LoadError::Error(format!(
                            "invalid frozen hash table at {offset}"
                        )));
                    }
                    let at = offset + 32 + 8 * nslots as u32;
                    let count = self.reader.word(at)? as usize;
                    let test = match self.reader.word(at + 16)? {
                        HASH_TEST_EQ => "eq",
                        HASH_TEST_EQL => "eql",
                        HASH_TEST_EQUAL => "equal",
                        code => {
                            return Err(LoadError::Error(format!(
                                "invalid frozen hash test {code}"
                            )));
                        }
                    };
                    let value = crate::lisp::primitives::make_hash_table_value(
                        self.interp,
                        Value::symbol(test),
                        count,
                        Value::Nil,
                        Value::Nil,
                        &Env::new(),
                    )
                    .map_err(|error| LoadError::Error(error.to_string()))?;
                    let Kind::HashTable(table) = value.kind() else {
                        unreachable!()
                    };
                    self.objects[number] = value;
                    hash_table_records.push((offset, table, nslots));
                }
                DumpType::Record | DumpType::Obarray => {
                    let id = self.reader.word(offset)?;
                    let kind_code = self.reader.word(offset + 8)? as u32;
                    let nslots = self.reader.word(offset + 24)? as usize;
                    let type_tag = self.symbol_or_immediate_at(offset + 16)?;
                    if kind_code == NATIVE_SUBR_CODE {
                        if nslots != 11
                            || self
                                .very_late_relocs
                                .reloc_at_from(&mut native_subr_hint, offset)
                                != Some(DumpRelocKind::NativeSubr)
                        {
                            return Err(LoadError::Error(
                                "invalid native subr image record".into(),
                            ));
                        }
                        let function = crate::lisp::types::NativeFunctionRef::blank();
                        self.objects[number] = Value::NativeFunction(function);
                        native_function_records.push((offset, function, nslots));
                        continue;
                    }
                    if kind_code == super::context::NATIVE_UNIT_CODE {
                        if nslots != 7
                            || self
                                .late_relocs
                                .reloc_at_from(&mut native_unit_hint, offset)
                                != Some(DumpRelocKind::NativeCompUnit)
                        {
                            return Err(LoadError::Error(
                                "invalid native compilation unit image record".into(),
                            ));
                        }
                        let unit = crate::lisp::types::NativeUnitRef::new();
                        self.objects[number] = Value::NativeCompUnit(unit);
                        native_units.push(unit);
                        continue;
                    }
                    let record_kind = record_kind_from_code(kind_code).ok_or_else(|| {
                        LoadError::Error(format!("unknown record kind {kind_code} at {offset}"))
                    })?;
                    self.interp.install_record(record_state_for_load(
                        id,
                        record_kind,
                        type_tag,
                        vec![Value::Nil; nslots],
                    ));
                    self.objects[number] = self.interp.record_value(id);
                    if kind == DumpType::Obarray {
                        obarray_records.push((offset, id, nslots));
                    }
                }
                DumpType::CharTable => {
                    let slots = self.reader.word(offset)? as usize;
                    if !(68..=crate::lisp::alloc::vectors::char_tables::CHAR_TABLE_MAX_SLOTS)
                        .contains(&slots)
                    {
                        return Err(LoadError::Error("invalid char-table slot count".into()));
                    }
                    let table = CharTableRef::new(Value::Nil, Value::Nil, slots - 68);
                    self.objects[number] = Value::CharTable(table);
                    char_tables.push((offset, table));
                }
                DumpType::SubCharTable => {
                    let depth = self.reader.word(offset)? as usize;
                    let min_char = self.reader.word(offset + 8)?;
                    if !(1..=3).contains(&depth) || min_char > 0x3fffff {
                        return Err(LoadError::Error("invalid sub-char-table header".into()));
                    }
                    let table = SubCharTableRef::new(depth, min_char as u32, Value::Nil);
                    self.objects[number] = Value::SubCharTable(table);
                    sub_char_tables.push((offset, table));
                }
                DumpType::Buffer => {
                    let id = self.reader.word(offset)?;
                    let flags = self.reader.word(offset + 8 * BUFFER_FLAGS)?;
                    if flags & BUFFER_FLAG_DEAD != 0 {
                        // A killed buffer: the object, and nothing to
                        // install.
                        self.objects[number] =
                            Value::Buffer(crate::lisp::types::BufferRef::for_restore(
                                id,
                                crate::buffer::Buffer::new(""),
                            ));
                        continue;
                    }
                    let name = self.value_at(offset + 8 * BUFFER_NAME)?;
                    let name = string_like(&name)
                        .map(|string| string.text)
                        .ok_or_else(|| LoadError::Error("buffer name is not a string".into()))?;
                    self.objects[number] =
                        Value::Buffer(crate::lisp::types::BufferRef::for_restore(
                            id,
                            crate::buffer::Buffer::new(&name),
                        ));
                    buffers.push((offset, number, id));
                }
                DumpType::Marker => {
                    let marker = crate::lisp::types::MarkerRef::new();
                    self.objects[number] = Value::Marker(marker);
                    markers.push((offset, marker));
                }
                DumpType::Overlay => {
                    let flags = self.reader.word(offset)?;
                    let overlay = crate::overlay::OverlayRef::new(
                        flags & OVERLAY_FRONT_ADVANCE != 0,
                        flags & OVERLAY_REAR_ADVANCE != 0,
                    );
                    self.objects[number] = Value::Overlay(overlay);
                    overlays.push((offset, overlay));
                }
                DumpType::Finalizer => {
                    let object = crate::lisp::alloc::FinalizerRef::new(Value::Nil);
                    self.objects[number] = Value::Finalizer(object);
                    finalizers.push((offset, object));
                }
                // Nilled pseudovectors: each is a dead object of its own
                // in the new process, whose live initial frame and
                // terminal were made anew (init_frame_once_for_pdumper,
                // init_tty); the dump-time id is not an identity here.
                DumpType::Frame => {
                    let id = self.interp.install_dead_frame();
                    self.objects[number] = Value::Frame(id);
                }
                DumpType::Terminal => {
                    let id = self.interp.install_dead_terminal();
                    self.objects[number] = Value::Terminal(id);
                }
                _ => {}
            }
        }

        // Phase 3: relocate the fields of the published containers.
        let mut obarray = Vec::new();
        let mut scratch = Vec::new();
        self.relocate_conses()?;
        for &number in &other_objects {
            let number = number as usize;
            let kind = kinds[number];
            let offset = self.starts.offset(number);
            match kind {
                DumpType::Closure => {
                    let Kind::Closure(closure) = self.objects[number].kind() else {
                        unreachable!()
                    };
                    scratch.resize(closure.public_len(), Value::Nil);
                    self.fields(offset + 8, &mut scratch)?;
                    for (index, &value) in scratch.iter().enumerate() {
                        closure.initialize_slot(index, value);
                    }
                    if !closure.is_bytecode() && !matches!(closure.body().kind(), Kind::Cons(_)) {
                        return Err(LoadError::Error(format!(
                            "invalid closure code at {offset}"
                        )));
                    }
                }
                DumpType::Vector => {
                    let size = self.reader.word(offset)? as usize;
                    let Kind::Vector(vector) = self.objects[number].kind() else {
                        unreachable!()
                    };
                    scratch.resize(size, Value::Nil);
                    self.fields(offset + 8, &mut scratch)?;
                    for (index, &value) in scratch.iter().enumerate() {
                        vector.set(index, value);
                    }
                }
                DumpType::LispRecord => {
                    let Kind::LispRecord(record) = self.objects[number].kind() else {
                        unreachable!("record placeholder installed in phase 2")
                    };
                    scratch.resize(record.len(), Value::Nil);
                    self.fields(offset + 8, &mut scratch)?;
                    for (index, &field) in scratch.iter().enumerate() {
                        record.set(index, field);
                    }
                }
                DumpType::Record | DumpType::Obarray => {
                    let id = self.reader.word(offset)?;
                    let nslots = self.reader.word(offset + 24)? as usize;
                    let mut slots = vec![Value::Nil; nslots];
                    self.fields(offset + 32, &mut slots)?;
                    if let Kind::NativeFunction(function) = self.objects[number].kind() {
                        let integer = |value: Value| {
                            value
                                .as_integer()
                                .map_err(|error| LoadError::Error(error.to_string()))
                        };
                        let minimum = integer(slots[1])? as i16;
                        let maximum = if slots[2].symbol_str() == Some("many") {
                            -2
                        } else {
                            integer(slots[2])? as i16
                        };
                        function.set_arity(minimum, maximum);
                        function.set_doc_index(integer(slots[5])? as isize);
                        function.set_fields([slots[6], slots[7], slots[8], slots[9], slots[4]]);
                    } else if let Kind::NativeCompUnit(unit) = self.objects[number].kind() {
                        unit.set_fields(
                            slots
                                .try_into()
                                .map_err(|_| LoadError::Error("invalid unit field count".into()))?,
                        );
                    } else {
                        let record = self.interp.find_record_mut(id).ok_or_else(|| {
                            LoadError::Error(format!("record {id} was installed"))
                        })?;
                        record.slots = slots;
                    }
                }
                _ => {}
            }
        }
        let mut native_functions = Vec::new();
        let mut name_hint = 0;
        for (offset, function, nslots) in native_function_records {
            let names_at = offset + 32 + 8 * nslots as u32;
            let name = self.image_c_string(names_at, &mut name_hint, "name")?;
            let c_name = self.image_c_string(names_at + 8, &mut name_hint, "C name")?;
            // SAFETY: this load's newly allocated object is unpublished, and
            // both names are C strings of the permanent image.
            unsafe { (*function.as_ptr()).set_image_names(name, c_name) };
            native_functions.push(crate::lisp::native_comp::DumpedNativeFunction { function });
        }
        for (offset, id, nslots) in obarray_records {
            let count_at = offset + 32 + 8 * nslots as u32;
            let count = self.reader.word(count_at)? as usize;
            for index in 0..count {
                let value = self.value_at(count_at + 8 * (index as u32 + 1))?;
                let symbol = symbol_of(value, "obarray entry")?;
                if self.interp.is_standard_obarray_id(id) {
                    obarray.push(symbol);
                }
            }
        }
        for (offset, table) in char_tables {
            scratch.resize(table.slot_count(), Value::Nil);
            self.fields(offset + 8, &mut scratch)?;
            table.set_slots(&scratch);
        }
        for (offset, table) in sub_char_tables {
            scratch.resize(table.slots().len(), Value::Nil);
            self.fields(offset + 16, &mut scratch)?;
            table.set_slots(&scratch);
        }
        for (string, props_offset) in string_props {
            let spans = self.load_text_properties(props_offset)?;
            match self.objects[string].kind() {
                Kind::StringObject(state) => {
                    state.borrow_mut().props = spans.into();
                }
                other => {
                    return Err(LoadError::Error(format!(
                        "text properties on a non-object string: {other:?}"
                    )));
                }
            }
        }
        // The buffers with their text, then the markers into them, the
        // deleted overlays on their lists, and the finalizers in list
        // order (the `finalizers.next' root leads the chain; a finalizer
        // off the chain follows in image order).
        let mut marker_chains = Vec::with_capacity(buffers.len());
        for (offset, number, id) in buffers {
            marker_chains.push(self.load_buffer(offset, number, id)?);
        }
        for (offset, marker) in markers {
            self.load_marker(offset, marker)?;
        }
        for (buffer, chain) in marker_chains {
            if !buffer.restore_marker_order(&chain) {
                return Err(LoadError::Error(
                    "buffer marker chain does not match marker fields".into(),
                ));
            }
        }
        for (offset, overlay) in overlays {
            self.load_overlay(offset, overlay)?;
        }
        let mut finalizer_records = Vec::new();
        for (offset, id) in finalizers {
            let function = self.value_at(offset + 8)?;
            id.set_function(function);
            let next = match (self.value_at(offset + 24)?).kind() {
                Kind::Finalizer(next) => Some(next),
                Kind::Nil => None,
                other => {
                    return Err(LoadError::Error(format!(
                        "finalizer {id:?}'s next is not a finalizer: {other:?}"
                    )));
                }
            };
            finalizer_records.push((id, next));
        }

        // thaw_hash_tables: every table on the hash list, from its frozen
        // contents. All object fields must be relocated before hashing keys.
        let thaw_list = self.hash_list_tables(&header)?;
        for (offset, table, nslots) in hash_table_records {
            if !thaw_list.contains(&table) {
                return Err(LoadError::Error(format!(
                    "hash table at {offset} is not on the hash list"
                )));
            }
            let mut at = offset + 32 + 8 * nslots as u32;
            let count = self.reader.word(at)? as usize;
            let weakness = self.value_at(at + 8)?;
            let mutable = self.reader.word(at + 24)? != 0;
            let purecopy = self.value_at(offset + 32 + 8 * 6)?;
            table.set_weakness(
                crate::lisp::primitives::hash_table_weakness(self.interp, weakness, &Env::new())
                    .map_err(|error| LoadError::Error(error.to_string()))?,
            );
            table.set_purecopy(purecopy.is_truthy());
            let test = table.test().standard_test().expect("frozen standard test");
            at += 32;
            for _ in 0..count {
                let key = self.value_at(at)?;
                let value = self.value_at(at + 8)?;
                let hash = crate::lisp::primitives::standard_hash_code(
                    self.interp,
                    test,
                    &key,
                    &Env::new(),
                );
                table.insert(hash, key, value);
                at += 16;
            }
            table.set_mutable(mutable);
        }

        // Phase 5: the symbol records.
        let mut symbols = Vec::with_capacity(symbol_records.len());
        for (offset, symbol, flags) in symbol_records {
            let [val, function, plist] = self.field_array(offset + 16)?;
            // A private obarray's chain; the initial obarray's was filled as
            // its symbols were filed.
            if (flags >> SYMBOL_INTERNED_SHIFT) & 3 == SYMBOL_INTERNED {
                let next = self.value_at(offset + SYMBOL_NEXT)?;
                symbol.cell().next.set(match next.kind() {
                    Kind::Integer(0) => None,
                    _ => Some(symbol_of(next, "obarray chain")?),
                });
            }
            let nwatchers = self.reader.word(offset + SYMBOL_NWATCHERS)? as u32;
            let mut watchers = Vec::new();
            for index in 0..nwatchers {
                watchers.push(self.value_at(offset + SYMBOL_WATCHERS + 8 * index)?);
            }
            let (value, alias) = if flags & SYMBOL_REDIRECT_MASK == SYMBOL_VARALIAS {
                (None, Some(symbol_of(val, "alias target")?))
            } else {
                ((!matches!(val.kind(), Kind::Unbound)).then_some(val), None)
            };
            symbols.push(LoadedSymbol {
                symbol,
                flags,
                value,
                alias,
                function,
                plist,
                watchers,
            });
        }

        // Phase 6: the Emacs relocations, the root slots.
        let mut roots = Vec::new();
        let mut builtin_cells = Vec::new();
        for index in 0..header.emacs_relocs.nr_entries {
            let at = header.emacs_relocs.offset + index * EMACS_RELOC_LEN as u32;
            let kind = EmacsRelocKind::from_u32(self.reader.u32(at)?)
                .ok_or_else(|| LoadError::Error(format!("unknown Emacs relocation at {at}")))?;
            let slot = RootSlot::from_u32(self.reader.u32(at + 4)?)
                .ok_or_else(|| LoadError::Error(format!("unknown root slot at {at}")))?;
            let payload = self.reader.word(at + 8)?;
            if matches!(slot, RootSlot::NilCells | RootSlot::TCells)
                && matches!(kind, EmacsRelocKind::DumpLv(_))
            {
                let name = if slot == RootSlot::NilCells {
                    "nil"
                } else {
                    "t"
                };
                builtin_cells.push(self.load_builtin_cells(payload as u32, name)?);
                continue;
            }
            let value = match kind {
                EmacsRelocKind::Immediate => immediate_value(payload)?,
                EmacsRelocKind::DumpLv(_) => self.object_at(payload as u32).ok_or_else(|| {
                    LoadError::Error(format!("root points at no object: {payload}"))
                })?,
                EmacsRelocKind::EmacsLv(kind) => self.emacs_image_object(payload as u32, kind)?,
            };
            roots.push((slot, value));
        }
        let mut chain = Vec::new();
        let mut cursor = roots
            .iter()
            .find_map(|(slot, value)| match (slot, value.kind()) {
                (RootSlot::FinalizersNext, Kind::Finalizer(id)) => Some(id),
                _ => None,
            });
        while let Some(id) = cursor {
            if chain.contains(&id) {
                return Err(LoadError::Error(format!("finalizer chain loops at {id:?}")));
            }
            chain.push(id);
            cursor = finalizer_records
                .iter()
                .find(|(candidate, _)| *candidate == id)
                .and_then(|(_, next)| *next);
        }
        for id in &chain {
            let Some((_, _)) = finalizer_records
                .iter()
                .find(|(candidate, _)| candidate == id)
            else {
                return Err(LoadError::Error(format!(
                    "finalizer {id:?} is on the chain but not in the image"
                )));
            };
            self.interp.install_finalizer(*id);
        }
        // The root groups: each reinstalled from its value.
        for (slot, value) in &roots {
            self.interp
                .install_root_group(*slot, value)
                .map_err(|message| LoadError::Error(format!("{slot:?}: {message}")))?;
        }

        Ok(LoadedImage {
            header,
            roots,
            symbols,
            obarray,
            builtin_cells,
            native_units,
            native_functions,
        })
    }

    /// The record ids of the vector at `header.hash_list'.
    fn hash_list_tables(
        &mut self,
        header: &DumpHeader,
    ) -> Result<Vec<crate::lisp::types::HashTableRef>, LoadError> {
        if header.hash_list == 0 {
            return Ok(Vec::new());
        }
        let offset = header.hash_list;
        let size = self.reader.word(offset)? as usize;
        let mut ids = Vec::with_capacity(size);
        for index in 0..size {
            match (self.value_at(offset + 8 * (index as u32 + 1))?).kind() {
                Kind::HashTable(table) => ids.push(table),
                other => {
                    return Err(LoadError::Error(format!(
                        "hash list entry is not a hash table: {other:?}"
                    )));
                }
            }
        }
        Ok(ids)
    }

    /// The object whose start is at OFFSET: a cons of the blocks, or one
    /// of the object starts.
    fn object_at(&self, offset: u32) -> Option<Value> {
        match self.starts.find(offset) {
            Some(number) => self.object(number),
            None => self.conses.cons_at(offset).ok(),
        }
    }

    /// Object NUMBER, once made.
    #[inline]
    fn object(&self, number: usize) -> Option<Value> {
        let value = *self.objects.get(number)?;
        (!value.is_nil()).then_some(value)
    }

    /// The relocation-table entry for the word at FIELD, if it has one.
    /// A field after the one read last and at or before the next entry's
    /// -- the next read of an ascending pass -- is that entry or none.
    #[inline(always)]
    fn reloc_entry(&mut self, field: u32) -> Option<usize> {
        if field > self.reloc_last && field <= self.reloc_next {
            self.reloc_last = field;
            if field != self.reloc_next {
                return None;
            }
            let index = self.reloc_cursor;
            self.reloc_cursor = index + 1;
            self.reloc_next = self.relocs.offset_or_end(index + 1);
            return Some(index);
        }
        self.seek_reloc(field)
    }

    /// reloc_entry for a field before the last one read or past the next
    /// entry.
    #[inline(never)]
    fn seek_reloc(&mut self, field: u32) -> Option<usize> {
        let cursor = self.reloc_position(field);
        let hit = self.relocs.offset_or_end(cursor) == field;
        self.reloc_last = field;
        self.reloc_cursor = cursor + usize::from(hit);
        self.reloc_next = self.relocs.offset_or_end(self.reloc_cursor);
        hit.then_some(cursor)
    }

    /// The first relocation at or after FIELD, searched from the cursor.
    fn reloc_position(&self, field: u32) -> usize {
        self.relocs.rank_from(self.reloc_cursor, field)
    }

    /// The image's cons blocks (`DumpHeader::cons_blocks'), made the
    /// heap's blocks where the image holds them, once their place in the
    /// hot section is checked.
    fn adopt_cons_blocks(&mut self, header: &DumpHeader) -> Result<ImageConses, LoadError> {
        let start = header.cons_blocks.offset as usize;
        let count = header.cons_blocks.nr_entries as usize;
        let end = start + crate::lisp::alloc::image_cons_bytes(count);
        let conses = ImageConses {
            base: self.image.address.as_ptr() as usize,
            start,
            count,
            end: count
                .checked_sub(1)
                .map_or(0, |last| crate::lisp::alloc::image_cons_offset(last) + 16),
        };
        if count == 0 {
            return Ok(conses);
        }
        if !start.is_multiple_of(crate::lisp::alloc::CONS_BLOCK_BYTES)
            || start < HEADER_LEN
            || end > header.discardable_start as usize
            || end > self.image.len
        {
            return Err(LoadError::Error(format!(
                "the cons blocks at {start} are not in the hot section"
            )));
        }
        // SAFETY: the image is aligned to a block (`IMAGE_ALIGNMENT'), so
        // the blocks from START are, and their bytes, inside the image by
        // the check above, stay for the life of the process; the dumper
        // wrote their metadata zero, and nothing else uses those bytes.
        unsafe {
            crate::lisp::alloc::adopt_image_cons_blocks(self.image.at(start).as_ptr(), count)
        };
        Ok(conses)
    }

    /// Phase 3 for the conses: pdumper.c's relocation of the dump where it
    /// is mapped, each relocation in the cons blocks applied to the word it
    /// names, in order; the other words are the heap's as the dumper wrote
    /// them.
    fn relocate_conses(&mut self) -> Result<(), LoadError> {
        if self.conses.count == 0 {
            return Ok(());
        }
        let (start, end) = self.conses.cells();
        let first = self.relocs.rank_from(0, start);
        let last = self.relocs.rank_from(first, end);
        for index in first..last {
            let entry = &self.relocs.entries[index];
            let field = entry_word(entry, 0);
            let slot = self.conses.field(field)?;
            // SAFETY: a word of an adopted cell, read where it is.
            let word = unsafe { slot.cast::<u64>().read() };
            let value =
                self.reloc_value(field, entry_word(entry, 4), entry_word(entry, 8), word)?;
            // SAFETY: a word of an adopted cell, which nothing has seen.
            unsafe { slot.write(value) };
        }
        // The next field read seeks its relocation afresh.
        self.reloc_last = u32::MAX;
        Ok(())
    }

    /// The values of the consecutive fields from START into OUT: the
    /// relocations walked alongside the words, as pdumper.c applies its
    /// sorted relocations in order.
    fn fields(&mut self, start: u32, out: &mut [Value]) -> Result<(), LoadError> {
        self.read_fields(start, out)
    }

    /// `fields' of N fields, the count known where it is read.
    #[inline(always)]
    fn field_array<const N: usize>(&mut self, start: u32) -> Result<[Value; N], LoadError> {
        let mut out = [Value::Nil; N];
        self.read_fields(start, &mut out)?;
        Ok(out)
    }

    #[inline(always)]
    fn read_fields(&mut self, start: u32, out: &mut [Value]) -> Result<(), LoadError> {
        if out.is_empty() {
            return Ok(());
        }
        let words = (start as usize)
            .checked_add(out.len() * 8)
            .and_then(|end| self.reader.bytes.get(start as usize..end))
            .ok_or_else(|| LoadError::Error(format!("fields at {start} are outside the image")))?;
        let (mut cursor, mut next) = if start > self.reloc_last && start <= self.reloc_next {
            (self.reloc_cursor, self.reloc_next)
        } else {
            let cursor = self.reloc_position(start);
            (cursor, self.relocs.offset_or_end(cursor))
        };
        let mut field = start;
        for (slot, word) in out.iter_mut().zip(words.as_chunks::<8>().0) {
            let word = u64::from_le_bytes(*word);
            *slot = if field == next {
                let entry = &self.relocs.entries[cursor];
                cursor += 1;
                next = self.relocs.offset_or_end(cursor);
                self.reloc_value(field, entry_word(entry, 4), entry_word(entry, 8), word)?
            } else {
                immediate_value(word)?
            };
            field += 8;
        }
        self.reloc_cursor = cursor;
        self.reloc_last = field - 8;
        self.reloc_next = next;
        Ok(())
    }

    /// The Lisp value a record field holds: the relocation says which
    /// object the word names, or the word is immediate. All container
    /// identities were installed before relocation, including closures.
    #[inline]
    fn value_at(&mut self, field_offset: u32) -> Result<Value, LoadError> {
        let word = self.reader.word(field_offset)?;
        let Some(reloc) = self.reloc_entry(field_offset) else {
            return immediate_value(word);
        };
        let code = self.relocs.code(reloc);
        if code == CONS_LV {
            return self.conses.cons_at(word as u32);
        }
        // DUMP_RELOC_DUMP_TO_DUMP_LV, whatever the target's type: the
        // entry names the object the word does.
        if code >> 8 == 1
            && let Some(value) = self.object(self.relocs.target(reloc) as usize)
        {
            debug_assert_eq!(
                Some(word as u32),
                self.relocs
                    .target(reloc)
                    .try_into()
                    .ok()
                    .map(|number| self.starts.offset(number)),
                "the relocation's object starts where its word names"
            );
            return Ok(value);
        }
        self.relocated_value(field_offset, code, word)
    }

    /// The value of the word WORD at FIELD that the relocation of CODE and
    /// object TARGET names: a cons of the blocks (pdumper.c's
    /// RELOC_DUMP_TO_DUMP_LV, the mapped image's address and the word's
    /// offset), the object the entry numbers, or what another relocation
    /// makes.
    #[inline(always)]
    fn reloc_value(
        &mut self,
        field: u32,
        code: u32,
        target: u32,
        word: u64,
    ) -> Result<Value, LoadError> {
        if code == CONS_LV {
            return self.conses.cons_at(word as u32);
        }
        match self.object(target as usize) {
            Some(value) if code >> 8 == 1 => Ok(value),
            _ => self.relocated_value(field, code, word),
        }
    }

    /// value_at for the relocations other than one naming an object.
    #[cold]
    fn relocated_value(
        &mut self,
        field_offset: u32,
        code: u32,
        word: u64,
    ) -> Result<Value, LoadError> {
        match DumpRelocKind::from_u32(code) {
            // RELOC_DUMP_TO_DUMP_LV of a cons: the word is the cell's
            // offset, and the cons is where the image is mapped.
            Some(DumpRelocKind::DumpToDumpLv(DumpType::Cons)) => self.conses.cons_at(word as u32),
            Some(DumpRelocKind::DumpToDumpLv(_)) => Err(LoadError::Error(format!(
                "field at {field_offset} names no object ({word})"
            ))),
            Some(DumpRelocKind::DumpToEmacsLv(kind)) => self.emacs_image_object(word as u32, kind),
            Some(other) => Err(LoadError::Error(format!(
                "field at {field_offset} has a non-object relocation {other:?}"
            ))),
            None => Err(LoadError::Error(format!(
                "unknown relocation {code} at {field_offset}"
            ))),
        }
    }

    /// A field that holds a symbol or an immediate, read before the
    /// symbol objects exist: the symbol record's name is a phase-1 string.
    fn symbol_or_immediate_at(&mut self, field_offset: u32) -> Result<Value, LoadError> {
        let word = self.reader.word(field_offset)?;
        let reloc = self.reloc_entry(field_offset);
        match reloc.map(|reloc| DumpRelocKind::from_u32(self.relocs.code(reloc))) {
            Some(Some(DumpRelocKind::DumpToDumpLv(DumpType::Symbol))) => {
                let target = word as u32;
                let number = self.relocs.target(reloc.expect("a relocation")) as usize;
                if let Some(value) = self.object(number) {
                    return Ok(value);
                }
                let flags = self.reader.word(target)?;
                let name = self.value_at(target + 8)?;
                if (flags >> SYMBOL_INTERNED_SHIFT) & 3 == SYMBOL_UNINTERNED {
                    return Err(LoadError::Error(
                        "a record's type tag is an uninterned symbol read early".into(),
                    ));
                }
                Ok(Value::Symbol(self.symbol_of_record(target, flags, name)?))
            }
            _ => self.value_at(field_offset),
        }
    }

    /// An object of the Emacs image, reached through its copied record in
    /// the discardable section (pdumper.c relocates such a word to the
    /// Emacs address; the copied record is what names it here).
    fn emacs_image_object(&mut self, offset: u32, kind: DumpType) -> Result<Value, LoadError> {
        match kind {
            DumpType::Subr => {
                if let Some(&subr) = self.subrs.get(&offset) {
                    return Ok(Value::BuiltinFunc(subr));
                }
                let name = self.value_at(offset)?;
                let subr = match name.kind() {
                    Kind::StringObject(state) => std::str::from_utf8(state.borrow().bytes())
                        .ok()
                        .and_then(crate::lisp::native_comp::abi::find_builtin),
                    _ => None,
                }
                .ok_or_else(|| {
                    LoadError::Error(format!("subr record at {offset} names no subr"))
                })?;
                self.subrs.insert(offset, subr);
                Ok(Value::BuiltinFunc(subr))
            }
            DumpType::MainThread => Ok(self
                .interp
                .record_value(self.interp.main_thread_record_id())),
            other => Err(LoadError::Error(format!(
                "{other:?} is not an Emacs-image object kind"
            ))),
        }
    }

    /// The symbol a record stands for.  A symbol `unintern' removed from
    /// the obarray keeps its ordinary name; a symbol made uninterned gets
    /// a fresh identity; one interned in another obarray (SYMBOL_INTERNED)
    /// is re-created under the internal name the record carries after
    /// its watchers, so its obarray's lookups find the same object.
    /// The symbol of a dumped record.  NAME is the record's name string,
    /// itself an object of the image: as pdumper.c's symbol points at
    /// that string, so does the interned state (no second copy of the
    /// text is accounted as a Lisp string).
    fn symbol_of_record(
        &mut self,
        offset: u32,
        flags: u64,
        name: Value,
    ) -> Result<SymbolName, LoadError> {
        let interned = (flags >> SYMBOL_INTERNED_SHIFT) & 3;
        if !matches!(name.kind(), Kind::StringObject(_)) {
            return Err(LoadError::Error("symbol name is not a string".into()));
        }
        let key = self.symbol_key(offset)?;
        if interned == SYMBOL_UNINTERNED {
            return Ok(SymbolName::make_uninterned(
                name,
                key,
                next_make_symbol_id(),
            ));
        }
        if interned == SYMBOL_INTERNED {
            if !crate::lisp::types::is_private_obarray_symbol(key) {
                return Err(LoadError::Error(format!(
                    "symbol {key:?}: not an other-obarray name"
                )));
            }
            return Ok(SymbolName::intern_with_lisp_name(
                key.to_owned(),
                Some(name),
            ));
        }
        if !crate::lisp::types::obarray::plain_ascii(key.as_bytes())
            && !crate::lisp::types::is_visible_symbol_name(key)
        {
            return Err(LoadError::Error(format!(
                "symbol {key:?}: an initial-obarray symbol with a marked key"
            )));
        }
        Ok(SymbolName::intern_image(name, key))
    }

    /// The host key a symbol record ends with: text of the permanent image.
    fn symbol_key(&self, offset: u32) -> Result<&'static str, LoadError> {
        let nwatchers = self.reader.word(offset + SYMBOL_NWATCHERS)?;
        let at = u64::from(offset + SYMBOL_WATCHERS) + 8 * nwatchers;
        let invalid = || LoadError::Error(format!("invalid symbol key at {offset}"));
        let at = u32::try_from(at).map_err(|_| invalid())?;
        let len = self.reader.word(at)? as usize;
        let start = at as usize + 8;
        let bytes = self
            .image
            .bytes()
            .get(start..start.checked_add(len).ok_or_else(invalid)?)
            .ok_or_else(invalid)?;
        if crate::lisp::types::obarray::plain_ascii(bytes) {
            // SAFETY: ASCII is UTF-8.
            return Ok(unsafe { std::str::from_utf8_unchecked(bytes) });
        }
        std::str::from_utf8(bytes).map_err(|_| invalid())
    }

    /// The C string the raw dump pointer at FIELD names: a native subr's
    /// name in the cold section (dump_cold_native_subr), NUL-terminated
    /// UTF-8 that lives as long as the process.
    /// FIELDs are asked in ascending order: the relocation's search
    /// starts at HINT, where the last one left it.
    fn image_c_string(
        &self,
        field: u32,
        hint: &mut usize,
        what: &str,
    ) -> Result<*const std::ffi::c_char, LoadError> {
        let invalid =
            || LoadError::Error(format!("native function {what} is not an image C string"));
        if self.relocs.reloc_at_from(hint, field) != Some(DumpRelocKind::DumpToDumpPtrRaw) {
            return Err(invalid());
        }
        let start = usize::try_from(self.reader.word(field)?).map_err(|_| invalid())?;
        let text = self
            .image
            .bytes()
            .get(start..)
            .and_then(|bytes| std::ffi::CStr::from_bytes_until_nul(bytes).ok())
            .ok_or_else(invalid)?;
        if text.is_empty() || text.to_str().is_err() {
            return Err(invalid());
        }
        Ok(text.as_ptr())
    }

    fn load_string(
        &mut self,
        offset: u32,
        kind: DumpType,
    ) -> Result<(Value, Option<u32>), LoadError> {
        let [size, size_byte, intervals, data] = self.reader.words(offset)?;
        let (size, intervals, data) = (size as usize, intervals as u32, data as u32);
        let multibyte = size_byte != u64::MAX;
        let nbytes = if multibyte { size_byte as usize } else { size };
        let bytes = self
            .reader
            .bytes
            .get(data as usize..data as usize + nbytes)
            .ok_or_else(|| {
                LoadError::Error(format!("string data at {data} is outside the image"))
            })?;
        let state = if kind == DumpType::EmptyString {
            if size != 0 || nbytes != 0 || intervals != 0 {
                return Err(LoadError::Error("nonempty static empty string".into()));
            }
            StringObjectRef::from_storage_kind(
                bytes,
                0,
                multibyte,
                crate::lisp::alloc::StringAllocation::Ordinary,
            )
        } else if nbytes == 0 {
            StringObjectRef::from_storage_kind(
                bytes,
                size,
                multibyte,
                crate::lisp::alloc::StringAllocation::Restored,
            )
        } else {
            // The data stays in the image, in the sdata entry the writer
            // laid out around it.
            use crate::lisp::alloc::{STRING_DATA_ALIGNMENT, STRING_DATA_PREFIX, string_data_size};
            let start = data as usize;
            if !start.is_multiple_of(STRING_DATA_ALIGNMENT)
                || start < STRING_DATA_PREFIX
                || start - STRING_DATA_PREFIX + string_data_size(nbytes) > self.image.len
            {
                return Err(LoadError::Error(format!(
                    "string data at {data} is not an entry inside the image"
                )));
            }
            // SAFETY: the permanent image's entry, checked to lie inside
            // it, aligned, with its owner word and padding; its string is
            // this one alone.
            unsafe {
                StringObjectRef::from_image_data(self.image.at(start), nbytes, size, multibyte)
            }
        }
        .map_err(|error| LoadError::Error(error.to_string()))?;
        Ok((
            Value::StringObject(state),
            (intervals != 0).then_some(intervals),
        ))
    }

    /// A bignum record after its fixup: sign/limb count and the cold limbs.
    fn load_bignum(&mut self, offset: u32) -> Result<Value, LoadError> {
        let sign_limbs = self.reader.word(offset)? as i64;
        let data = self.reader.word(offset + 8)? as u32;
        let nlimbs = sign_limbs.unsigned_abs() as u32;
        let mut limbs = Vec::with_capacity(nlimbs as usize);
        for index in 0..nlimbs {
            limbs.push(self.reader.word(data + 8 * index)?);
        }
        let magnitude = num_bigint::BigUint::from_slice(
            &limbs
                .iter()
                .flat_map(|limb| [*limb as u32, (*limb >> 32) as u32])
                .collect::<Vec<_>>(),
        );
        let sign = if sign_limbs < 0 {
            num_bigint::Sign::Minus
        } else if num_traits::Zero::is_zero(&magnitude) {
            num_bigint::Sign::NoSign
        } else {
            num_bigint::Sign::Plus
        };
        let integer = num_bigint::BigInt::from_biguint(sign, magnitude);
        Ok(match i64::try_from(&integer) {
            Ok(value) if fixnum_word(value).is_none() => Value::Integer(value),
            _ => Value::big_integer(integer),
        })
    }

    /// A bool-vector's cold record: id, bit count, packed bits.
    fn load_bool_vector(&mut self, offset: u32) -> Result<Value, LoadError> {
        let id = self.reader.word(offset)?;
        let nbits = self.reader.word(offset + 8)? as usize;
        let mut slots = Vec::with_capacity(nbits);
        for index in 0..nbits {
            let word = self.reader.word(offset + 16 + 8 * (index / 64) as u32)?;
            slots.push(if word & (1 << (index % 64)) != 0 {
                Value::T
            } else {
                Value::Nil
            });
        }
        self.interp.install_record(record_state_for_load(
            id,
            RecordKind::BoolVector,
            Value::symbol("bool-vector"),
            slots,
        ));
        Ok(self.interp.record_value(id))
    }

    /// A text-properties record: count, then (start, end, nprops, (name,
    /// value)*).
    fn load_text_properties(
        &mut self,
        offset: u32,
    ) -> Result<Vec<crate::lisp::types::StringPropertySpan>, LoadError> {
        let count = self.reader.word(offset)? as usize;
        let mut at = offset + 8;
        let mut spans = Vec::with_capacity(count);
        for _ in 0..count {
            let start = self.reader.word(at)? as usize;
            let end = self.reader.word(at + 8)? as usize;
            let nprops = self.reader.word(at + 16)? as usize;
            at += 24;
            let mut props = Vec::with_capacity(nprops);
            for _ in 0..nprops {
                let name = crate::lisp::types::PropertyName::new(self.value_at(at)?);
                let value = self.value_at(at + 8)?;
                at += 16;
                props.push((name, value));
            }
            spans.push(crate::lisp::types::StringPropertySpan { start, end, props });
        }
        Ok(spans)
    }

    /// A buffer record: the fields `dump_buffer' wrote, the text and the
    /// saved snapshot from the cold section, the property spans, the
    /// local bindings, the syntax and case tables and the undo entries;
    /// the buffer is installed with its id.
    fn load_buffer(
        &mut self,
        offset: u32,
        number: usize,
        id: u64,
    ) -> Result<
        (
            crate::lisp::types::BufferRef,
            Vec<crate::lisp::types::MarkerRef>,
        ),
        LoadError,
    > {
        let word = |loader: &Self, index: u32| loader.reader.word(offset + 8 * index);
        let flags = word(self, BUFFER_FLAGS)?;
        let multibyte = flags & BUFFER_FLAG_MULTIBYTE != 0;
        let name = string_like(&self.value_at(offset + 8 * BUFFER_NAME)?)
            .map(|string| string.text)
            .ok_or_else(|| LoadError::Error("buffer name is not a string".into()))?;
        let file = self.optional_string_at(offset + 8 * BUFFER_FILE)?;
        let file_truename = self.optional_string_at(offset + 8 * BUFFER_FILE_TRUENAME)?;
        let size = word(self, BUFFER_Z)? as usize;
        let (text, extended_chars) = self.cold_text_at(
            word(self, BUFFER_TEXT)? as u32,
            word(self, BUFFER_Z_BYTE)? as usize,
            multibyte,
        )?;
        if text.chars().count() != size {
            return Err(LoadError::Error(format!(
                "buffer {id} decodes to {} characters, record says {size}",
                text.chars().count()
            )));
        }
        let (saved_text, _) = self.cold_text_at(
            word(self, BUFFER_SAVED_TEXT)? as u32,
            word(self, BUFFER_SAVED_BYTES)? as usize,
            multibyte,
        )?;
        let position = |word: u64| (word != NO_POSITION).then_some(word as usize);
        let visited_file_modtime = (flags & BUFFER_FLAG_HAS_MODTIME != 0).then(|| {
            let secs = word(self, BUFFER_MODTIME_SECS).unwrap_or(0) as i64;
            let nanos = word(self, BUFFER_MODTIME_NANOS).unwrap_or(0) as u32;
            let duration = std::time::Duration::new(secs.unsigned_abs(), nanos);
            let modified = if secs < 0 {
                std::time::UNIX_EPOCH - duration
            } else {
                std::time::UNIX_EPOCH + duration
            };
            crate::buffer::FileModTime { modified }
        });
        let base = match (self.value_at(offset + 8 * BUFFER_BASE)?).kind() {
            Kind::Buffer(base) => Some(base.id),
            Kind::Nil => None,
            other => {
                return Err(LoadError::Error(format!(
                    "buffer {id}'s base is not a buffer: {other:?}"
                )));
            }
        };
        let intervals = word(self, BUFFER_INTERVALS)? as u32;
        let text_properties = if intervals == 0 {
            Vec::new()
        } else {
            self.load_text_properties(intervals)?
                .into_iter()
                .map(|span| crate::buffer::TextPropertySpan {
                    start: span.start,
                    end: span.end,
                    props: span.props,
                })
                .collect()
        };
        let mark_marker = match (self.value_at(offset + 8 * BUFFER_MARK_MARKER)?).kind() {
            Kind::Marker(marker) => Some(marker),
            Kind::Nil => None,
            other => {
                return Err(LoadError::Error(format!(
                    "buffer {id}'s mark is not a marker: {other:?}"
                )));
            }
        };
        let syntax_table = self.optional_char_table_at(offset + 8 * BUFFER_SYNTAX_TABLE)?;
        let case_tables = match (
            self.optional_char_table_at(offset + 8 * BUFFER_CASE_TABLE)?,
            self.optional_char_table_at(offset + 8 * BUFFER_UPCASE_TABLE)?,
            self.optional_char_table_at(offset + 8 * BUFFER_CASE_CANON_TABLE)?,
            self.optional_char_table_at(offset + 8 * BUFFER_CASE_EQV_TABLE)?,
        ) {
            (Some(down), Some(up), Some(canon), Some(eqv)) => {
                Some(crate::lisp::eval::coding::CaseTables {
                    down,
                    up,
                    canon,
                    eqv,
                })
            }
            (None, None, None, None) => None,
            _ => {
                return Err(LoadError::Error(format!(
                    "buffer {id}'s case tables are incomplete"
                )));
            }
        };
        let mut at = offset + 8 * BUFFER_VARIABLE_PART;
        let nlocals = self.reader.word(at)? as usize;
        at += 8;
        let mut locals = Vec::with_capacity(nlocals);
        for _ in 0..nlocals {
            let symbol = symbol_of(self.value_at(at)?, "buffer-local variable")?;
            let value = self.value_at(at + 8)?;
            locals.push((symbol, value));
            at += 16;
        }
        let nhooks = self.reader.word(at)? as usize;
        at += 8;
        let mut hooks = Vec::with_capacity(nhooks);
        for _ in 0..nhooks {
            let name = symbol_of(self.value_at(at)?, "buffer-local hook")?;
            let nfunctions = self.reader.word(at + 8)? as usize;
            at += 16;
            let mut functions = Vec::with_capacity(nfunctions);
            for _ in 0..nfunctions {
                functions.push(self.value_at(at)?);
                at += 8;
            }
            hooks.push((name.as_str().to_owned(), functions));
        }
        let nmarkers = self.reader.word(at)? as usize;
        at += 8;
        let mut marker_chain = Vec::with_capacity(nmarkers);
        for _ in 0..nmarkers {
            let Kind::Marker(marker) = self.value_at(at)?.kind() else {
                return Err(LoadError::Error(format!(
                    "buffer {id}'s marker chain holds a non-marker"
                )));
            };
            marker_chain.push(marker);
            at += 8;
        }
        let nundo = self.reader.word(at)? as usize;
        at += 8;
        let mut undo_list = Vec::with_capacity(nundo);
        for _ in 0..nundo {
            let (entry, next) = self.undo_entry_at(at)?;
            undo_list.push(entry);
            at = next;
        }
        let buffer = crate::buffer::Buffer::from_image_parts(crate::buffer::BufferImage {
            name,
            text,
            pt: word(self, BUFFER_PT)? as usize,
            mark: position(word(self, BUFFER_MARK)?),
            mark_active: self.value_at(offset + 8 * BUFFER_MARK_ACTIVE)?,
            modiff: word(self, BUFFER_MODIFF)? as i64,
            chars_modiff: word(self, BUFFER_CHARS_MODIFF)? as i64,
            save_modiff: word(self, BUFFER_SAVE_MODIFF)? as i64,
            saved_text,
            forced_modified: flags & BUFFER_FLAG_FORCED_MODIFIED != 0,
            autosaved: flags & BUFFER_FLAG_AUTOSAVED != 0,
            begv: word(self, BUFFER_BEGV)? as usize,
            zv: word(self, BUFFER_ZV)? as usize,
            file,
            file_truename,
            visited_file_modtime,
            undo_list,
            undo_disabled: flags & BUFFER_FLAG_UNDO_DISABLED != 0,
            point_before_last_boundary: position(word(self, BUFFER_POINT_BEFORE_BOUNDARY)?),
            text_properties,
            extended_chars: extended_chars
                .into_iter()
                .map(|(position, code)| (position + 1, code))
                .collect(),
            inhibit_hooks: flags & BUFFER_FLAG_INHIBIT_HOOKS != 0,
            text_conversion_style: self.value_at(offset + 8 * BUFFER_TEXT_CONVERSION_STYLE)?,
            text_conversion_style_is_local: flags & BUFFER_FLAG_LOCAL_TEXT_CONVERSION_STYLE != 0,
            multibyte,
        });
        let Kind::Buffer(object) = self.objects[number].kind() else {
            return Err(LoadError::Error(
                "buffer relocation has the wrong object kind".into(),
            ));
        };
        object.detach_markers();
        *object.borrow_mut() = buffer;
        object.borrow_mut().install_mark_object(object, mark_marker);
        self.interp.install_buffer(object);
        if let Some(base) = base {
            self.interp.register_indirect_buffer(id, base);
        }
        self.interp.install_buffer_local_cells(id, locals);
        for (name, functions) in hooks {
            self.interp.set_buffer_local_hook(id, &name, functions);
        }
        if let Some(table) = syntax_table {
            self.interp.install_buffer_syntax_table(id, table);
        }
        if let Some(tables) = case_tables {
            self.interp.install_buffer_case_tables(id, tables);
        }
        Ok((object, marker_chain))
    }

    /// One undo entry at AT: the entry and the offset after it.
    fn undo_entry_at(&mut self, at: u32) -> Result<(crate::buffer::UndoEntry, u32), LoadError> {
        use crate::buffer::UndoEntry;
        let kind = self.reader.word(at)?;
        Ok(match kind {
            UNDO_INSERT => (
                UndoEntry::Insert {
                    pos: self.reader.word(at + 8)? as usize,
                    len: self.reader.word(at + 16)? as usize,
                },
                at + 24,
            ),
            UNDO_DELETE => {
                let pos = self.reader.word(at + 8)? as usize;
                let point_after = self.reader.word(at + 16)? != 0;
                let text = string_like(&self.value_at(at + 24)?)
                    .map(|string| string.text)
                    .ok_or_else(|| LoadError::Error("deleted text is not a string".into()))?;
                let props_offset = self.reader.word(at + 32)? as u32;
                let props = if props_offset == 0 {
                    Vec::new()
                } else {
                    self.load_text_properties(props_offset)?
                        .into_iter()
                        .map(|span| crate::buffer::TextPropertySpan {
                            start: span.start,
                            end: span.end,
                            props: span.props,
                        })
                        .collect()
                };
                let mut next = at + 40;
                let nextended = self.reader.word(next)? as usize;
                next += 8;
                let mut extended_chars = Vec::with_capacity(nextended);
                for _ in 0..nextended {
                    extended_chars.push((
                        self.reader.word(next)? as usize,
                        self.reader.word(next + 8)? as u32,
                    ));
                    next += 16;
                }
                let nmarkers = self.reader.word(next)? as usize;
                next += 8;
                let mut markers = Vec::with_capacity(nmarkers);
                for _ in 0..nmarkers {
                    let Kind::Marker(marker) = self.value_at(next)?.kind() else {
                        return Err(LoadError::Error("undo entry has a non-marker".into()));
                    };
                    markers.push(crate::buffer::UndoMarker {
                        id: marker,
                        original_pos: self.reader.word(next + 8)? as usize,
                        collapsed_pos: self.reader.word(next + 16)? as usize,
                    });
                    next += 24;
                }
                (
                    UndoEntry::Delete {
                        pos,
                        point_after,
                        text,
                        props,
                        extended_chars,
                        markers,
                    },
                    next,
                )
            }
            UNDO_COMBINED => {
                let display = self.value_at(at + 8)?;
                let count = self.reader.word(at + 16)? as usize;
                let mut next = at + 24;
                let mut entries = Vec::with_capacity(count);
                for _ in 0..count {
                    let (entry, after) = self.undo_entry_at(next)?;
                    entries.push(entry);
                    next = after;
                }
                (UndoEntry::Combined { display, entries }, next)
            }
            UNDO_OPAQUE => (UndoEntry::Opaque(self.value_at(at + 8)?), at + 16),
            UNDO_BOUNDARY => (UndoEntry::Boundary, at + 8),
            other => {
                return Err(LoadError::Error(format!(
                    "unknown undo entry kind {other} at {at}"
                )));
            }
        })
    }

    /// Relocate a marker into the object allocated before graph restoration.
    fn load_marker(
        &mut self,
        offset: u32,
        marker: crate::lisp::types::MarkerRef,
    ) -> Result<(), LoadError> {
        let charpos = self.reader.word(offset + 8)? as usize;
        let bytepos = self.reader.word(offset + 16)? as usize;
        marker.set_insertion_type(self.reader.word(offset + 24)? != 0);
        match self.value_at(offset)?.kind() {
            Kind::Buffer(buffer) => {
                if charpos == 0 || charpos > buffer.borrow().size_total() + 1 {
                    return Err(LoadError::Error(
                        "marker position is outside its buffer".into(),
                    ));
                }
                marker.attach(buffer, charpos, bytepos);
            }
            Kind::Nil => marker.set_positions(charpos, bytepos),
            _ => return Err(LoadError::Error("marker buffer is not a buffer".into())),
        }
        Ok(())
    }

    /// Relocate fields after all overlay identities exist, preserving cycles
    /// and shared plists without allocating a temporary object or identity ID.
    fn load_overlay(
        &mut self,
        offset: u32,
        overlay: crate::overlay::OverlayRef,
    ) -> Result<(), LoadError> {
        let beg = self.reader.word(offset + 8)? as isize;
        let end = self.reader.word(offset + 16)? as isize;
        overlay.restore_bounds(beg, end);
        overlay.set_plist(self.value_at(offset + 32)?);
        match self.value_at(offset + 24)?.kind() {
            Kind::Nil => {}
            Kind::Buffer(buffer) if 0 < beg && beg <= end => {
                overlay.move_to(buffer, beg as usize, end as usize)
            }
            _ => return Err(LoadError::Error("invalid overlay buffer or bounds".into())),
        }
        Ok(())
    }

    /// A cold text run: NBYTES of GNU's internal representation at DATA.
    fn cold_text_at(
        &mut self,
        data: u32,
        nbytes: usize,
        multibyte: bool,
    ) -> Result<(String, Vec<(usize, u32)>), LoadError> {
        let bytes = self
            .reader
            .bytes
            .get(data as usize..data as usize + nbytes)
            .ok_or_else(|| LoadError::Error(format!("text at {data} is outside the image")))?;
        decode_internal_bytes(bytes, multibyte)
    }

    /// A field that is a string or nil.
    fn optional_string_at(&mut self, field: u32) -> Result<Option<String>, LoadError> {
        match (self.value_at(field)?).kind() {
            Kind::Nil => Ok(None),
            value => string_like(&value.value())
                .map(|string| Some(string.text))
                .ok_or_else(|| LoadError::Error(format!("field at {field} is not a string"))),
        }
    }

    /// A field that is a char-table or nil.
    fn optional_char_table_at(&mut self, field: u32) -> Result<Option<CharTableRef>, LoadError> {
        match (self.value_at(field)?).kind() {
            Kind::Nil => Ok(None),
            Kind::CharTable(id) => Ok(Some(id)),
            other => Err(LoadError::Error(format!(
                "field at {field} is not a char-table: {other:?}"
            ))),
        }
    }

    /// The cells record of `nil' or `t': flags, function, plist, watchers.
    fn load_builtin_cells(&mut self, offset: u32, name: &str) -> Result<LoadedSymbol, LoadError> {
        let flags = self.reader.word(offset)?;
        let function = self.value_at(offset + 8)?;
        let plist = self.value_at(offset + 16)?;
        let nwatchers = self.reader.word(offset + 24)? as u32;
        let mut watchers = Vec::with_capacity(nwatchers as usize);
        for index in 0..nwatchers {
            watchers.push(self.value_at(offset + 32 + 8 * index)?);
        }
        Ok(LoadedSymbol {
            symbol: SymbolName::intern_str(name),
            flags,
            value: None,
            alias: None,
            function,
            plist,
            watchers,
        })
    }
}

/// A symbol record's fields: the flags, name, value, function and plist,
/// then these.
const SYMBOL_NEXT: u32 = 40;
const SYMBOL_NWATCHERS: u32 = 48;
const SYMBOL_WATCHERS: u32 = 56;

/// A symbol-valued field: `nil' and `t' arrive as their immediate words.
fn symbol_of(value: Value, what: &str) -> Result<SymbolName, LoadError> {
    match value.kind() {
        Kind::Symbol(symbol) => Ok(symbol),
        Kind::Nil => Ok(SymbolName::intern_str("nil")),
        Kind::T => Ok(SymbolName::intern_str("t")),
        other => Err(LoadError::Error(format!(
            "{what} is not a symbol: {other:?}"
        ))),
    }
}

/// A word without a relocation is self-representing.
#[inline(always)]
fn immediate_value(word: u64) -> Result<Value, LoadError> {
    if let Some(fixnum) = word_fixnum(word) {
        return Ok(Value::Integer(fixnum));
    }
    match word {
        WORD_NIL => Ok(Value::Nil),
        WORD_T => Ok(Value::T),
        WORD_UNBOUND => Ok(Value::Unbound),
        _ => Err(unknown_immediate(word)),
    }
}

#[cold]
fn unknown_immediate(word: u64) -> LoadError {
    LoadError::Error(format!("unknown immediate word {word:#x}"))
}

/// GNU's internal multibyte form back to Emaxx's text plus the
/// characters Rust cannot spell (a placeholder in the text, the code in
/// the side list); a unibyte string's octets back to its characters.
pub(crate) fn decode_internal_bytes(
    bytes: &[u8],
    multibyte: bool,
) -> Result<(String, Vec<(usize, u32)>), LoadError> {
    // The common cases decode to the bytes themselves: a unibyte string of
    // ASCII, and a multibyte string that is valid UTF-8 (Emacs's internal
    // encoding is UTF-8 for every Unicode character; the raw-byte forms
    // C0/C1 xx, the five-byte forms and the surrogates, which need the
    // loop below, are not valid UTF-8).
    if !multibyte {
        if let Ok(text) = std::str::from_utf8(bytes)
            && text.is_ascii()
        {
            return Ok((text.to_owned(), Vec::new()));
        }
    } else if let Ok(text) = std::str::from_utf8(bytes) {
        return Ok((text.to_owned(), Vec::new()));
    }
    let mut text = String::new();
    let mut extended_chars = Vec::new();
    if !multibyte {
        for &byte in bytes {
            text.push(if byte < 0x80 {
                byte as char
            } else {
                crate::lisp::primitives::case::raw_byte_regex_char(byte)
            });
        }
        return Ok((text, extended_chars));
    }
    let mut index = 0;
    let mut chars = 0;
    while index < bytes.len() {
        let lead = bytes[index];
        let (width, mut code) = match lead {
            0x00..=0x7F => (1, u32::from(lead)),
            0xC0..=0xDF => (2, u32::from(lead & 0x1F)),
            0xE0..=0xEF => (3, u32::from(lead & 0x0F)),
            0xF0..=0xF7 => (4, u32::from(lead & 0x07)),
            0xF8 => (5, 0),
            _ => {
                return Err(LoadError::Error(format!(
                    "invalid internal multibyte lead byte {lead:#x} at {index}"
                )));
            }
        };
        if index + width > bytes.len() {
            return Err(LoadError::Error(
                "truncated internal multibyte sequence".into(),
            ));
        }
        for &continuation in &bytes[index + 1..index + width] {
            if continuation & 0xC0 != 0x80 {
                return Err(LoadError::Error(format!(
                    "invalid internal multibyte continuation {continuation:#x}"
                )));
            }
            code = (code << 6) | u32::from(continuation & 0x3F);
        }
        // The two-byte C0/C1 forms carry a raw byte (0x3FFF80..0x3FFFFF).
        if width == 2 && (0xC0..=0xC1).contains(&lead) {
            code += 0x3F_FF80;
        }
        index += width;
        if (0x3F_FF80..=0x3F_FFFF).contains(&code) {
            // character.h:CHAR_TO_BYTE8: the byte is the code less 0x3FFF00.
            text.push(crate::lisp::primitives::case::raw_byte_regex_char(
                (code - 0x3F_FF00) as u8,
            ));
        } else if let Some(ch) = char::from_u32(code) {
            text.push(ch);
        } else {
            extended_chars.push((chars, code));
            text.push(crate::lisp::json::INVALID_UNICODE_SENTINEL);
        }
        chars += 1;
    }
    Ok((text, extended_chars))
}
