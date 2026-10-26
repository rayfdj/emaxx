//! The bytes of a Lisp string, in character.h's internal encoding.
//!
//! Unibyte characters are their actual octets. Multibyte strings use GNU's
//! one-to-five-byte encoding, including byte8 and non-Unicode characters.
//! Rust text and its non-Unicode side list are transient consumer views;
//! neither is retained beside the authoritative bytes.

use super::{Kind, LispError, StringObjectRef, StringPropertySpan, Value};
use crate::lisp::alloc::{
    PendingStringData, StringAllocation, retire_string_data, string_data_size,
};
use std::cell::Cell;
use std::ptr::{self, NonNull};

mod concat;
mod properties;
pub use properties::StringProperties;

/// lisp.h:struct Lisp_String's four authoritative words. The collector's
/// allocation/borrow metadata belongs to the containing string cell.
/// Positive SIZE_BYTE is a multibyte byte count; -1 denotes unibyte data.
#[repr(C)]
pub struct SharedStringState {
    size: usize,
    size_byte: isize,
    pub props: StringProperties,
    data: NonNull<u8>,
}

const _: () = {
    assert!(std::mem::size_of::<SharedStringState>() == 32);
    assert!(std::mem::offset_of!(SharedStringState, size) == 0);
    assert!(std::mem::offset_of!(SharedStringState, size_byte) == 8);
    assert!(std::mem::offset_of!(SharedStringState, props) == 16);
    assert!(std::mem::offset_of!(SharedStringState, data) == 24);
};

static EMPTY_DATA: u8 = 0;

impl std::fmt::Debug for SharedStringState {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SharedStringState")
            .field("bytes", &self.bytes())
            .field("characters", &self.len())
            .field("multibyte", &self.is_multibyte())
            .field("props", &self.props)
            .finish()
    }
}

impl PartialEq for SharedStringState {
    fn eq(&self, other: &Self) -> bool {
        self.size == other.size
            && self.size_byte == other.size_byte
            && self.bytes() == other.bytes()
            && self.props == other.props
    }
}

impl Drop for SharedStringState {
    fn drop(&mut self) {
        self.forget_char_byte_cache();
        let bytes = self.storage_bytes();
        if bytes != 0 {
            // SAFETY: the swept header exclusively owns its sdata entry;
            // no guard survives reclamation. Block freeing follows sweep.
            unsafe { retire_string_data(self.data, bytes) };
        }
    }
}

/// Storage construction is independent of an interpreter. Carry allocation
/// failure without allocating a replacement message; the primitive resolves
/// it to the current, preallocated `memory-signal-data` only on error.
#[derive(Debug)]
pub(crate) enum StringError {
    Condition(LispError),
    AllocationFailed,
}

impl From<LispError> for StringError {
    fn from(error: LispError) -> Self {
        Self::Condition(error)
    }
}

impl std::fmt::Display for StringError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Condition(error) => error.fmt(f),
            Self::AllocationFailed => f.write_str("Memory exhausted"),
        }
    }
}

/// The characters BYTES hold are CHARACTERS: a unibyte string and ASCII
/// text have a character a byte, other multibyte text is decoded.
fn check_character_count(
    bytes: &[u8],
    characters: usize,
    multibyte: bool,
) -> Result<(), StringError> {
    let count = if !multibyte || bytes.is_ascii() {
        bytes.len()
    } else {
        let mut count = 0;
        let mut offset = 0;
        while offset < bytes.len() {
            offset += if bytes[offset] < 0x80 {
                1
            } else {
                decode_character(&bytes[offset..])?.1
            };
            count += 1;
        }
        count
    };
    if count != characters {
        return Err(
            LispError::Signal("String character count does not match its bytes".into()).into(),
        );
    }
    Ok(())
}

impl StringObjectRef {
    fn from_data(
        nbytes: usize,
        characters: usize,
        multibyte: bool,
        kind: StringAllocation,
        initialize: impl FnOnce(*mut u8) -> Vec<StringPropertySpan>,
    ) -> Result<Self, StringError> {
        validate_size(nbytes)?;
        crate::lisp::alloc::allocate_string(characters, multibyte, kind, |state| {
            assert!(nbytes < isize::MAX as usize - 16 && (multibyte || characters == nbytes));
            if nbytes == 0 {
                state.props = initialize(state.data.as_ptr()).into();
                return Ok(());
            }
            // SAFETY: STATE is exclusively borrowed at its final address.
            let data =
                unsafe { PendingStringData::new(state, nbytes, kind == StringAllocation::Pure) }?;
            let props = initialize(data.as_ptr());
            state.size = characters;
            state.size_byte = if multibyte { nbytes as isize } else { -1 };
            state.data = data.install();
            state.props = props.into();
            Ok(())
        })
    }

    fn from_encoded(
        bytes: Vec<u8>,
        characters: usize,
        multibyte: bool,
        props: Vec<StringPropertySpan>,
    ) -> Self {
        Self::from_slice(
            &bytes,
            characters,
            multibyte,
            props,
            StringAllocation::Ordinary,
        )
    }

    fn from_slice(
        bytes: &[u8],
        characters: usize,
        multibyte: bool,
        props: Vec<StringPropertySpan>,
        kind: StringAllocation,
    ) -> Self {
        Self::try_from_slice(bytes, characters, multibyte, props, kind)
            .expect("infallible string copy allocation")
    }

    fn try_from_slice(
        bytes: &[u8],
        characters: usize,
        multibyte: bool,
        props: Vec<StringPropertySpan>,
        kind: StringAllocation,
    ) -> Result<Self, StringError> {
        Self::from_data(bytes.len(), characters, multibyte, kind, |data| {
            if !bytes.is_empty() {
                // SAFETY: the fresh output contains BYTES.len() writable bytes.
                unsafe { ptr::copy_nonoverlapping(bytes.as_ptr(), data, bytes.len()) };
            }
            props
        })
    }

    /// character.c:Fstring validates all characters before allocating the
    /// payload. Only an entirely ASCII result has one byte per character.
    pub(crate) fn from_characters(characters: &[Value]) -> Result<Self, StringError> {
        let mut nbytes = 0usize;
        for &character in characters {
            let (_, width) = encode_character(character_code(character)?)?;
            nbytes = nbytes.checked_add(width).ok_or_else(string_overflow)?;
        }
        validate_size(nbytes)?;
        Self::from_data(
            nbytes,
            characters.len(),
            nbytes != characters.len(),
            StringAllocation::Ordinary,
            |data| {
                let mut offset = 0;
                for &character in characters {
                    let (encoded, width) =
                        encode_character(character_code(character).expect("validated character"))
                            .expect("validated code");
                    // SAFETY: validation summed these exact encoded widths.
                    unsafe { ptr::copy_nonoverlapping(encoded.as_ptr(), data.add(offset), width) };
                    offset += width;
                }
                Vec::new()
            },
        )
    }

    /// alloc.c:Fmake_string encodes INIT once, then repeats its actual bytes.
    pub(crate) fn repeated_character(
        code: u32,
        length: usize,
        force_multibyte: bool,
    ) -> Result<Self, StringError> {
        let (encoded, width) = encode_character(code)?;
        let nbytes = length.checked_mul(width).ok_or_else(string_overflow)?;
        validate_size(nbytes)?;
        Self::from_data(
            nbytes,
            length,
            force_multibyte || width != 1,
            StringAllocation::Ordinary,
            |data| {
                // SAFETY: all writes remain inside the validated output extent;
                // doubling copies only already initialized, disjoint bytes.
                unsafe {
                    if width == 1 && nbytes != 0 {
                        ptr::write_bytes(data, encoded[0], nbytes);
                    } else if nbytes != 0 {
                        ptr::copy_nonoverlapping(encoded.as_ptr(), data, width);
                        let mut filled = width;
                        while filled < nbytes {
                            let count = filled.min(nbytes - filled);
                            ptr::copy_nonoverlapping(data, data.add(filled), count);
                            filled += count;
                        }
                    }
                }
                Vec::new()
            },
        )
    }

    pub(crate) fn from_unibyte(bytes: Vec<u8>) -> Self {
        let characters = bytes.len();
        Self::from_encoded(bytes, characters, false, Vec::new())
    }

    /// alloc.c:make_string classifies a C byte sequence as multibyte only
    /// when every byte belongs to valid internal character encoding.
    pub(crate) fn from_c_bytes(bytes: &[u8]) -> Result<Self, StringError> {
        let mut offset = 0;
        let mut characters = 0;
        while offset < bytes.len() {
            // A run of ASCII, each byte a character.
            let run = bytes[offset..]
                .iter()
                .take_while(|byte| **byte < 0x80)
                .count();
            offset += run;
            characters += run;
            if offset == bytes.len() {
                break;
            }
            let decoded = decode_character(&bytes[offset..])
                .ok()
                .filter(|&(code, width)| {
                    // character.h:multibyte_length(..., allow_8bit=false)
                    // rejects raw-byte forms and overlong character encodings.
                    code < 0x3fff80 && encode_character(code).is_ok_and(|(_, n)| n == width)
                });
            let Some((_, width)) = decoded else {
                return Self::try_from_slice(
                    bytes,
                    bytes.len(),
                    false,
                    Vec::new(),
                    StringAllocation::Ordinary,
                );
            };
            offset += width;
            characters += 1;
        }
        Self::try_from_slice(
            bytes,
            characters,
            characters != bytes.len(),
            Vec::new(),
            StringAllocation::Ordinary,
        )
    }

    pub(crate) fn from_storage(
        bytes: Vec<u8>,
        characters: usize,
        multibyte: bool,
    ) -> Result<Self, StringError> {
        Self::from_storage_kind(
            &bytes,
            characters,
            multibyte,
            crate::lisp::alloc::StringAllocation::Ordinary,
        )
    }

    pub(crate) fn from_storage_kind(
        bytes: &[u8],
        characters: usize,
        multibyte: bool,
        kind: crate::lisp::alloc::StringAllocation,
    ) -> Result<Self, StringError> {
        check_character_count(bytes, characters, multibyte)?;
        Self::try_from_slice(bytes, characters, multibyte, Vec::new(), kind)
    }

    pub(crate) fn from_text(
        text: String,
        props: Vec<StringPropertySpan>,
        multibyte: bool,
        extended_chars: Vec<(usize, u32)>,
    ) -> Self {
        // ASCII is its own encoding, one byte a character: the text goes
        // into the string's data with no byte vector between and no count.
        if extended_chars.is_empty() && text.is_ascii() {
            return Self::from_slice(
                text.as_bytes(),
                text.len(),
                multibyte,
                props,
                StringAllocation::Ordinary,
            );
        }
        let bytes = encode_text(&text, multibyte, &extended_chars)
            .expect("string constructor must supply valid Lisp characters");
        Self::from_encoded(bytes, text.chars().count(), multibyte, props)
    }

    /// `SharedText::new' for borrowed host text: ASCII is copied once,
    /// into the string's data, and other text takes `new's conversion.
    pub(crate) fn from_host_str(text: &str) -> Self {
        if text.is_ascii() {
            return Self::from_slice(
                text.as_bytes(),
                text.len(),
                false,
                Vec::new(),
                StringAllocation::Ordinary,
            );
        }
        Self::new(text.to_owned())
    }

    /// `from_text' for borrowed text: ASCII is copied once, into the
    /// string's data.
    pub(crate) fn from_text_ref(
        text: &str,
        props: Vec<StringPropertySpan>,
        multibyte: bool,
        extended_chars: &[(usize, u32)],
    ) -> Self {
        if extended_chars.is_empty() && text.is_ascii() {
            return Self::from_slice(
                text.as_bytes(),
                text.len(),
                multibyte,
                props,
                StringAllocation::Ordinary,
            );
        }
        Self::from_text(text.to_owned(), props, multibyte, extended_chars.to_vec())
    }
}

/// fns.c's string_char_byte_cache_string, _charpos and _bytepos: the
/// string of the last conversion between character and byte indices and
/// the positions it found.  The header's address names the string, and
/// its data and sizes must agree too: a store that moves characters
/// forgets the entry, as Faset's clear_string_char_byte_cache does.
/// TEXT_IN_PLACE memoizes `stored_text_in_place' for the same contents.
#[derive(Clone, Copy)]
struct StringCharByteCache {
    header: *const SharedStringState,
    data: *const u8,
    size: usize,
    size_byte: isize,
    charpos: usize,
    bytepos: usize,
    text_in_place: Option<bool>,
}

thread_local! {
    static STRING_CHAR_BYTE_CACHE: Cell<Option<StringCharByteCache>> =
        const { Cell::new(None) };
}

/// character.h BYTES_BY_CHAR_HEAD.
#[inline]
fn bytes_by_char_head(byte: u8) -> usize {
    if byte & 0x80 == 0 {
        1
    } else if byte & 0x20 == 0 {
        2
    } else if byte & 0x10 == 0 {
        3
    } else if byte & 0x08 == 0 {
        4
    } else {
        5
    }
}

/// character.h CHAR_HEAD_P.
#[inline]
fn char_head_p(byte: u8) -> bool {
    byte & 0xC0 != 0x80
}

impl SharedStringState {
    pub(crate) fn empty(multibyte: bool) -> Self {
        Self {
            size: 0,
            size_byte: if multibyte { 0 } else { -1 },
            props: StringProperties::default(),
            data: NonNull::from(&EMPTY_DATA),
        }
    }

    /// The state of an image string whose data stays where the image
    /// holds it, as pdumper.c leaves a dumped string's data in the mapped
    /// dump: CHARACTERS in NBYTES at DATA, checked as the loader checks
    /// what it reads (a unibyte string has a character a byte).  The entry
    /// is no block's, so compaction never moves it (as
    /// compact_small_strings never reads the dump), and the header owns it
    /// as it owns allocated data -- retiring or resizing it within its
    /// slop writes the entry, as they write any.  An empty string's data
    /// is the static empty data, whatever DATA is.
    ///
    /// # Safety
    /// Unless NBYTES is zero, DATA is the data of an sdata entry for
    /// NBYTES bytes and their NUL -- aligned, with its owner word before
    /// DATA and its padding after -- in writable memory that lives as long
    /// as the process and that nothing else uses.
    pub(crate) unsafe fn from_image(
        data: NonNull<u8>,
        nbytes: usize,
        characters: usize,
        multibyte: bool,
    ) -> Result<Self, StringError> {
        validate_size(nbytes)?;
        let data = if nbytes == 0 {
            NonNull::from(&EMPTY_DATA)
        } else {
            data
        };
        if multibyte {
            // SAFETY: the caller's entry holds NBYTES bytes at DATA.
            let bytes = unsafe { std::slice::from_raw_parts(data.as_ptr(), nbytes) };
            check_character_count(bytes, characters, true)?;
        } else if characters != nbytes {
            return Err(LispError::Signal(
                "String character count does not match its bytes".into(),
            )
            .into());
        }
        Ok(Self {
            size: characters,
            size_byte: if multibyte { nbytes as isize } else { -1 },
            props: StringProperties::default(),
            data,
        })
    }

    /// This string's entry in the conversion cache, if it holds one.
    fn char_byte_cache(&self) -> Option<StringCharByteCache> {
        STRING_CHAR_BYTE_CACHE.with(Cell::get).filter(|cache| {
            ptr::eq(cache.header, self)
                && cache.data == self.data.as_ptr().cast_const()
                && cache.size == self.size
                && cache.size_byte == self.size_byte
        })
    }

    fn set_char_byte_cache(&self, charpos: usize, bytepos: usize, text_in_place: Option<bool>) {
        STRING_CHAR_BYTE_CACHE.with(|cell| {
            cell.set(Some(StringCharByteCache {
                header: self,
                data: self.data.as_ptr().cast_const(),
                size: self.size,
                size_byte: self.size_byte,
                charpos,
                bytepos,
                text_in_place,
            }));
        });
    }

    /// fns.c:clear_string_char_byte_cache, for a store into this string's
    /// bytes or its reclamation.
    fn forget_char_byte_cache(&self) {
        STRING_CHAR_BYTE_CACHE.with(|cell| {
            if cell.get().is_some_and(|cache| {
                ptr::eq(cache.header, self) || cache.data == self.data.as_ptr().cast_const()
            }) {
                cell.set(None);
            }
        });
    }

    /// fns.c:string_char_to_byte: the byte index of character CHAR_INDEX
    /// (at most the length), scanned from the nearest of the start, the
    /// end and the cached position, which then moves there.
    pub(crate) fn char_to_byte(&self, char_index: usize) -> usize {
        assert!(char_index <= self.size, "character index within the string");
        let (mut below, mut below_byte) = (0, 0);
        let (mut above, mut above_byte) = (self.size, self.storage_bytes());
        if above == above_byte {
            return char_index;
        }
        let cache = self.char_byte_cache();
        if let Some(cache) = cache {
            if cache.charpos < char_index {
                (below, below_byte) = (cache.charpos, cache.bytepos);
            } else {
                (above, above_byte) = (cache.charpos, cache.bytepos);
            }
        }
        let bytes = self.bytes();
        let byte_index = if char_index - below < above - char_index {
            let mut p = below_byte;
            while below < char_index {
                p += bytes_by_char_head(bytes[p]);
                below += 1;
            }
            p
        } else {
            let mut p = above_byte;
            while above > char_index {
                p -= 1;
                while !char_head_p(bytes[p]) {
                    p -= 1;
                }
                above -= 1;
            }
            p
        };
        self.set_char_byte_cache(
            char_index,
            byte_index,
            cache.and_then(|cache| cache.text_in_place),
        );
        byte_index
    }

    /// fns.c:string_byte_to_char: the character index of the character
    /// that starts at byte BYTE_INDEX (at most the byte length).
    pub(crate) fn byte_to_char(&self, byte_index: usize) -> usize {
        let (mut below, mut below_byte) = (0, 0);
        let (mut above, mut above_byte) = (self.size, self.storage_bytes());
        assert!(byte_index <= above_byte, "byte index within the string");
        if above == above_byte {
            return byte_index;
        }
        let cache = self.char_byte_cache();
        if let Some(cache) = cache {
            if cache.bytepos < byte_index {
                (below, below_byte) = (cache.charpos, cache.bytepos);
            } else {
                (above, above_byte) = (cache.charpos, cache.bytepos);
            }
        }
        let bytes = self.bytes();
        let (char_index, byte_index) = if byte_index - below_byte < above_byte - byte_index {
            let mut p = below_byte;
            while p < byte_index {
                p += bytes_by_char_head(bytes[p]);
                below += 1;
            }
            (below, p)
        } else {
            let mut p = above_byte;
            while p > byte_index {
                p -= 1;
                while !char_head_p(bytes[p]) {
                    p -= 1;
                }
                above -= 1;
            }
            (above, p)
        };
        self.set_char_byte_cache(
            char_index,
            byte_index,
            cache.and_then(|cache| cache.text_in_place),
        );
        char_index
    }

    /// The byte offset COUNT characters after byte offset FROM.
    pub(crate) fn bytes_after_chars(&self, from: usize, count: usize) -> usize {
        if !self.is_multibyte() || self.size == self.storage_bytes() {
            return from + count;
        }
        let bytes = self.bytes();
        let mut p = from;
        for _ in 0..count {
            p += bytes_by_char_head(bytes[p]);
        }
        p
    }

    /// Whether this string's bytes are already the text a consumer reads,
    /// as COMPUTE decides, remembered with the conversion cache while the
    /// string holds it.
    pub(crate) fn stored_text_in_place(&self, compute: impl FnOnce(&[u8]) -> bool) -> bool {
        let cache = self.char_byte_cache();
        if let Some(known) = cache.and_then(|cache| cache.text_in_place) {
            return known;
        }
        let in_place = compute(self.bytes());
        let (charpos, bytepos) = cache.map_or((0, 0), |cache| (cache.charpos, cache.bytepos));
        self.set_char_byte_cache(charpos, bytepos, Some(in_place));
        in_place
    }

    /// The caller has excluded all borrows before relocating this header's
    /// data during collection. Properties and the Lisp identity do not move.
    pub(crate) fn relocate_data(&mut self, data: NonNull<u8>) {
        self.data = data;
    }

    fn bytes_mut(&mut self) -> &mut [u8] {
        let nbytes = self.storage_bytes();
        if nbytes == 0 {
            return &mut [];
        }
        // SAFETY: the unique mutable header borrow covers its owned bytes;
        // the trailing NUL remains outside the mutable contents slice.
        unsafe { std::slice::from_raw_parts_mut(self.data.as_ptr(), nbytes) }
    }

    fn replace_storage(&mut self, bytes: Vec<u8>, characters: usize, multibyte: bool) {
        self.forget_char_byte_cache();
        let nbytes = bytes.len();
        assert!(nbytes < isize::MAX as usize - 16 && (multibyte || characters == nbytes));
        let old_bytes = self.storage_bytes();
        if nbytes != 0 && old_bytes != 0 && string_data_size(nbytes) == string_data_size(old_bytes)
        {
            // SAFETY: allocation alignment slop covers the new bytes and NUL.
            unsafe {
                ptr::copy_nonoverlapping(bytes.as_ptr(), self.data.as_ptr(), nbytes);
                self.data.as_ptr().add(nbytes).write(0);
            }
        } else {
            let data = if nbytes == 0 {
                NonNull::from(&EMPTY_DATA)
            } else {
                // SAFETY: this exclusive borrow names a stable, impure header.
                let allocation = unsafe { PendingStringData::new(self, nbytes, false) }
                    .expect("infallible string replacement allocation");
                unsafe { ptr::copy_nonoverlapping(bytes.as_ptr(), allocation.as_ptr(), nbytes) };
                crate::lisp::native_comp::note_lisp_allocation(string_data_size(nbytes));
                allocation.install()
            };
            if old_bytes != 0 {
                // SAFETY: replacement invalidates the exclusively owned data.
                unsafe { retire_string_data(self.data, old_bytes) };
            }
            self.data = data;
        }
        self.size = characters;
        self.size_byte = if multibyte { nbytes as isize } else { -1 };
    }

    pub(crate) fn bytes(&self) -> &[u8] {
        // SAFETY: the header owns STORAGE_BYTES initialized bytes plus NUL.
        // A zero-length header points at EMPTY_DATA; neither case is null.
        unsafe { std::slice::from_raw_parts(self.data.as_ptr(), self.storage_bytes()) }
    }

    /// `bytes', for a caller whose guard on the state is its own contract
    /// (`StringObjectRef::bytes_unguarded').
    ///
    /// # Safety
    /// The bytes stay in place and unchanged while the slice is used.
    #[inline(always)]
    pub(crate) unsafe fn bytes_unbounded<'a>(&self) -> &'a [u8] {
        // SAFETY: as `bytes', for the caller's lifetime.
        unsafe { std::slice::from_raw_parts(self.data.as_ptr(), self.storage_bytes()) }
    }
    pub(crate) fn len(&self) -> usize {
        self.size
    }
    pub(crate) fn is_multibyte(&self) -> bool {
        self.size_byte >= 0
    }
    pub(crate) fn storage_bytes(&self) -> usize {
        if self.size_byte >= 0 {
            self.size_byte as usize
        } else {
            self.size
        }
    }

    /// fns.c:Fstring_equal and internal_equal compare SCHARS, SBYTES and
    /// the stored bytes. The multibyte flag and text properties do not
    /// participate: ASCII contents compare equal in either representation.
    pub(crate) fn contents_equal(&self, other: &Self) -> bool {
        self.len() == other.len() && self.bytes() == other.bytes()
    }

    /// fns.c:string_cmp compares unibyte characters as their octets and
    /// multibyte characters as their full internal codes. It does not
    /// promote unibyte octets to byte8 as Fcompare_strings does.
    pub(crate) fn compare_contents(&self, other: &Self) -> std::cmp::Ordering {
        use std::cmp::Ordering;

        if (!self.is_multibyte() || self.len() == self.bytes().len())
            && (!other.is_multibyte() || other.len() == other.bytes().len())
        {
            return self.bytes().cmp(other.bytes());
        }
        if self.is_multibyte() && other.is_multibyte() {
            // Skip equal words and the remaining equal bytes, then return
            // to the beginning of the differing internal character. Safe
            // slices also cover GNU's possibly unaligned pure-string data.
            let word = std::mem::size_of::<usize>();
            let mut offset = self
                .bytes()
                .chunks_exact(word)
                .zip(other.bytes().chunks_exact(word))
                .take_while(|(left, right)| left == right)
                .count()
                * word;
            let limit = self.bytes().len().min(other.bytes().len());
            while offset < limit && self.bytes()[offset] == other.bytes()[offset] {
                offset += 1;
            }
            if offset == limit {
                return self.bytes().len().cmp(&other.bytes().len());
            }
            while self.bytes()[offset] & 0xc0 == 0x80 {
                offset -= 1;
            }
            let left = decode_character(&self.bytes()[offset..]).expect("stored character");
            let right = decode_character(&other.bytes()[offset..]).expect("stored character");
            return left.0.cmp(&right.0);
        }
        if !self.is_multibyte() {
            return other.compare_contents(self).reverse();
        }
        let mut offset = 0;
        for &byte in other.bytes().iter().take(self.len()) {
            let (code, width) =
                decode_character(&self.bytes()[offset..]).expect("stored character");
            let order = code.cmp(&u32::from(byte));
            if order != Ordering::Equal {
                return order;
            }
            offset += width;
        }
        self.len().cmp(&other.len())
    }

    /// character.c:str_as_unibyte preserves every internal byte except
    /// the two-byte byte8 forms, which become their one-byte equivalent.
    pub(crate) fn as_unibyte_bytes(&self) -> Vec<u8> {
        if !self.is_multibyte() {
            return self.bytes().to_vec();
        }
        let mut result = Vec::with_capacity(self.bytes().len());
        let mut offset = 0;
        while offset < self.bytes().len() {
            let (code, width) =
                decode_character(&self.bytes()[offset..]).expect("stored character");
            if (0x3fff80..=0x3fffff).contains(&code) {
                result.push((code - 0x3fff00) as u8);
            } else {
                result.extend_from_slice(&self.bytes()[offset..offset + width]);
            }
            offset += width;
        }
        result
    }

    pub(crate) fn text_parts(&self) -> (String, Vec<(usize, u32)>) {
        decode_bytes(self.bytes(), self.is_multibyte())
            .expect("stored string bytes have the internal encoding")
    }

    pub(crate) fn text(&self) -> String {
        self.text_parts().0
    }

    pub(crate) fn copy_with_properties(&self) -> StringObjectRef {
        let copy = StringObjectRef::from_slice(
            self.bytes(),
            self.len(),
            self.is_multibyte(),
            self.props.to_vec(),
            StringAllocation::Ordinary,
        );
        // fns.c Fcopy_sequence copies the intervals, every plist of which
        // may be empty.
        if self.props.is_empty() && self.props.has_intervals() && self.len() != 0 {
            copy.borrow_mut().props = StringProperties::empty_tree();
        }
        copy
    }

    pub(crate) fn copy_without_properties(
        &self,
        kind: crate::lisp::alloc::StringAllocation,
    ) -> StringObjectRef {
        StringObjectRef::from_slice(
            self.bytes(),
            self.len(),
            self.is_multibyte(),
            Vec::new(),
            kind,
        )
    }

    /// fns.c:Fsubstring copies the actual byte range and preserves encoding.
    /// The primitive validates both character bounds before reaching here.
    pub(crate) fn substring(&self, from: usize, to: usize, properties: bool) -> StringObjectRef {
        assert!(from <= to && to <= self.len());
        let offset = |index| {
            if index == self.len() {
                self.bytes().len()
            } else {
                self.byte_offset(index).expect("validated character bound")
            }
        };
        let bytes = &self.bytes()[offset(from)..offset(to)];
        let props = if properties {
            self.props
                .iter()
                .filter_map(|span| {
                    let start = span.start.max(from);
                    let end = span.end.min(to);
                    (start < end).then(|| StringPropertySpan {
                        start: start - from,
                        end: end - from,
                        // copy_text_properties adds each pair at the head.
                        props: span.props.iter().rev().cloned().collect(),
                    })
                })
                .collect()
        } else {
            Vec::new()
        };
        StringObjectRef::from_slice(
            bytes,
            to - from,
            self.is_multibyte(),
            props,
            StringAllocation::Ordinary,
        )
    }

    pub(crate) fn character_at(&self, index: usize) -> Option<i64> {
        if !self.is_multibyte() {
            return self.bytes().get(index).map(|byte| i64::from(*byte));
        }
        let offset = self.byte_offset(index)?;
        decode_character(&self.bytes()[offset..])
            .ok()
            .map(|(code, _)| i64::from(code))
    }

    pub(crate) fn byte_offset(&self, index: usize) -> Option<usize> {
        (index <= self.len()).then(|| self.char_to_byte(index))
    }

    /// Every character as fetch_string_char_advance reads it: a unibyte
    /// string's bytes, a multibyte one's characters.
    pub(crate) fn codes(&self) -> Vec<u32> {
        let mut codes = Vec::with_capacity(self.len());
        let mut offset = 0;
        while let Some(code) = self.character_advance(&mut offset) {
            codes.push(code as u32);
        }
        codes
    }

    /// character.h:fetch_string_char_advance: the character at byte
    /// OFFSET, a unibyte string's byte as itself, advancing the cursor.
    pub(crate) fn character_advance(&self, offset: &mut usize) -> Option<i64> {
        let (code, width) = if self.is_multibyte() {
            decode_character(self.bytes().get(*offset..)?).ok()?
        } else {
            (u32::from(*self.bytes().get(*offset)?), 1)
        };
        *offset += width;
        Some(i64::from(code))
    }

    /// character.h:fetch_string_char_as_multibyte_advance reads one
    /// character from the actual bytes, promoting an unibyte octet to
    /// byte8 before any case-table lookup. The caller owns the byte cursor.
    pub(crate) fn character_as_multibyte_advance(&self, offset: &mut usize) -> Option<i64> {
        let (code, width) = if self.is_multibyte() {
            decode_character(self.bytes().get(*offset..)?).ok()?
        } else {
            let byte = u32::from(*self.bytes().get(*offset)?);
            (if byte >= 128 { 0x3fff00 + byte } else { byte }, 1)
        };
        *offset += width;
        Some(i64::from(code))
    }

    /// data.c:Faset. The caller checks the index and character first.
    /// False means a non-ASCII unibyte string cannot be promoted in place.
    pub(crate) fn store_character(&mut self, index: usize, code: u32) -> Result<bool, StringError> {
        assert!(index < self.len() && code <= 0x3f_ffff);
        if !self.is_multibyte() && code <= 255 {
            self.bytes_mut()[index] = code as u8;
            return Ok(true);
        }
        let (offset, old_width) = if self.is_multibyte() {
            let offset = self.byte_offset(index).expect("checked string index");
            (
                offset,
                decode_character(&self.bytes()[offset..])
                    .expect("stored character")
                    .1,
            )
        } else {
            if !self.bytes().is_ascii() {
                return Ok(false);
            }
            self.size_byte = self.len() as isize;
            (index, 1)
        };
        let (encoded, width) = encode_character(code).expect("checked Lisp character");
        if width == old_width {
            self.bytes_mut()[offset..offset + width].copy_from_slice(&encoded[..width]);
        } else {
            let nbytes = self
                .storage_bytes()
                .checked_add(width)
                .and_then(|length| length.checked_sub(old_width))
                .ok_or_else(string_overflow)?;
            validate_size(nbytes)?;
            let old_bytes = self.storage_bytes();
            let tail = old_bytes - offset - old_width;
            if string_data_size(nbytes) == string_data_size(old_bytes) {
                // alloc.c:resize_string_data reuses alignment slop, and moves
                // the old tail including its NUL with overlap allowed.
                unsafe {
                    let data = self.data.as_ptr();
                    ptr::copy(
                        data.add(offset + old_width),
                        data.add(offset + width),
                        tail + 1,
                    );
                    ptr::copy_nonoverlapping(encoded.as_ptr(), data.add(offset), width);
                }
            } else {
                // SAFETY: all three disjoint output ranges are initialized;
                // the pending entry supplies the final NUL and stable owner.
                let allocation = unsafe { PendingStringData::new(self, nbytes, false) }?;
                unsafe {
                    let data = allocation.as_ptr();
                    ptr::copy_nonoverlapping(self.data.as_ptr(), data, offset);
                    ptr::copy_nonoverlapping(encoded.as_ptr(), data.add(offset), width);
                    ptr::copy_nonoverlapping(
                        self.data.as_ptr().add(offset + old_width),
                        data.add(offset + width),
                        tail,
                    );
                    retire_string_data(self.data, old_bytes);
                }
                self.data = allocation.install();
                crate::lisp::native_comp::note_lisp_allocation(string_data_size(nbytes));
            }
            self.size_byte = nbytes as isize;
        }
        // The characters before INDEX kept their bytes, so the cached
        // position Faset found stays right (string_char_to_byte's cache
        // names the string, not its size).
        self.set_char_byte_cache(index, offset, None);
        Ok(true)
    }

    pub(crate) fn replace_text(
        &mut self,
        text: String,
        multibyte: bool,
        extended: Vec<(usize, u32)>,
    ) {
        let bytes = encode_text(&text, multibyte, &extended)
            .expect("string replacement must supply valid Lisp characters");
        self.replace_storage(bytes, text.chars().count(), multibyte);
    }

    /// fns.c:Ffillarray validates ITEM even for an empty string. Unibyte
    /// strings store its low byte; multibyte fills cannot resize the payload.
    /// Existing intervals and aliases continue to name the same object.
    pub(crate) fn fill(&mut self, item: Value) -> Result<(), LispError> {
        let code = character_code(item)?;
        if self.len() == 0 {
            return Ok(());
        }
        let (encoded, width) = if self.is_multibyte() {
            encode_character(code)?
        } else {
            ([code as u8, 0, 0, 0, 0], 1)
        };
        if self.len().checked_mul(width) != Some(self.bytes().len()) {
            return Err(LispError::Signal(
                "Attempt to change byte length of a string".into(),
            ));
        }
        self.forget_char_byte_cache();
        if width == 1 {
            self.bytes_mut().fill(encoded[0]);
        } else {
            for bytes in self.bytes_mut().chunks_exact_mut(width) {
                bytes.copy_from_slice(&encoded[..width]);
            }
        }
        Ok(())
    }

    /// fns.c:Fclear_string clears actual storage bytes and makes it unibyte.
    pub(crate) fn clear(&mut self) {
        // STRING_SET_UNIBYTE replaces a zero-length local value; it must
        // not change the original shared empty multibyte header.
        let nbytes = self.storage_bytes();
        self.forget_char_byte_cache();
        if nbytes != 0 {
            self.bytes_mut().fill(0);
            self.size = nbytes;
            self.size_byte = -1;
        }
    }
}

pub(crate) fn character_code(value: Value) -> Result<u32, LispError> {
    match value.kind() {
        Kind::Integer(code) if (0..=0x3f_ffff).contains(&code) => Ok(code as u32),
        _ => Err(LispError::WrongTypeArgument("characterp".into(), value)),
    }
}

fn string_overflow() -> LispError {
    LispError::Signal("Maximum string size exceeded".into())
}

fn validate_size(length: usize) -> Result<(), LispError> {
    // lisp.h:STRING_BYTES_BOUND and alloc.c:STRING_BYTES_MAX. On the
    // supported 64-bit targets the 61-bit fixnum bound is the tightest.
    if length > (1usize << 61) - 1 {
        return Err(string_overflow());
    }
    Ok(())
}

pub(crate) fn encode_text(
    text: &str,
    multibyte: bool,
    extended: &[(usize, u32)],
) -> Result<Vec<u8>, LispError> {
    if extended.is_empty() && text.is_ascii() {
        // Each ASCII character is its own byte, unibyte or multibyte.
        return Ok(text.as_bytes().to_vec());
    }
    let mut bytes = Vec::with_capacity(text.len());
    for (index, ch) in text.chars().enumerate() {
        let code = extended
            .binary_search_by_key(&index, |(position, _)| *position)
            .ok()
            .map(|found| extended[found].1)
            .unwrap_or_else(|| {
                crate::lisp::primitives::string_character_code(multibyte, ch) as u32
            });
        if multibyte {
            let (encoded, width) = encode_character(code)?;
            bytes.extend_from_slice(&encoded[..width]);
        } else if code <= 255 {
            bytes.push(code as u8);
        } else {
            return Err(LispError::Signal(format!(
                "unibyte string holds character {code:#x}, not a byte"
            )));
        }
    }
    Ok(bytes)
}

pub(crate) fn encode_character(code: u32) -> Result<([u8; 5], usize), LispError> {
    let mut bytes = [0; 5];
    let width = match code {
        0..=0x7f => {
            bytes[0] = code as u8;
            1
        }
        0x80..=0x7ff => {
            bytes[0] = 0xc0 | (code >> 6) as u8;
            bytes[1] = 0x80 | (code & 63) as u8;
            2
        }
        0x800..=0xffff => {
            bytes[0] = 0xe0 | (code >> 12) as u8;
            bytes[1] = 0x80 | ((code >> 6) & 63) as u8;
            bytes[2] = 0x80 | (code & 63) as u8;
            3
        }
        0x10000..=0x1fffff => {
            bytes[0] = 0xf0 | (code >> 18) as u8;
            bytes[1] = 0x80 | ((code >> 12) & 63) as u8;
            bytes[2] = 0x80 | ((code >> 6) & 63) as u8;
            bytes[3] = 0x80 | (code & 63) as u8;
            4
        }
        0x200000..=0x3fff7f => {
            bytes[0] = 0xf8;
            bytes[1] = 0x80 | ((code >> 18) & 15) as u8;
            bytes[2] = 0x80 | ((code >> 12) & 63) as u8;
            bytes[3] = 0x80 | ((code >> 6) & 63) as u8;
            bytes[4] = 0x80 | (code & 63) as u8;
            5
        }
        0x3fff80..=0x3fffff => {
            let raw = code - 0x3fff80;
            bytes[0] = 0xc0 | (raw >> 6) as u8;
            bytes[1] = 0x80 | (raw & 63) as u8;
            2
        }
        _ => return Err(LispError::Signal("Invalid character".into())),
    };
    Ok((bytes, width))
}

pub(crate) fn decode_character(bytes: &[u8]) -> Result<(u32, usize), LispError> {
    let Some(&lead) = bytes.first() else {
        return Err(LispError::Signal("Truncated string character".into()));
    };
    let (width, mut code) = match lead {
        0..=0x7f => (1, u32::from(lead)),
        0xc0..=0xdf => (2, u32::from(lead & 31)),
        0xe0..=0xef => (3, u32::from(lead & 15)),
        0xf0..=0xf7 => (4, u32::from(lead & 7)),
        0xf8 => (5, 0),
        _ => {
            return Err(LispError::Signal(format!(
                "Invalid internal multibyte lead {lead:#x}"
            )));
        }
    };
    let tail = bytes
        .get(1..width)
        .ok_or_else(|| LispError::Signal("Truncated string character".into()))?;
    for &byte in tail {
        if byte & 0xc0 != 0x80 {
            return Err(LispError::Signal(
                "Invalid internal multibyte continuation".into(),
            ));
        }
        code = (code << 6) | u32::from(byte & 63);
    }
    if width == 2 && lead <= 0xc1 {
        code += 0x3fff80;
    }
    if code > 0x3fffff {
        return Err(LispError::Signal("Invalid internal character".into()));
    }
    Ok((code, width))
}

/// character.h multibyte_length (P, PEND, true, ALLOW_8BIT): the length of
/// the internal multibyte sequence BYTES starts with, or 0 when they start
/// with none.  ALLOW_8BIT accepts the two-byte spelling of an eight-bit
/// character (lead 0xC0 or 0xC1).
pub(crate) fn multibyte_length(bytes: &[u8], allow_8bit: bool) -> usize {
    let Some(&c) = bytes.first() else {
        return 0;
    };
    if c < 0x80 {
        return 1;
    }
    let Some(&d) = bytes.get(1) else {
        return 0;
    };
    let mut w = (u32::from(d & 0xC0) << 2) + u32::from(c);
    if (if allow_8bit { 0x2C0 } else { 0x2C2 }) <= w && w <= 0x2DF {
        return 2;
    }
    let Some(&e) = bytes.get(2) else {
        return 0;
    };
    w += u32::from(e & 0xC0) << 4;
    let w1 = w | (u32::from(d & 0x20) >> 2);
    if (0xAE1..=0xAEF).contains(&w1) {
        return 3;
    }
    let Some(&f) = bytes.get(3) else {
        return 0;
    };
    w += u32::from(f & 0xC0) << 6;
    let w2 = w | (u32::from(d & 0x30) >> 3);
    if (0x2AF1..=0x2AF7).contains(&w2) {
        return 4;
    }
    let Some(&g) = bytes.get(4) else {
        return 0;
    };
    let lw = i64::from(w) + (i64::from(g & 0xC0) << 8);
    let w3 = (lw << 24) + (i64::from(d) << 16) + (i64::from(e) << 8) + i64::from(f);
    if (0xAA_F888_8080..=0xAA_F88F_BFBD).contains(&w3) {
        return 5;
    }
    0
}

pub(crate) fn decode_bytes(
    bytes: &[u8],
    multibyte: bool,
) -> Result<(String, Vec<(usize, u32)>), LispError> {
    if bytes.is_ascii() {
        // Each byte is its own character, unibyte or multibyte.
        // SAFETY: ASCII is UTF-8.
        let text = unsafe { std::str::from_utf8_unchecked(bytes) };
        return Ok((text.to_owned(), Vec::new()));
    }
    let mut text = String::with_capacity(bytes.len());
    let mut extended = Vec::new();
    let mut offset = 0;
    let mut index = 0;
    while offset < bytes.len() {
        let (code, width) = if multibyte {
            decode_character(&bytes[offset..])?
        } else {
            (u32::from(bytes[offset]), 1)
        };
        offset += width;
        if !multibyte && code >= 128 {
            text.push(crate::lisp::primitives::raw_byte_regex_char(code as u8));
        } else if (0x3fff80..=0x3fffff).contains(&code) {
            text.push(crate::lisp::primitives::raw_byte_regex_char(
                (code - 0x3fff00) as u8,
            ));
        } else if let Some(ch) = char::from_u32(code) {
            text.push(ch);
            // A transient Rust text view uses this private-use range for
            // byte8. Preserve actual Unicode characters in the same range
            // explicitly so rebuilding the view cannot turn them into bytes.
            if crate::lisp::primitives::raw_byte_from_regex_char(ch).is_some() {
                extended.push((index, code));
            }
        } else {
            text.push(crate::lisp::json::INVALID_UNICODE_SENTINEL);
            extended.push((index, code));
        }
        index += 1;
    }
    Ok((text, extended))
}
