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

    pub(crate) fn from_storage(
        bytes: Vec<u8>,
        characters: usize,
        multibyte: bool,
    ) -> Result<Self, StringError> {
        Self::from_storage_kind(
            bytes,
            characters,
            multibyte,
            crate::lisp::alloc::StringAllocation::Ordinary,
        )
    }

    pub(crate) fn from_storage_kind(
        bytes: Vec<u8>,
        characters: usize,
        multibyte: bool,
        kind: crate::lisp::alloc::StringAllocation,
    ) -> Result<Self, StringError> {
        let mut count = 0;
        let mut offset = 0;
        while offset < bytes.len() {
            offset += if multibyte {
                decode_character(&bytes[offset..])?.1
            } else {
                1
            };
            count += 1;
        }
        if count != characters {
            return Err(LispError::Signal(
                "String character count does not match its bytes".into(),
            )
            .into());
        }
        Self::try_from_slice(&bytes, characters, multibyte, Vec::new(), kind)
    }

    pub(crate) fn from_text(
        text: String,
        props: Vec<StringPropertySpan>,
        multibyte: bool,
        extended_chars: Vec<(usize, u32)>,
    ) -> Self {
        let bytes = encode_text(&text, multibyte, &extended_chars)
            .expect("string constructor must supply valid Lisp characters");
        Self::from_encoded(bytes, text.chars().count(), multibyte, props)
    }
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
        StringObjectRef::from_slice(
            self.bytes(),
            self.len(),
            self.is_multibyte(),
            self.props.to_vec(),
            StringAllocation::Ordinary,
        )
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
        if index > self.len() {
            return None;
        }
        if !self.is_multibyte() {
            return Some(index);
        }
        let mut offset = 0;
        for _ in 0..index {
            offset += decode_character(&self.bytes()[offset..]).ok()?.1;
        }
        Some(offset)
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

fn decode_character(bytes: &[u8]) -> Result<(u32, usize), LispError> {
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

pub(crate) fn decode_bytes(
    bytes: &[u8],
    multibyte: bool,
) -> Result<(String, Vec<(usize, u32)>), LispError> {
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
