//! pdumper.c:pdumper_load's validation and object reconstruction (the
//! part D08 needs to prove its images round-trip; the process-level
//! restore is D12/D13).
//!
//! The loader validates the file exactly as `pdumper_load' does (size,
//! magic, the incomplete marker, the fingerprint), then rebuilds one Rust
//! object per object-start entry and applies the relocation tables to
//! its fields, so sharing and cycles come back as they were written.

use super::super::*;
use super::context::*;
use super::image::*;
use crate::lisp::types::{SharedText, SymbolName};
use std::collections::HashMap;

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
    pub(crate) header: DumpHeader,
    pub(crate) roots: Vec<(RootSlot, Value)>,
    pub(crate) symbols: Vec<LoadedSymbol>,
    /// The obarray record's symbols in image order.
    pub(crate) obarray: Vec<SymbolName>,
}

struct Reader<'a> {
    bytes: &'a [u8],
}

impl Reader<'_> {
    fn word(&self, offset: u32) -> Result<u64, LoadError> {
        let start = offset as usize;
        self.bytes
            .get(start..start + 8)
            .map(|slice| u64::from_le_bytes(slice.try_into().expect("eight bytes")))
            .ok_or_else(|| LoadError::Error(format!("word at {offset} is outside the image")))
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
        eprintln!("desired fingerprint: {}", hex(desired));
        eprintln!("found fingerprint: {}", hex(&header.fingerprint));
        return Err(LoadError::VersionMismatch);
    }
    Ok(header)
}

pub(crate) fn load_image(bytes: &[u8]) -> Result<LoadedImage, LoadError> {
    let header = validate_header(bytes)?;
    let reader = Reader { bytes };

    // The tables.
    let mut object_starts = Vec::new();
    for index in 0..header.object_starts.nr_entries {
        let at = header.object_starts.offset + index * TABLE_ENTRY_LEN as u32;
        let offset = reader.u32(at)?;
        let kind = DumpType::from_u32(reader.u32(at + 4)?)
            .ok_or_else(|| LoadError::Error(format!("unknown object type at {at}")))?;
        object_starts.push((offset, kind));
    }
    let mut relocs: HashMap<u32, DumpRelocKind> = HashMap::new();
    for phase in 0..RELOC_NUM_PHASES {
        let locator = header.dump_relocs[phase];
        for index in 0..locator.nr_entries {
            let at = locator.offset + index * TABLE_ENTRY_LEN as u32;
            let offset = reader.u32(at)?;
            let kind = DumpRelocKind::from_u32(reader.u32(at + 4)?)
                .ok_or_else(|| LoadError::Error(format!("unknown relocation at {at}")))?;
            relocs.insert(offset, kind);
        }
    }

    // Objects that need nothing but their own bytes first (strings,
    // floats, bignums), then those whose construction needs a name
    // (symbols, subrs), then the containers, filled through relocations.
    let mut objects: HashMap<u32, Value> = HashMap::new();
    let mut symbol_records = Vec::new();
    let mut string_props: Vec<(u32, u32)> = Vec::new();
    for &(offset, kind) in &object_starts {
        match kind {
            DumpType::String | DumpType::StringObject => {
                let (value, props) = load_string(&reader, offset, kind)?;
                if let Some(props) = props {
                    string_props.push((offset, props));
                }
                objects.insert(offset, value);
            }
            DumpType::Float => {
                objects.insert(offset, Value::float(f64::from_bits(reader.word(offset)?)));
            }
            DumpType::Bignum => {
                objects.insert(offset, load_bignum(&reader, offset)?);
            }
            _ => {}
        }
    }
    for &(offset, kind) in &object_starts {
        match kind {
            DumpType::Symbol => {
                let flags = reader.word(offset)?;
                let name = field_value(&reader, &relocs, &objects, offset + 8)?;
                let name_text = string_like(&name)
                    .map(|string| string.text)
                    .ok_or_else(|| LoadError::Error("symbol name is not a string".into()))?;
                let interned = (flags >> SYMBOL_INTERNED_SHIFT) & 3;
                let symbol = if interned == SYMBOL_UNINTERNED {
                    SymbolName::make_uninterned(name, &name_text, next_make_symbol_id())
                } else {
                    SymbolName::intern_str(&name_text)
                };
                objects.insert(offset, Value::Symbol(symbol.clone()));
                symbol_records.push((offset, symbol, flags));
            }
            DumpType::Cons => {
                objects.insert(offset, Value::cons(Value::Nil, Value::Nil));
            }
            DumpType::Vector => {
                let size = reader.word(offset)? as usize;
                objects.insert(offset, Value::vector(vec![Value::Nil; size]));
            }
            DumpType::Obarray => {
                objects.insert(offset, obarray_value());
            }
            _ => {}
        }
    }

    // The containers' fields.
    let mut obarray = Vec::new();
    for &(offset, kind) in &object_starts {
        match kind {
            DumpType::Cons => {
                let car = field_value(&reader, &relocs, &objects, offset)?;
                let cdr = field_value(&reader, &relocs, &objects, offset + 8)?;
                let cell = &objects[&offset];
                cell.set_car(car).map_err(lisp_error)?;
                cell.set_cdr(cdr).map_err(lisp_error)?;
            }
            DumpType::Vector => {
                let size = reader.word(offset)? as usize;
                let Value::Vector(vector) = &objects[&offset] else {
                    unreachable!()
                };
                for index in 0..size {
                    let slot =
                        field_value(&reader, &relocs, &objects, offset + 8 * (index as u32 + 1))?;
                    vector.slots_mut()[index] = slot;
                }
            }
            DumpType::Obarray => {
                let count = reader.word(offset)? as usize;
                for index in 0..count {
                    let value =
                        field_value(&reader, &relocs, &objects, offset + 8 * (index as u32 + 1))?;
                    let Value::Symbol(symbol) = value else {
                        return Err(LoadError::Error("obarray entry is not a symbol".into()));
                    };
                    obarray.push(symbol);
                }
            }
            _ => {}
        }
    }
    for (string_offset, props_offset) in string_props {
        let spans = load_text_properties(&reader, &relocs, &objects, props_offset)?;
        match &objects[&string_offset] {
            Value::StringObject(state) => {
                state.borrow_mut().props = spans;
            }
            other => {
                return Err(LoadError::Error(format!(
                    "text properties on a non-object string: {other:?}"
                )));
            }
        }
    }

    let mut symbols = Vec::new();
    for (offset, symbol, flags) in symbol_records {
        let val = field_value(&reader, &relocs, &objects, offset + 16)?;
        let function = field_value(&reader, &relocs, &objects, offset + 24)?;
        let plist = field_value(&reader, &relocs, &objects, offset + 32)?;
        let nwatchers = reader.word(offset + 40)? as u32;
        let mut watchers = Vec::new();
        for index in 0..nwatchers {
            watchers.push(field_value(
                &reader,
                &relocs,
                &objects,
                offset + 48 + 8 * index,
            )?);
        }
        let (value, alias) = if flags & SYMBOL_REDIRECT_MASK == SYMBOL_VARALIAS {
            let Value::Symbol(target) = val else {
                return Err(LoadError::Error("alias target is not a symbol".into()));
            };
            (None, Some(target))
        } else {
            ((!matches!(val, Value::Unbound)).then_some(val), None)
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

    // The Emacs relocations: the root slots.
    let mut roots = Vec::new();
    for index in 0..header.emacs_relocs.nr_entries {
        let at = header.emacs_relocs.offset + index * EMACS_RELOC_LEN as u32;
        let kind = EmacsRelocKind::from_u32(reader.u32(at)?)
            .ok_or_else(|| LoadError::Error(format!("unknown Emacs relocation at {at}")))?;
        let slot = RootSlot::from_u32(reader.u32(at + 4)?)
            .ok_or_else(|| LoadError::Error(format!("unknown root slot at {at}")))?;
        let payload = reader.word(at + 8)?;
        let value = match kind {
            EmacsRelocKind::Immediate => immediate_value(payload)?,
            EmacsRelocKind::DumpLv(_) => objects
                .get(&(payload as u32))
                .cloned()
                .ok_or_else(|| LoadError::Error(format!("root points at no object: {payload}")))?,
            EmacsRelocKind::EmacsLv(kind) => {
                emacs_image_object(&reader, &relocs, &objects, payload as u32, kind)?
            }
        };
        roots.push((slot, value));
    }

    Ok(LoadedImage {
        header,
        roots,
        symbols,
        obarray,
    })
}

fn lisp_error(error: LispError) -> LoadError {
    LoadError::Error(format!("{error:?}"))
}

/// A word without a relocation is self-representing.
fn immediate_value(word: u64) -> Result<Value, LoadError> {
    if let Some(fixnum) = word_fixnum(word) {
        return Ok(Value::Integer(fixnum));
    }
    Ok(match word {
        WORD_NIL => Value::Nil,
        WORD_T => Value::T,
        WORD_UNBOUND => Value::Unbound,
        _ => {
            return Err(LoadError::Error(format!(
                "unknown immediate word {word:#x}"
            )));
        }
    })
}

/// The Lisp value a record field holds: the relocation says which object
/// the word names, or the word is immediate.
fn field_value(
    reader: &Reader<'_>,
    relocs: &HashMap<u32, DumpRelocKind>,
    objects: &HashMap<u32, Value>,
    field_offset: u32,
) -> Result<Value, LoadError> {
    let word = reader.word(field_offset)?;
    match relocs.get(&field_offset) {
        None => immediate_value(word),
        Some(DumpRelocKind::DumpToDumpLv(_)) => {
            objects.get(&(word as u32)).cloned().ok_or_else(|| {
                LoadError::Error(format!("field at {field_offset} names no object ({word})"))
            })
        }
        Some(DumpRelocKind::DumpToEmacsLv(kind)) => {
            emacs_image_object(reader, relocs, objects, word as u32, *kind)
        }
        Some(other) => Err(LoadError::Error(format!(
            "field at {field_offset} has a non-object relocation {other:?}"
        ))),
    }
}

/// An object of the Emacs image, reached through its copied record in the
/// discardable section (pdumper.c relocates such a word to the Emacs
/// address; the copied record is what names it here).  A built-in
/// function's record holds its name.
fn emacs_image_object(
    reader: &Reader<'_>,
    relocs: &HashMap<u32, DumpRelocKind>,
    objects: &HashMap<u32, Value>,
    offset: u32,
    kind: DumpType,
) -> Result<Value, LoadError> {
    match kind {
        DumpType::Subr => {
            let name = field_value(reader, relocs, objects, offset)?;
            let name_text = string_like(&name)
                .map(|string| string.text)
                .ok_or_else(|| LoadError::Error("subr name is not a string".into()))?;
            Ok(Value::BuiltinFunc(SymbolName::intern_str(&name_text)))
        }
        other => Err(LoadError::Error(format!(
            "{other:?} is not an Emacs-image object kind"
        ))),
    }
}

/// A string record: size, size_byte, intervals, data; the bytes at the
/// cold offset in GNU's internal representation.
fn load_string(
    reader: &Reader<'_>,
    offset: u32,
    kind: DumpType,
) -> Result<(Value, Option<u32>), LoadError> {
    let size = reader.word(offset)? as usize;
    let size_byte = reader.word(offset + 8)?;
    let intervals = reader.word(offset + 16)? as u32;
    let data = reader.word(offset + 24)? as u32;
    let multibyte = size_byte != u64::MAX;
    let nbytes = if multibyte { size_byte as usize } else { size };
    let bytes = reader
        .bytes
        .get(data as usize..data as usize + nbytes)
        .ok_or_else(|| LoadError::Error(format!("string data at {data} is outside the image")))?;
    let (text, extended_chars) = decode_internal_bytes(bytes, multibyte)?;
    if text.chars().count() != size {
        return Err(LoadError::Error(format!(
            "string at {offset} decodes to {} characters, record says {size}",
            text.chars().count()
        )));
    }
    let value = match kind {
        DumpType::String => Value::String(SharedText::new(text)),
        _ => crate::lisp::primitives::strings::make_shared_string_value_with_extended_chars(
            text,
            Vec::new(),
            multibyte,
            extended_chars,
        ),
    };
    Ok((value, (intervals != 0).then_some(intervals)))
}

/// GNU's internal multibyte form back to Emaxx's text plus the
/// characters Rust cannot spell (a placeholder in the text, the code in
/// the side list); a unibyte string's octets back to its characters.
pub(crate) fn decode_internal_bytes(
    bytes: &[u8],
    multibyte: bool,
) -> Result<(String, Vec<(usize, u32)>), LoadError> {
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

/// A bignum record after its fixup: sign/limb count and the cold limbs.
fn load_bignum(reader: &Reader<'_>, offset: u32) -> Result<Value, LoadError> {
    let sign_limbs = reader.word(offset)? as i64;
    let data = reader.word(offset + 8)? as u32;
    let nlimbs = sign_limbs.unsigned_abs() as u32;
    let mut limbs = Vec::with_capacity(nlimbs as usize);
    for index in 0..nlimbs {
        limbs.push(reader.word(data + 8 * index)?);
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

/// A text-properties record: count, then (start, end, nprops, (name,
/// value)*).
fn load_text_properties(
    reader: &Reader<'_>,
    relocs: &HashMap<u32, DumpRelocKind>,
    objects: &HashMap<u32, Value>,
    offset: u32,
) -> Result<Vec<crate::lisp::types::StringPropertySpan>, LoadError> {
    let count = reader.word(offset)? as usize;
    let mut at = offset + 8;
    let mut spans = Vec::with_capacity(count);
    for _ in 0..count {
        let start = reader.word(at)? as usize;
        let end = reader.word(at + 8)? as usize;
        let nprops = reader.word(at + 16)? as usize;
        at += 24;
        let mut props = Vec::with_capacity(nprops);
        for _ in 0..nprops {
            let name = field_value(reader, relocs, objects, at)?;
            let value = field_value(reader, relocs, objects, at + 8)?;
            at += 16;
            let Value::Symbol(name) = name else {
                return Err(LoadError::Error("property name is not a symbol".into()));
            };
            props.push((name.as_str().to_owned(), value));
        }
        spans.push(crate::lisp::types::StringPropertySpan { start, end, props });
    }
    Ok(spans)
}
