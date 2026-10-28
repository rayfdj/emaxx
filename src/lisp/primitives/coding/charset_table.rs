//! charset.c's charset table: each charset's attributes as `struct
//! charset' keeps them, read once from the charset's plist (which carries
//! every `define-charset-internal' argument) instead of through the plist
//! by name at each use, and a map charset's decoder and encoder, which
//! load_charset_map builds once, made when it first decodes or encodes.
//! The interpreter keeps the table; defining a charset, an alias or a
//! plist, or (un)unifying one, empties it.

use super::*;
use crate::lisp::primitives::FnvBuildHasher;
use std::cell::OnceCell;
use std::rc::Rc;

/// The charsets charset.c defines in C (syms_of_charset), whose codes are
/// their characters.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CharsetBuiltin {
    Ascii,
    Iso8859_1,
    Unicode,
    Emacs,
    EightBit,
    None,
}

/// Where a map charset's mapping comes from: a vector of code and
/// character pairs, or a map file under etc/charsets.
#[derive(Clone, Debug)]
enum MapSource {
    Pairs(Vec<(u32, u32)>),
    File(String),
}

/// A charset's mapping (load_charset_map): its pairs in the map's order,
/// the character of each code and the code of each character, the first
/// pair for either winning.
pub(crate) struct CharsetMap {
    pub(crate) entries: Vec<(u32, u32)>,
    decoder: HashMap<u32, u32, FnvBuildHasher>,
    encoder: HashMap<u32, u32, FnvBuildHasher>,
}

impl CharsetMap {
    fn new(entries: Vec<(u32, u32)>) -> Self {
        let mut decoder =
            HashMap::with_capacity_and_hasher(entries.len(), FnvBuildHasher::default());
        let mut encoder =
            HashMap::with_capacity_and_hasher(entries.len(), FnvBuildHasher::default());
        for &(code, character) in &entries {
            decoder.entry(code).or_insert(character);
            encoder.entry(character).or_insert(code);
        }
        CharsetMap {
            entries,
            decoder,
            encoder,
        }
    }

    /// The character CODE maps to.
    #[inline]
    pub(crate) fn decode(&self, code: u32) -> Option<u32> {
        self.decoder.get(&code).copied()
    }

    /// The code CHARACTER maps from.
    #[inline]
    pub(crate) fn encode(&self, character: u32) -> Option<u32> {
        self.encoder.get(&character).copied()
    }
}

/// struct charset: what a charset's plist said when the table read it.
pub(crate) struct CharsetInfo {
    /// The canonical name.
    pub(crate) name: String,
    pub(crate) builtin: CharsetBuiltin,
    /// CHARSET_DIMENSION: `:dimension', else the code space's.
    pub(crate) dimension: u32,
    /// `:code-space' as (min, max) byte ranges, least significant first.
    pub(crate) code_space: Option<Vec<(u32, u32)>>,
    /// CHARSET_MIN_CODE and CHARSET_MAX_CODE.
    pub(crate) code_bounds: Option<(u32, u32)>,
    /// The code-space index of `:min-code' (char_index_offset).
    char_index_offset: Option<u32>,
    /// CHARSET_METHOD_OFFSET's `:code-offset'.
    pub(crate) code_offset: Option<i64>,
    /// Whether `:map' is given (CHARSET_METHOD_MAP).
    pub(crate) has_direct_map: bool,
    /// CHARSET_UNIFIED_P when the table read the charset.
    pub(crate) unified: bool,
    map_source: Option<MapSource>,
    /// CHARSET_METHOD_SUBSET: (PARENT MIN MAX OFFSET).
    pub(crate) subset: Option<(String, i64, i64, i64)>,
    /// CHARSET_METHOD_SUPERSET: (CHILD . OFFSET) pairs.
    pub(crate) superset: Option<Vec<(String, i64)>>,
    /// `:ascii-compatible-p', when given.
    pub(crate) ascii_compatible_p: Option<bool>,
    /// CHARSET_EMACS_MULE_ID: `:emacs-mule-id', when given.
    pub(crate) emacs_mule_id: Option<u8>,
    map: OnceCell<Option<Rc<CharsetMap>>>,
}

/// The value of KEY in the property list ITEMS (`plist-get').
fn plist_get(items: &[Value], key: &str) -> Option<Value> {
    let (pairs, _) = items.as_chunks::<2>();
    pairs.iter().find_map(|[property, value]| {
        matches!(property.kind(), Kind::Symbol(name) if name == key).then_some(*value)
    })
}

/// The elements of a vector, or of a list.
fn vector_items(value: &Value) -> Option<Vec<Value>> {
    crate::lisp::primitives::vector_or_list_items(value).ok()
}

impl CharsetInfo {
    /// The attributes of the charset CANONICAL as its plist gives them.
    fn read(interp: &Interpreter, canonical: &str) -> Self {
        let items = interp
            .charset_plist_value(canonical)
            .and_then(|plist| plist.to_vec().ok())
            .unwrap_or_default();
        let property = |key: &str| plist_get(&items, key);
        let builtin = match canonical {
            "ascii" => CharsetBuiltin::Ascii,
            "iso-8859-1" => CharsetBuiltin::Iso8859_1,
            "unicode" => CharsetBuiltin::Unicode,
            "emacs" => CharsetBuiltin::Emacs,
            "eight-bit" => CharsetBuiltin::EightBit,
            _ => CharsetBuiltin::None,
        };

        // charset.c syms_of_charset defines the five C charsets with the
        // code spaces below; the bootstrap plists carry no :code-space.
        let code_space = match property(":code-space") {
            Some(space) => vector_items(&space).and_then(|values| {
                let bounds: Vec<(u32, u32)> = values
                    .as_chunks::<2>()
                    .0
                    .iter()
                    .filter_map(|pair| {
                        Some((
                            u32::try_from(pair[0].integer_value()?).ok()?,
                            u32::try_from(pair[1].integer_value()?).ok()?,
                        ))
                    })
                    .collect();
                (!bounds.is_empty()).then_some(bounds)
            }),
            None => match builtin {
                CharsetBuiltin::Ascii => Some(vec![(0, 127)]),
                CharsetBuiltin::Iso8859_1 => Some(vec![(0, 255)]),
                CharsetBuiltin::Unicode => Some(vec![(0, 255), (0, 255), (0, 16)]),
                CharsetBuiltin::Emacs => Some(vec![(0, 255), (0, 255), (0, 63)]),
                CharsetBuiltin::EightBit => Some(vec![(128, 255)]),
                CharsetBuiltin::None => None,
            },
        };
        let dimension = property(":dimension")
            .and_then(|value| value.integer_value())
            .and_then(|value| u32::try_from(value).ok())
            .or_else(|| code_space.as_ref().map(|bounds| bounds.len() as u32))
            .unwrap_or(1);

        // CHARSET_MIN_CODE / CHARSET_MAX_CODE: the `:min-code' and
        // `:max-code' attributes (an integer, or a (HIGH . LOW) pair of
        // 16-bit halves as `charset-plist' shows them), defaulting to the
        // corners of the :code-space.
        let code_bounds = code_space.as_ref().map(|bounds| {
            let mut min_code = 0u32;
            let mut max_code = 0u32;
            for (dimension, (min, max)) in bounds.iter().enumerate() {
                min_code |= min << (8 * dimension);
                max_code |= max << (8 * dimension);
            }
            let attribute = |name: &str| -> Option<u32> {
                let value = property(name)?;
                if let Some((high, low)) = value.cons_values() {
                    let high = u32::try_from(high.integer_value()?).ok()?;
                    let low = u32::try_from(low.integer_value()?).ok()?;
                    return Some((high << 16) | low);
                }
                u32::try_from(value.integer_value()?).ok()
            };
            (
                attribute(":min-code").unwrap_or(min_code),
                attribute(":max-code").unwrap_or(max_code),
            )
        });
        let char_index_offset = match (&code_space, code_bounds) {
            (Some(bounds), Some((min_code, _))) => code_point_to_raw_index(bounds, min_code),
            _ => None,
        };

        let code_offset = property(":code-offset").and_then(|value| value.integer_value());
        let direct_map = property(":map");
        let unified = interp.charset_is_unified(canonical);
        // Unified charsets (charset.c's UNIFIED_P, set by `unify-charset')
        // convert through their :unify-map table; only codes absent from
        // the table fall back to the code-offset rule.
        let map_value = direct_map.or_else(|| unified.then(|| property(":unify-map")).flatten());
        let map_source = map_value.and_then(|map| {
            if is_vector_value(&map) {
                let values = vector_items(&map)?;
                Some(MapSource::Pairs(
                    values
                        .as_chunks::<2>()
                        .0
                        .iter()
                        .filter_map(|pair| {
                            Some((
                                u32::try_from(pair[0].integer_value()?).ok()?,
                                u32::try_from(pair[1].integer_value()?).ok()?,
                            ))
                        })
                        .collect(),
                ))
            } else {
                string_text(&map).ok().map(MapSource::File)
            }
        });

        // A `:subset (PARENT MIN MAX OFFSET)' charset (latin-jisx0201 and
        // katakana-jisx0201, both windows onto jisx0201) converts through
        // PARENT: its code C is PARENT's code C - OFFSET, valid when the
        // parent code lies in MIN..=MAX.
        let subset = property(":subset").and_then(|subset| {
            let items = subset.to_vec().ok()?;
            if items.len() != 4 {
                return None;
            }
            Some((
                items[0].symbol_str()?.to_string(),
                items[1].integer_value()?,
                items[2].integer_value()?,
                items[3].integer_value()?,
            ))
        });
        // A `:superset (CHILD ...)' charset (japanese-jisx0213.2004-1 is
        // the superset of jisx0213-a and jisx0213-1) shares its children's
        // code space: conversion tries each child in order.  An entry may
        // also be (CHILD . OFFSET), shifting the codes by OFFSET.
        let superset = property(":superset").and_then(|superset| {
            let items = superset.to_vec().ok()?;
            let children: Vec<(String, i64)> = items
                .iter()
                .filter_map(|item| match item.kind() {
                    Kind::Symbol(name) => Some((name.to_string(), 0)),
                    other => {
                        let (car, cdr) = other.value().cons_values()?;
                        Some((car.symbol_str()?.to_string(), cdr.integer_value()?))
                    }
                })
                .collect();
            (!children.is_empty()).then_some(children)
        });
        let ascii_compatible_p = property(":ascii-compatible-p").map(|value| value.is_truthy());
        let emacs_mule_id = property(":emacs-mule-id")
            .and_then(|value| value.integer_value())
            .and_then(|id| u8::try_from(id).ok());

        CharsetInfo {
            name: canonical.to_string(),
            builtin,
            dimension,
            code_space,
            code_bounds,
            char_index_offset,
            code_offset,
            has_direct_map: property(":map").is_some(),
            unified,
            map_source,
            subset,
            superset,
            ascii_compatible_p,
            emacs_mule_id,
            map: OnceCell::new(),
        }
    }

    /// The charset's mapping, read the first time it is asked for: a map
    /// file is parsed against the code space (charset.c load_charset_map).
    pub(crate) fn map(&self, interp: &Interpreter) -> Option<Rc<CharsetMap>> {
        self.map
            .get_or_init(|| {
                let entries = match self.map_source.as_ref()? {
                    MapSource::Pairs(pairs) => pairs.clone(),
                    MapSource::File(name) => {
                        load_charset_map_file(interp, name, self.code_space.as_deref()?)?
                    }
                };
                Some(Rc::new(CharsetMap::new(entries)))
            })
            .clone()
    }

    /// charset.c CODE_POINT_TO_INDEX with the charset's `char_index_offset':
    /// CODE must lie in `:min-code'..`:max-code' (DECODE_CHAR's range
    /// test), and the index counts from the minimum code's position, so an
    /// offset charset's first character is `:code-offset' itself.
    pub(crate) fn code_to_index(&self, code: u32) -> Option<u32> {
        let bounds = self.code_space.as_deref()?;
        let (min_code, max_code) = self.code_bounds?;
        if code < min_code || code > max_code {
            return None;
        }
        code_point_to_raw_index(bounds, code)?.checked_sub(self.char_index_offset?)
    }

    /// INDEX_TO_CODE_POINT with the `char_index_offset' added back; None
    /// past `:max-code'.
    pub(crate) fn index_to_code(&self, index: u32) -> Option<u32> {
        let bounds = self.code_space.as_deref()?;
        let (_, max_code) = self.code_bounds?;
        let code = raw_index_to_code_point(bounds, index.checked_add(self.char_index_offset?)?)?;
        (code <= max_code).then_some(code)
    }

    /// DECODE_CHAR: the character CODE is in this charset, None for a code
    /// that is none of its characters.
    pub(crate) fn decode(&self, interp: &Interpreter, code: u32) -> Option<u32> {
        match self.builtin {
            CharsetBuiltin::Ascii => return (code <= 0x7f).then_some(code),
            CharsetBuiltin::Iso8859_1 => return (code <= 0xff).then_some(code),
            // charset.c: the code space of `unicode' ends at
            // MAX_UNICODE_CHAR and that of `emacs' at MAX_5_BYTE_CHAR (the
            // oracle answers nil for `(decode-char 'emacs #x3fff80)');
            // `eight-bit' is the code-offset charset for the raw bytes
            // 128..255.
            CharsetBuiltin::Unicode => return (code <= MAX_UNICODE_CHAR).then_some(code),
            CharsetBuiltin::Emacs => return (code <= MAX_5_BYTE_CHAR).then_some(code),
            CharsetBuiltin::EightBit => {
                return (0x80..=0xff)
                    .contains(&code)
                    .then_some(RAW_BYTE8_BASE + code);
            }
            CharsetBuiltin::None => {}
        }
        if let Some(map) = self.map(interp)
            && let Some(character) = map.decode(code)
        {
            return Some(character);
        }
        if let Some(children) = &self.superset {
            return children.iter().find_map(|(child, offset)| {
                let child_code = u32::try_from(i64::from(code).checked_sub(*offset)?).ok()?;
                charset_info(interp, child)?.decode(interp, child_code)
            });
        }
        if let Some((parent, min, max, offset)) = &self.subset {
            let parent_code = i64::from(code).checked_sub(*offset)?;
            if !(*min..=*max).contains(&parent_code) {
                return None;
            }
            return charset_info(interp, parent)?.decode(interp, u32::try_from(parent_code).ok()?);
        }
        let offset = self.code_offset?;
        // DECODE_CHAR: a code outside `:min-code'..`:max-code' or the code
        // space is no character of the charset.
        let index = self.code_to_index(code)?;
        u32::try_from(i64::from(index).checked_add(offset)?).ok()
    }

    /// ENCODE_CHAR for an actual Emacs character number: its code in this
    /// charset, None when it is no character of it.
    pub(crate) fn encode(&self, interp: &Interpreter, character: u32) -> Option<u32> {
        match self.builtin {
            CharsetBuiltin::Ascii => return (character <= 0x7f).then_some(character),
            CharsetBuiltin::Iso8859_1 => return (character <= 0xff).then_some(character),
            CharsetBuiltin::Unicode => {
                return (character <= MAX_UNICODE_CHAR).then_some(character);
            }
            CharsetBuiltin::Emacs => return (character <= MAX_5_BYTE_CHAR).then_some(character),
            CharsetBuiltin::EightBit => {
                return (RAW_BYTE8_BASE + 0x80..=RAW_BYTE8_BASE + 0xff)
                    .contains(&character)
                    .then(|| character - RAW_BYTE8_BASE);
            }
            CharsetBuiltin::None => {}
        }
        if character > MAX_5_BYTE_CHAR {
            // A raw byte belongs to no map, superset or offset charset.
            return None;
        }
        if let Some(map) = self.map(interp)
            && let Some(code) = map.encode(character)
        {
            return Some(code);
        }
        if let Some(children) = &self.superset {
            return children.iter().find_map(|(child, offset)| {
                let child_code = charset_info(interp, child)?.encode(interp, character)?;
                u32::try_from(i64::from(child_code).checked_add(*offset)?).ok()
            });
        }
        if let Some((parent, min, max, offset)) = &self.subset {
            let parent_code = i64::from(charset_info(interp, parent)?.encode(interp, character)?);
            if !(*min..=*max).contains(&parent_code) {
                return None;
            }
            return u32::try_from(parent_code.checked_add(*offset)?).ok();
        }
        let offset = self.code_offset?;
        let index = u32::try_from(i64::from(character).checked_sub(offset)?).ok()?;
        // charset.c ENCODE_CHAR for the offset method: the index must lie
        // within the code space (latin-iso8859-1 spans 96 codes; a
        // character past them is not its member).
        self.index_to_code(index)
    }
}

/// The charset CHARSET (a name or an alias) as the charset table holds
/// it, read from its plist the first time.
pub(crate) fn charset_info(interp: &Interpreter, charset: &str) -> Option<Rc<CharsetInfo>> {
    if let Some(info) = interp.charset_table.borrow().get(charset) {
        return Some(info.clone());
    }
    let canonical = interp.charset_canonical_name(charset)?;
    let info = Rc::new(CharsetInfo::read(interp, &canonical));
    interp
        .charset_table
        .borrow_mut()
        .insert(charset.to_string(), info.clone());
    Some(info)
}

/// The pairs of the map file NAME (etc/charsets/NAME.map) over the code
/// space BOUNDS, parsed once a process.
fn load_charset_map_file(
    interp: &Interpreter,
    name: &str,
    bounds: &[(u32, u32)],
) -> Option<Vec<(u32, u32)>> {
    let data_directory = interp
        .lookup_var("data-directory", &crate::lisp::types::Env::new())
        .and_then(|value| string_like(&value).map(|string| PathBuf::from(string.text)))
        .or_else(|| compat_data_directory().map(PathBuf::from))?;
    let path = data_directory.join("charsets").join(format!("{name}.map"));
    type CharsetMapCache = HashMap<(PathBuf, Vec<(u32, u32)>), Vec<(u32, u32)>>;
    static CACHE: OnceLock<Mutex<CharsetMapCache>> = OnceLock::new();
    let cache = CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    let key = (path, bounds.to_vec());
    if let Some(map) = cache.lock().ok()?.get(&key).cloned() {
        return Some(map);
    }
    let parsed = parse_charset_map(&fs::read(&key.0).ok()?, &key.1);
    cache.lock().ok()?.insert(key, parsed.clone());
    Some(parsed)
}
