//! charset.c's charset table: each charset's attributes as `struct
//! charset' keeps them, read once from the `define-charset-internal'
//! arguments the charset was defined with (apart from its plist, which
//! `set-charset-plist' replaces without touching the charset), and a map
//! charset's decoder and encoder, which load_charset_map builds once, made
//! when it first decodes or encodes.  The interpreter keeps the table;
//! defining a charset or an alias, or (un)unifying one, empties it.

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

/// The slots of a charset's definition: `define-charset-internal''s
/// arguments from charset_arg_dimension to charset_arg_unify_map, as
/// Fdefine_charset_internal checked them (the dimension computed when not
/// given, the minimum and maximum codes as integers, a subset's parent and
/// a superset's members by canonical name).
pub(crate) const DEF_DIMENSION: usize = 0;
pub(crate) const DEF_CODE_SPACE: usize = 1;
pub(crate) const DEF_MIN_CODE: usize = 2;
pub(crate) const DEF_MAX_CODE: usize = 3;
pub(crate) const DEF_ISO_FINAL: usize = 4;
pub(crate) const DEF_ISO_REVISION: usize = 5;
pub(crate) const DEF_EMACS_MULE_ID: usize = 6;
pub(crate) const DEF_ASCII_COMPATIBLE_P: usize = 7;
pub(crate) const DEF_SUPPLEMENTARY_P: usize = 8;
pub(crate) const DEF_INVALID_CODE: usize = 9;
pub(crate) const DEF_CODE_OFFSET: usize = 10;
pub(crate) const DEF_MAP: usize = 11;
pub(crate) const DEF_SUBSET: usize = 12;
pub(crate) const DEF_SUPERSET: usize = 13;
pub(crate) const DEF_UNIFY_MAP: usize = 14;
/// CHARSET_ATTR_DEUNIFIER: t once load_charset has made the deunifier
/// of a unified charset.
pub(crate) const DEF_DEUNIFIER: usize = 15;
pub(crate) const DEF_SLOTS: usize = 16;

/// Where a map charset's mapping comes from: a vector of codes (or code
/// ranges) and characters, or a map file on `charset-map-path'.
#[derive(Clone, Debug)]
enum MapSource {
    Pairs(Vec<(u32, u32, u32)>),
    File(String),
}

/// A charset's mapping (load_charset_map): its pairs in the map's order,
/// the character of each code, which the decoder vector takes from the
/// last pair naming the code, and the code of each character, which the
/// encoder char-table takes from the first pair naming the character.
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
            decoder.insert(code, character);
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
    /// CHARSET_ASCII_COMPATIBLE_P: `:ascii-compatible-p', or an offset
    /// charset from 0 reaching past ASCII.
    pub(crate) ascii_compatible_p: Option<bool>,
    /// CHARSET_EMACS_MULE_ID: `:emacs-mule-id', when given.
    pub(crate) emacs_mule_id: Option<u8>,
    /// CHARSET_ISO_FINAL and CHARSET_ISO_REVISION, when given.
    pub(crate) iso_final: Option<u8>,
    pub(crate) iso_revision: Option<u8>,
    /// CHARSET_INVALID_CODE.
    pub(crate) invalid_code: Option<u32>,
    map: OnceCell<Option<Rc<CharsetMap>>>,
}

/// The elements of a vector, or of a list.
fn vector_items(value: &Value) -> Option<Vec<Value>> {
    crate::lisp::primitives::vector_or_list_items(value).ok()
}

impl CharsetInfo {
    /// The attributes of the charset CANONICAL as its definition gives
    /// them: struct charset's method is the first of a code offset, a map,
    /// a subset and a superset the definition names.
    fn read(interp: &Interpreter, canonical: &str) -> Self {
        let def = interp
            .charset_def_value(canonical)
            .and_then(|def| vector_items(&def))
            .unwrap_or_default();
        let slot = |index: usize| def.get(index).copied().filter(|value| !value.is_nil());
        let builtin = match canonical {
            "ascii" => CharsetBuiltin::Ascii,
            "iso-8859-1" => CharsetBuiltin::Iso8859_1,
            "unicode" => CharsetBuiltin::Unicode,
            "emacs" => CharsetBuiltin::Emacs,
            "eight-bit" => CharsetBuiltin::EightBit,
            _ => CharsetBuiltin::None,
        };

        // charset.code_space: (min, max) of each of the four bytes.
        let code_space = slot(DEF_CODE_SPACE).and_then(|space| {
            let values = vector_items(&space)?;
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
        });
        let dimension = slot(DEF_DIMENSION)
            .and_then(|value| value.integer_value())
            .and_then(|value| u32::try_from(value).ok())
            .or_else(|| code_space.as_ref().map(|bounds| bounds.len() as u32))
            .unwrap_or(1);

        // CHARSET_MIN_CODE / CHARSET_MAX_CODE: the definition's codes,
        // defaulting to the corners of the code space.
        let code_bounds = code_space.as_ref().map(|bounds| {
            let mut min_code = 0u32;
            let mut max_code = 0u32;
            for (dimension, (min, max)) in bounds.iter().enumerate() {
                min_code |= min << (8 * dimension);
                max_code |= max << (8 * dimension);
            }
            let code =
                |index: usize| -> Option<u32> { u32::try_from(slot(index)?.integer_value()?).ok() };
            (
                code(DEF_MIN_CODE).unwrap_or(min_code),
                code(DEF_MAX_CODE).unwrap_or(max_code),
            )
        });
        let char_index_offset = match (&code_space, code_bounds) {
            (Some(bounds), Some((min_code, _))) => code_point_to_raw_index(bounds, min_code),
            _ => None,
        };

        let code_offset = slot(DEF_CODE_OFFSET).and_then(|value| value.integer_value());
        let direct_map = slot(DEF_MAP).filter(|_| code_offset.is_none());
        let unified = interp.charset_is_unified(canonical);
        // Unified charsets (charset.c's UNIFIED_P, set by `unify-charset')
        // convert through their unify-map; only codes absent from the
        // table fall back to the code-offset rule.
        let map_value = direct_map.or_else(|| {
            (unified && code_offset.is_some())
                .then(|| slot(DEF_UNIFY_MAP))
                .flatten()
        });
        let map_source = map_value.and_then(|map| {
            if is_vector_value(&map) {
                let values = vector_items(&map)?;
                Some(MapSource::Pairs(
                    values
                        .as_chunks::<2>()
                        .0
                        .iter()
                        .filter_map(|pair| {
                            // load_charset_map_from_vector: a code or a
                            // (FROM . TO) range, then its first character.
                            let (from, to) = match pair[0].cons_values() {
                                Some((from, to)) => (from, to),
                                None => (pair[0], pair[0]),
                            };
                            Some((
                                u32::try_from(from.integer_value()?).ok()?,
                                u32::try_from(to.integer_value()?).ok()?,
                                u32::try_from(pair[1].integer_value()?).ok()?,
                            ))
                        })
                        .collect(),
                ))
            } else {
                string_text(&map).ok().map(MapSource::File)
            }
        });

        let other_method = code_offset.is_none() && direct_map.is_none();
        // CHARSET_METHOD_SUBSET: (PARENT MIN MAX OFFSET).
        let subset = slot(DEF_SUBSET)
            .filter(|_| other_method)
            .and_then(|subset| {
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
        // CHARSET_METHOD_SUPERSET: (MEMBER . OFFSET) in order.
        let superset = slot(DEF_SUPERSET)
            .filter(|_| other_method && subset.is_none())
            .and_then(|superset| {
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
        let mut ascii_compatible_p = slot(DEF_ASCII_COMPATIBLE_P).is_some();
        if code_offset == Some(0)
            && let (Some(bounds), Some((_, max_code))) = (&code_space, code_bounds)
            && code_point_to_raw_index(bounds, max_code)
                .and_then(|index| index.checked_sub(char_index_offset?))
                .is_some_and(|index| index >= 0x80)
        {
            // Fdefine_charset_internal: an offset charset from 0 whose
            // characters reach past ASCII is ASCII-compatible.
            ascii_compatible_p = true;
        }
        let emacs_mule_id = slot(DEF_EMACS_MULE_ID)
            .and_then(|value| value.integer_value())
            .and_then(|id| u8::try_from(id).ok());
        let iso_final = slot(DEF_ISO_FINAL)
            .and_then(|value| value.integer_value())
            .and_then(|value| u8::try_from(value).ok());
        let iso_revision = slot(DEF_ISO_REVISION)
            .and_then(|value| value.integer_value())
            .and_then(|value| u8::try_from(value).ok());
        let invalid_code = slot(DEF_INVALID_CODE)
            .and_then(|value| value.integer_value())
            .and_then(|value| u32::try_from(value).ok());

        CharsetInfo {
            name: canonical.to_string(),
            builtin,
            dimension,
            code_space,
            code_bounds,
            char_index_offset,
            code_offset: code_offset.filter(|_| direct_map.is_none()),
            has_direct_map: direct_map.is_some(),
            unified,
            map_source,
            subset,
            superset,
            ascii_compatible_p: Some(ascii_compatible_p),
            emacs_mule_id,
            iso_final,
            iso_revision,
            invalid_code,
            map: OnceCell::new(),
        }
    }

    /// The charset's mapping, read the first time it is asked for: a map
    /// file is parsed against the code space (charset.c load_charset_map).
    pub(crate) fn map(&self, interp: &Interpreter) -> Option<Rc<CharsetMap>> {
        self.map
            .get_or_init(|| {
                let bounds = self.code_space.as_deref()?;
                let (min_code, max_code) = self.code_bounds?;
                let entries = match self.map_source.as_ref()? {
                    MapSource::Pairs(ranges) => {
                        let mut pairs = Vec::new();
                        for &(from, to, character) in ranges {
                            // load_charset_map_from_vector's filter.
                            if from < min_code || to > max_code || from > to || character > MAX_CHAR
                            {
                                continue;
                            }
                            let (Some(from_index), Some(to_index)) = (
                                code_point_to_raw_index(bounds, from),
                                code_point_to_raw_index(bounds, to),
                            ) else {
                                continue;
                            };
                            for (step, index) in (from_index..=to_index).enumerate() {
                                if let Some(code) = raw_index_to_code_point(bounds, index) {
                                    pairs.push((code, character + step as u32));
                                }
                            }
                        }
                        pairs
                    }
                    MapSource::File(name) => load_charset_map_file(interp, name, bounds)?
                        .into_iter()
                        .filter(|(code, _)| (min_code..=max_code).contains(code))
                        .collect(),
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

    /// charset.c CODE_POINT_TO_INDEX as map_charset_chars applies it to
    /// any code: a linear code space's index is the difference from the
    /// minimum code (taken as the C's int), another's -1 for a code outside
    /// it.
    pub(crate) fn code_point_to_index_c(&self, code: u32) -> Option<i64> {
        let bounds = self.code_space.as_deref()?;
        let (min_code, _) = self.code_bounds?;
        // code_linear_p: one dimension, or each byte below the charset's
        // highest spanning all 256 values.
        let spans_all = |byte: usize| {
            bounds
                .get(byte)
                .is_some_and(|(min, max)| max - min + 1 == 256)
        };
        let linear = self.dimension == 1
            || (spans_all(0)
                && (self.dimension == 2
                    || (spans_all(1) && (self.dimension == 3 || spans_all(2)))));
        if linear {
            return Some(i64::from(code.wrapping_sub(min_code) as i32));
        }
        Some(match super::code_point_to_raw_index(bounds, code) {
            Some(raw) => i64::from(raw) - i64::from(self.char_index_offset?),
            None => -1,
        })
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
    /// that is none of its characters.  An ASCII code of an
    /// ASCII-compatible charset is that character, mapped or not.
    pub(crate) fn decode(&self, interp: &Interpreter, code: u32) -> Option<u32> {
        if code < 0x80 && self.ascii_compatible_p == Some(true) {
            return Some(code);
        }
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
    /// charset, None when it is no character of it.  An ASCII character of
    /// an ASCII-compatible charset is its own code.
    pub(crate) fn encode(&self, interp: &Interpreter, character: u32) -> Option<u32> {
        if character < 0x80 && self.ascii_compatible_p == Some(true) {
            return Some(character);
        }
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
        if self.unified {
            // encode_char: a unified charset's deunifier is loaded first.
            load_deunifier(interp, &self.name);
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

/// load_charset (CHARSET, 2) for the unified charset NAME: its deunifier
/// goes into its attributes, unless `inhibit-load-charset-map' (which
/// loadup sets while dumping) sends the table to temp_charset_work, so
/// that the dumped charset has none.
pub(crate) fn load_deunifier(interp: &Interpreter, name: &str) {
    let Some(def) = interp.charset_def_value(name) else {
        return;
    };
    let crate::lisp::types::Kind::Vector(slots) = def.kind() else {
        return;
    };
    if slots
        .get(DEF_DEUNIFIER)
        .is_none_or(|deunifier| !deunifier.is_nil())
    {
        return;
    }
    let inhibited = interp
        .lookup_var("inhibit-load-charset-map", &crate::lisp::types::Env::new())
        .is_some_and(|value| !value.is_nil());
    if !inhibited {
        slots.set(DEF_DEUNIFIER, Value::T);
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

/// load_charset_map_from_file's openp: the file NAME names on
/// `charset-map-path', with the suffix .map or .TXT.
pub(crate) fn charset_map_file(interp: &Interpreter, name: &str) -> Option<PathBuf> {
    let env = crate::lisp::types::Env::new();
    let directories: Vec<PathBuf> = interp
        .lookup_var("charset-map-path", &env)
        .and_then(|path| path.to_vec().ok())
        .unwrap_or_default()
        .iter()
        .filter_map(|directory| string_like(directory).map(|string| PathBuf::from(string.text)))
        .collect();
    let candidates = |base: PathBuf| {
        [".map", ".TXT"].map(move |suffix| {
            let mut file = base.clone().into_os_string();
            file.push(suffix);
            PathBuf::from(file)
        })
    };
    if Path::new(name).is_absolute() {
        return candidates(PathBuf::from(name))
            .into_iter()
            .find(|path| path.is_file());
    }
    directories
        .into_iter()
        .flat_map(|directory| candidates(directory.join(name)))
        .find(|path| path.is_file())
}

/// The pairs of the map file NAME (on `charset-map-path') over the code
/// space BOUNDS, parsed once a process.
fn load_charset_map_file(
    interp: &Interpreter,
    name: &str,
    bounds: &[(u32, u32)],
) -> Option<Vec<(u32, u32)>> {
    let path = charset_map_file(interp, name).or_else(|| {
        let data_directory = interp
            .lookup_var("data-directory", &crate::lisp::types::Env::new())
            .and_then(|value| string_like(&value).map(|string| PathBuf::from(string.text)))
            .or_else(|| compat_data_directory().map(PathBuf::from))?;
        Some(data_directory.join("charsets").join(format!("{name}.map")))
    })?;
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
