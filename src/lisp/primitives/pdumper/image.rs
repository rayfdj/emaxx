//! The image file's fixed layout: pdumper.c's `dump_header', table
//! locators, relocation records and object-start records, with the word
//! encodings the records use.
//!
//! GNU's dump is a memory image of C structs whose relocations point into
//! the emacs binary; an Emaxx image is a record per Rust object with the
//! same section structure (header, hot, discardable, cold), the same
//! table kinds (dump relocations per phase, object starts, Emacs
//! relocations) and the same validation on load.  The two are not
//! byte-compatible and the fingerprint keeps each loader to its own
//! binary, exactly as GNU's does across builds.

use std::sync::OnceLock;

/// pdumper.c:dump_magic.  The first byte is `!' while a dump is being
/// written; a file that keeps it was never completed.
pub(crate) const DUMP_MAGIC: [u8; 16] = *b"DUMPEDGNUEMACS\0\0";
pub(crate) const INCOMPLETE_MAGIC_BYTE: u8 = b'!';
pub(crate) const FINGERPRINT_LEN: usize = 32;
/// max (GCALIGNMENT, DUMP_RELOCATION_ALIGNMENT).
pub(crate) const DUMP_ALIGNMENT: usize = 8;
/// `dump_off' is `int_least32_t'.
pub(crate) const DUMP_OFF_MAX: usize = i32::MAX as usize;
pub(crate) const HEADER_LEN: usize = 100;
pub(crate) const RELOC_NUM_PHASES: usize = 3;
pub(crate) const EARLY_RELOCS: usize = 0;

/// The fingerprint "unique to each build of Emacs": the SHA-256 of this
/// executable, computed once per process.  `pdumper-fingerprint' prints
/// the same bytes as hex.
pub(crate) fn executable_fingerprint() -> &'static [u8; FINGERPRINT_LEN] {
    static FINGERPRINT: OnceLock<[u8; FINGERPRINT_LEN]> = OnceLock::new();
    FINGERPRINT.get_or_init(|| {
        use sha2::Digest;
        let bytes = std::env::current_exe()
            .ok()
            .and_then(|path| std::fs::read(path).ok())
            .unwrap_or_default();
        let mut hasher = sha2::Sha256::new();
        hasher.update(&bytes);
        let digest = hasher.finalize();
        let mut out = [0_u8; FINGERPRINT_LEN];
        out.copy_from_slice(&digest);
        out
    })
}

/// dump_fingerprint's rendering: lowercase hex.
pub(crate) fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// pdumper.c:dump_table_locator.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct TableLocator {
    pub(crate) offset: u32,
    pub(crate) nr_entries: u32,
}

/// pdumper.c:dump_header, 100 bytes in the same field order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DumpHeader {
    pub(crate) magic: [u8; 16],
    pub(crate) fingerprint: [u8; FINGERPRINT_LEN],
    pub(crate) dump_relocs: [TableLocator; RELOC_NUM_PHASES],
    pub(crate) object_starts: TableLocator,
    pub(crate) emacs_relocs: TableLocator,
    pub(crate) discardable_start: u32,
    pub(crate) cold_start: u32,
    pub(crate) hash_list: u32,
}

impl DumpHeader {
    pub(crate) fn incomplete() -> Self {
        let mut magic = DUMP_MAGIC;
        magic[0] = INCOMPLETE_MAGIC_BYTE;
        Self {
            magic,
            fingerprint: *executable_fingerprint(),
            dump_relocs: [TableLocator::default(); RELOC_NUM_PHASES],
            object_starts: TableLocator::default(),
            emacs_relocs: TableLocator::default(),
            discardable_start: 0,
            cold_start: 0,
            hash_list: 0,
        }
    }

    pub(crate) fn to_bytes(&self) -> [u8; HEADER_LEN] {
        let mut out = [0_u8; HEADER_LEN];
        out[..16].copy_from_slice(&self.magic);
        out[16..48].copy_from_slice(&self.fingerprint);
        let mut at = 48;
        let mut put = |value: u32| {
            out[at..at + 4].copy_from_slice(&value.to_le_bytes());
            at += 4;
        };
        for locator in &self.dump_relocs {
            put(locator.offset);
            put(locator.nr_entries);
        }
        put(self.object_starts.offset);
        put(self.object_starts.nr_entries);
        put(self.emacs_relocs.offset);
        put(self.emacs_relocs.nr_entries);
        put(self.discardable_start);
        put(self.cold_start);
        put(self.hash_list);
        out
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < HEADER_LEN {
            return None;
        }
        let mut magic = [0_u8; 16];
        magic.copy_from_slice(&bytes[..16]);
        let mut fingerprint = [0_u8; FINGERPRINT_LEN];
        fingerprint.copy_from_slice(&bytes[16..48]);
        let mut at = 48;
        let mut get = || {
            let value = u32::from_le_bytes(bytes[at..at + 4].try_into().expect("four bytes"));
            at += 4;
            value
        };
        let mut dump_relocs = [TableLocator::default(); RELOC_NUM_PHASES];
        for locator in &mut dump_relocs {
            locator.offset = get();
            locator.nr_entries = get();
        }
        let object_starts = TableLocator {
            offset: get(),
            nr_entries: get(),
        };
        let emacs_relocs = TableLocator {
            offset: get(),
            nr_entries: get(),
        };
        Some(Self {
            magic,
            fingerprint,
            dump_relocs,
            object_starts,
            emacs_relocs,
            discardable_start: get(),
            cold_start: get(),
            hash_list: get(),
        })
    }
}

/// The object classes an image records, the counterpart of the
/// `Lisp_Type' a GNU object-start entry carries.  Emaxx's heap has more
/// classes than GNU's five tags, so the entry carries one of these.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u32)]
pub(crate) enum DumpType {
    Cons = 1,
    /// An immutable host string (`Value::String').
    String = 2,
    /// A mutable string object (`Value::StringObject').
    StringObject = 3,
    Symbol = 4,
    Vector = 5,
    Float = 6,
    Bignum = 7,
    /// A built-in function, addressed by name (a copied object in GNU).
    Subr = 8,
    Obarray = 9,
    /// A string's text-property spans (GNU: the interval tree).
    TextProperties = 10,
    /// An interpreted closure (`Value::Lambda').
    Closure = 11,
    /// A closure's parameter vector, shared between closures made from
    /// one lambda form.
    LambdaParams = 12,
    /// A closure's body forms, shared likewise.
    LambdaBody = 13,
    /// A closure's captured lexical environment (`SharedEnv').
    LexicalEnvironment = 14,
    /// One frame of a lexical environment (`EnvFrame').
    LexicalFrame = 15,
    CharTable = 16,
    /// A record or pseudovector kept as its slots (`Value::Record').
    Record = 17,
    /// A bool-vector: bits in the cold section.
    BoolVector = 18,
    /// The value cells of `nil' and `t', whose references are
    /// self-representing words.
    BuiltinSymbolCells = 19,
    /// The main thread: an object of the running process (copied record).
    MainThread = 20,
    /// A hash table, frozen: its record, count, weakness, test and
    /// mutability, then the compact key/value contents.
    HashTable = 21,
    /// A buffer: `dump_buffer's copy of the struct, its text in the
    /// cold section.
    Buffer = 22,
    Marker = 23,
    /// An overlay (only a deleted one can be written: a live one's
    /// buffer refuses).
    Overlay = 24,
    Finalizer = 25,
    /// A frame, nilled as `dump_nilled_pseudovec' writes it.
    Frame = 26,
    /// A terminal, nilled likewise.
    Terminal = 27,
}

impl DumpType {
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn from_u32(value: u32) -> Option<Self> {
        Some(match value {
            1 => Self::Cons,
            2 => Self::String,
            3 => Self::StringObject,
            4 => Self::Symbol,
            5 => Self::Vector,
            6 => Self::Float,
            7 => Self::Bignum,
            8 => Self::Subr,
            9 => Self::Obarray,
            10 => Self::TextProperties,
            11 => Self::Closure,
            12 => Self::LambdaParams,
            13 => Self::LambdaBody,
            14 => Self::LexicalEnvironment,
            15 => Self::LexicalFrame,
            16 => Self::CharTable,
            17 => Self::Record,
            18 => Self::BoolVector,
            19 => Self::BuiltinSymbolCells,
            20 => Self::MainThread,
            21 => Self::HashTable,
            22 => Self::Buffer,
            23 => Self::Marker,
            24 => Self::Overlay,
            25 => Self::Finalizer,
            26 => Self::Frame,
            27 => Self::Terminal,
            _ => return None,
        })
    }
}

/// pdumper.c:dump_reloc_type.  A relocation names a word in the dump and
/// how the loader must rewrite it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DumpRelocKind {
    /// dump_ptr = dump_ptr + dump_base: a raw offset into the dump.
    DumpToDumpPtrRaw,
    /// Rebuild a bignum from its cold limbs.
    Bignum,
    /// A Lisp word that names an object in the dump, by dump offset.
    DumpToDumpLv(DumpType),
    /// A Lisp word that names an object of the Emacs image (a built-in
    /// function), through its copied record.
    DumpToEmacsLv(DumpType),
}

impl DumpRelocKind {
    pub(crate) fn to_u32(self) -> u32 {
        match self {
            Self::DumpToDumpPtrRaw => 1,
            Self::Bignum => 4,
            Self::DumpToDumpLv(kind) => 0x100 | kind as u32,
            Self::DumpToEmacsLv(kind) => 0x200 | kind as u32,
        }
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn from_u32(value: u32) -> Option<Self> {
        Some(match value {
            1 => Self::DumpToDumpPtrRaw,
            4 => Self::Bignum,
            0x100..=0x1FF => Self::DumpToDumpLv(DumpType::from_u32(value & 0xFF)?),
            0x200..=0x2FF => Self::DumpToEmacsLv(DumpType::from_u32(value & 0xFF)?),
            _ => return None,
        })
    }
}

/// One entry of a dump-relocation table or of the object-start table:
/// the dump offset and either the relocation kind or the object type.
/// GNU packs both into one 32-bit word; the image keeps two.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) const TABLE_ENTRY_LEN: usize = 8;

/// pdumper.c:emacs_reloc_type: how a slot of the running process (an
/// Emacs C variable there, a root slot of the interpreter here) is set
/// from the image.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum EmacsRelocKind {
    /// The slot holds the immediate word.
    Immediate,
    /// The slot holds the object at the dump offset.
    DumpLv(DumpType),
    /// The slot holds an object of the Emacs image (through its copied
    /// record at the dump offset).
    EmacsLv(DumpType),
}

impl EmacsRelocKind {
    pub(crate) fn to_u32(self) -> u32 {
        match self {
            Self::Immediate => 1,
            Self::DumpLv(kind) => 0x100 | kind as u32,
            Self::EmacsLv(kind) => 0x200 | kind as u32,
        }
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn from_u32(value: u32) -> Option<Self> {
        Some(match value {
            1 => Self::Immediate,
            0x100..=0x1FF => Self::DumpLv(DumpType::from_u32(value & 0xFF)?),
            0x200..=0x2FF => Self::EmacsLv(DumpType::from_u32(value & 0xFF)?),
            _ => return None,
        })
    }
}

/// An Emacs relocation on disk: kind, root slot, payload (16 bytes, as
/// GNU's `struct emacs_reloc').
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) const EMACS_RELOC_LEN: usize = 16;

/// The slots of the running process the image sets: pdumper.c's
/// `staticpro' roots and the built-in symbol list, which here are the
/// interpreter fields alloc.c's mark phase starts from.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u32)]
pub(crate) enum RootSlot {
    /// The initial obarray (Vobarray).
    Obarray = 1,
    QuitFlag = 2,
    InhibitQuit = 3,
    ThrowOnInput = 4,
    OverridingPlistEnvironment = 5,
    LoadPath = 6,
    LoadsInProgress = 7,
    LocalTimeZoneRule = 8,
    FrameAndBufferState = 9,
    CurrentGlobalMap = 10,
    /// The value cells of the built-in symbol `nil' (GNU: the copied
    /// lispsym entry).
    NilCells = 11,
    TCells = 12,
    /// alloc.c's `finalizers' list head: its `prev' (the last finalizer)
    /// and `next' (the first) pointers, as dump_finalizer_list_head_ptr
    /// writes them.
    FinalizersPrev = 13,
    FinalizersNext = 14,
    /// `doomed_finalizers' likewise.
    DoomedFinalizersPrev = 15,
    DoomedFinalizersNext = 16,
}

impl RootSlot {
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn from_u32(value: u32) -> Option<Self> {
        Some(match value {
            1 => Self::Obarray,
            2 => Self::QuitFlag,
            3 => Self::InhibitQuit,
            4 => Self::ThrowOnInput,
            5 => Self::OverridingPlistEnvironment,
            6 => Self::LoadPath,
            7 => Self::LoadsInProgress,
            8 => Self::LocalTimeZoneRule,
            9 => Self::FrameAndBufferState,
            10 => Self::CurrentGlobalMap,
            11 => Self::NilCells,
            12 => Self::TCells,
            13 => Self::FinalizersPrev,
            14 => Self::FinalizersNext,
            15 => Self::DoomedFinalizersPrev,
            16 => Self::DoomedFinalizersNext,
            _ => return None,
        })
    }
}

// Self-representing words.  lisp.h: a fixnum is its value shifted past
// the two tag bits with Lisp_Int0 (2) in them; Qnil is word 0; the other
// built-in symbols are offsets into `lispsym'.  Emaxx represents three
// symbols specially (nil, t and the unbound marker), and those three are
// the image's self-representing symbol words.
pub(crate) const WORD_NIL: u64 = 0;
pub(crate) const WORD_UNBOUND: u64 = 8;
pub(crate) const WORD_T: u64 = 16;
const FIXNUM_TAG: u64 = 2;
/// lisp.h: MOST_POSITIVE_FIXNUM is 2^61 - 1 on this configuration.
pub(crate) const MOST_POSITIVE_FIXNUM: i64 = (1_i64 << 61) - 1;
pub(crate) const MOST_NEGATIVE_FIXNUM: i64 = -(1_i64 << 61);

pub(crate) fn fixnum_word(value: i64) -> Option<u64> {
    (MOST_NEGATIVE_FIXNUM..=MOST_POSITIVE_FIXNUM)
        .contains(&value)
        .then_some(((value as u64) << 2) | FIXNUM_TAG)
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn word_fixnum(word: u64) -> Option<i64> {
    (word & 3 == FIXNUM_TAG).then_some((word as i64) >> 2)
}

/// A placeholder for a word a fixup fills in later (pdumper.c writes
/// 0xDEADF00D there until dump_do_fixups).
pub(crate) const FIXUP_PLACEHOLDER: u64 = 0xDEAD_F00D;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_round_trips_through_its_hundred_bytes() {
        let mut header = DumpHeader::incomplete();
        header.dump_relocs[EARLY_RELOCS] = TableLocator {
            offset: 1000,
            nr_entries: 7,
        };
        header.object_starts = TableLocator {
            offset: 2000,
            nr_entries: 9,
        };
        header.discardable_start = 500;
        header.cold_start = 4096;
        let bytes = header.to_bytes();
        assert_eq!(bytes.len(), HEADER_LEN);
        assert_eq!(&bytes[..16], b"!UMPEDGNUEMACS\0\0");
        assert_eq!(DumpHeader::from_bytes(&bytes), Some(header));
        assert!(DumpHeader::from_bytes(&bytes[..HEADER_LEN - 1]).is_none());
    }

    #[test]
    fn fixnum_words_follow_lisp_h_tagging() {
        assert_eq!(fixnum_word(0), Some(2));
        assert_eq!(fixnum_word(1), Some(6));
        assert_eq!(fixnum_word(-1), Some(!1));
        assert_eq!(
            word_fixnum(fixnum_word(-1).expect("-1 is a fixnum")),
            Some(-1)
        );
        assert_eq!(fixnum_word(MOST_POSITIVE_FIXNUM + 1), None);
        assert_eq!(fixnum_word(MOST_NEGATIVE_FIXNUM - 1), None);
        assert_eq!(
            word_fixnum(fixnum_word(MOST_NEGATIVE_FIXNUM).expect("the bound is a fixnum")),
            Some(MOST_NEGATIVE_FIXNUM)
        );
        assert_eq!(word_fixnum(WORD_NIL), None);
        assert_eq!(word_fixnum(WORD_T), None);
    }

    #[test]
    fn relocation_kinds_round_trip() {
        for kind in [
            DumpRelocKind::DumpToDumpPtrRaw,
            DumpRelocKind::Bignum,
            DumpRelocKind::DumpToDumpLv(DumpType::Cons),
            DumpRelocKind::DumpToEmacsLv(DumpType::Subr),
        ] {
            assert_eq!(DumpRelocKind::from_u32(kind.to_u32()), Some(kind));
        }
        assert_eq!(DumpRelocKind::from_u32(0x1FF), None);
        for kind in [
            EmacsRelocKind::Immediate,
            EmacsRelocKind::DumpLv(DumpType::Obarray),
            EmacsRelocKind::EmacsLv(DumpType::Subr),
        ] {
            assert_eq!(EmacsRelocKind::from_u32(kind.to_u32()), Some(kind));
        }
    }
}
