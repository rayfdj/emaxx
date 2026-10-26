//! lread.c's obarray, GNU 30's `struct Lisp_Obarray': 2^size_bits buckets,
//! each empty or the first of the symbols whose names hash to it, the rest
//! chained through each symbol's `next' (Lisp_Symbol.u.s.next), newest
//! first.  A lookup compares each chained symbol's current name, its bytes
//! and its characters, as oblookup does: a symbol whose name string is
//! changed is found by neither name until a rehash files it anew.
//!
//! The buckets are the initial obarray's own (`StandardBuckets') or a
//! private obarray record's slots (`RecordBuckets'); lread.c's algorithms
//! below serve both.

use super::{Kind, SymbolName, Value};

/// lread.c's obarray_default_bits.
pub(crate) const DEFAULT_BITS: u32 = 3;
/// lread.c's obarray_max_bits on a 64-bit host: min (32, 64 - 6) - 1.
pub(crate) const MAX_BITS: u32 = 31;
/// init_obarray_once: make_obarray (15).
pub(crate) const INITIAL_BITS: u32 = 15;

/// lisp.h's reduce_emacs_uint_to_hash_hash.
#[inline(always)]
fn reduce_to_hash_hash(x: u64) -> u32 {
    (x ^ (x >> 32)) as u32
}

/// lisp.h's knuth_hash: HASH reduced to BITS bits.
#[inline(always)]
fn knuth_hash(hash: u32, bits: u32) -> usize {
    // The 64-bit shift makes BITS = 0 the bucket 0.
    (u64::from(hash.wrapping_mul(2_654_435_769)) >> (32 - bits)) as usize
}

/// obarray_index: the bucket of a name of BYTES among 2^BITS.
#[inline]
pub(crate) fn bucket_index(bytes: &[u8], bits: u32) -> usize {
    knuth_hash(
        reduce_to_hash_hash(super::symbols::hash_string(bytes)),
        bits,
    )
}

/// The bytes and characters of a Lisp string NAME (SDATA, SCHARS), or
/// None for a name that is no string.
#[inline(always)]
fn with_name_key<R>(name: Value, body: impl FnOnce(&[u8], usize) -> R) -> Option<R> {
    let Kind::StringObject(name) = name.kind() else {
        return None;
    };
    // SAFETY: the comparison runs no Lisp and stores into no string.
    let (bytes, chars) = unsafe { name.bytes_and_chars_unguarded() };
    Some(body(bytes, chars))
}

/// oblookup's comparison: SYMBOL's name has NCHARS characters in BYTES.
#[inline(always)]
fn name_is(symbol: SymbolName, bytes: &[u8], nchars: usize) -> bool {
    with_name_key(symbol.lisp_name(), |name, chars| {
        chars == nchars && same_bytes(name, bytes)
    })
    .unwrap_or(false)
}

/// The bucket of SYMBOL's current name among 2^BITS.
pub(crate) fn name_bucket(symbol: SymbolName, bits: u32) -> usize {
    with_name_key(symbol.lisp_name(), |name, _| bucket_index(name, bits))
        .unwrap_or_else(|| bucket_index(b"", bits))
}

/// Whether every byte of BYTES is printable ASCII or DEL (0x20 to 0x7f): a
/// name of one byte a character that carries neither key marker, which
/// nearly every symbol's is.  Eight bytes a step: a byte at or above 0x80
/// sets its top bit, and one below 0x20 borrows into its top bit when 0x20
/// is taken from each byte (exact for the question whether any does).
/// A shorter name is read as overlapping words, the rest padded with
/// spaces.
#[inline(always)]
pub(crate) fn plain_ascii(bytes: &[u8]) -> bool {
    use super::symbols::{read_u16, read_u32, read_word};
    const LOW: u64 = 0x2020_2020_2020_2020;
    const HIGH: u64 = 0x8080_8080_8080_8080;
    let bad = |word: u64| (word | (word.wrapping_sub(LOW) & !word)) & HIGH;
    let len = bytes.len();
    if len >= 8 {
        let mut found = bad(read_word(bytes, len - 8));
        let mut at = 0;
        while at + 8 < len {
            found |= bad(read_word(bytes, at));
            at += 8;
        }
        return found == 0;
    }
    let word = if len >= 4 {
        u64::from(read_u32(bytes, 0)) | u64::from(read_u32(bytes, len - 4)) << 32
    } else if len >= 2 {
        u64::from(read_u16(bytes, 0))
            | u64::from(read_u16(bytes, len - 2)) << 16
            | 0x2020_2020_0000_0000
    } else if len == 1 {
        u64::from(bytes[0]) | 0x2020_2020_2020_2000
    } else {
        return true;
    };
    bad(word) == 0
}

/// Whether A and B, of one length, hold the same bytes: a short name's as
/// overlapping words, a longer one's by memcmp.
#[inline(always)]
fn same_bytes(a: &[u8], b: &[u8]) -> bool {
    use super::symbols::{read_u32, read_word};
    let len = a.len();
    if len != b.len() {
        return false;
    }
    match len {
        0 => true,
        1..4 => a[0] == b[0] && a[len / 2] == b[len / 2] && a[len - 1] == b[len - 1],
        4..8 => read_u32(a, 0) == read_u32(b, 0) && read_u32(a, len - 4) == read_u32(b, len - 4),
        8..=16 => {
            read_word(a, 0) == read_word(b, 0) && read_word(a, len - 8) == read_word(b, len - 8)
        }
        _ => a == b,
    }
}

/// The key of host TEXT: its bytes (UTF-8, the internal encoding of every
/// character it can hold) and its characters.
#[inline]
pub(crate) fn text_key(text: &str) -> (&[u8], usize) {
    let bytes = text.as_bytes();
    let chars = if plain_ascii(bytes) || bytes.is_ascii() {
        bytes.len()
    } else {
        text.chars().count()
    };
    (bytes, chars)
}

/// The storage of an obarray's buckets.
pub(crate) trait Buckets {
    fn size_bits(&self) -> u32;
    fn count(&self) -> u32;
    fn set_count(&mut self, count: u32);
    fn head(&self, index: usize) -> Option<SymbolName>;
    fn set_head(&mut self, index: usize, head: Option<SymbolName>);
    /// 2^BITS empty buckets in place of the present ones.
    fn reset(&mut self, bits: u32);

    /// obarray_size.
    fn size(&self) -> usize {
        1 << self.size_bits()
    }

    /// oblookup: the symbol whose name is NCHARS characters in BYTES, or
    /// the bucket where one would be.
    #[inline]
    fn lookup(&self, bytes: &[u8], nchars: usize) -> Result<SymbolName, usize> {
        let index = bucket_index(bytes, self.size_bits());
        let mut next = self.head(index);
        while let Some(symbol) = next {
            if name_is(symbol, bytes, nchars) {
                return Ok(symbol);
            }
            next = symbol.cell().next.get();
        }
        Err(index)
    }

    /// oblookup of the name of a Lisp string NAME; None for no string.
    fn lookup_name(&self, name: Value) -> Option<Result<SymbolName, usize>> {
        with_name_key(name, |bytes, chars| self.lookup(bytes, chars))
    }

    /// intern_sym's filing: SYMBOL first in bucket INDEX, and the buckets
    /// doubled once there are more symbols than buckets.
    fn insert(&mut self, symbol: SymbolName, index: usize) {
        symbol.cell().next.set(self.head(index));
        self.set_head(index, Some(symbol));
        let count = self.count() + 1;
        self.set_count(count);
        if count as usize > self.size() {
            self.grow();
        }
    }

    /// grow_obarray: twice the buckets, every symbol filed again by its
    /// current name, in bucket and chain order.
    fn grow(&mut self) {
        let bits = self.size_bits() + 1;
        assert!(bits <= MAX_BITS, "Obarray too big");
        let symbols = self.symbols();
        self.reset(bits);
        for symbol in symbols {
            let index = name_bucket(symbol, bits);
            symbol.cell().next.set(self.head(index));
            self.set_head(index, Some(symbol));
        }
    }

    /// Funintern's unlinking of SYMBOL, which bucket INDEX chains.  The
    /// symbol keeps its `next', as GNU's does.
    fn remove(&mut self, symbol: SymbolName, index: usize) {
        let head = self.head(index).expect("the symbol's bucket holds a chain");
        if head == symbol {
            self.set_head(index, symbol.cell().next.get());
        } else {
            let mut previous = head;
            loop {
                let next = previous
                    .cell()
                    .next
                    .get()
                    .expect("the bucket chains the symbol");
                if next == symbol {
                    previous.cell().next.set(symbol.cell().next.get());
                    break;
                }
                previous = next;
            }
        }
        self.set_count(self.count() - 1);
    }

    /// Fobarray_clear: the default number of empty buckets.  The symbols
    /// keep their interned state, as GNU's do.
    fn clear(&mut self) {
        self.reset(DEFAULT_BITS);
        self.set_count(0);
    }

    /// The first symbol of the first bucket at or after INDEX, with its
    /// bucket: DOOBARRAY's advance to the next nonempty bucket.
    fn first_from(&self, index: usize) -> Option<(usize, SymbolName)> {
        (index..self.size()).find_map(|index| self.head(index).map(|symbol| (index, symbol)))
    }

    /// DOOBARRAY's walk: BODY of each symbol, bucket by bucket, each chain
    /// in order.
    fn for_each(&self, mut body: impl FnMut(SymbolName)) {
        for index in 0..self.size() {
            let mut next = self.head(index);
            while let Some(symbol) = next {
                body(symbol);
                next = symbol.cell().next.get();
            }
        }
    }

    /// The symbols in DOOBARRAY's order.
    fn symbols(&self) -> Vec<SymbolName> {
        let mut symbols = Vec::with_capacity(self.count() as usize);
        self.for_each(|symbol| symbols.push(symbol));
        symbols
    }

    /// Finternal__obarray_buckets: each bucket's symbols, in chain order.
    fn bucket_lists(&self) -> Vec<Vec<SymbolName>> {
        (0..self.size())
            .map(|index| {
                let mut bucket = Vec::new();
                let mut next = self.head(index);
                while let Some(symbol) = next {
                    bucket.push(symbol);
                    next = symbol.cell().next.get();
                }
                bucket
            })
            .collect()
    }
}

/// The initial obarray's buckets.
pub(crate) struct StandardBuckets {
    buckets: Vec<Option<SymbolName>>,
    size_bits: u32,
    count: u32,
}

impl StandardBuckets {
    /// make_obarray (BITS).
    pub(crate) fn new(bits: u32) -> Self {
        Self {
            buckets: vec![None; 1 << bits],
            size_bits: bits,
            count: 0,
        }
    }
}

impl Default for StandardBuckets {
    fn default() -> Self {
        Self::new(INITIAL_BITS)
    }
}

impl Buckets for StandardBuckets {
    #[inline(always)]
    fn size_bits(&self) -> u32 {
        self.size_bits
    }

    #[inline(always)]
    fn count(&self) -> u32 {
        self.count
    }

    fn set_count(&mut self, count: u32) {
        self.count = count;
    }

    #[inline(always)]
    fn head(&self, index: usize) -> Option<SymbolName> {
        self.buckets[index]
    }

    fn set_head(&mut self, index: usize, head: Option<SymbolName>) {
        self.buckets[index] = head;
    }

    fn reset(&mut self, bits: u32) {
        self.buckets = vec![None; 1 << bits];
        self.size_bits = bits;
    }
}

/// A private obarray's buckets in its record's slots: the size bits, the
/// count, then each bucket, its first symbol or the fixnum 0.
pub(crate) struct RecordBuckets<'a>(pub(crate) &'a mut Vec<Value>);

/// The slots of a new private obarray record with 2^BITS buckets.
pub(crate) fn record_slots(bits: u32) -> Vec<Value> {
    let mut slots = Vec::with_capacity(2 + (1 << bits));
    slots.push(Value::Integer(i64::from(bits)));
    slots.push(Value::Integer(0));
    slots.resize(2 + (1 << bits), Value::Integer(0));
    slots
}

/// Whether SLOTS are a private obarray's: the size bits, the count and
/// that many buckets.
pub(crate) fn is_record_slots(slots: &[Value]) -> bool {
    match (
        slots.first().map(|v| v.kind()),
        slots.get(1).map(|v| v.kind()),
    ) {
        (Some(Kind::Integer(bits)), Some(Kind::Integer(count))) => {
            (0..=i64::from(MAX_BITS)).contains(&bits)
                && count >= 0
                && slots.len() == 2 + (1_usize << bits)
        }
        _ => false,
    }
}

/// A bucket word's symbol: a private obarray chains symbols of its own,
/// never the `nil' and `t' of the initial obarray.
fn bucket_symbol(value: &Value) -> Option<SymbolName> {
    match value.kind() {
        Kind::Symbol(symbol) => Some(symbol),
        Kind::Nil => Some(super::symbols::builtin_boolean(false)),
        Kind::T => Some(super::symbols::builtin_boolean(true)),
        _ => None,
    }
}

impl Buckets for RecordBuckets<'_> {
    fn size_bits(&self) -> u32 {
        self.0[0].as_integer().unwrap_or(0) as u32
    }

    fn count(&self) -> u32 {
        self.0[1].as_integer().unwrap_or(0) as u32
    }

    fn set_count(&mut self, count: u32) {
        self.0[1] = Value::Integer(i64::from(count));
    }

    fn head(&self, index: usize) -> Option<SymbolName> {
        bucket_symbol(&self.0[2 + index])
    }

    fn set_head(&mut self, index: usize, head: Option<SymbolName>) {
        self.0[2 + index] = head.map_or(Value::Integer(0), Value::Symbol);
    }

    fn reset(&mut self, bits: u32) {
        let count = self.0[1];
        *self.0 = record_slots(bits);
        self.0[1] = count;
    }
}

/// A private obarray's buckets read in place.
pub(crate) struct RecordBucketsRef<'a>(pub(crate) &'a [Value]);

impl RecordBucketsRef<'_> {
    fn size_bits(&self) -> u32 {
        self.0[0].as_integer().unwrap_or(0) as u32
    }

    /// oblookup of NCHARS characters in BYTES.
    pub(crate) fn lookup(&self, bytes: &[u8], nchars: usize) -> Result<SymbolName, usize> {
        let index = bucket_index(bytes, self.size_bits());
        let mut next = bucket_symbol(&self.0[2 + index]);
        while let Some(symbol) = next {
            if name_is(symbol, bytes, nchars) {
                return Ok(symbol);
            }
            next = symbol.cell().next.get();
        }
        Err(index)
    }

    /// DOOBARRAY's walk.
    pub(crate) fn symbols(&self) -> Vec<SymbolName> {
        let mut symbols = Vec::new();
        for bucket in &self.0[2..] {
            let mut next = bucket_symbol(bucket);
            while let Some(symbol) = next {
                symbols.push(symbol);
                next = symbol.cell().next.get();
            }
        }
        symbols
    }

    /// Finternal__obarray_buckets.
    pub(crate) fn bucket_lists(&self) -> Vec<Vec<SymbolName>> {
        self.0[2..]
            .iter()
            .map(|bucket| {
                let mut list = Vec::new();
                let mut next = bucket_symbol(bucket);
                while let Some(symbol) = next {
                    list.push(symbol);
                    next = symbol.cell().next.get();
                }
                list
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::{bucket_index, knuth_hash, plain_ascii, same_bytes};

    #[test]
    fn same_bytes_compares_every_byte_of_each_length() {
        for len in 0..40 {
            let a: Vec<u8> = (0..len).map(|i| b'a' + (i % 26) as u8).collect();
            assert!(same_bytes(&a, &a.clone()), "{len}");
            for at in 0..len {
                let mut b = a.clone();
                b[at] ^= 0x20;
                assert!(!same_bytes(&a, &b), "{len} at {at}");
            }
        }
    }

    #[test]
    fn plain_ascii_is_every_byte_printable_ascii() {
        for (text, plain) in [
            ("", true),
            ("car", true),
            ("emacs-lisp-mode-hook", true),
            ("~!@#$%^&*()_+{}|:<>?\x7f", true),
            ("foo\u{1e}3", false),
            ("long-name-with-a-marker\u{1f}17", false),
            ("abcdefg\u{1f}", false),
            ("\u{1f}abcdefgh", false),
            ("abcdefgh\u{1}", false),
            ("λ", false),
            ("abcdefghλ", false),
            ("abc d", true),
            ("abc\td", false),
        ] {
            assert_eq!(plain_ascii(text.as_bytes()), plain, "{text:?}");
        }
        for byte in 0_u8..=255 {
            for at in 0..17 {
                let mut bytes = vec![b'a'; 17];
                bytes[at] = byte;
                assert_eq!(
                    plain_ascii(&bytes),
                    (0x20..0x80).contains(&byte),
                    "{byte} at {at}"
                );
            }
        }
    }

    #[test]
    fn bucket_indices_are_gnu_obarray_index() {
        // lread.c's obarray_index in GNU 30.2 for an obarray of 16
        // buckets: `internal--obarray-buckets' of (obarray-make) with "a"
        // to "j" interned in order.
        let mut buckets = vec![Vec::new(); 16];
        for name in ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"] {
            buckets[bucket_index(name.as_bytes(), 4)].insert(0, name);
        }
        let expected: [&[&str]; 16] = [
            &[],
            &["c"],
            &["h"],
            &[],
            &["e"],
            &[],
            &["j"],
            &["b"],
            &["g"],
            &[],
            &[],
            &["d"],
            &["i"],
            &["a"],
            &["f"],
            &[],
        ];
        for (index, names) in expected.iter().enumerate() {
            assert_eq!(buckets[index], *names, "bucket {index}");
        }
        assert_eq!(knuth_hash(u32::MAX, 0), 0);
    }
}
