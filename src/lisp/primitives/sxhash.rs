//! fns.c's hash code computation: `sxhash_obj' and its helpers, the
//! hashes the standard hash table tests file a key under, and the
//! `sxhash-*' functions.

use super::*;
use crate::lisp::alloc::VectorHeader;
use crate::lisp::types::symbols::{hash_string, sxhash_combine};
use crate::lisp::types::{BoolVectorRef, Kind};

/// fns.c:5336 `SXHASH_MAX_DEPTH' and fns.c:5341 `SXHASH_MAX_LEN'.  GNU stops
/// descending at depth 3 and folds in at most seven elements of a list or
/// vector, so hashing a cyclic structure terminates instead of recursing
/// forever: `cl-print' labels a closure `#<bytecode %#x>' by calling
/// `sxhash' on it, and a closure's constants routinely point back at the
/// closure (ert builds exactly that graph while reporting a failure).
const SXHASH_MAX_DEPTH: u32 = 3;
const SXHASH_MAX_LEN: usize = 7;

/// lisp.h's `PSEUDOVECTOR_FLAG', the tag's place in a pseudovector's size
/// word and the tags of the record and font, whose size words
/// `sxhash_vector' starts from where this implementation has no GNU
/// header to read (a font is a host record, a record literal a reader
/// form).
const PSEUDOVECTOR_FLAG: u64 = 1 << 62;
const PSEUDOVECTOR_AREA_BITS: u32 = 24;
const PVEC_RECORD: u64 = 34;
const PVEC_FONT: u64 = 35;

/// lisp.h's `INTMASK': the bits of a fixnum.
const INTMASK: u64 = (1 << 62) - 1;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum HashMode {
    Eq,
    Eql,
    Equal,
    EqualIncludingProperties,
}

/// lisp.h's `XHASH': the word less its two low bits (`XUFIXNUM' of a
/// fixnum).
#[inline(always)]
fn xhash(value: Value) -> u64 {
    (value.word() >> 2) as u64
}

/// fns.c:sxhash_eq: `XHASH' with `XTYPE' folded in, a symbol with
/// position taken as its bare symbol when REMOVE_POS
/// (`maybe_remove_pos_from_symbol').
#[inline]
fn sxhash_eq(key: Value, remove_pos: bool) -> u64 {
    let key = match key.kind() {
        Kind::SymbolWithPos(object) if remove_pos => object.symbol(),
        _ => key,
    };
    xhash(key) ^ (key.word() & 7) as u64
}

/// fns.c:sxhash_eql: a float or bignum by its value, as `eql' compares
/// them.
#[inline]
fn sxhash_eql(interp: &Interpreter, key: Value, remove_pos: bool) -> u64 {
    match key.kind() {
        Kind::Float(_) | Kind::BigInteger(_) => sxhash(interp, key),
        _ => sxhash_eq(key, remove_pos),
    }
}

/// fns.c:sxhash: a hash for OBJ that `equal' objects share.
pub(crate) fn sxhash(interp: &Interpreter, obj: Value) -> u64 {
    sxhash_obj(interp, obj, 0)
}

/// fns.c:sxhash_obj: OBJ's hash, DEPTH levels into the structure being
/// hashed; past `SXHASH_MAX_DEPTH' every object hashes as 0.
///
/// A symbol with position hashes as its bare symbol, as GNU's does
/// while `symbols-with-pos-enabled': `equal' compares a pair of them by
/// their symbols and positions otherwise, which GNU's address hash leaves
/// unequal.  The pseudovectors this implementation keeps as host records
/// or reader forms hash by what `equal' compares of them.
fn sxhash_obj(interp: &Interpreter, obj: Value, depth: u32) -> u64 {
    if depth > SXHASH_MAX_DEPTH {
        return 0;
    }
    match obj.kind() {
        Kind::Integer(_) | Kind::Nil | Kind::T | Kind::Unbound | Kind::Symbol(_) => xhash(obj),
        Kind::StringObject(string) => hash_string(string.borrow().bytes()),
        Kind::Cons(_) => sxhash_list(interp, obj, depth),
        Kind::Float(number) => sxhash_float(number.to_bits()),
        Kind::Vector(_) | Kind::Closure(_) | Kind::CharTable(_) | Kind::LispRecord(_) => {
            sxhash_vector(interp, obj, depth)
        }
        // sxhash_vector can't be applied to a sub-char-table.
        Kind::SubCharTable(_) => 42,
        Kind::BigInteger(number) => sxhash_bignum(&number),
        Kind::Marker(marker) => match marker.buffer() {
            Some(buffer) => sxhash_combine(buffer.identity() as u64, marker.bytepos() as u64),
            None => sxhash_combine(0, 0),
        },
        Kind::BoolVector(vector) => sxhash_bool_vector(vector),
        Kind::Overlay(overlay) => {
            let (start, end) = if overlay.is_dead() {
                (-1, -1)
            } else {
                overlay.bounds()
            };
            let hash = sxhash_combine(start as u64, end as u64);
            sxhash_combine(hash, sxhash_obj(interp, overlay.plist(), depth))
        }
        Kind::SymbolWithPos(object) => xhash(object.symbol()),
        Kind::Record(record) => sxhash_host_record(interp, obj, record, depth),
        Kind::ReaderForm(_) => sxhash_reader_form(interp, obj, depth),
        // Others are `equal' if they are `eq', so take their address.
        _ => xhash(obj),
    }
}

/// fns.c:sxhash_float: the double's word.
#[inline]
fn sxhash_float(bits: u64) -> u64 {
    sxhash_combine(0, bits)
}

/// fns.c:sxhash_list: at most `SXHASH_MAX_LEN' elements while shallower
/// than `SXHASH_MAX_DEPTH', then whatever tail is left one level deeper,
/// which is what ends a circular list.
fn sxhash_list(interp: &Interpreter, mut list: Value, depth: u32) -> u64 {
    let mut hash = 0;
    if depth < SXHASH_MAX_DEPTH {
        for _ in 0..SXHASH_MAX_LEN {
            let Some((car, cdr)) = list.cons_values() else {
                break;
            };
            hash = sxhash_combine(hash, sxhash_obj(interp, car, depth + 1));
            list = cdr;
        }
    }
    if !list.is_nil() {
        hash = sxhash_combine(hash, sxhash_obj(interp, list, depth + 1));
    }
    hash
}

/// fns.c:sxhash_vector: the size word, then the leading slots.
fn sxhash_vector(interp: &Interpreter, vector: Value, depth: u32) -> u64 {
    // SAFETY: VECTOR is a reachable vector, closure, char-table or
    // record, whose Lisp slots follow its header; hashing allocates
    // nothing, so no collection frees it while the slots are read.
    let (size, slots) = unsafe {
        crate::lisp::alloc::vectors::vector_size_and_slots(
            (vector.word() & !7) as *mut VectorHeader,
        )
    };
    sxhash_slots(
        interp,
        size as u64,
        slots.iter().map(std::cell::Cell::get),
        depth,
    )
}

/// sxhash_vector's loop: SLOTS combined into HASH.
fn sxhash_slots(
    interp: &Interpreter,
    mut hash: u64,
    slots: impl Iterator<Item = Value>,
    depth: u32,
) -> u64 {
    for slot in slots.take(SXHASH_MAX_LEN) {
        hash = sxhash_combine(hash, sxhash_obj(interp, slot, depth + 1));
    }
    hash
}

/// fns.c:sxhash_bool_vector: the length, then the leading words.
fn sxhash_bool_vector(vector: BoolVectorRef) -> u64 {
    let mut hash = vector.len() as u64;
    for word in vector.words().iter().take(SXHASH_MAX_LEN) {
        hash = sxhash_combine(hash, word.get() as u64);
    }
    hash
}

/// fns.c:sxhash_bignum: the sign, then the limbs from the least
/// significant.  A bignum in fixnum range, which `equal' takes as that
/// fixnum, hashes as the fixnum does.
fn sxhash_bignum(number: &BigInt) -> u64 {
    const MOST_POSITIVE_FIXNUM: i64 = (1 << 61) - 1;
    const MOST_NEGATIVE_FIXNUM: i64 = -(1 << 61);
    if let Some(fixnum) = number.to_i64()
        && (MOST_NEGATIVE_FIXNUM..=MOST_POSITIVE_FIXNUM).contains(&fixnum)
    {
        return fixnum as u64 & INTMASK;
    }
    let mut hash = u64::from(number.sign() == Sign::Minus);
    for limb in number.iter_u64_digits() {
        hash = sxhash_combine(hash, limb);
    }
    hash
}

/// sxhash_obj of a pseudovector kept as a host record: a font, past
/// `PVEC_CLOSURE', by `sxhash_vector' of its type and slots; a
/// tree-sitter node by the node it names, which `equal' compares
/// (treesit_node_eq); the rest, `equal' only when `eq', by address.
fn sxhash_host_record(
    interp: &Interpreter,
    obj: Value,
    record: crate::lisp::types::RecordRef,
    depth: u32,
) -> u64 {
    use crate::lisp::eval::RecordKind;
    let Some(state) = interp.find_record(record) else {
        return xhash(obj);
    };
    match state.kind {
        RecordKind::Font => {
            let size = PSEUDOVECTOR_FLAG
                | (PVEC_FONT << PSEUDOVECTOR_AREA_BITS)
                | (1 + state.slots.len()) as u64;
            sxhash_slots(
                interp,
                size,
                std::iter::once(state.type_tag).chain(state.slots.iter().copied()),
                depth,
            )
        }
        RecordKind::TreeSitterNode => match interp.treesit_node_state(&obj) {
            Some(node) => sxhash_combine(
                sxhash_combine(node.parser_id, node.generation),
                node.node_id as u64,
            ),
            None => xhash(obj),
        },
        _ => xhash(obj),
    }
}

/// sxhash_obj of a reader form: a record literal as `sxhash_vector'
/// hashes the record it is `equal' to; any other form by address.
fn sxhash_reader_form(interp: &Interpreter, obj: Value, depth: u32) -> u64 {
    let Some(items) = record_literal_items(&obj) else {
        return xhash(obj);
    };
    let slots = &items[1..];
    let size = PSEUDOVECTOR_FLAG | (PVEC_RECORD << PSEUDOVECTOR_AREA_BITS) | slots.len() as u64;
    sxhash_slots(
        interp,
        size,
        slots.iter().map(record_literal_slot_data),
        depth,
    )
}

/// fns.c:Fsxhash_equal_including_properties: `sxhash', and for a string
/// each interval's position, length and plist (traverse_intervals with
/// hash_interval).
fn sxhash_including_properties(interp: &Interpreter, obj: Value) -> u64 {
    let mut hash = sxhash(interp, obj);
    let Kind::StringObject(string) = obj.kind() else {
        return hash;
    };
    let string = string.borrow();
    let Some(intervals) = string.props.intervals() else {
        return hash;
    };
    for (position, end, plist) in intervals.iter() {
        hash = sxhash_combine(hash, position as u64);
        hash = sxhash_combine(hash, (end - position) as u64);
        hash = sxhash_combine(hash, sxhash_obj(interp, plist, 0));
    }
    hash
}

/// lisp.h:SXHASH_REDUCE with make_ufixnum: HASH as the fixnum the
/// `sxhash-*' functions return (reduce_emacs_uint_to_fixnum).
#[inline]
fn reduce_emacs_uint_to_fixnum(hash: u64) -> i64 {
    let reduced = (hash ^ (hash >> 2)) & INTMASK;
    ((reduced << 2) as i64) >> 2
}

/// fns.c's Fsxhash_eq, Fsxhash_eql, Fsxhash_equal and
/// Fsxhash_equal_including_properties.
pub(crate) fn sxhash_value_in_env(
    interp: &Interpreter,
    value: &Value,
    mode: HashMode,
    env: &Env,
) -> i64 {
    // maybe_remove_pos_from_symbol reads the variable for a symbol with
    // position only.
    let remove_pos =
        || matches!(value.kind(), Kind::SymbolWithPos(_)) && symbols_with_pos_enabled(interp, env);
    reduce_emacs_uint_to_fixnum(match mode {
        HashMode::Eq => sxhash_eq(*value, remove_pos()),
        HashMode::Eql => sxhash_eql(interp, *value, remove_pos()),
        HashMode::Equal => sxhash(interp, *value),
        HashMode::EqualIncludingProperties => sxhash_including_properties(interp, *value),
    })
}

/// The hash a standard hash table files KEY under, before
/// `reduce_emacs_uint_to_hash_hash': fns.c's hashfn_eq, hashfn_eql and
/// hashfn_equal.  A symbol with position hashes as its bare symbol here
/// whatever `symbols-with-pos-enabled' says, so a key stays in the
/// bucket every key `eq' to it is in while the variable changes.
pub(crate) fn standard_table_hash(
    interp: &Interpreter,
    test: crate::lisp::eval::RuntimeHashTest,
    key: Value,
) -> u64 {
    use crate::lisp::eval::RuntimeHashTest;
    match test {
        RuntimeHashTest::Eq => sxhash_eq(key, true),
        RuntimeHashTest::Eql => sxhash_eql(interp, key, true),
        RuntimeHashTest::Equal => sxhash(interp, key),
    }
}
