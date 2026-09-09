//! D08 controls: an image written from explicit roots reads back as the
//! same object graph (sharing and cycles intact, at different addresses),
//! and the file validation refuses what pdumper_load refuses.

use super::context::{DumpContext, object_key};
use super::image::*;
use super::load::{LoadError, load_image, validate_header};
use super::{RootSource, write_image};
use crate::buffer::TextPropertySpan;
use crate::lisp::eval::Interpreter;
use crate::lisp::primitives::strings::{make_shared_string_value_with_extended_chars, string_like};
use crate::lisp::types::{SymbolName, Value};
use std::collections::HashMap;

fn dump(interp: &Interpreter, roots: Vec<(RootSlot, Value)>) -> Vec<u8> {
    let mut ctx = DumpContext::new(true);
    let summary = match write_image(&mut ctx, interp, RootSource::Explicit(roots)) {
        Ok(summary) => summary,
        Err(super::context::DumpError::Unsupported(unsupported)) => {
            panic!("unsupported object: {}", unsupported.message)
        }
        Err(super::context::DumpError::Lisp(error)) => panic!("dump failed: {error:?}"),
    };
    assert_eq!(summary.header_bytes, HEADER_LEN as u32);
    ctx.buffer().to_vec()
}

/// Structural equality with identity correspondence: every object of A
/// maps to exactly one object of B, so sharing and cycles are preserved.
fn graph_matches(
    a: &Value,
    b: &Value,
    seen: &mut HashMap<super::context::ObjectKey, super::context::ObjectKey>,
) -> Result<(), String> {
    match (object_key(a), object_key(b)) {
        (Some(key_a), Some(key_b)) => {
            if let Some(mapped) = seen.get(&key_a) {
                return if *mapped == key_b {
                    Ok(())
                } else {
                    Err(format!("{a:?} maps to two objects"))
                };
            }
            if seen.values().any(|mapped| *mapped == key_b) {
                return Err(format!("{b:?} is the image of two objects"));
            }
            seen.insert(key_a, key_b);
        }
        (None, None) => {}
        _ => return Err(format!("identity class differs: {a:?} vs {b:?}")),
    }
    match (a, b) {
        (Value::Nil, Value::Nil) | (Value::T, Value::T) | (Value::Unbound, Value::Unbound) => {
            Ok(())
        }
        (Value::Integer(x), Value::Integer(y)) if x == y => Ok(()),
        (Value::BigInteger(x), Value::BigInteger(y)) if **x == **y => Ok(()),
        (Value::Float(x), Value::Float(y)) if x.to_bits() == y.to_bits() => Ok(()),
        (Value::String(_), Value::String(_)) | (Value::StringObject(_), Value::StringObject(_)) => {
            let x = string_like(a).expect("a string");
            let y = string_like(b).expect("a string");
            if x.text != y.text
                || x.multibyte != y.multibyte
                || x.extended_chars != y.extended_chars
            {
                return Err(format!("string differs: {x:?} vs {y:?}"));
            }
            if x.props.len() != y.props.len() {
                return Err(format!(
                    "property spans differ: {:?} vs {:?}",
                    x.props, y.props
                ));
            }
            for (span_x, span_y) in x.props.iter().zip(&y.props) {
                if span_x.start != span_y.start
                    || span_x.end != span_y.end
                    || span_x.props.len() != span_y.props.len()
                {
                    return Err(format!("span differs: {span_x:?} vs {span_y:?}"));
                }
                for ((name_x, value_x), (name_y, value_y)) in span_x.props.iter().zip(&span_y.props)
                {
                    if name_x != name_y {
                        return Err(format!("property name differs: {name_x} vs {name_y}"));
                    }
                    graph_matches(value_x, value_y, seen)?;
                }
            }
            Ok(())
        }
        (Value::Symbol(x), Value::Symbol(y)) => {
            let name = |symbol: &SymbolName| string_like(&symbol.lisp_name()).map(|s| s.text);
            if x.as_str() != y.as_str() && name(x) != name(y) {
                return Err(format!("symbol differs: {x:?} vs {y:?}"));
            }
            if (x.id() & crate::lisp::types::UNINTERNED_SYMBOL_ID_BIT != 0)
                != (y.id() & crate::lisp::types::UNINTERNED_SYMBOL_ID_BIT != 0)
            {
                return Err(format!("internedness differs: {x:?} vs {y:?}"));
            }
            Ok(())
        }
        (Value::BuiltinFunc(x), Value::BuiltinFunc(y)) if x.as_str() == y.as_str() => Ok(()),
        (Value::Cons(_), Value::Cons(_)) => {
            graph_matches(&a.car().expect("car"), &b.car().expect("car"), seen)?;
            graph_matches(&a.cdr().expect("cdr"), &b.cdr().expect("cdr"), seen)
        }
        (Value::Vector(x), Value::Vector(y)) => {
            let x = x.slots().clone();
            let y = y.slots().clone();
            if x.len() != y.len() {
                return Err("vector length differs".into());
            }
            for (slot_x, slot_y) in x.iter().zip(&y) {
                graph_matches(slot_x, slot_y, seen)?;
            }
            Ok(())
        }
        _ => Err(format!("value differs: {a:?} vs {b:?}")),
    }
}

#[test]
fn image_round_trips_sharing_cycles_and_every_supported_object_kind() {
    let interp = Interpreter::new();
    // A shared sublist, a self-referential cons, a vector holding the
    // list twice, immutable and mutable strings (multibyte, unibyte with
    // raw bytes, text properties, an out-of-Unicode character), floats
    // that are one object and two equal objects, a bignum, a wide
    // integer, fixnums at both fixnum bounds, nil, t, the unbound marker,
    // an interned and an uninterned symbol, and a built-in function.
    let shared = Value::list([Value::Integer(1), Value::Integer(2)]);
    let cycle = Value::cons(Value::symbol("loop"), Value::Nil);
    cycle.set_cdr(cycle.clone()).expect("setcdr");
    let float = Value::float(2.5);
    let props = vec![TextPropertySpan {
        start: 1,
        end: 3,
        props: vec![
            ("face".into(), Value::symbol("bold")),
            ("shared".into(), shared.clone()),
        ],
    }];
    let propertized =
        make_shared_string_value_with_extended_chars("héllo".into(), props, true, Vec::new());
    let extended = make_shared_string_value_with_extended_chars(
        format!("a{}b", crate::lisp::json::INVALID_UNICODE_SENTINEL),
        Vec::new(),
        true,
        vec![(1, 0x20_0000)],
    );
    let unibyte = make_shared_string_value_with_extended_chars(
        format!(
            "raw{}{}",
            crate::lisp::primitives::case::raw_byte_regex_char(0xC3),
            crate::lisp::primitives::case::raw_byte_regex_char(0xA9)
        ),
        Vec::new(),
        false,
        Vec::new(),
    );
    let multibyte_raw = make_shared_string_value_with_extended_chars(
        format!(
            "x{}",
            crate::lisp::primitives::case::raw_byte_regex_char(0x80)
        ),
        Vec::new(),
        true,
        Vec::new(),
    );
    let uninterned = Value::Symbol(SymbolName::make_uninterned(
        Value::string("gensym"),
        "gensym",
        crate::lisp::primitives::next_make_symbol_id(),
    ));
    let bignum = Value::big_integer(
        "123456789012345678901234567890123456789"
            .parse::<num_bigint::BigInt>()
            .expect("a bignum literal"),
    );
    let negative_bignum = Value::big_integer(
        "-987654321098765432109876543210"
            .parse::<num_bigint::BigInt>()
            .expect("a bignum literal"),
    );
    let graph = Value::vector([
        shared.clone(),
        shared.clone(),
        cycle.clone(),
        Value::string("plain ascii"),
        Value::string("ünïcödé"),
        propertized,
        extended,
        unibyte,
        multibyte_raw,
        float.clone(),
        float.clone(),
        Value::float(2.5),
        bignum,
        negative_bignum,
        Value::Integer(MOST_POSITIVE_FIXNUM + 1),
        Value::Integer(i64::MIN),
        Value::Integer(MOST_POSITIVE_FIXNUM),
        Value::Integer(MOST_NEGATIVE_FIXNUM),
        Value::Integer(0),
        Value::Integer(-7),
        Value::Nil,
        Value::T,
        Value::Unbound,
        Value::symbol("car"),
        uninterned.clone(),
        uninterned.clone(),
        Value::BuiltinFunc("car".into()),
        Value::BuiltinFunc("car".into()),
        Value::string(""),
        Value::vector([]),
    ]);
    let roots = vec![
        (RootSlot::LoadPath, graph.clone()),
        (RootSlot::QuitFlag, Value::Integer(42)),
        (RootSlot::InhibitQuit, Value::T),
        (RootSlot::CurrentGlobalMap, Value::Unbound),
        (RootSlot::ThrowOnInput, Value::BuiltinFunc("cdr".into())),
    ];
    let bytes = dump(&interp, roots);

    let image = load_image(&bytes).unwrap_or_else(|error| panic!("load: {error:?}"));
    assert_eq!(image.header.magic, DUMP_MAGIC);
    assert_eq!(image.header.cold_start % (64 * 1024), 0);
    assert!(image.header.discardable_start <= image.header.cold_start);
    let root = |slot: RootSlot| {
        image
            .roots
            .iter()
            .find(|(candidate, _)| *candidate == slot)
            .map(|(_, value)| value.clone())
            .unwrap_or_else(|| panic!("root {slot:?} missing"))
    };
    assert_eq!(root(RootSlot::QuitFlag), Value::Integer(42));
    assert_eq!(root(RootSlot::InhibitQuit), Value::T);
    assert_eq!(root(RootSlot::CurrentGlobalMap), Value::Unbound);
    assert!(matches!(root(RootSlot::ThrowOnInput), Value::BuiltinFunc(name) if name == "cdr"));
    let loaded = root(RootSlot::LoadPath);
    let mut seen = HashMap::new();
    graph_matches(&graph, &loaded, &mut seen).unwrap_or_else(|error| panic!("{error}"));
    // The loaded graph is a different allocation.
    assert_ne!(object_key(&graph), object_key(&loaded));

    // The symbol records carry the interned/uninterned distinction and
    // the symbols themselves come back once each.
    let gensyms = image
        .symbols
        .iter()
        .filter(|symbol| symbol.symbol.as_str().starts_with("gensym"))
        .count();
    assert_eq!(gensyms, 1);
    assert!(image.symbols.iter().any(|symbol| symbol.symbol == "car"));
}

#[test]
fn image_records_symbol_cells_from_the_interpreter() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    interp
        .eval(
            &crate::lisp::reader::Reader::new(
                r#"(progn
                     (defvar zz-dump-special '(1 2 3))
                     (defvaralias 'zz-dump-alias 'zz-dump-special)
                     (setq zz-dump-plain 7)
                     (put 'zz-dump-plain 'zz-prop "value")
                     (fset 'zz-dump-fn 'car)
                     (add-variable-watcher 'zz-dump-plain 'ignore)
                     (makunbound 'zz-dump-void))"#,
            )
            .read()
            .expect("setup parses")
            .expect("setup has a form"),
            &mut env,
        )
        .expect("setup evaluates");
    let roots = vec![(
        RootSlot::LoadPath,
        Value::list([
            Value::symbol("zz-dump-special"),
            Value::symbol("zz-dump-alias"),
            Value::symbol("zz-dump-plain"),
            Value::symbol("zz-dump-fn"),
            Value::symbol("zz-dump-void"),
        ]),
    )];
    let bytes = dump(&interp, roots);
    let image = load_image(&bytes).unwrap_or_else(|error| panic!("load: {error:?}"));
    let record = |name: &str| {
        image
            .symbols
            .iter()
            .find(|symbol| symbol.symbol == name)
            .unwrap_or_else(|| panic!("no record for {name}"))
            .clone()
    };
    let special = record("zz-dump-special");
    assert_eq!(
        special.flags & super::context::FLAG_DECLARED_SPECIAL,
        super::context::FLAG_DECLARED_SPECIAL
    );
    assert_eq!(
        special.value.as_ref().and_then(|value| value.to_vec().ok()),
        Some(vec![
            Value::Integer(1),
            Value::Integer(2),
            Value::Integer(3)
        ])
    );
    let alias = record("zz-dump-alias");
    assert_eq!(
        alias.flags & super::context::SYMBOL_REDIRECT_MASK,
        super::context::SYMBOL_VARALIAS
    );
    assert_eq!(
        alias.alias.as_ref().map(SymbolName::as_str),
        Some("zz-dump-special")
    );
    let plain = record("zz-dump-plain");
    assert_eq!(plain.value, Some(Value::Integer(7)));
    assert_eq!(
        plain.flags & super::context::FLAG_TRAPPED_WRITE,
        super::context::FLAG_TRAPPED_WRITE
    );
    assert_eq!(plain.watchers, vec![Value::symbol("ignore")]);
    assert_eq!(plain.plist.to_vec().ok().map(|plist| plist.len()), Some(2));
    let function = record("zz-dump-fn");
    assert_eq!(function.function, Value::symbol("car"));
    assert_eq!(record("zz-dump-void").value, None);
    assert!(image.obarray.is_empty(), "explicit roots dump no obarray");
}

#[test]
fn load_refuses_what_pdumper_load_refuses() {
    let interp = Interpreter::new();
    let bytes = dump(&interp, vec![(RootSlot::QuitFlag, Value::Nil)]);
    assert!(validate_header(&bytes).is_ok());
    // Too short to hold a header: PDUMPER_LOAD_BAD_FILE_TYPE.
    assert_eq!(
        validate_header(&bytes[..HEADER_LEN - 1]).err(),
        Some(LoadError::BadFileType)
    );
    // The incomplete marker: PDUMPER_LOAD_FAILED_DUMP.
    let mut incomplete = bytes.clone();
    incomplete[0] = INCOMPLETE_MAGIC_BYTE;
    assert_eq!(
        validate_header(&incomplete).err(),
        Some(LoadError::FailedDump)
    );
    // Another magic: PDUMPER_LOAD_BAD_FILE_TYPE.
    let mut other = bytes.clone();
    other[1] = b'X';
    assert_eq!(validate_header(&other).err(), Some(LoadError::BadFileType));
    // Another binary's fingerprint: PDUMPER_LOAD_VERSION_MISMATCH.
    let mut foreign = bytes.clone();
    foreign[16] ^= 0xFF;
    assert_eq!(
        validate_header(&foreign).err(),
        Some(LoadError::VersionMismatch)
    );
}

#[test]
fn queue_order_writes_referents_after_their_referrer_and_each_object_once() {
    let interp = Interpreter::new();
    let inner = Value::list([Value::string("a"), Value::string("b")]);
    let outer = Value::vector([inner.clone(), inner.clone(), Value::string("c")]);
    let bytes = dump(&interp, vec![(RootSlot::LoadPath, outer.clone())]);
    let image = load_image(&bytes).unwrap_or_else(|error| panic!("load: {error:?}"));
    // Object starts are unique and ascending.
    let header = &image.header;
    let mut previous = 0;
    let mut kinds = Vec::new();
    for index in 0..header.object_starts.nr_entries {
        let at = (header.object_starts.offset + index * TABLE_ENTRY_LEN as u32) as usize;
        let offset = u32::from_le_bytes(bytes[at..at + 4].try_into().expect("four bytes"));
        let kind = u32::from_le_bytes(bytes[at + 4..at + 8].try_into().expect("four bytes"));
        assert!(offset > previous, "object starts ascend");
        previous = offset;
        kinds.push(DumpType::from_u32(kind).expect("a known object type"));
    }
    // One vector, two conses, three strings: nothing twice.
    assert_eq!(
        kinds
            .iter()
            .filter(|kind| **kind == DumpType::Vector)
            .count(),
        1
    );
    assert_eq!(
        kinds.iter().filter(|kind| **kind == DumpType::Cons).count(),
        2
    );
    assert_eq!(
        kinds
            .iter()
            .filter(|kind| **kind == DumpType::String)
            .count(),
        3
    );
    // The root vector is the first heap object after the header.
    assert_eq!(kinds[0], DumpType::Vector);
}
