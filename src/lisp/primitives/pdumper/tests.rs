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
use std::rc::Rc;

fn dump(interp: &mut Interpreter, roots: Vec<(RootSlot, Value)>) -> Vec<u8> {
    let mut ctx = DumpContext::new(true, interp.main_thread_record_id());
    let summary = match write_image(&mut ctx, interp, RootSource::Explicit(roots)) {
        Ok(summary) => summary,
        Err(super::context::DumpError::Unsupported(unsupported)) => {
            ctx.print_paths_to_root(interp, &mut Vec::new(), &unsupported.object);
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
        // Identity-bearing kinds compared by the tests through the
        // interpreters that own them.
        (Value::Lambda(_), Value::Lambda(_))
        | (Value::CharTable(_), Value::CharTable(_))
        | (Value::Record(_), Value::Record(_)) => Ok(()),
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
    let mut interp = Interpreter::new();
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
    let bytes = dump(&mut interp, roots);

    let mut target = Interpreter::new();
    let image = load_image(&bytes, &mut target).unwrap_or_else(|error| panic!("load: {error:?}"));
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
    let bytes = dump(&mut interp, roots);
    let mut target = Interpreter::new();
    let image = load_image(&bytes, &mut target).unwrap_or_else(|error| panic!("load: {error:?}"));
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
    let mut interp = Interpreter::new();
    let bytes = dump(&mut interp, vec![(RootSlot::QuitFlag, Value::Nil)]);
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
fn supported_image_starts_in_a_fresh_process_with_new_process_values() {
    use std::os::unix::fs::DirBuilderExt;
    // A supported C-state image, not ordinary loadup or a native image.
    // Exercise production sibling discovery, fingerprint validation,
    // installation and process initialization in a different executable
    // location and OS process, without disabling native loading anywhere.
    const CHILD: &str = "EMAXX_PDUMPER_PROCESS_TEST_CHILD";
    if let Some(parent_pid) = std::env::var_os(CHILD) {
        let mut interpreter = Interpreter::new();
        let record = super::load_pdump_at_startup(&mut interpreter, None)
            .expect("the copied executable discovers its actual sibling image");
        interpreter
            .init_after_pdump_load()
            .expect("initialize loaded process");
        assert_eq!(
            interpreter
                .symbol_value_cell("zz-builder-pid")
                .expect("saved marker"),
            Value::Integer(parent_pid.to_string_lossy().parse().expect("parent pid"))
        );
        assert_ne!(parent_pid.to_string_lossy(), std::process::id().to_string());
        let executable = std::env::current_exe().expect("child executable");
        assert_eq!(record.filename, format!("{}.pdmp", executable.display()));
        assert_eq!(
            interpreter
                .symbol_value_cell("invocation-name")
                .expect("fresh name"),
            Value::string("restored-process-test")
        );
        assert_eq!(
            interpreter.lookup_var("default-directory", &Vec::new()),
            Some(Value::string(&crate::lisp::primitives::default_directory()))
        );
        let environment = interpreter
            .symbol_value_cell("process-environment")
            .expect("fresh environment")
            .to_vec()
            .expect("environment list");
        assert!(
            environment
                .iter()
                .any(|value| string_like(value).is_some_and(
                    |string| string.text == format!("{CHILD}={}", parent_pid.to_string_lossy())
                ))
        );
        assert!(interpreter.dump_loaded_p());
        return;
    }

    let root = std::env::temp_dir().join(format!("emaxx-pdump-process-{}", std::process::id()));
    // A process id is recycled: a fixture left by an earlier run killed
    // before its cleanup must not fail this one.
    let _ = std::fs::remove_dir_all(&root);
    std::fs::DirBuilder::new()
        .mode(0o700)
        .create(&root)
        .expect("new private process fixture");
    let executable = root.join("restored-process-test");
    std::fs::copy(
        std::env::current_exe().expect("test executable"),
        &executable,
    )
    .expect("copy unchanged executable bytes");
    let mut interpreter = Interpreter::new();
    interpreter.set_global_binding(
        "zz-builder-pid",
        Value::Integer(i64::from(std::process::id())),
    );
    interpreter.set_buffer_local_value(
        interpreter.current_buffer_id(),
        "default-directory",
        Value::string("/saved-builder-directory/"),
    );
    let mut context = DumpContext::new(false, interpreter.main_thread_record_id());
    if let Err(error) = write_image(&mut context, &interpreter, RootSource::Interpreter) {
        match error {
            super::context::DumpError::Unsupported(unsupported) => {
                panic!("unsupported C-state object: {}", unsupported.message)
            }
            super::context::DumpError::Lisp(error) => panic!("write C-state image: {error:?}"),
        }
    }
    let image = root.join("restored-process-test.pdmp");
    std::fs::write(&image, context.buffer()).expect("write complete image");
    let child = std::process::Command::new(&executable)
        .args(["--exact", "lisp::primitives::pdumper::tests::supported_image_starts_in_a_fresh_process_with_new_process_values",
            "--test-threads=1"])
        .env(CHILD, std::process::id().to_string())
        .current_dir(&root)
        .output().expect("launch the fresh process");
    assert!(
        child.status.success(),
        "child failed:\n{}\n{}",
        String::from_utf8_lossy(&child.stdout),
        String::from_utf8_lossy(&child.stderr)
    );
    assert!(
        String::from_utf8_lossy(&child.stdout)
            .contains("test result: ok. 1 passed; 0 failed; 0 ignored;"),
        "the selected child test must actually execute: {}",
        String::from_utf8_lossy(&child.stdout)
    );
    std::fs::remove_dir_all(&root).expect("remove successful process fixture");
}

#[test]
fn queue_order_writes_referents_after_their_referrer_and_each_object_once() {
    let mut interp = Interpreter::new();
    let inner = Value::list([Value::string("a"), Value::string("b")]);
    let outer = Value::vector([inner.clone(), inner.clone(), Value::string("c")]);
    let bytes = dump(&mut interp, vec![(RootSlot::LoadPath, outer.clone())]);
    let mut target = Interpreter::new();
    let image = load_image(&bytes, &mut target).unwrap_or_else(|error| panic!("load: {error:?}"));
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

#[test]
fn image_round_trips_closures_char_tables_records_and_bool_vectors() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let program = r#"
        (progn
          (put 'zz-purpose 'char-table-extra-slots 1)
          (let ((shared (eval '(let ((x 1))
                                 (list (function (lambda () x))
                                       (function (lambda (y) (setq x (+ x y))))))
                              t))
                (table (make-char-table 'zz-purpose 'dflt))
                (bits (make-bool-vector 70 nil)))
            (set-char-table-range table '(?a . ?z) 'lower)
            (set-char-table-range table ?A 'upper)
            (set-char-table-extra-slot table 0 "extra")
            (aset bits 0 t)
            (aset bits 65 t)
            (aset bits 69 t)
            (vector shared table bits (record 'zz-rec 1 "two" shared)
                    main-thread)))"#;
    let form = crate::lisp::reader::Reader::new(program)
        .read()
        .expect("setup parses")
        .expect("setup has a form");
    let graph = interp.eval(&form, &mut env).expect("setup evaluates");
    let bytes = dump(&mut interp, vec![(RootSlot::LoadPath, graph.clone())]);
    let mut target = Interpreter::new();
    let image = load_image(&bytes, &mut target).unwrap_or_else(|error| panic!("load: {error:?}"));
    let loaded = image
        .roots
        .iter()
        .find(|(slot, _)| *slot == RootSlot::LoadPath)
        .map(|(_, value)| value.clone())
        .expect("root");
    let mut seen = HashMap::new();
    graph_matches(&graph, &loaded, &mut seen).unwrap_or_else(|error| panic!("{error}"));
    let Value::Vector(vector) = &loaded else {
        panic!("root vector")
    };
    let slots = vector.slots().clone();

    // Two closures over one environment: the frame is shared, and calling
    // them in the restored interpreter mutates the shared binding.
    let closures = slots[0].to_vec().expect("closure list");
    let (Value::Lambda(first), Value::Lambda(second)) = (&closures[0], &closures[1]) else {
        panic!("closures")
    };
    assert!(
        Rc::ptr_eq(&first.env, &second.env),
        "one environment object"
    );
    assert_eq!(second.params.as_slice().len(), 1);
    assert_eq!(second.params[0].as_str(), "y");
    let call = |target: &mut Interpreter, function: &Value, args: &[Value]| {
        target
            .call_function_value(function.clone(), None, args, &mut Vec::new())
            .expect("closure call")
    };
    assert_eq!(call(&mut target, &closures[0], &[]), Value::Integer(1));
    assert_eq!(
        call(&mut target, &closures[1], &[Value::Integer(5)]),
        Value::Integer(6)
    );
    assert_eq!(call(&mut target, &closures[0], &[]), Value::Integer(6));

    // The char-table with its ranges, subtype, default and extra slot.
    let Value::CharTable(table_id) = slots[1] else {
        panic!("char-table")
    };
    let table = target
        .find_char_table(table_id)
        .expect("installed char-table");
    assert_eq!(table.subtype.as_deref(), Some("zz-purpose"));
    assert_eq!(table.default, Value::symbol("dflt"));
    assert_eq!(
        table
            .entries
            .iter()
            .map(|entry| (entry.start, entry.end, entry.value.clone()))
            .collect::<Vec<_>>(),
        vec![
            (97, 122, Value::symbol("lower")),
            (65, 65, Value::symbol("upper"))
        ]
    );
    assert_eq!(
        table
            .extra_slots
            .iter()
            .map(|slot| string_like(slot).map(|s| s.text))
            .collect::<Vec<_>>(),
        vec![Some("extra".to_owned())]
    );

    // The bool-vector's bits came through the cold section.
    let Value::Record(bits_id) = slots[2] else {
        panic!("bool-vector")
    };
    let bits = target.find_record(bits_id).expect("installed bool-vector");
    assert_eq!(bits.kind, crate::lisp::eval::RecordKind::BoolVector);
    assert_eq!(bits.slots.len(), 70);
    let set = bits
        .slots
        .iter()
        .enumerate()
        .filter(|(_, slot)| slot.is_truthy())
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    assert_eq!(set, vec![0, 65, 69]);

    // The record's slots, with the shared list being the same object.
    let Value::Record(record_id) = slots[3] else {
        panic!("record")
    };
    let record = target.find_record(record_id).expect("installed record");
    assert_eq!(record.type_tag, Value::symbol("zz-rec"));
    assert_eq!(record.slots[0], Value::Integer(1));
    assert_eq!(
        string_like(&record.slots[1]).map(|s| s.text),
        Some("two".to_owned())
    );
    assert_eq!(object_key(&record.slots[2]), object_key(&slots[0]));

    // The main thread is the restoring process's own.  (The standard
    // obarray reaches every symbol's value, hash tables included: it joins
    // the controls with D10.)
    assert_eq!(slots[4], Value::Record(target.main_thread_record_id()));
    assert!(image.obarray.is_empty());
    assert_eq!(image.builtin_cells.len(), 2);
    assert_eq!(image.builtin_cells[0].symbol.as_str(), "nil");
    assert_eq!(image.builtin_cells[1].symbol.as_str(), "t");
}

#[test]
fn image_freezes_and_thaws_hash_tables_as_pdumper_c_does() {
    fn call_in(target: &mut Interpreter, name: &str, args: &[Value]) -> Value {
        crate::lisp::native_comp::call_c_primitive(target, &mut Vec::new(), name, args)
            .unwrap_or_else(|error| panic!("{name}: {error:?}"))
    }
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let program = r#"
        (let ((eq-table (make-hash-table :test 'eq))
              (equal-table (make-hash-table :test 'equal :size 100))
              (weak (make-hash-table :weakness 'key))
              (empty (make-hash-table))
              (shared (list 1 2)))
          (puthash 'a 1 eq-table)
          (puthash 'b shared eq-table)
          (puthash "k1" "v1" equal-table)
          (puthash "k2" 2 equal-table)
          (remhash "k1" equal-table)
          (puthash "k3" 3 equal-table)
          (puthash 'w 'x weak)
          (vector eq-table equal-table weak empty shared))"#;
    let form = crate::lisp::reader::Reader::new(program)
        .read()
        .expect("setup parses")
        .expect("setup has a form");
    let graph = interp.eval(&form, &mut env).expect("setup evaluates");
    let bytes = dump(&mut interp, vec![(RootSlot::LoadPath, graph.clone())]);
    let mut target = Interpreter::new();
    let image = load_image(&bytes, &mut target).unwrap_or_else(|error| panic!("load: {error:?}"));
    assert_ne!(image.header.hash_list, 0, "the hash list is written");
    let loaded = image
        .roots
        .iter()
        .find(|(slot, _)| *slot == RootSlot::LoadPath)
        .map(|(_, value)| value.clone())
        .expect("root");
    let Value::Vector(vector) = &loaded else {
        panic!("root vector")
    };
    let slots = vector.slots().clone();
    let eq_table = slots[0].clone();
    let equal_table = slots[1].clone();
    let weak = slots[2].clone();
    let empty = slots[3].clone();
    let shared = slots[4].clone();
    assert_eq!(
        call_in(
            &mut target,
            "hash-table-count",
            std::slice::from_ref(&eq_table)
        ),
        Value::Integer(2)
    );
    assert_eq!(
        call_in(
            &mut target,
            "hash-table-test",
            std::slice::from_ref(&eq_table)
        ),
        Value::symbol("eq")
    );
    assert_eq!(
        call_in(
            &mut target,
            "gethash",
            &[Value::symbol("a"), eq_table.clone()]
        ),
        Value::Integer(1)
    );
    // The value is the shared list object, not a copy.
    assert_eq!(
        object_key(&call_in(
            &mut target,
            "gethash",
            &[Value::symbol("b"), eq_table.clone()]
        )),
        object_key(&shared)
    );
    // `equal' lookups work through the thawed index; the removed key is
    // gone and the compacted order is the slot order.
    assert_eq!(
        call_in(
            &mut target,
            "hash-table-test",
            std::slice::from_ref(&equal_table)
        ),
        Value::symbol("equal")
    );
    assert_eq!(
        call_in(
            &mut target,
            "gethash",
            &[Value::string("k2"), equal_table.clone()]
        ),
        Value::Integer(2)
    );
    assert_eq!(
        call_in(
            &mut target,
            "gethash",
            &[Value::string("k3"), equal_table.clone()]
        ),
        Value::Integer(3)
    );
    assert_eq!(
        call_in(
            &mut target,
            "gethash",
            &[Value::string("k1"), equal_table.clone()]
        ),
        Value::Nil
    );
    // Weakness and an empty table survive.
    assert_eq!(
        call_in(
            &mut target,
            "hash-table-weakness",
            std::slice::from_ref(&weak)
        ),
        Value::symbol("key")
    );
    assert_eq!(
        call_in(&mut target, "gethash", &[Value::symbol("w"), weak.clone()]),
        Value::symbol("x")
    );
    assert_eq!(
        call_in(
            &mut target,
            "hash-table-count",
            std::slice::from_ref(&empty)
        ),
        Value::Integer(0)
    );
    assert_eq!(
        call_in(&mut target, "hash-table-test", std::slice::from_ref(&empty)),
        Value::symbol("eql")
    );
    // The thawed table is mutable.
    call_in(
        &mut target,
        "puthash",
        &[Value::symbol("c"), Value::Integer(3), eq_table.clone()],
    );
    assert_eq!(
        call_in(&mut target, "hash-table-count", &[eq_table]),
        Value::Integer(3)
    );
    let Value::Record(equal_id) = equal_table else {
        panic!("hash table")
    };
    let keys = target
        .hash_table_runtime_entries(equal_id)
        .expect("thawed runtime entries")
        .iter()
        .map(|(key, _)| string_like(key).map(|s| s.text).unwrap_or_default())
        .collect::<Vec<_>>();
    // fns.c reuses the slot `remhash' freed: k3 sits in k1's slot 0, so
    // the compact contents (hash_table_contents) walk k3 before k2.
    assert_eq!(keys, vec!["k3".to_owned(), "k2".to_owned()]);
    // hash_table_thaw: the allocation is minimal, count entries.
    assert_eq!(target.gnu_hash_table_capacity(equal_id), Some(2));
}

#[test]
fn image_refuses_hash_tables_with_user_defined_tests_as_gnu_does() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let program = r#"
        (progn
          (define-hash-table-test 'zz-test 'equal 'sxhash-equal)
          (let ((table (make-hash-table :test 'zz-test)))
            (puthash "k" 1 table)
            table))"#;
    let form = crate::lisp::reader::Reader::new(program)
        .read()
        .expect("setup parses")
        .expect("setup has a form");
    let table = interp.eval(&form, &mut env).expect("setup evaluates");
    let mut ctx = DumpContext::new(false, interp.main_thread_record_id());
    let result = write_image(
        &mut ctx,
        &interp,
        RootSource::Explicit(vec![(RootSlot::LoadPath, table)]),
    );
    match result {
        Err(super::context::DumpError::Lisp(crate::lisp::types::LispError::Signal(message))) => {
            assert_eq!(message, "cannot dump hash tables with user-defined tests");
        }
        Err(super::context::DumpError::Lisp(other)) => panic!("other error: {other:?}"),
        Err(super::context::DumpError::Unsupported(unsupported)) => {
            panic!("unsupported: {}", unsupported.message)
        }
        Ok(_) => panic!("a user-defined test was dumped"),
    }
}

#[test]
fn image_keeps_a_private_obarray_symbol_apart_from_its_namesake() {
    // lisp.h: a symbol interned in another obarray is its own object with
    // SYMBOL_INTERNED, not SYMBOL_INTERNED_IN_INITIAL_OBARRAY.  Emaxx keys
    // it by an internal name; the image carries that name so the loaded
    // object is the one its obarray's lookups produce, and its cells never
    // touch the initial obarray's symbol of the same Lisp name (the
    // loadup state has such a pair: lisp.el's functions were void after
    // a load until this).
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let program = r#"
        (let* ((ob (obarray-make))
               (sym (intern "car" ob))
               (only (intern "zz-private-only" ob)))
          (set sym 7)
          (fset sym 'cdr)
          (put sym 'zz 'yes)
          (set only 'p)
          (vector ob sym only))"#;
    let form = crate::lisp::reader::Reader::new(program)
        .read()
        .expect("setup parses")
        .expect("setup has a form");
    let graph = interp.eval(&form, &mut env).expect("setup evaluates");
    let bytes = dump(&mut interp, vec![(RootSlot::LoadPath, graph)]);

    let mut target = Interpreter::new();
    let image = load_image(&bytes, &mut target).unwrap_or_else(|error| panic!("load: {error:?}"));
    let vector = image
        .roots
        .iter()
        .find(|(slot, _)| *slot == RootSlot::LoadPath)
        .map(|(_, value)| value.clone())
        .expect("the root vector");
    target
        .install_image(
            &image,
            crate::lisp::eval::PdumperLoadRecord {
                filename: "zz.pdmp".into(),
                load_time: std::time::Duration::ZERO,
                dump_size: bytes.len() as u64,
            },
        )
        .expect("install");
    target.set_global_binding("zz-v", vector);
    let probe = r#"
        (let ((ob (aref zz-v 0)) (sym (aref zz-v 1)) (only (aref zz-v 2)))
          (list (symbol-value sym) (symbol-function sym) (symbol-plist sym)
                (symbol-name sym) (eq sym 'car) (eq (intern-soft "car" ob) sym)
                (eq (intern "car" ob) sym) (boundp 'car) (subrp (symbol-function 'car))
                (symbol-plist 'car) (eq (intern-soft "zz-private-only" ob) only)
                (symbol-value only) (boundp 'zz-private-only)
                (eq (intern "zz-new" ob) (intern "zz-new" ob))
                (length (let (all) (mapatoms #'(lambda (s) (setq all (cons s all))) ob) all))))"#;
    let form = crate::lisp::reader::Reader::new(probe)
        .read()
        .expect("probe parses")
        .expect("probe has a form");
    let value = target
        .eval(&form, &mut Vec::new())
        .unwrap_or_else(|error| panic!("probe: {error:?}"));
    let printed =
        crate::lisp::primitives::call(&mut target, "prin1-to-string", &[value], &mut Vec::new())
            .expect("print");
    assert_eq!(
        string_like(&printed).expect("printed").text,
        "(7 cdr (zz yes) \"car\" nil t t nil t nil t p nil t 3)"
    );
}

#[test]
fn image_round_trips_buffers_markers_finalizers_and_nilled_frames() {
    fn printed(interp: &mut Interpreter, value: &Value) -> String {
        let value = crate::lisp::native_comp::call_c_primitive(
            interp,
            &mut Vec::new(),
            "prin1-to-string",
            std::slice::from_ref(value),
        )
        .unwrap_or_else(|error| panic!("prin1-to-string: {error:?}"));
        string_like(&value).expect("a string").text
    }
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    // A buffer with text (an out-of-Unicode character in it), a text
    // property, a local variable, its own syntax table, a mark, a
    // narrowing, undo entries (an insertion, a deletion of propertized
    // text, a boundary) and a modtime; a marker into it and a detached
    // one; a deleted overlay; two finalizers; the frame and terminal.
    let program = r#"
        (let* ((buf (get-buffer-create "zz-dump-buf"))
               (m1 (make-marker))
               (m2 (make-marker))
               (f1 (make-finalizer 'car))
               (f2 (make-finalizer 'cdr))
               ov)
          (set-buffer buf)
          (insert "héllo wörld")
          (insert 2097152)
          (put-text-property 1 3 'face 'bold)
          (set (make-local-variable 'zz-dump-local) 42)
          (set-syntax-table (make-char-table 'syntax-table))
          (set-visited-file-modtime '(0 100))
          (goto-char 4)
          (set-marker (mark-marker) 2)
          (set-marker m1 3 buf)
          (set-marker-insertion-type m1 t)
          (setq ov (make-overlay 1 2))
          (overlay-put ov 'zz-prop 'yes)
          (delete-overlay ov)
          (delete-region 1 2)
          (undo-boundary)
          (narrow-to-region 2 6)
          (vector buf m1 m2 f1 f2 (selected-frame) ov
                  (let ((killed (get-buffer-create "zz-killed")))
                    (kill-buffer killed)
                    killed)))"#;
    let form = crate::lisp::reader::Reader::new(program)
        .read()
        .expect("setup parses")
        .expect("setup has a form");
    let graph = interp.eval(&form, &mut env).expect("setup evaluates");
    let Value::Vector(source_vector) = &graph else {
        panic!("root vector")
    };
    let source = source_vector.slots().clone();
    let Value::Buffer(source_buffer) = &source[0] else {
        panic!("buffer")
    };
    let source_id = source_buffer.id;
    interp.set_buffer_local_hook(source_id, "zz-dump-hook", vec![Value::symbol("car")]);
    let source_undo_value = interp
        .get_buffer_by_id(source_id)
        .expect("live")
        .undo_list_value();
    let source_undo = printed(&mut interp, &source_undo_value);
    let source_syntax_table = interp
        .buffer_syntax_table_id(source_id)
        .expect("the buffer set a syntax table");
    let source_finalizers = interp.finalizer_ids();
    let terminal = Value::Terminal(interp.terminals.first().expect("initial terminal").id);
    let roots = vec![
        (RootSlot::LoadPath, graph.clone()),
        (RootSlot::QuitFlag, terminal),
    ];
    let bytes = dump(&mut interp, roots);

    let mut target = Interpreter::new();
    let image = load_image(&bytes, &mut target).unwrap_or_else(|error| panic!("load: {error:?}"));
    let root = |slot: RootSlot| {
        image
            .roots
            .iter()
            .find(|(candidate, _)| *candidate == slot)
            .map(|(_, value)| value.clone())
            .unwrap_or_else(|| panic!("root {slot:?} missing"))
    };
    let Value::Vector(vector) = root(RootSlot::LoadPath) else {
        panic!("root vector")
    };
    let slots = vector.slots().clone();

    // The buffer: text, positions, narrowing, flags, property spans, the
    // side list, the undo entries, the modtime, the local binding, the
    // syntax table, the mark.
    let Value::Buffer(loaded_buffer) = &slots[0] else {
        panic!("buffer")
    };
    assert_eq!(loaded_buffer.id, source_id);
    let buffer = target
        .get_buffer_by_id(source_id)
        .expect("the buffer was installed");
    assert_eq!(buffer.name, "zz-dump-buf");
    assert_eq!(
        buffer.full_buffer_string(),
        format!("éllo wörld{}", crate::lisp::json::INVALID_UNICODE_SENTINEL)
    );
    assert_eq!(buffer.extended_char_at(11), Some(0x20_0000));
    assert_eq!(buffer.point(), 3);
    assert_eq!(buffer.restriction(), (2, 6));
    // The deletion at 1 moved the mark (set at 2) and m1 (set at 3) back.
    assert_eq!(buffer.mark(), Some(1));
    assert!(buffer.is_multibyte());
    assert!(buffer.is_modified());
    assert_eq!(
        buffer.full_property_spans(),
        vec![TextPropertySpan {
            start: 1,
            end: 2,
            props: vec![("face".into(), Value::symbol("bold"))],
        }]
    );
    assert_eq!(
        buffer
            .visited_file_modtime()
            .map(|modtime| modtime.modified),
        Some(std::time::UNIX_EPOCH + std::time::Duration::from_secs(100))
    );
    assert!(buffer.overlays.iter().all(|overlay| overlay.is_dead()));
    let loaded_undo = buffer.undo_list_value();
    assert_eq!(printed(&mut target, &loaded_undo), source_undo);
    let locals = target.buffer_local_cells(source_id);
    assert_eq!(
        locals.len(),
        3,
        "default-directory, buffer-read-only, zz-dump-local: {locals:?}"
    );
    assert!(
        locals
            .iter()
            .any(|(symbol, value)| symbol == "zz-dump-local" && *value == Value::Integer(42))
    );
    assert_eq!(
        target.buffer_syntax_table_id(source_id),
        Some(source_syntax_table)
    );
    assert!(target.find_char_table(source_syntax_table).is_some());
    let mark_marker = target
        .buffer_mark_marker_id(source_id)
        .expect("the mark marker relation");
    let mark = target.find_marker(mark_marker).expect("mark marker");
    assert_eq!(mark.buffer_id, Some(source_id));
    assert_eq!(mark.position, Some(1));
    assert_eq!(mark.mark_buffer_id, Some(source_id));

    // The markers: one into the buffer with its insertion type, one
    // detached.
    let Value::Marker(m1) = slots[1] else {
        panic!("marker")
    };
    let m1 = target.find_marker(m1).expect("marker 1");
    assert_eq!(m1.buffer_id, Some(source_id));
    assert_eq!(m1.position, Some(2));
    assert!(m1.insertion_type);
    let Value::Marker(m2) = slots[2] else {
        panic!("marker")
    };
    let m2 = target.find_marker(m2).expect("marker 2");
    assert_eq!(m2.buffer_id, None);
    assert_eq!(m2.position, None);
    assert_eq!(
        target.buffer_marker_ids(source_id).len(),
        interp.buffer_marker_ids(source_id).len()
    );

    // The finalizers, in list order, with their functions.
    let Value::Finalizer(f1) = slots[3] else {
        panic!("finalizer")
    };
    let Value::Finalizer(f2) = slots[4] else {
        panic!("finalizer")
    };
    assert_eq!(target.finalizer_ids(), source_finalizers);
    assert_eq!(target.finalizer_ids(), vec![f1, f2]);
    assert_eq!(target.finalizer_function(f1), Some(Value::symbol("car")));
    assert_eq!(target.finalizer_function(f2), Some(Value::symbol("cdr")));

    // The frame is nilled: a dead frame object of its own, beside the
    // live initial frame of the loading process.  The terminal is nilled
    // likewise: a dead terminal beside the live initial one.
    let Value::Frame(frame) = slots[5] else {
        panic!("frame")
    };
    let state = target.frame_state(frame).expect("dead frame installed");
    assert!(!state.live);
    assert_eq!(state.name, Value::Nil);
    assert!(
        target
            .frame_state(target.selected_frame_id)
            .is_some_and(|frame| frame.live)
    );
    assert_ne!(frame, target.selected_frame_id);
    let Value::Terminal(dead_terminal) = root(RootSlot::QuitFlag) else {
        panic!("terminal")
    };
    assert!(
        target
            .terminal_state(dead_terminal)
            .is_some_and(|terminal| !terminal.live)
    );
    assert_ne!(
        Value::Terminal(dead_terminal),
        Value::Terminal(target.terminals.first().expect("initial terminal").id)
    );
    assert!(target.terminals.first().expect("initial terminal").live);

    // The deleted overlay, on the buffer's list, with its properties.
    let Value::Overlay(ov) = slots[6] else {
        panic!("overlay")
    };
    let overlay = target.find_overlay(ov).expect("overlay installed");
    assert!(overlay.is_dead());
    assert_eq!(
        overlay.plist,
        vec![(Value::symbol("zz-prop"), Value::symbol("yes"))]
    );
    assert_eq!(target.overlay_holder_id(ov), Some(source_id));

    // The local hook list came with the buffer.
    assert_eq!(
        target.buffer_local_hook_lists(source_id),
        vec![("zz-dump-hook".to_owned(), vec![Value::symbol("car")])]
    );

    // The killed buffer is an object with no buffer behind it.
    let Value::Buffer(killed) = &slots[7] else {
        panic!("killed buffer")
    };
    assert!(target.get_buffer_by_id(killed.id).is_none());
    assert!(!target.has_buffer_id(killed.id));
}

#[test]
fn image_refuses_buffers_with_overlays_as_gnu_does() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let program = r#"
        (let ((buf (get-buffer-create "zz-overlaid")))
          (set-buffer buf)
          (insert "text")
          (make-overlay 1 3)
          buf)"#;
    let form = crate::lisp::reader::Reader::new(program)
        .read()
        .expect("setup parses")
        .expect("setup has a form");
    let buffer = interp.eval(&form, &mut env).expect("setup evaluates");
    let mut ctx = DumpContext::new(false, interp.main_thread_record_id());
    let result = write_image(
        &mut ctx,
        &interp,
        RootSource::Explicit(vec![(RootSlot::LoadPath, buffer)]),
    );
    match result {
        Err(super::context::DumpError::Lisp(crate::lisp::types::LispError::Signal(message))) => {
            assert_eq!(message, "dumping overlays is not yet implemented");
        }
        Err(super::context::DumpError::Lisp(other)) => panic!("other error: {other:?}"),
        Err(super::context::DumpError::Unsupported(unsupported)) => {
            panic!("unsupported: {}", unsupported.message)
        }
        Ok(_) => panic!("a buffer with a live overlay was dumped"),
    }
}

#[test]
fn image_carries_the_root_groups_as_pdumper_c_dumps_the_static_roots() {
    fn printed(interp: &mut Interpreter, value: &Value) -> String {
        let value = crate::lisp::native_comp::call_c_primitive(
            interp,
            &mut Vec::new(),
            "prin1-to-string",
            std::slice::from_ref(value),
        )
        .unwrap_or_else(|error| panic!("prin1-to-string: {error:?}"));
        string_like(&value).expect("a string").text
    }
    // A bare interpreter with state in the groups a fresh one leaves
    // empty: a second buffer, keys, a detached forwarded variable, a
    // charset with an alias, a timer, an ert test, a labeled restriction
    // in the current buffer, a fringe bitmap, a composition, a face.
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let program = r#"
        (progn
          (get-buffer-create "zz-second")
          (define-fringe-bitmap 'zz-bitmap [1 2 3])
          (let ((ov (make-marker))) ov)
          (internal--labeled-narrow-to-region 1 1 'zz-label)
          (define-charset-alias 'zz-alias 'ascii)
          (let ((m (make-sparse-keymap)) (p (make-sparse-keymap)))
            (set-keymap-parent m p)
            (define-key p "\M-q" 'ignore)
            (define-key m "\C-c" 'car))
          t)"#;
    let form = crate::lisp::reader::Reader::new(program)
        .read()
        .expect("setup parses")
        .expect("setup has a form");
    interp.eval(&form, &mut env).expect("setup evaluates");
    interp.keyboard_input.recent_keys = vec![Value::Integer(97), Value::symbol("f1")];
    interp.keyboard_input.command_keys = vec![Value::Integer(97)];
    interp
        .detached_forwarded_variables
        .insert("zz-detached".into(), Value::list([Value::Integer(1)]));
    interp.schedule_timer_after(Value::symbol("car"), vec![Value::Nil], 1000.0, Some(5.0));
    interp.ert_tests.push(crate::lisp::eval::ErtTestDefinition {
        name: "zz-test".into(),
        body: Value::list([Value::symbol("should"), Value::T]),
        source_file: Some("zz.el".into()),
        tags: vec!["fast".into()],
        expected_result: ":passed".into(),
    });
    interp
        .composition_states
        .push(crate::lisp::eval::CompositionState {
            components: Value::vector([Value::Integer(97), Value::Integer(98)]),
            relative: true,
            width: 2,
        });
    let source_groups = interp.dump_root_groups();
    let source_printed = source_groups
        .iter()
        .map(|(slot, value)| (*slot, printed(&mut interp, value)))
        .collect::<Vec<_>>();
    assert!(
        source_printed
            .iter()
            .any(|(slot, text)| *slot == RootSlot::BufferAlist && text.contains("zz-second"))
    );
    assert!(
        source_printed
            .iter()
            .any(|(slot, text)| *slot == RootSlot::TimerList && text.contains("car"))
    );

    let mut ctx = DumpContext::new(true, interp.main_thread_record_id());
    let summary = match write_image(&mut ctx, &interp, RootSource::Interpreter) {
        Ok(summary) => summary,
        Err(super::context::DumpError::Unsupported(unsupported)) => {
            ctx.print_paths_to_root(&mut interp, &mut Vec::new(), &unsupported.object);
            panic!("unsupported object: {}", unsupported.message)
        }
        Err(super::context::DumpError::Lisp(error)) => panic!("dump failed: {error:?}"),
    };
    assert!(summary.hot_bytes > 0);
    let bytes = ctx.buffer().to_vec();
    let mut target = Interpreter::new();
    let image = load_image(&bytes, &mut target).unwrap_or_else(|error| panic!("load: {error:?}"));
    for (slot, _) in &source_groups {
        assert!(
            image.roots.iter().any(|(candidate, _)| candidate == slot),
            "root group {slot:?} is in the image"
        );
    }
    // Every group prints the same from the restored interpreter (the
    // timer's due time is the seconds still to wait, which passed).
    let target_groups = target.dump_root_groups();
    for ((slot, source_text), (target_slot, target_value)) in
        source_printed.iter().zip(&target_groups)
    {
        assert_eq!(slot, target_slot);
        let target_text = printed(&mut target, target_value);
        if *slot == RootSlot::TimerList {
            assert!(target_text.starts_with("([car (nil) "), "{target_text}");
            assert!(target_text.ends_with(" 5.0 car])"), "{target_text}");
            continue;
        }
        assert_eq!(&target_text, source_text, "root group {slot:?}");
    }
    // The buffers behind the alist are live in the target, in order.
    assert_eq!(
        target
            .buffer_list
            .iter()
            .map(|(_, name)| name.clone())
            .collect::<Vec<_>>(),
        interp
            .buffer_list
            .iter()
            .map(|(_, name)| name.clone())
            .collect::<Vec<_>>()
    );
    assert!(target.has_buffer("zz-second"));
    // The keymap facade records came through as a group, and the loader
    // rebuilt the view-to-record index: the child's list resolves to its
    // record, whose parent answers a binding through the list.
    let keymap_records = image
        .roots
        .iter()
        .find(|(slot, _)| *slot == RootSlot::KeymapRecords)
        .map(|(_, value)| value.to_vec().expect("a list"))
        .expect("the keymap records group");
    assert!(keymap_records.len() >= 2, "{keymap_records:?}");
    let mut resolved = 0;
    for record in &keymap_records {
        let Value::Record(id) = record else {
            panic!("not a record: {record:?}")
        };
        let view = target
            .find_record(*id)
            .expect("keymap record installed")
            .slots
            .get(crate::lisp::primitives::values::KEYMAP_PUBLIC_VIEW_SLOT)
            .cloned()
            .expect("the public view");
        target.set_global_binding("zz-loaded-keymap", view);
        let form = crate::lisp::reader::Reader::new(
            "(list (keymapp zz-loaded-keymap) (lookup-key zz-loaded-keymap \"\\C-c\") \
                   (keymapp (keymap-parent zz-loaded-keymap)) (lookup-key zz-loaded-keymap \"\\M-q\"))",
        )
        .read()
        .expect("probe parses")
        .expect("a form");
        let answer = target
            .eval(&form, &mut Vec::new())
            .expect("probe evaluates");
        if printed(&mut target, &answer) == "(t car t ignore)" {
            resolved += 1;
        }
    }
    assert_eq!(resolved, 1, "the child keymap resolves through its record");
}

#[test]
fn image_refuses_pending_transient_state_it_cannot_carry() {
    let mut interp = Interpreter::new();
    interp.pending_thread_events.push(Value::symbol("zz-event"));
    let mut ctx = DumpContext::new(false, interp.main_thread_record_id());
    match write_image(&mut ctx, &interp, RootSource::Interpreter) {
        Err(super::context::DumpError::Lisp(crate::lisp::types::LispError::Signal(message))) => {
            assert_eq!(
                message,
                "cannot dump with 1 entries of pending_thread_events pending"
            );
        }
        Err(super::context::DumpError::Lisp(other)) => panic!("other error: {other:?}"),
        Err(super::context::DumpError::Unsupported(unsupported)) => {
            panic!("unsupported: {}", unsupported.message)
        }
        Ok(_) => panic!("pending transient state was dumped"),
    }
}
