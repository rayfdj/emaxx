//! D08 controls: an image written from explicit roots reads back as the
//! same object graph (sharing and cycles intact, at different addresses),
//! and the file validation refuses what pdumper_load refuses.

use super::context::{DumpContext, object_key};
use super::image::*;
use super::load::{LoadError, load_image, validate_header};
use super::{RootSource, write_image};
use crate::intervals::{Interval, Intervals};
use crate::lisp::eval::Interpreter;
use crate::lisp::primitives::strings::{make_shared_string_value_with_extended_chars, string_like};
use crate::lisp::types::{Kind, LispErrorKind, SymbolName, Value};
use std::collections::HashMap;

fn dump(interp: &mut Interpreter, roots: Vec<(RootSlot, Value)>) -> Vec<u8> {
    let mut ctx = DumpContext::new(true, interp.main_thread_record_id());
    let summary = match write_image(&mut ctx, interp, RootSource::Explicit(roots)) {
        Ok(summary) => summary,
        Err(super::context::DumpError::Unsupported(unsupported)) => {
            ctx.print_paths_to_root(
                interp,
                &mut crate::lisp::types::Env::new(),
                &unsupported.object,
            );
            panic!("unsupported object: {}", unsupported.message)
        }
        Err(super::context::DumpError::Lisp(error)) => panic!("dump failed: {error:?}"),
    };
    assert_eq!(summary.header_bytes, HEADER_LEN as u32);
    ctx.buffer().to_vec()
}

#[test]
fn symbol_constant_state_survives_image_copy_and_restore() {
    use crate::lisp::types::Env;

    fn check(interp: &mut Interpreter, graph: Value) {
        interp.select_symbol_owner();
        let Kind::Vector(slots) = graph.kind() else {
            panic!("constant graph remains a vector")
        };
        let old_keyword = slots.get(0).expect("detached keyword in graph");
        let new_keyword = slots.get(1).expect("replacement keyword in graph");
        let private = slots.get(2).expect("private keyword in graph");
        let old_constant = slots.get(3).expect("detached built-in constant in graph");
        let fresh = slots.get(4).expect("replacement plain symbol in graph");
        let alias = slots.get(5).expect("constant alias in graph");
        let previous = slots.get(6).expect("original constant value in graph");
        assert!(!old_keyword.eq_value(new_keyword));
        assert!(!old_constant.eq_value(fresh));
        let mut env = Env::new();
        let mut call = |name: &str, args: &[Value]| {
            crate::lisp::primitives::call(interp, name, args, &mut env)
        };
        for (symbol, value) in [
            (old_keyword, old_keyword),
            (old_constant, previous),
            (alias, old_keyword),
        ] {
            for read in ["symbol-value", "default-value", "default-toplevel-value"] {
                assert!(
                    call(read, &[symbol])
                        .expect("constant object operation survives image relocation")
                        .eq_value(value),
                    "{read}"
                );
            }
            assert!(
                call("special-variable-p", &[symbol])
                    .expect("constant object operation survives image relocation")
                    .is_truthy()
            );
            for store in ["set", "set-default", "set-default-toplevel-value"] {
                let error = call(store, &[symbol, Value::Integer(47)])
                    .expect_err("the actual constant object rejects stores");
                let LispErrorKind::SignalValue(error) = error.into_kind() else {
                    panic!("setting-constant carries the actual symbol")
                };
                assert_eq!(
                    error
                        .to_vec()
                        .expect("setting-constant condition is a list"),
                    vec![Value::symbol("setting-constant"), symbol],
                );
            }
        }
        assert!(
            call("keywordp", &[old_keyword])
                .expect("constant object operation survives image relocation")
                .is_nil()
        );
        assert!(
            call("keywordp", &[new_keyword])
                .expect("constant object operation survives image relocation")
                .is_truthy()
        );
        assert!(
            call("set", &[new_keyword, new_keyword])
                .expect("constant object operation survives image relocation")
                .eq_value(new_keyword)
        );
        assert!(
            call("indirect-variable", &[alias])
                .expect("constant object operation survives image relocation")
                .eq_value(old_keyword)
        );
        for symbol in [private, fresh] {
            assert!(
                call("default-boundp", &[symbol])
                    .expect("constant object operation survives image relocation")
                    .is_nil()
            );
            assert!(
                call("special-variable-p", &[symbol])
                    .expect("constant object operation survives image relocation")
                    .is_nil()
            );
            call("set-default", &[symbol, Value::Integer(71)])
                .expect("constant object operation survives image relocation");
            assert_eq!(
                call("symbol-value", &[symbol])
                    .expect("constant object operation survives image relocation"),
                Value::Integer(71)
            );
            call("makunbound", &[symbol])
                .expect("constant object operation survives image relocation");
            assert!(
                call("boundp", &[symbol])
                    .expect("constant object operation survives image relocation")
                    .is_nil()
            );
        }
    }

    let mut source = Interpreter::new();
    let form = crate::lisp::reader::Reader::new(
        r#"(let* ((old (intern ":image-constant-λ"))
                  (constant (intern "most-positive-fixnum"))
                  (previous (symbol-value constant))
                  (alias (make-symbol "image-constant-alias")))
             (defvaralias alias old)
             (unintern old)
             (unintern constant)
             (vector old (intern ":image-constant-λ")
                     (make-symbol ":image-constant-λ")
                     constant (intern "most-positive-fixnum") alias previous))"#,
    )
    .read()
    .expect("constant image setup reads")
    .expect("constant image setup form");
    let graph = source
        .eval(&form, &mut Env::new())
        .expect("constant image setup");
    check(&mut source, graph);
    source.set_global_binding("constant-image-root", graph);
    let mut copied = source.deep_clone_image();
    let copied_graph = copied
        .lookup_var("constant-image-root", &Env::new())
        .expect("constant object operation survives image relocation");
    check(&mut copied, copied_graph);

    source.select_symbol_owner();
    let bytes = dump(&mut source, vec![(RootSlot::LoadPath, graph)]);
    let mut restored = Interpreter::new();
    let image = load_image(&bytes, &mut restored).expect("constant image loads");
    let loaded_graph = image
        .roots
        .iter()
        .find(|(slot, _)| *slot == RootSlot::LoadPath)
        .map(|(_, value)| *value)
        .expect("constant object operation survives image relocation");
    restored
        .install_image(
            &image,
            crate::lisp::eval::PdumperLoadRecord {
                filename: "symbol-constants.pdmp".into(),
                load_time: std::time::Duration::ZERO,
                dump_size: bytes.len() as u64,
            },
        )
        .expect("constant image installs");
    check(&mut restored, loaded_graph);
}

#[test]
fn standard_unintern_images_keep_both_same_named_objects() {
    use crate::lisp::types::Env;

    fn check(interp: &mut Interpreter, graph: Value) {
        interp.select_symbol_owner();
        let Kind::Vector(slots) = graph.kind() else {
            panic!("the symbol graph remains a vector")
        };
        let old = slots.get(0).expect("old symbol");
        let new = slots.get(1).expect("new symbol");
        assert!(!old.eq_value(new), "same spelling must not merge objects");
        assert!(old.eq_value(slots.get(2).expect("shared old symbol")));
        assert!(new.eq_value(slots.get(3).expect("shared new symbol")));
        let mut env = Env::new();
        for (symbol, value) in [(old, 17), (new, 43)] {
            assert_eq!(
                crate::lisp::primitives::call(interp, "symbol-value", &[symbol], &mut env)
                    .expect("independent symbol value"),
                Value::Integer(value)
            );
            assert_eq!(
                crate::lisp::primitives::call(interp, "funcall", &[symbol], &mut env)
                    .expect("independent function cell"),
                Value::Integer(value)
            );
            assert!(
                crate::lisp::primitives::call(
                    interp,
                    "get",
                    &[symbol, Value::symbol("owner")],
                    &mut env,
                )
                .expect("self-referential plist")
                .eq_value(symbol)
            );
        }
        assert!(
            crate::lisp::primitives::call(interp, "intern-soft", &[old], &mut env)
                .expect("detached membership")
                .is_nil()
        );
        assert!(
            crate::lisp::primitives::call(interp, "intern-soft", &[new], &mut env)
                .expect("current membership")
                .eq_value(new)
        );
    }

    let mut source = Interpreter::new();
    let form = crate::lisp::reader::Reader::new(
        r#"(let* ((name (concat "symbol-image-" "detached"))
                  (old (intern name)) new)
             (set old 17)
             (fset old '(lambda () 17))
             (setplist old (list 'owner old))
             (unintern old)
             (setq new (intern (copy-sequence name)))
             (set new 43)
             (fset new '(lambda () 43))
             (setplist new (list 'owner new))
             (vector old new old new))"#,
    )
    .read()
    .expect("image setup reads")
    .expect("image setup form");
    let graph = source.eval(&form, &mut Env::new()).expect("image setup");
    check(&mut source, graph);
    source.set_global_binding("membership-image-root", graph);
    let mut clone = source.deep_clone_image();
    let copied = clone
        .lookup_var("membership-image-root", &Env::new())
        .expect("cloned root");
    check(&mut clone, copied);

    source.select_symbol_owner();
    let bytes = dump(&mut source, vec![(RootSlot::LoadPath, graph)]);
    let mut restored = Interpreter::new();
    let image = load_image(&bytes, &mut restored).expect("symbol image loads");
    let loaded = image
        .roots
        .iter()
        .find(|(slot, _)| *slot == RootSlot::LoadPath)
        .map(|(_, value)| *value)
        .expect("loaded graph");
    restored
        .install_image(
            &image,
            crate::lisp::eval::PdumperLoadRecord {
                filename: "symbol-membership.pdmp".into(),
                load_time: std::time::Duration::ZERO,
                dump_size: bytes.len() as u64,
            },
        )
        .expect("symbol image installs");
    check(&mut restored, loaded);
}

#[test]
fn string_images_preserve_empty_roots_distinct_headers_and_restored_mutability() {
    use crate::lisp::types::{Env, StringObjectRef};
    let mut source = Interpreter::new();
    let mut environment = Env::new();
    source.define_special_variable("purify-flag", Value::T);
    let unibyte = Value::StringObject(StringObjectRef::from_unibyte(Vec::new()));
    let multibyte = Value::StringObject(
        StringObjectRef::from_storage(Vec::new(), 0, true).expect("empty multibyte"),
    );
    let ordinary = Value::string("mutable after dump");
    let mut slots = vec![unibyte, multibyte];
    for value in [unibyte, multibyte, ordinary] {
        slots.push(
            crate::lisp::primitives::purecopy_value(&mut source, &value, &mut environment)
                .expect("pure copy"),
        );
    }
    slots.push(slots[4]);
    assert!(!slots[0].eq_value(slots[2]));
    assert!(!slots[1].eq_value(slots[3]));
    let graph = Value::vector(slots);
    source.set_global_binding("string-image-root", graph);
    let cloned = source.deep_clone_image();
    let Kind::Vector(cloned_graph) = cloned
        .lookup_var("string-image-root", &Env::new())
        .expect("cloned graph")
        .kind()
    else {
        panic!("cloned vector")
    };
    for index in 2..=4 {
        let Kind::StringObject(state) = cloned_graph.get(index).expect("cloned pure string").kind()
        else {
            panic!("cloned string")
        };
        assert!(
            state.is_pure(),
            "image cloning preserves read-only allocation state"
        );
    }
    let bytes = dump(&mut source, vec![(RootSlot::LoadPath, graph)]);
    let mut target = Interpreter::new();
    let image = load_image(&bytes, &mut target).expect("restore string graph");
    let loaded = image
        .roots
        .iter()
        .find(|(slot, _)| *slot == RootSlot::LoadPath)
        .expect("string graph root")
        .1;
    let Kind::Vector(vector) = loaded.kind() else {
        panic!("string graph vector")
    };
    let restored = vector.slots().collect::<Vec<_>>();
    assert!(restored[0].eq_value(unibyte));
    assert!(restored[1].eq_value(multibyte));
    assert!(!restored[0].eq_value(restored[2]));
    assert!(!restored[1].eq_value(restored[3]));
    assert!(restored[4].eq_value(restored[5]));
    target.set_global_binding("string-image-root", loaded);
    let cloned = target.deep_clone_image();
    let Kind::Vector(cloned_graph) = cloned
        .lookup_var("string-image-root", &Env::new())
        .expect("cloned restored graph")
        .kind()
    else {
        panic!("cloned restored vector")
    };
    assert!(
        !cloned_graph
            .get(0)
            .expect("empty root")
            .eq_value(cloned_graph.get(2).expect("distinct empty"))
    );
    assert!(
        !cloned_graph
            .get(1)
            .expect("multibyte empty root")
            .eq_value(cloned_graph.get(3).expect("distinct multibyte empty"))
    );
    for &value in &restored {
        let Kind::StringObject(state) = value.kind() else {
            panic!("restored string")
        };
        // GNU's dumped headers are outside PURE_P's active pure space.
        assert!(!state.is_pure());
    }
    crate::lisp::primitives::call(
        &mut target,
        "aset",
        &[restored[4], Value::Integer(0), Value::Integer(90)],
        &mut Env::new(),
    )
    .expect("write restored formerly-pure string");
    assert_eq!(
        restored[5].as_string().expect("shared restored bytes"),
        "Zutable after dump"
    );
}

#[test]
fn captured_menu_case_table_is_an_independent_image_root() {
    use crate::lisp::primitives::restore_unicode_menu_case_table;
    let mut source = Interpreter::new();
    // This anonymous table is deliberately absent from the Unicode property
    // alist: GNU's captured static root must survive that registry changing.
    let table = source.make_char_table(None, Value::Nil);
    let Kind::CharTable(original) = table.kind() else {
        panic!("char table")
    };
    original.set(81, Value::Integer(122));
    restore_unicode_menu_case_table(&mut source, table).expect("install captured table");
    let roots = source.dump_root_groups();
    assert!(
        roots.iter().any(|(slot, value)| {
            *slot == RootSlot::UnicodeMenuCaseTable && value.eq_value(table)
        })
    );
    let bytes = dump(&mut source, roots);
    restore_unicode_menu_case_table(&mut source, Value::Nil).expect("clear before load");
    let mut target = Interpreter::new();
    assert!(target.unicode_menu_case_table.is_nil());
    let image = load_image(&bytes, &mut target).expect("restore captured table");
    let loaded = image
        .roots
        .iter()
        .find(|(slot, _)| *slot == RootSlot::UnicodeMenuCaseTable)
        .expect("table root is present")
        .1;
    assert!(target.unicode_menu_case_table.eq_value(loaded));
    assert!(
        !loaded.eq_value(table),
        "the loaded table has its own allocation"
    );
    let Kind::CharTable(restored) = loaded.kind() else {
        panic!("restored char table")
    };
    assert_eq!(restored.get(81), Value::Integer(122));
}

#[test]
fn captured_menu_case_table_belongs_to_its_editor_and_image() {
    use crate::lisp::primitives::restore_unicode_menu_case_table;
    let mut source = Interpreter::new();
    let table = source.make_char_table(None, Value::Nil);
    let Kind::CharTable(original) = table.kind() else {
        panic!("char table")
    };
    original.set(81, Value::Integer(122));
    restore_unicode_menu_case_table(&mut source, table).expect("capture source table");
    let second = Interpreter::new();
    assert!(second.unicode_menu_case_table.is_nil());
    assert!(source.unicode_menu_case_table.eq_value(table));
    let image = source.deep_clone_image();
    let Kind::CharTable(copied) = image.unicode_menu_case_table.kind() else {
        panic!("copied char table")
    };
    assert!(!Value::CharTable(copied).eq_value(table));
    assert_eq!(copied.get(81), Value::Integer(122));
    original.set(81, Value::Integer(97));
    assert_eq!(copied.get(81), Value::Integer(122));
    assert!(second.unicode_menu_case_table.is_nil());
}

#[test]
fn text_conversion_buffer_fields_preserve_flags_sharing_and_cycles_in_images() {
    fn check(first: Value, second: Value, original_graph: Value) {
        let Kind::Buffer(first_buffer) = first.kind() else {
            panic!("first buffer")
        };
        let Kind::Buffer(second_buffer) = second.kind() else {
            panic!("second buffer")
        };
        let first_state = first_buffer.borrow();
        let second_state = second_buffer.borrow();
        assert!(!first_state.text_conversion_style_is_local);
        assert!(second_state.text_conversion_style_is_local);
        let graph = first_state.text_conversion_style;
        assert_eq!(graph.word(), second_state.text_conversion_style.word());
        assert_ne!(graph.word(), original_graph.word());
        let Kind::Vector(slots) = graph.kind() else {
            panic!("shared graph")
        };
        assert_eq!(slots.get(0).expect("buffer cycle").word(), first.word());
        assert_eq!(
            slots.get(1).expect("payload"),
            Value::list([Value::Integer(353)])
        );
    }

    let mut source = Interpreter::new();
    let (first_id, _) = source.create_buffer("style-no-local-277");
    let (second_id, _) = source.create_buffer("style-local-281");
    let first = source.buffer_value(first_id).expect("first buffer");
    let second = source.buffer_value(second_id).expect("second buffer");
    let graph = Value::vector([first, Value::list([Value::Integer(353)])]);
    source
        .get_buffer_by_id_mut(first_id)
        .expect("C store")
        .text_conversion_style = graph;
    source.set_buffer_local_value(second_id, "text-conversion-style", graph);
    for id in [first_id, second_id] {
        assert!(
            source
                .buffer_local_cells(id)
                .iter()
                .all(|(name, _)| name != "text-conversion-style"),
            "the native field has no duplicate local payload; unrelated locals remain"
        );
    }
    let clone = source.deep_clone_image();
    check(
        clone.buffer_value(first_id).expect("cloned first"),
        clone.buffer_value(second_id).expect("cloned second"),
        graph,
    );

    let bytes = dump(
        &mut source,
        vec![(RootSlot::LoadPath, Value::vector([first, second]))],
    );
    let mut target = Interpreter::new();
    let image = load_image(&bytes, &mut target).expect("restore native buffer fields");
    let restored = image
        .roots
        .iter()
        .find(|(slot, _)| *slot == RootSlot::LoadPath)
        .expect("buffer roots")
        .1;
    let Kind::Vector(slots) = restored.kind() else {
        panic!("root vector")
    };
    check(
        slots.get(0).expect("restored first"),
        slots.get(1).expect("restored second"),
        graph,
    );
}

#[test]
fn positioned_symbols_are_rejected_as_in_gnu_dump_vectorlike() {
    let interpreter = Interpreter::new();
    let object = Value::positioned_symbol(Value::symbol("undumpable-position"), Value::Integer(19));
    let mut context = DumpContext::new(true, interpreter.main_thread_record_id());
    let result = write_image(
        &mut context,
        &interpreter,
        RootSource::Explicit(vec![(RootSlot::LoadPath, Value::vector([object, object]))]),
    );
    match result {
        Err(super::context::DumpError::Unsupported(error)) => {
            assert_eq!(error.object.word(), object.word());
            assert_eq!(error.message, "pseudovector type 6");
        }
        Err(super::context::DumpError::Lisp(error)) => panic!("unrelated dump error: {error}"),
        Ok(_) => panic!("GNU rejects a reachable PVEC_SYMBOL_WITH_POS"),
    }
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
    match (a.kind(), b.kind()) {
        (Kind::Nil, Kind::Nil) | (Kind::T, Kind::T) | (Kind::Unbound, Kind::Unbound) => Ok(()),
        (Kind::Integer(x), Kind::Integer(y)) if x == y => Ok(()),
        (Kind::BigInteger(x), Kind::BigInteger(y)) if *x == *y => Ok(()),
        (Kind::Float(x), Kind::Float(y)) if x.to_bits() == y.to_bits() => Ok(()),
        (Kind::StringObject(_), Kind::StringObject(_)) => {
            let x = string_like(a).expect("a string");
            let y = string_like(b).expect("a string");
            if x.text != y.text
                || x.multibyte != y.multibyte
                || x.extended_chars != y.extended_chars
            {
                return Err(format!("string differs: {x:?} vs {y:?}"));
            }
            match (&x.props, &y.props) {
                (None, None) => {}
                (Some(x_intervals), Some(y_intervals)) => {
                    let spans = |intervals: &Intervals| -> Vec<(usize, usize)> {
                        intervals
                            .iter()
                            .map(|(start, end, _)| (start, end))
                            .collect()
                    };
                    if spans(x_intervals) != spans(y_intervals) {
                        return Err(format!("intervals differ: {:?} vs {:?}", x.props, y.props));
                    }
                    for (plist_x, plist_y) in x_intervals.plists().zip(y_intervals.plists()) {
                        graph_matches(&plist_x, &plist_y, seen)?;
                    }
                }
                _ => return Err(format!("intervals differ: {:?} vs {:?}", x.props, y.props)),
            }
            Ok(())
        }
        (Kind::Symbol(x), Kind::Symbol(y)) => {
            let name = |symbol: &SymbolName| string_like(&symbol.lisp_name()).map(|s| s.text);
            if x.as_str() != y.as_str() && name(&x) != name(&y) {
                return Err(format!("symbol differs: {x:?} vs {y:?}"));
            }
            if x.cell().is_uninterned() != y.cell().is_uninterned() {
                return Err(format!("internedness differs: {x:?} vs {y:?}"));
            }
            Ok(())
        }
        (Kind::BuiltinFunc(x), Kind::BuiltinFunc(y)) if x.as_str() == y.as_str() => Ok(()),
        // Identity-bearing kinds compared by the tests through the
        // interpreters that own them.
        (Kind::Closure(left), Kind::Closure(right)) => {
            if left.public_len() != right.public_len() {
                return Err("closure length differs".into());
            }
            for (left, right) in left.slots().zip(right.slots()) {
                graph_matches(&left, &right, seen)?;
            }
            Ok(())
        }
        (Kind::LispRecord(left), Kind::LispRecord(right)) => {
            if left.len() != right.len() {
                return Err("record length differs".into());
            }
            for (left, right) in left.slots().zip(right.slots()) {
                graph_matches(&left, &right, seen)?;
            }
            Ok(())
        }
        (Kind::BoolVector(left), Kind::BoolVector(right)) => {
            if left.bits().ne(right.bits()) {
                return Err("bool vector differs".into());
            }
            Ok(())
        }
        (Kind::CharTable(_), Kind::CharTable(_)) | (Kind::Record(_), Kind::Record(_)) => Ok(()),
        (Kind::Cons(_), Kind::Cons(_)) => {
            graph_matches(&a.car().expect("car"), &b.car().expect("car"), seen)?;
            graph_matches(&a.cdr().expect("cdr"), &b.cdr().expect("cdr"), seen)
        }
        (Kind::Vector(x), Kind::Vector(y)) => {
            let x = x.slots().collect::<Vec<_>>();
            let y = y.slots().collect::<Vec<_>>();
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
    cycle.set_cdr(cycle).expect("setcdr");
    let float = Value::float(2.5);
    let props = Some(Intervals::from_items(
        vec![
            Interval {
                start: 0,
                plist: Value::Nil,
            },
            Interval {
                start: 1,
                plist: Value::list([
                    Value::symbol("face"),
                    Value::symbol("bold"),
                    Value::symbol("shared"),
                    shared,
                ]),
            },
            Interval {
                start: 3,
                plist: Value::Nil,
            },
        ],
        5,
    ));
    let propertized =
        make_shared_string_value_with_extended_chars("héllo".into(), props, true, Vec::new());
    let extended = make_shared_string_value_with_extended_chars(
        format!("a{}b", crate::lisp::json::INVALID_UNICODE_SENTINEL),
        None,
        true,
        vec![(1, 0x20_0000)],
    );
    let unibyte = make_shared_string_value_with_extended_chars(
        format!(
            "raw{}{}",
            crate::lisp::primitives::case::raw_byte_regex_char(0xC3),
            crate::lisp::primitives::case::raw_byte_regex_char(0xA9)
        ),
        None,
        false,
        Vec::new(),
    );
    let multibyte_raw = make_shared_string_value_with_extended_chars(
        format!(
            "x{}",
            crate::lisp::primitives::case::raw_byte_regex_char(0x80)
        ),
        None,
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
        shared,
        shared,
        cycle,
        Value::string("plain ascii"),
        Value::string("ünïcödé"),
        propertized,
        extended,
        unibyte,
        multibyte_raw,
        float,
        float,
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
        uninterned,
        uninterned,
        Value::BuiltinFunc("car".into()),
        Value::BuiltinFunc("car".into()),
        Value::string(""),
        Value::vector([]),
    ]);
    let roots = vec![
        (RootSlot::LoadPath, graph),
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
            .map(|(_, value)| *value)
            .unwrap_or_else(|| panic!("root {slot:?} missing"))
    };
    assert_eq!(root(RootSlot::QuitFlag), Value::Integer(42));
    assert_eq!(root(RootSlot::InhibitQuit), Value::T);
    assert_eq!(root(RootSlot::CurrentGlobalMap), Value::Unbound);
    assert!(
        matches!(root(RootSlot::ThrowOnInput).kind(), Kind::BuiltinFunc(name) if name == "cdr")
    );
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
    let mut env = crate::lisp::types::Env::new();
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
    // The symbols but the watched one are mapped where the image holds
    // them, each the initial obarray's symbol of its name once the image
    // loads.
    let loaded = image
        .roots
        .iter()
        .find(|(slot, _)| *slot == RootSlot::LoadPath)
        .and_then(|(_, value)| value.to_vec().ok())
        .expect("the root list");
    let mapped = |index: usize, name: &str| {
        assert!(
            image.symbols.iter().all(|symbol| symbol.symbol != name),
            "{name} is mapped, not a record"
        );
        assert_eq!(loaded[index], Value::symbol(name), "{name} is interned");
        let Kind::Symbol(symbol) = loaded[index].kind() else {
            panic!("{name} is a symbol")
        };
        symbol
    };
    let special = mapped(0, "zz-dump-special");
    assert_ne!(
        special.cell().flags.get() & crate::lisp::eval::symbol_cell_flags::SPECIAL,
        0
    );
    assert_eq!(
        special.cell().value().and_then(|value| value.to_vec().ok()),
        Some(vec![
            Value::Integer(1),
            Value::Integer(2),
            Value::Integer(3)
        ])
    );
    let alias = mapped(1, "zz-dump-alias");
    assert_eq!(alias.cell().alias(), Some(special));
    let plain = record("zz-dump-plain");
    assert_eq!(plain.value, Some(Value::Integer(7)));
    assert_eq!(
        plain.flags & super::context::FLAG_TRAPPED_WRITE,
        super::context::FLAG_TRAPPED_WRITE
    );
    // data.c keeps the watchers in the `watchers' property.
    assert_eq!(
        plain.plist.to_vec().ok(),
        Some(vec![
            Value::symbol("zz-prop"),
            Value::string("value"),
            Value::symbol("watchers"),
            Value::list([Value::symbol("ignore")]),
        ])
    );
    let function = mapped(3, "zz-dump-fn");
    assert_eq!(function.cell().function.get(), Value::symbol("car"));
    assert_eq!(mapped(4, "zz-dump-void").cell().value(), None);
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
        assert_eq!(
            interpreter
                .symbol_value_cell("executing-kbd-macro")
                .expect("saved macro array"),
            Value::string("saved macro")
        );
        interpreter
            .init_after_pdump_load()
            .expect("initialize loaded process");
        assert_eq!(
            interpreter
                .symbol_value_cell("executing-kbd-macro")
                .expect("new process macro state"),
            Value::Nil
        );
        assert_eq!(
            interpreter
                .symbol_value_cell("executing-kbd-macro-index")
                .expect("saved macro index"),
            Value::Integer(3)
        );
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
            interpreter.lookup_var("default-directory", &crate::lisp::types::Env::new()),
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
    interpreter.set_global_binding("executing-kbd-macro", Value::string("saved macro"));
    interpreter.set_global_binding("executing-kbd-macro-index", Value::Integer(3));
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
    let outer = Value::vector([inner, inner, Value::string("c")]);
    let bytes = dump(&mut interp, vec![(RootSlot::LoadPath, outer)]);
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
    // One vector, two conses and three strings, in the image's blocks,
    // not among the starts: nothing twice.
    assert!(!kinds.contains(&DumpType::Vector));
    assert!(!kinds.contains(&DumpType::Cons));
    assert!(!kinds.contains(&DumpType::String));
    assert_eq!(header.cons_blocks.nr_entries, 2);
    assert_eq!(header.string_blocks.nr_entries, 3);
    assert_eq!(header.vector_blocks.nr_entries, 1);
    assert_eq!(header.large_vectors.nr_entries, 0);
    // The root vector starts its block, its size word the heap's.
    let at = header.vector_blocks.offset as usize;
    assert_eq!(
        u64::from_le_bytes(bytes[at..at + 8].try_into().expect("a word")),
        3
    );
}

#[test]
fn image_conses_are_cells_of_the_image_where_it_is_mapped() {
    // pdumper.c maps the dump and relocates it in place: the conses come
    // back as cells of the image's own blocks, their sharing and their
    // immediates as they were written, and a collection keeps them.
    let mut interp = Interpreter::new();
    let tail = Value::list([Value::Integer(-7), Value::T, Value::string("x")]);
    let root = Value::cons(tail, Value::cons(tail, Value::Nil));
    let bytes = dump(&mut interp, vec![(RootSlot::LoadPath, root)]);
    let mut target = Interpreter::new();
    let image = load_image(&bytes, &mut target).expect("the cons graph loads");
    assert_eq!(image.header.cons_blocks.nr_entries, 5);
    assert!(
        (image.header.cons_blocks.offset as usize)
            .is_multiple_of(crate::lisp::alloc::CONS_BLOCK_BYTES)
    );
    assert!(image.header.cons_blocks.offset < image.header.discardable_start);
    let loaded = image
        .roots
        .iter()
        .find(|(slot, _)| *slot == RootSlot::LoadPath)
        .map(|(_, value)| *value)
        .expect("the root was installed");
    let first = loaded.car().expect("a cons");
    let second = loaded
        .cdr()
        .and_then(|rest| rest.car())
        .expect("a second element");
    assert_eq!(first.word(), second.word(), "the shared tail is one cons");
    assert_eq!(first.to_string(), "(-7 t \"x\")");
    // Reached from a variable, the cells survive collections where they
    // are, the image's storage.
    target.set_symbol_value_cell("zz-image-conses", loaded);
    let answer = crate::test_support::eval_lisp(
        &mut target,
        &mut crate::lisp::types::Env::new(),
        "(progn (garbage-collect) (garbage-collect)
           (list (car zz-image-conses) (eq (car zz-image-conses) (car (cdr zz-image-conses)))
                 (length (cdr zz-image-conses))))",
    )
    .expect("the image conses stay live");
    assert_eq!(answer.to_string(), "((-7 t \"x\") t 1)");
    assert_eq!(super::load::CONS_LV, RelocType::DumpToDumpCons as u32);
}

#[test]
fn image_strings_are_cells_of_the_image_where_it_is_mapped() {
    // pdumper.c maps the dump and relocates it in place: the strings come
    // back as cells of the image's own string blocks, each string once,
    // with the bytes, characters and properties they were written with,
    // and collections keep them, see them changed and drop them as they
    // do the heap's own.
    let mut interp = Interpreter::new();
    let mut env = crate::lisp::types::Env::new();
    let program = r#"
        (let ((shared "shared")
              (many nil))
          (let ((i 0))
            (while (< i 200)
              (setq many (cons (format "s%d" i) many) i (1+ i))))
          (vector shared shared "abc" "h\u00e9llo" (propertize "pq" 'face 'bold)
                  "\377x" many ""))"#;
    let form = crate::lisp::reader::Reader::new(program)
        .read()
        .expect("setup parses")
        .expect("setup has a form");
    let graph = interp.eval(&form, &mut env).expect("setup evaluates");
    let bytes = dump(&mut interp, vec![(RootSlot::LoadPath, graph)]);
    let mut target = Interpreter::new();
    let image = load_image(&bytes, &mut target).expect("the strings load");
    // The 205 strings, the empty singleton aside, and the property
    // names', over three blocks.
    assert!(image.header.string_blocks.nr_entries >= 205);
    assert!(
        (image.header.string_blocks.offset as usize)
            .is_multiple_of(crate::lisp::alloc::IMAGE_STRING_BLOCK_BYTES)
    );
    assert!(
        image.header.string_blocks.offset as usize
            + crate::lisp::alloc::image_string_bytes(
                image.header.string_blocks.nr_entries as usize
            )
            <= image.header.discardable_start as usize
    );
    let loaded = image
        .roots
        .iter()
        .find(|(slot, _)| *slot == RootSlot::LoadPath)
        .map(|(_, value)| *value)
        .expect("the root was installed");
    target.set_symbol_value_cell("zz-image-strings", loaded);
    let eval = |target: &mut Interpreter, source: &str| {
        crate::test_support::eval_lisp(target, &mut crate::lisp::types::Env::new(), source)
            .unwrap_or_else(|error| panic!("{source}: {error:?}"))
            .to_string()
    };
    assert_eq!(
        eval(
            &mut target,
            "(let ((v zz-image-strings))
               (garbage-collect)
               (list (eq (aref v 0) (aref v 1)) (aref v 2) (multibyte-string-p (aref v 3))
                     (length (aref v 3)) (string-bytes (aref v 3))
                     (get-text-property 1 'face (aref v 4)) (text-properties-at 0 (aref v 2))
                     (multibyte-string-p (aref v 5)) (aref (aref v 5) 0)
                     (length (aref v 6)) (car (aref v 6)) (car (nthcdr 199 (aref v 6)))
                     (eq (aref v 7) \"\")))"
        ),
        "(t \"abc\" t 5 6 bold nil nil 255 200 \"s199\" \"s0\" t)"
    );
    // Stores into image strings, the one that becomes multibyte and the
    // one whose bytes shrink, and image strings no longer reached, which
    // the collections free, the image's blocks staying where they are.
    assert_eq!(
        eval(
            &mut target,
            "(let ((v zz-image-strings))
               (aset (aref v 2) 0 ?\\N{U+3B1})
               (aset (aref v 3) 1 ?e)
               (aset v 6 nil)
               (garbage-collect)
               (make-string 1000 ?z) (garbage-collect)
               (make-string 1000 ?z) (garbage-collect)
               (list (append (aref v 2) nil) (multibyte-string-p (aref v 2))
                     (aref v 3) (string-bytes (aref v 3)) (aref v 0)
                     (get-text-property 0 'face (aref v 4))))"
        ),
        "((945 98 99) t \"hello\" 5 \"shared\" bold)"
    );
    assert_eq!(super::load::STRING_LV, RelocType::DumpToDumpString as u32);
}

#[test]
fn image_vectors_are_the_image_where_it_is_mapped() {
    // pdumper.c maps the dump and relocates it in place: the vectors, the
    // closures, the records and the char tables come back as the vectors
    // of the image's own blocks and large vectors, their sharing, cycles
    // and immediates as they were written, and collections keep them,
    // see them changed and drop them as they do the heap's own.
    let mut interp = Interpreter::new();
    let mut env = crate::lisp::types::Env::new();
    let program = r#"
        (let ((shared (vector 1 "one" 'one))
              (cycle (make-vector 2 nil))
              (table (make-char-table 'zz-vectors 'dflt))
              (many nil))
          (aset cycle 0 cycle)
          (set-char-table-range table '(?a . ?z) 'lower)
          (set-char-table-range table ?\N{U+3B1} 'alpha)
          (let ((i 0))
            (while (< i 600)
              (setq many (cons (vector i) many) i (1+ i))))
          (vector shared shared cycle (make-vector 300 'big)
                  (eval '(let ((k 3)) (function (lambda (x) (* k x)))) t)
                  (record 'zz-rec 1 shared) table [] many))"#;
    let form = crate::lisp::reader::Reader::new(program)
        .read()
        .expect("setup parses")
        .expect("setup has a form");
    let graph = interp.eval(&form, &mut env).expect("setup evaluates");
    let bytes = dump(&mut interp, vec![(RootSlot::LoadPath, graph)]);
    let mut target = Interpreter::new();
    let image = load_image(&bytes, &mut target).expect("the vectors load");
    let header = &image.header;
    // The 600 small vectors take more than one block; the 300-slot one is
    // large.
    assert!(header.vector_blocks.nr_entries >= 3);
    assert_eq!(header.large_vectors.nr_entries, 1);
    assert!(
        (header.vector_blocks.offset as usize)
            .is_multiple_of(crate::lisp::alloc::vectors::IMAGE_VECTOR_BLOCK_BYTES)
    );
    assert_eq!(
        header.large_vectors.offset,
        header.vector_blocks.offset
            + header.vector_blocks.nr_entries
                * crate::lisp::alloc::vectors::IMAGE_VECTOR_BLOCK_BYTES as u32
    );
    assert!(header.large_vectors.offset < header.discardable_start);
    let loaded = image
        .roots
        .iter()
        .find(|(slot, _)| *slot == RootSlot::LoadPath)
        .map(|(_, value)| *value)
        .expect("the root was installed");
    let mut seen = HashMap::new();
    graph_matches(&graph, &loaded, &mut seen).unwrap_or_else(|error| panic!("{error}"));
    target.set_symbol_value_cell("zz-image-vectors", loaded);
    let eval = |target: &mut Interpreter, source: &str| {
        crate::test_support::eval_lisp(target, &mut crate::lisp::types::Env::new(), source)
            .unwrap_or_else(|error| panic!("{source}: {error:?}"))
            .to_string()
    };
    assert_eq!(
        eval(
            &mut target,
            "(let ((v zz-image-vectors))
               (garbage-collect)
               (list (eq (aref v 0) (aref v 1)) (aref v 0)
                     (eq (aref (aref v 2) 0) (aref v 2)) (length (aref v 3))
                     (aref (aref v 3) 299) (funcall (aref v 4) 5)
                     (aref (aref v 5) 0) (eq (aref (aref v 5) 2) (aref v 0))
                     (aref (aref v 6) ?q) (aref (aref v 6) ?\\N{U+3B1})
                     (aref (aref v 6) ?Q) (eq (aref v 7) []) (length (aref v 8))
                     (aref (car (aref v 8)) 0)))"
        ),
        "(t [1 \"one\" one] t 300 big 15 zz-rec t lower alpha dflt t 600 599)"
    );
    // Stores into image vectors and tables, and image vectors no longer
    // reached, the large one among them, which the collections free, the
    // image's storage staying where it is.
    assert_eq!(
        eval(
            &mut target,
            "(let ((v zz-image-vectors))
               (aset (aref v 0) 0 'changed)
               (aset (aref v 6) ?b 'bee)
               (set-char-table-range (aref v 6) '(?x . ?y) 'ex)
               (aset v 3 nil)
               (aset v 8 nil)
               (garbage-collect)
               (make-vector 1000 'z) (garbage-collect)
               (make-vector 1000 'z) (garbage-collect)
               (list (aref v 1) (aref (aref v 6) ?b) (aref (aref v 6) ?c)
                     (aref (aref v 6) ?y) (aref (aref v 6) ?z)
                     (funcall (aref v 4) 2) (aref v 3)))"
        ),
        "([changed \"one\" one] bee lower ex lower 6 nil)"
    );
    // Nothing of the image reached: every image vector dies, its blocks
    // stay the image's, and later collections and vectors use the heap.
    assert_eq!(
        eval(
            &mut target,
            "(progn
               (setq zz-image-vectors nil load-path nil)
               (garbage-collect) (garbage-collect)
               (let ((fresh (make-vector 3 'fresh)))
                 (garbage-collect)
                 (list (aref fresh 2) (length (make-vector 400 0)))))"
        ),
        "(fresh 400)"
    );
    assert_eq!(
        super::load::VECTORLIKE_LV,
        RelocType::DumpToDumpVectorlike as u32
    );
}

#[test]
fn image_round_trips_closures_char_tables_records_and_bool_vectors() {
    let mut interp = Interpreter::new();
    let mut env = crate::lisp::types::Env::new();
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
    let bytes = dump(&mut interp, vec![(RootSlot::LoadPath, graph)]);
    let mut target = Interpreter::new();
    let image = load_image(&bytes, &mut target).unwrap_or_else(|error| panic!("load: {error:?}"));
    let loaded = image
        .roots
        .iter()
        .find(|(slot, _)| *slot == RootSlot::LoadPath)
        .map(|(_, value)| *value)
        .expect("root");
    let mut seen = HashMap::new();
    graph_matches(&graph, &loaded, &mut seen).unwrap_or_else(|error| panic!("{error}"));
    let Kind::Vector(vector) = loaded.kind() else {
        panic!("root vector")
    };
    let slots = vector.slots().collect::<Vec<_>>();

    // Two closures over one environment: the frame is shared, and calling
    // them in the restored interpreter mutates the shared binding.
    let closures = slots[0].to_vec().expect("closure list");
    let (Kind::Closure(first), Kind::Closure(second)) = (closures[0].kind(), closures[1].kind())
    else {
        panic!("closures")
    };
    assert!(
        Interpreter::same_environment(&first.environment_value(), &second.environment_value()),
        "one environment object"
    );
    assert_eq!(
        second.parameters().to_vec().expect("parameter list"),
        vec![Value::symbol("y")]
    );
    let call = |target: &mut Interpreter, function: &Value, args: &[Value]| {
        target
            .call_function_value(*function, None, args, &mut crate::lisp::types::Env::new())
            .expect("closure call")
    };
    assert_eq!(call(&mut target, &closures[0], &[]), Value::Integer(1));
    assert_eq!(
        call(&mut target, &closures[1], &[Value::Integer(5)]),
        Value::Integer(6)
    );
    assert_eq!(call(&mut target, &closures[0], &[]), Value::Integer(6));

    // The char-table with its ranges, subtype, default and extra slot.
    let Kind::CharTable(table_id) = slots[1].kind() else {
        panic!("char-table")
    };
    let table = table_id;
    assert!(table.has_purpose("zz-purpose"));
    assert_eq!(table.default(), Value::symbol("dflt"));
    assert_eq!(
        table
            .ranges()
            .iter()
            .map(|e| (e.start, e.end, e.value))
            .collect::<Vec<_>>(),
        vec![
            (0, 64, Value::symbol("dflt")),
            (65, 65, Value::symbol("upper")),
            (66, 96, Value::symbol("dflt")),
            (97, 122, Value::symbol("lower")),
            (123, 0x3fffff, Value::symbol("dflt")),
        ]
    );
    assert_eq!(table.extra_count(), 1);
    assert_eq!(
        table
            .extra(0)
            .and_then(|value| string_like(&value))
            .map(|s| s.text),
        Some("extra".into())
    );

    // The bool-vector is mapped where the image's vector blocks hold it.
    let Kind::BoolVector(bits) = slots[2].kind() else {
        panic!("bool-vector")
    };
    assert_eq!(bits.len(), 70);
    let set = bits
        .bits()
        .enumerate()
        .filter(|(_, bit)| *bit)
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    assert_eq!(set, vec![0, 65, 69]);

    // The record's slots, with the shared list being the same object.
    let Kind::LispRecord(record) = slots[3].kind() else {
        panic!("record")
    };
    assert_eq!(record.type_tag(), Value::symbol("zz-rec"));
    assert_eq!(record.get(1), Some(Value::Integer(1)));
    assert_eq!(
        record
            .get(2)
            .and_then(|value| string_like(&value))
            .map(|s| s.text),
        Some("two".to_owned())
    );
    assert_eq!(
        object_key(&record.get(3).expect("shared field")),
        object_key(&slots[0])
    );

    // The main thread is the restoring process's own.  (The standard
    // obarray reaches every symbol's value, hash tables included: it joins
    // the controls with D10.)
    assert_eq!(
        slots[4],
        target.record_value(target.main_thread_record_id())
    );
    assert!(image.obarray.is_empty());
    assert_eq!(image.builtin_cells.len(), 2);
    assert_eq!(image.builtin_cells[0].symbol.as_str(), "nil");
    assert_eq!(image.builtin_cells[1].symbol.as_str(), "t");
}

#[test]
fn image_freezes_and_thaws_hash_tables_as_pdumper_c_does() {
    fn call_in(target: &mut Interpreter, name: &str, args: &[Value]) -> Value {
        crate::lisp::native_comp::call_c_primitive(
            target,
            &mut crate::lisp::types::Env::new(),
            name,
            args,
        )
        .unwrap_or_else(|error| panic!("{name}: {error:?}"))
    }
    let mut interp = Interpreter::new();
    let mut env = crate::lisp::types::Env::new();
    let program = r#"
        (let ((eq-table (make-hash-table :test 'eq))
              (equal-table (make-hash-table :test 'equal :size 100))
              (byte-table (make-hash-table :test 'equal))
              (weak (make-hash-table :weakness 'key))
              (empty (make-hash-table))
              (shared (list 1 2)))
          (puthash 'a 1 eq-table)
          (puthash 'b shared eq-table)
          (puthash "k1" "v1" equal-table)
          (puthash "k2" 2 equal-table)
          (remhash "k1" equal-table)
          (puthash "k3" 3 equal-table)
          (puthash (unibyte-string #x80) 10 byte-table)
          (puthash (string #x3fff80) 20 byte-table)
          (puthash (string #xe080) 30 byte-table)
          (puthash 'w 'x weak)
          (vector eq-table equal-table weak empty shared byte-table))"#;
    let form = crate::lisp::reader::Reader::new(program)
        .read()
        .expect("setup parses")
        .expect("setup has a form");
    let graph = interp.eval(&form, &mut env).expect("setup evaluates");
    let bytes = dump(&mut interp, vec![(RootSlot::LoadPath, graph)]);
    let mut target = Interpreter::new();
    let image = load_image(&bytes, &mut target).unwrap_or_else(|error| panic!("load: {error:?}"));
    assert_ne!(image.header.hash_list, 0, "the hash list is written");
    let loaded = image
        .roots
        .iter()
        .find(|(slot, _)| *slot == RootSlot::LoadPath)
        .map(|(_, value)| *value)
        .expect("root");
    let Kind::Vector(vector) = loaded.kind() else {
        panic!("root vector")
    };
    let slots = vector.slots().collect::<Vec<_>>();
    let eq_table = slots[0];
    let equal_table = slots[1];
    let weak = slots[2];
    let empty = slots[3];
    let shared = slots[4];
    let byte_table = slots[5];
    assert_eq!(
        call_in(&mut target, "hash-table-count", &[byte_table]),
        Value::Integer(3)
    );
    // The image stores key/value pairs and thaw recomputes their hashes
    // after relocation. Storage distinctions must survive the new index.
    for (constructor, code, expected) in [
        ("unibyte-string", 0x80, 10),
        ("string", 0x3fff80, 20),
        ("string", 0xe080, 30),
    ] {
        let key = call_in(&mut target, constructor, &[Value::Integer(code)]);
        assert_eq!(
            call_in(&mut target, "gethash", &[key, byte_table]),
            Value::Integer(expected)
        );
    }
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
        call_in(&mut target, "gethash", &[Value::symbol("a"), eq_table]),
        Value::Integer(1)
    );
    // The value is the shared list object, not a copy.
    assert_eq!(
        object_key(&call_in(
            &mut target,
            "gethash",
            &[Value::symbol("b"), eq_table]
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
        call_in(&mut target, "gethash", &[Value::string("k2"), equal_table]),
        Value::Integer(2)
    );
    assert_eq!(
        call_in(&mut target, "gethash", &[Value::string("k3"), equal_table]),
        Value::Integer(3)
    );
    assert_eq!(
        call_in(&mut target, "gethash", &[Value::string("k1"), equal_table]),
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
        call_in(&mut target, "gethash", &[Value::symbol("w"), weak]),
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
        &[Value::symbol("c"), Value::Integer(3), eq_table],
    );
    assert_eq!(
        call_in(&mut target, "hash-table-count", &[eq_table]),
        Value::Integer(3)
    );
    let Kind::HashTable(equal_id) = equal_table.kind() else {
        panic!("hash table")
    };
    let keys = equal_id
        .entries()
        .map(|(_, key, _)| string_like(&key).map(|s| s.text).unwrap_or_default())
        .collect::<Vec<_>>();
    // fns.c reuses the slot `remhash' freed: k3 sits in k1's slot 0, so
    // the compact contents (hash_table_contents) walk k3 before k2.
    assert_eq!(keys, vec!["k3".to_owned(), "k2".to_owned()]);
    // hash_table_thaw: the allocation is minimal, count entries.
    assert_eq!(Some(equal_id.capacity()), Some(2));
}

#[test]
fn image_refuses_hash_tables_with_user_defined_tests_as_gnu_does() {
    let mut interp = Interpreter::new();
    let mut env = crate::lisp::types::Env::new();
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
        Err(super::context::DumpError::Lisp(error))
            if matches!(error.kind(), LispErrorKind::Signal(_)) =>
        {
            let LispErrorKind::Signal(message) = error.kind() else {
                unreachable!()
            };
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
    let mut env = crate::lisp::types::Env::new();
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
        .map(|(_, value)| *value)
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
        .eval(&form, &mut crate::lisp::types::Env::new())
        .unwrap_or_else(|error| panic!("probe: {error:?}"));
    let printed = crate::lisp::primitives::call(
        &mut target,
        "prin1-to-string",
        &[value],
        &mut crate::lisp::types::Env::new(),
    )
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
            &mut crate::lisp::types::Env::new(),
            "prin1-to-string",
            std::slice::from_ref(value),
        )
        .unwrap_or_else(|error| panic!("prin1-to-string: {error:?}"));
        string_like(&value).expect("a string").text
    }
    let mut interp = Interpreter::new();
    let mut env = crate::lisp::types::Env::new();
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
          (set-category-table (make-category-table))
          (use-local-map (let ((map (make-sparse-keymap))) (define-key map "z" 'zz-dump-command) map))
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
                    killed)
                  nil))"#;
    let form = crate::lisp::reader::Reader::new(program)
        .read()
        .expect("setup parses")
        .expect("setup has a form");
    let graph = interp.eval(&form, &mut env).expect("setup evaluates");
    let Kind::Vector(source_vector) = graph.kind() else {
        panic!("root vector")
    };
    let source = source_vector.slots().collect::<Vec<_>>();
    let Kind::Overlay(source_overlay) = source[6].kind() else {
        panic!("overlay")
    };
    let source_plist = source_overlay.plist();
    // A second owner of the actual list must relocate to the same conses.
    // Serializing pairs and rebuilding a new spine would lose this sharing.
    source_vector.set(8, source_plist);
    let Kind::Buffer(source_buffer) = source[0].kind() else {
        panic!("buffer")
    };
    let source_id = source_buffer.id;
    interp.set_buffer_local_hook(source_id, "zz-dump-hook", vec![Value::symbol("car")]);
    let source_undo_value = interp
        .get_buffer_by_id(source_id)
        .expect("live")
        .undo_list();
    let source_undo = printed(&mut interp, &source_undo_value);
    let source_syntax_table = interp
        .buffer_syntax_table_id(source_id)
        .expect("the buffer set a syntax table");
    let source_syntax = printed(&mut interp, &Value::CharTable(source_syntax_table));
    let source_category_table = interp
        .buffer_category_table_id(source_id)
        .expect("the buffer set a category table");
    let source_category = printed(&mut interp, &Value::CharTable(source_category_table));
    let source_finalizers = interp.finalizer_objects();
    let terminal = Value::Terminal(*interp.terminals.first().expect("initial terminal"));
    let roots = vec![(RootSlot::LoadPath, graph), (RootSlot::QuitFlag, terminal)];
    let bytes = dump(&mut interp, roots);

    let mut target = Interpreter::new();
    let image = load_image(&bytes, &mut target).unwrap_or_else(|error| panic!("load: {error:?}"));
    let root = |slot: RootSlot| {
        image
            .roots
            .iter()
            .find(|(candidate, _)| *candidate == slot)
            .map(|(_, value)| *value)
            .unwrap_or_else(|| panic!("root {slot:?} missing"))
    };
    let Kind::Vector(vector) = root(RootSlot::LoadPath).kind() else {
        panic!("root vector")
    };
    let slots = vector.slots().collect::<Vec<_>>();

    // The buffer: text, positions, narrowing, flags, property spans, the
    // side list, the undo entries, the modtime, the local binding, the
    // syntax table, the mark.
    let Kind::Buffer(loaded_buffer) = slots[0].kind() else {
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
    let intervals = buffer.intervals().expect("the buffer's intervals");
    assert_eq!(
        intervals
            .iter()
            .map(|(start, end, _)| (start, end))
            .collect::<Vec<_>>(),
        [(1, 2), (2, 12)]
    );
    assert_eq!(
        crate::intervals::plist_get(intervals.plist(0), Value::symbol("face")),
        Value::symbol("bold")
    );
    assert!(intervals.plist(1).is_nil());
    assert_eq!(
        buffer
            .visited_file_modtime()
            .map(|modtime| modtime.modified),
        Some(std::time::UNIX_EPOCH + std::time::Duration::from_secs(100))
    );
    assert!(buffer.overlays.iter().all(|overlay| overlay.is_dead()));
    let loaded_undo = buffer.undo_list();
    drop(buffer);
    assert_eq!(printed(&mut target, &loaded_undo), source_undo);
    // The buffer's 14 always-local slots (reset_buffer gives every buffer
    // its own), then `deactivate-mark' -- the edits set it, and
    // keyboard.c makes it buffer-local when set -- and zz-dump-local.
    let locals = target.buffer_local_cells(source_id);
    assert_eq!(
        locals.len(),
        16,
        "the always-local slots, deactivate-mark, zz-dump-local: {locals:?}"
    );
    assert!(
        locals.iter().any(|(symbol, value)| symbol == "mode-name"
            && printed(&mut target, value) == "\"Fundamental\"")
    );
    assert!(
        locals
            .iter()
            .any(|(symbol, value)| symbol == "deactivate-mark" && *value == Value::T)
    );
    assert!(
        locals
            .iter()
            .any(|(symbol, value)| symbol == "zz-dump-local" && *value == Value::Integer(42))
    );
    let loaded_syntax_table = target
        .buffer_syntax_table_id(source_id)
        .expect("restored syntax table");
    assert_ne!(loaded_syntax_table, source_syntax_table);
    // The purpose symbol relocates to the target editor. Check that actual
    // identity, then compare the complete table payload through its printer.
    assert_eq!(loaded_syntax_table.purpose(), Value::symbol("syntax-table"));
    assert_ne!(
        loaded_syntax_table.purpose().word(),
        source_syntax_table.purpose().word()
    );
    assert_eq!(
        printed(&mut target, &Value::CharTable(loaded_syntax_table)),
        source_syntax
    );
    // BVAR (b, category_table) is a slot of its own, not a local binding.
    let loaded_category_table = target
        .buffer_category_table_id(source_id)
        .expect("restored category table");
    assert_ne!(loaded_category_table, source_category_table);
    assert_eq!(
        loaded_category_table.purpose(),
        Value::symbol("category-table")
    );
    assert_eq!(
        printed(&mut target, &Value::CharTable(loaded_category_table)),
        source_category
    );
    assert!(!locals.iter().any(|(symbol, _)| symbol == "category-table"));
    // BVAR (b, keymap) likewise.
    assert!(
        !locals
            .iter()
            .any(|(symbol, _)| symbol == "current-local-map")
    );
    let loaded_keymap = target
        .buffer_object(source_id)
        .expect("restored buffer")
        .borrow()
        .keymap();
    assert_eq!(
        printed(&mut target, &loaded_keymap),
        "(keymap (122 . zz-dump-command))"
    );
    let mark_marker = target
        .buffer_mark_marker_id(source_id)
        .expect("the mark marker relation");
    let mark = mark_marker;
    assert_eq!(mark.buffer().map(|b| b.id), Some(source_id));
    assert_eq!(mark.position(), Some(1));
    assert_eq!(
        target
            .buffer_object(source_id)
            .expect("restored buffer")
            .mark_object(),
        Some(mark)
    );

    // The markers: one into the buffer with its insertion type, one
    // detached.
    let Kind::Marker(m1) = slots[1].kind() else {
        panic!("marker")
    };
    assert_eq!(m1.buffer().map(|b| b.id), Some(source_id));
    assert_eq!(m1.position(), Some(2));
    assert!(m1.insertion_type());
    let Kind::Marker(m2) = slots[2].kind() else {
        panic!("marker")
    };
    assert_eq!(m2.buffer().map(|b| b.id), None);
    assert_eq!(m2.position(), None);
    assert_eq!(
        target.buffer_marker_ids(source_id).len(),
        interp.buffer_marker_ids(source_id).len()
    );

    // The finalizers, in list order, with their functions.
    let Kind::Finalizer(f1) = slots[3].kind() else {
        panic!("finalizer")
    };
    let Kind::Finalizer(f2) = slots[4].kind() else {
        panic!("finalizer")
    };
    assert_eq!(target.finalizer_objects().len(), source_finalizers.len());
    assert_eq!(target.finalizer_objects(), vec![f1, f2]);
    assert_ne!(f1, source_finalizers[0]);
    assert_ne!(f2, source_finalizers[1]);
    assert_eq!(f1.function(), Value::symbol("car"));
    assert_eq!(f2.function(), Value::symbol("cdr"));

    // The frame is nilled: a dead frame object of its own, beside the
    // live initial frame of the loading process.  The terminal is nilled
    // likewise: a dead terminal beside the live initial one.
    let Kind::Frame(frame) = slots[5].kind() else {
        panic!("frame")
    };
    let state = frame.borrow();
    assert!(state.terminal.is_none());
    assert_eq!(frame.name.get(), Value::Nil);
    assert!(target.selected_frame_id.is_live());
    assert_ne!(frame, target.selected_frame_id);
    let Kind::Terminal(dead_terminal) = root(RootSlot::QuitFlag).kind() else {
        panic!("terminal")
    };
    assert!(!dead_terminal.borrow().live);
    assert!(
        !target
            .terminals
            .iter()
            .any(|terminal| terminal.ptr_eq(&dead_terminal))
    );
    assert_ne!(
        Value::Terminal(dead_terminal),
        Value::Terminal(*target.terminals.first().expect("initial terminal"))
    );
    assert!(
        target
            .terminals
            .first()
            .expect("initial terminal")
            .borrow()
            .live
    );

    // The deleted overlay retains its properties without a holding buffer.
    let Kind::Overlay(ov) = slots[6].kind() else {
        panic!("overlay")
    };
    let overlay = ov;
    assert!(overlay.is_dead());
    assert_eq!(
        overlay.plist(),
        Value::list([Value::symbol("zz-prop"), Value::symbol("yes")])
    );
    assert_eq!(overlay.plist().word(), slots[8].word());
    assert!(ov.buffer().is_none());

    // The local hook list came with the buffer.
    assert_eq!(
        target.buffer_local_hook_lists(source_id),
        vec![("zz-dump-hook".to_owned(), vec![Value::symbol("car")])]
    );

    // The killed buffer is an object with no buffer behind it.
    let Kind::Buffer(killed) = slots[7].kind() else {
        panic!("killed buffer")
    };
    assert!(target.get_buffer_by_id(killed.id).is_none());
    assert!(!target.has_buffer_id(killed.id));
}

#[test]
fn image_refuses_buffers_with_overlays_as_gnu_does() {
    let mut interp = Interpreter::new();
    let mut env = crate::lisp::types::Env::new();
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
        Err(super::context::DumpError::Lisp(error))
            if matches!(error.kind(), LispErrorKind::Signal(_)) =>
        {
            let LispErrorKind::Signal(message) = error.kind() else {
                unreachable!()
            };
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
            &mut crate::lisp::types::Env::new(),
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
    let mut env = crate::lisp::types::Env::new();
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
            (define-key m "\C-c" 'car)
            (setq zz-image-child m zz-image-parent p))
          t)"#;
    // As in the ordinary reader, publish the names in the interpreter's
    // obarray before evaluation. That real image root owns the symbols
    // whose value cells hold the two maps.
    let form = crate::lisp::read_next_load_form(&mut interp, program, &mut 0, &mut env)
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
    let mut ctx = DumpContext::new(true, interp.main_thread_record_id());
    let summary = match write_image(&mut ctx, &interp, RootSource::Interpreter) {
        Ok(summary) => summary,
        Err(super::context::DumpError::Unsupported(unsupported)) => {
            ctx.print_paths_to_root(
                &mut interp,
                &mut crate::lisp::types::Env::new(),
                &unsupported.object,
            );
            panic!("unsupported object: {}", unsupported.message)
        }
        Err(super::context::DumpError::Lisp(error)) => panic!("dump failed: {error:?}"),
    };
    assert!(summary.hot_bytes > 0);
    let bytes = ctx.buffer().to_vec();
    // Capture the image before presentation. GNU print_prepare temporarily
    // binds escape flags, so prin1-to-string advances this interpreter's
    // binding IDs even though it restores the variables afterwards. The
    // remembered scalar values must come from the same pre-print snapshot.
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
    // Ordinary startup installs the decoded symbol cells after loading
    // the object graph. The Lisp probe needs the same second step.
    target
        .install_image(
            &image,
            crate::lisp::eval::PdumperLoadRecord {
                filename: "root-groups.pdmp".into(),
                load_time: std::time::Duration::ZERO,
                dump_size: bytes.len() as u64,
            },
        )
        .expect("install the dumped symbol cells");
    // Actual Lisp roots carry both maps; the restored child shares the
    // parent's cons cells. Collection must preserve both while reachable.
    let form = crate::lisp::reader::Reader::new(
        r#"(progn (garbage-collect)
           (list (keymapp zz-image-child) (lookup-key zz-image-child "\C-c")
                 (keymapp (keymap-parent zz-image-child))
                 (lookup-key zz-image-child "\M-q")
                 (eq (keymap-parent zz-image-child) zz-image-parent)
                 (eq (nthcdr 2 zz-image-child) zz-image-parent)))"#,
    )
    .read()
    .expect("probe parses")
    .expect("a form");
    let answer = target
        .eval(&form, &mut crate::lisp::types::Env::new())
        .expect("restored maps remain live");
    assert_eq!(printed(&mut target, &answer), "(t car t ignore t t)");
}

#[test]
fn image_refuses_pending_transient_state_it_cannot_carry() {
    let mut interp = Interpreter::new();
    interp.pending_thread_events.push(Value::symbol("zz-event"));
    let mut ctx = DumpContext::new(false, interp.main_thread_record_id());
    match write_image(&mut ctx, &interp, RootSource::Interpreter) {
        Err(super::context::DumpError::Lisp(error))
            if matches!(error.kind(), LispErrorKind::Signal(_)) =>
        {
            let LispErrorKind::Signal(message) = error.kind() else {
                unreachable!()
            };
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

#[test]
fn closure_slot_graphs_survive_interpreter_cloning_and_dump_restoration() {
    let mut source = Interpreter::new();
    let arguments = Value::list([Value::symbol("parameter")]);
    let body = Value::list([Value::Nil]);
    let environment = Value::list([Value::T]);
    let documentation = Value::cons(Value::Nil, body);
    let closure = source
        .make_interpreted_closure(
            arguments,
            body,
            environment,
            documentation,
            Value::list([Value::symbol("interactive")]),
        )
        .expect("construct a six-slot closure");
    body.set_car(Value::list([Value::symbol("quote"), closure]))
        .expect("body cycle");
    documentation.set_car(closure).expect("documentation cycle");
    let malformed_arguments = Value::list([Value::Nil]);
    let malformed = source
        .make_interpreted_closure(
            malformed_arguments,
            Value::list([Value::Nil]),
            environment,
            Value::Nil,
            Value::Nil,
        )
        .expect("parameter validation is deferred until a call");
    malformed_arguments
        .set_car(malformed)
        .expect("parameter cycle");
    let root = Value::vector([
        closure,
        arguments,
        body,
        environment,
        documentation,
        malformed,
    ]);
    source.set_variable(
        "closure-slot-graph",
        root,
        &mut crate::lisp::types::Env::new(),
    );
    let clone = source.clone();
    let cloned = clone
        .symbol_value_cell("closure-slot-graph")
        .expect("cloned graph root");
    graph_matches(&root, &cloned, &mut HashMap::new())
        .expect("clone preserves slot sharing and cycles");
    let image = dump(&mut source, vec![(RootSlot::LoadPath, root)]);
    let mut target = Interpreter::new();
    let loaded = load_image(&image, &mut target).expect("restore closure graph");
    let restored = loaded
        .roots
        .iter()
        .find(|(slot, _)| *slot == RootSlot::LoadPath)
        .expect("restored explicit graph root")
        .1;
    graph_matches(&root, &restored, &mut HashMap::new())
        .expect("dump preserves slot sharing and cycles");
    let Kind::Vector(vector) = restored.kind() else {
        panic!("restored root vector")
    };
    let restored_closure = vector.get(0).expect("first vector slot");
    let returned = target
        .call_function_value(
            restored_closure,
            None,
            &[Value::Integer(19)],
            &mut crate::lisp::types::Env::new(),
        )
        .expect("call restored closure");
    assert_eq!(
        returned.word(),
        restored_closure.word(),
        "the stored body refers to the restored closure itself"
    );
}

#[test]
fn voided_builtin_cells_and_saved_subrs_survive_image_cloning_and_restoration() {
    fn check(interp: &mut Interpreter, saved: Value) {
        let mut env = crate::lisp::types::Env::new();
        assert!(interp.raw_function_binding("identity", &env).is_none());
        assert!(interp.raw_function_binding("if", &env).is_none());
        assert!(matches!(
            interp
                .raw_function_binding("car", &env)
                .map(|value| value.kind()),
            Some(Kind::BuiltinFunc(_))
        ));
        assert_eq!(
            interp
                .call_function_value(saved, None, &[Value::Integer(73)], &mut env)
                .expect("the saved subr remains callable"),
            Value::Integer(73),
        );
    }

    let mut source = Interpreter::new();
    let saved = source
        .raw_function_binding("identity", &crate::lisp::types::Env::new())
        .expect("defsubr installs identity");
    source.set_function_binding("identity", None);
    source.set_function_binding("if", None);
    check(&mut source.deep_clone_image(), saved);

    let graph = Value::vector([Value::symbol("identity"), Value::symbol("if"), saved]);
    let bytes = dump(&mut source, vec![(RootSlot::LoadPath, graph)]);
    let mut target = Interpreter::new();
    let image = load_image(&bytes, &mut target).expect("load the symbol and subr graph");
    let restored = image
        .roots
        .iter()
        .find(|(slot, _)| *slot == RootSlot::LoadPath)
        .map(|(_, value)| *value)
        .expect("saved subr root");
    let Kind::Vector(restored) = restored.kind() else {
        panic!("the root remains a vector")
    };
    let restored_subr = restored.slots().nth(2).expect("saved subr slot");
    assert_eq!(restored_subr.word(), saved.word());
    target
        .install_image(
            &image,
            crate::lisp::eval::PdumperLoadRecord {
                filename: "void-function-cells.pdmp".into(),
                load_time: std::time::Duration::ZERO,
                dump_size: bytes.len() as u64,
            },
        )
        .expect("install the actual dumped function cells");
    check(&mut target, restored_subr);
}

#[test]
fn image_nilled_frames_preserve_address_identity_and_shared_references() {
    let mut source = Interpreter::new();
    let other = Interpreter::new();
    let first = source.selected_frame_value();
    let second = other.selected_frame_value();
    assert_ne!(first, second);
    let bytes = dump(
        &mut source,
        vec![(RootSlot::LoadPath, Value::vector([first, second, first]))],
    );
    let mut target = Interpreter::new();
    let image = load_image(&bytes, &mut target).expect("load nilled frames");
    let graph = image
        .roots
        .iter()
        .find(|(slot, _)| *slot == RootSlot::LoadPath)
        .expect("root")
        .1;
    let Kind::Vector(slots) = graph.kind() else {
        panic!("frame vector")
    };
    let restored = slots.slots().collect::<Vec<_>>();
    assert_eq!(restored.len(), 3);
    assert_ne!(restored[0], restored[1]);
    assert_eq!(restored[0], restored[2]);
    for value in restored {
        let Kind::Frame(frame) = value.kind() else {
            panic!("dead restored frame")
        };
        assert!(!frame.is_live());
        assert!(frame.name.get().is_nil());
        assert_ne!(frame, target.selected_frame_id);
    }
    assert_eq!(
        target.frame_states.len(),
        1,
        "nilled objects are not live frame roots"
    );
}

#[test]
fn inline_record_type_and_data_cycles_survive_clone_and_dump() {
    use crate::lisp::types::LispRecordRef;
    let mut source = Interpreter::new();
    let record = LispRecordRef::filled(Value::Nil, 3, Value::Nil);
    let object = Value::LispRecord(record);
    let descriptor = LispRecordRef::new(
        Value::symbol("record-descriptor"),
        &[Value::symbol("cycle-type"), object],
    );
    let shared = Value::vector([object, Value::LispRecord(descriptor)]);
    record.set(0, Value::LispRecord(descriptor));
    record.set(1, object);
    record.set(2, shared);
    record.set(3, shared);
    let root = Value::vector([object, shared, Value::LispRecord(descriptor)]);
    source.set_variable(
        "inline-record-graph",
        root,
        &mut crate::lisp::types::Env::new(),
    );
    let clone = source.deep_clone_image();
    let cloned = clone
        .symbol_value_cell("inline-record-graph")
        .expect("cloned root");
    graph_matches(&root, &cloned, &mut HashMap::new())
        .expect("clone preserves every record field and identity relation");
    assert_ne!(root.word(), cloned.word());
    let bytes = dump(&mut source, vec![(RootSlot::LoadPath, root)]);
    let mut target = Interpreter::new();
    let image = load_image(&bytes, &mut target).expect("record graph loads");
    let restored = image
        .roots
        .iter()
        .find(|(slot, _)| *slot == RootSlot::LoadPath)
        .expect("restored graph")
        .1;
    graph_matches(&root, &restored, &mut HashMap::new())
        .expect("type descriptors, cycles and shared fields relocate");
    let Kind::Vector(vector) = restored.kind() else {
        panic!("root vector")
    };
    let Kind::LispRecord(record) = vector.get(0).expect("record field").kind() else {
        panic!("inline record")
    };
    let words = record.identity() as *const usize;
    assert_eq!(unsafe { words.add(1).read() }, record.type_tag().word());
    assert_eq!(
        unsafe { words.add(2).read() },
        Value::LispRecord(record).word()
    );
    record.set(3, Value::Integer(101));
    assert_eq!(unsafe { words.add(4).read() }, Value::Integer(101).word());
}
