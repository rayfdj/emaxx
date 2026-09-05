use super::*;
use crate::lisp::reader::Reader;
use std::path::PathBuf;
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

fn panic_eval_error(interp: &mut Interpreter, error: LispError) -> ! {
    let rendered_error = match &error {
        LispError::SignalValue(value) => {
            crate::lisp::primitives::render_prin1_ephemeral(interp, value, &Vec::new())
                .unwrap_or_else(|_| error.to_string())
        }
        _ => error.to_string(),
    };
    let backtrace = interp
        .take_batch_error_backtrace()
        .map(|snapshot| {
            snapshot
                .frames
                .into_iter()
                .take(12)
                .map(|(_, function, args, _)| {
                    let mut frame = bounded_lisp_display(&function);
                    for arg in args.into_iter().take(5) {
                        frame.push(' ');
                        frame.push_str(&bounded_lisp_display(&arg));
                    }
                    frame
                })
                .collect::<Vec<_>>()
                .join(" <- ")
        })
        .unwrap_or_default();
    panic!("evaluation failed: {rendered_error}; Lisp backtrace: {backtrace}")
}

fn bounded_lisp_display(value: &Value) -> String {
    const LIMIT: usize = 120;
    let rendered = value
        .to_string()
        .chars()
        .flat_map(char::escape_debug)
        .collect::<String>();
    let mut chars = rendered.chars();
    let prefix = chars.by_ref().take(LIMIT).collect::<String>();
    if chars.next().is_some() {
        format!("{prefix}…")
    } else {
        prefix
    }
}

fn eval_str_bare(src: &str) -> Value {
    let mut interp = Interpreter::new();
    let mut env: Env = Vec::new();
    let forms = Reader::new(src).read_all().unwrap();
    let mut result = Value::Nil;
    for form in &forms {
        // GNU's reader interns every symbol it reads, so `intern-soft'
        // must hit symbols that only occur in test source.
        interp.intern_symbols_in_value(form);
        result = interp
            .eval(form, &mut env)
            .unwrap_or_else(|error| panic_eval_error(&mut interp, error));
    }
    result
}

/// Evaluate ordinary Elisp test forms after executing GNU's real early Lisp
/// owners in their `loadup.el` order.  A user-visible GNU process has these
/// definitions in its dumped image; loading the upstream files here preserves
/// that ownership without restoring any Rust fallback.  Tests that explicitly
/// exercise the file-less C/Rust host must call `eval_str_bare` instead.
fn eval_str(src: &str) -> Value {
    eval_str_with_gnu_early_lisp(src)
}

fn eval_str_with(interp: &mut Interpreter, src: &str) -> Value {
    let mut env: Env = Vec::new();
    let forms = Reader::new(src).read_all().unwrap();
    let mut result = Value::Nil;
    for form in &forms {
        // GNU's reader interns every symbol it reads, so `intern-soft'
        // must hit symbols that only occur in test source.
        interp.intern_symbols_in_value(form);
        result = interp
            .eval(form, &mut env)
            .unwrap_or_else(|error| panic_eval_error(interp, error));
    }
    result
}

fn eval_str_with_upstream_batch(src: &str) -> Value {
    // GNU's batch image executes these same GNU Lisp owners from its dump.
    // Use their compiled `.elc' representation so each ownership-sensitive
    // test does not pay the unrelated source bootstrap cost.
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    eval_str_with(&mut interp, src)
}

fn eval_str_with_upstream_batch_feature(feature: &str, src: &str) -> Value {
    eval_str_with_upstream_batch_features(&[feature], src)
}

fn upstream_batch_interpreter_with_features(
    features: &[&str],
) -> (crate::test_support::HostTestPermit, Interpreter) {
    let permit = crate::test_support::acquire_host_test_permit();
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    for feature in features {
        eval_str_with(&mut interp, &format!("(require '{feature})"));
    }
    (permit, interp)
}

fn eval_str_with_upstream_batch_features(features: &[&str], src: &str) -> Value {
    let (_permit, mut interp) = upstream_batch_interpreter_with_features(features);
    eval_str_with(&mut interp, src)
}

fn eval_str_with_gnu_early_lisp(src: &str) -> Value {
    let mut interp = gnu_early_lisp_interpreter();
    eval_str_with(&mut interp, src)
}

fn gnu_early_lisp_interpreter() -> Interpreter {
    crate::test_support::initialized_gnu_early_lisp_interpreter()
}

fn load_gnu_batch_runtime(interp: &mut Interpreter) {
    crate::test_support::replace_with_gnu_batch_runtime(interp);
}

fn upstream_emacs_repo() -> PathBuf {
    crate::compat::project_root().join("../emacs")
}

fn assert_string_value(value: Value, expected: &str) {
    assert_eq!(primitives::string_text(&value).unwrap(), expected);
}

fn assert_string_list(value: Value, expected: &[&str]) {
    let items = value.to_vec().unwrap();
    assert_eq!(items.len(), expected.len());
    for (item, expected) in items.iter().zip(expected.iter()) {
        assert_eq!(primitives::string_text(item).unwrap(), *expected);
    }
}

fn run_with_large_stack(test: impl FnOnce() + Send + 'static) {
    let permit = crate::test_support::acquire_host_test_permit();
    thread::Builder::new()
        .stack_size(128 * 1024 * 1024)
        .spawn(move || {
            let _permit = permit;
            crate::test_support::note_host_permit_moved_to_this_thread();
            test();
        })
        .unwrap()
        .join()
        .unwrap();
}

fn run_large_stack_test(test_fn: fn()) {
    let permit = crate::test_support::acquire_host_test_permit();
    thread::Builder::new()
        .stack_size(16 * 1024 * 1024)
        .spawn(move || {
            let _permit = permit;
            crate::test_support::note_host_permit_moved_to_this_thread();
            test_fn();
        })
        .unwrap()
        .join()
        .unwrap();
}

fn run_exclusive_with_large_stack(test: impl FnOnce() + Send + 'static) {
    let permit = crate::test_support::acquire_exclusive_host_test_permit();
    thread::Builder::new()
        .stack_size(128 * 1024 * 1024)
        .spawn(move || {
            let _permit = permit;
            crate::test_support::note_host_permit_moved_to_this_thread();
            test();
        })
        .unwrap()
        .join()
        .unwrap();
}

#[test]
fn gnu_hash_storage_layout_and_growth_match_fns_c() {
    assert_eq!(gnu_hash_grown_capacity(0, 0), 0);
    assert_eq!(gnu_hash_grown_capacity(0, 1), 6);
    assert_eq!(gnu_hash_grown_capacity(0, 6), 6);
    assert_eq!(gnu_hash_grown_capacity(0, 7), 24);
    assert_eq!(gnu_hash_grown_capacity(1, 2), 24);
    assert_eq!(gnu_hash_grown_capacity(64, 65), 256);
    assert_eq!(gnu_hash_grown_capacity(65, 66), 130);

    assert_eq!(gnu_hash_table_index_slots(0), 1);
    assert_eq!(gnu_hash_table_index_slots(6), 8);
    assert_eq!(gnu_hash_table_index_slots(64), 128);
    assert_eq!(gnu_hash_table_index_slots(65), 128);
    assert_eq!(gnu_hash_table_storage_bytes(0), 0);
    assert_eq!(gnu_hash_table_storage_bytes(6), 176);
    assert_eq!(gnu_hash_table_storage_bytes(65), 2_072);
}

#[test]
fn gnu_hash_capacity_survives_clear_and_grows_at_the_same_boundary() {
    let mut interp = Interpreter::new();
    let Value::Record(id) =
        crate::lisp::json::make_hash_table_with_capacity(&mut interp, "eql", Vec::new(), 0)
    else {
        unreachable!("hash-table constructor must return a record")
    };
    let env = Vec::new();
    assert_eq!(interp.gnu_hash_table_capacity(id), Some(0));

    for key in 0..6 {
        assert!(interp.equal_hash_put(id, Value::Integer(key), Value::Integer(key), &env,));
    }
    assert_eq!(interp.gnu_hash_table_capacity(id), Some(6));
    interp.replace_hash_table_runtime_entries(id, "eql", Vec::new());
    assert_eq!(interp.gnu_hash_table_capacity(id), Some(6));

    for key in 0..7 {
        assert!(interp.equal_hash_put(id, Value::Integer(key), Value::Integer(key), &env,));
    }
    assert_eq!(interp.gnu_hash_table_capacity(id), Some(24));
}

#[test]
fn gc_reachability_reuses_empty_workspace_without_retaining_old_marks() {
    let mut interp = Interpreter::new();
    let record = interp.create_record("gc-workspace-record", Vec::new());
    let Value::Record(record_id) = record else {
        unreachable!("record constructor")
    };
    let keys = vec![
        Value::Symbol(SymbolName::make_uninterned(
            Value::String("gc-workspace-key".into()),
            "gc-workspace-key",
            90210,
        )),
        Value::list([Value::Integer(71)]),
        Value::vector([Value::Integer(83)]),
        Value::String("gc-workspace-text".into()),
        record,
    ];
    let table = crate::lisp::json::make_hash_table(
        &mut interp,
        "eq",
        keys.iter().cloned().map(|key| (key, Value::T)).collect(),
    );
    let Value::Record(table_id) = table else {
        unreachable!("hash table constructor")
    };
    interp.find_record_mut(table_id).unwrap().slots[5] = Value::symbol("key");
    interp.set_global_binding("gc-workspace-table", table);

    let mut previous_cons_capacity = 0;
    for rooted in [true, false, true, false] {
        let roots = if rooted { keys.as_slice() } else { &[] };
        let result = interp.weak_hash_reachability(&Env::new(), roots);
        let (_, entries, keep) = result
            .tables
            .iter()
            .find(|(id, _, _)| *id == table_id)
            .expect("original weak table");
        assert_eq!(entries.len(), keys.len());
        assert_eq!(keep, &vec![rooted; keys.len()]);
        assert_eq!(result.live_records.contains(&record_id), rooted);
        interp.install_gc_record_census(result.live_records);
        assert_eq!(interp.gc_live_record_ids.contains(&record_id), rooted);
        let scratch = interp.gc_reachability_scratch.borrow();
        macro_rules! assert_empty {
            ($($field:ident),* $(,)?) => {
                $(assert!(scratch.$field.is_empty(), stringify!($field));)*
            };
        }
        assert_empty!(
            big_integers,
            floats,
            strings,
            string_objects,
            symbols,
            conses,
            vectors,
            lambdas,
            buffers,
            markers,
            overlays,
            char_tables,
            frames,
            terminals,
            records,
            finalizers,
            reader_forms,
        );
        assert!(scratch.symbols.capacity() > 0);
        assert!(scratch.conses.capacity() > 0);
        assert!(scratch.conses.capacity() >= previous_cons_capacity);
        previous_cons_capacity = scratch.conses.capacity();
    }
}

#[test]
fn gc_reachability_preserves_weak_table_fixed_point_across_collections() {
    let mut interp = Interpreter::new();
    let first = Value::list([Value::Integer(11)]);
    let middle = Value::list([Value::Integer(22)]);
    let last = Value::list([Value::Integer(33)]);
    // Reverse dependency order: the first visited table cannot mark LAST
    // until the later table marks MIDDLE, requiring another fixed-point pass.
    let mut table_ids = Vec::new();
    for (name, key, value) in [
        ("gc-chain-tail", middle.clone(), last),
        ("gc-chain-head", first.clone(), middle),
    ] {
        let table = crate::lisp::json::make_hash_table(&mut interp, "eq", vec![(key, value)]);
        let Value::Record(id) = table else {
            unreachable!("hash table constructor")
        };
        interp.find_record_mut(id).unwrap().slots[5] = Value::symbol("key");
        interp.set_global_binding(name, table);
        table_ids.push(id);
    }
    for rooted in [true, false, true] {
        let roots = if rooted {
            std::slice::from_ref(&first)
        } else {
            &[]
        };
        let result = interp.weak_hash_reachability(&Env::new(), roots);
        for table_id in &table_ids {
            let (_, entries, keep) = result
                .tables
                .iter()
                .find(|(id, _, _)| id == table_id)
                .unwrap();
            assert_eq!(entries.len(), 1);
            assert_eq!(keep, &[rooted]);
        }
    }
}

#[test]
fn gc_reachability_preserves_symbol_equality_across_distinct_name_allocations() {
    let interp = Interpreter::new();
    let symbol = SymbolName::make_uninterned(
        Value::String("gc-symbol-key".into()),
        "gc-symbol-key",
        90211,
    );
    let alias = SymbolName::intern(symbol.as_str().to_owned());
    let other = SymbolName::make_uninterned(
        Value::String("gc-symbol-key".into()),
        "gc-symbol-key",
        90212,
    );
    assert_ne!(symbol.identity_ptr(), alias.identity_ptr());
    assert_eq!(symbol, alias);
    let mut marked = LispReachability::default();
    assert!(marked.mark(&interp, &Value::Symbol(symbol)));
    assert!(marked.contains(&Value::Symbol(alias.clone())));
    assert!(!marked.mark(&interp, &Value::Symbol(alias.clone())));
    assert!(!marked.contains(&Value::Symbol(other)));
    marked.clear();
    assert!(!marked.contains(&Value::Symbol(alias)));
}

#[test]
fn gc_reachability_traces_cyclic_live_slots_after_mutation() {
    let mut interp = Interpreter::new();
    let value = eval_str_with(
        &mut interp,
        r#"
        (defvar gc-slot-observer (make-hash-table :test 'eq :weakness 'key))
        (defvar gc-slot-root nil)
        (defvar gc-slot-results nil)
        (let* ((key (list 'old))
               (vector (vector key nil))
               (string (propertize "x" 'back vector))
               (record (record 'gc-slot-record string vector)))
          (aset vector 1 record)
          (puthash key t gc-slot-observer)
          (setq gc-slot-root vector))
        (garbage-collect)
        (setq gc-slot-results
              (cons (list (hash-table-count gc-slot-observer)
                          (gethash (aref gc-slot-root 0) gc-slot-observer))
                    gc-slot-results))
        (aset gc-slot-root 0 (list 'new))
        (puthash (aref gc-slot-root 0) t gc-slot-observer)
        (garbage-collect)
        (setq gc-slot-results
              (cons (list (hash-table-count gc-slot-observer)
                          (gethash (aref gc-slot-root 0) gc-slot-observer))
                    gc-slot-results))
        (setq gc-slot-root nil)
        (garbage-collect)
        (setq gc-slot-results
              (cons (hash-table-count gc-slot-observer) gc-slot-results))
        (reverse gc-slot-results)
        "#,
    );
    assert_eq!(value.to_string(), "((1 t) (1 t) 0)");
}

#[test]
fn gc_reachability_keeps_live_strong_tables_out_of_the_weak_sweep() {
    for test in ["eq", "eql", "equal"] {
        let mut interp = Interpreter::new();
        let key = Value::list([Value::Integer(101)]);
        let value = Value::list([Value::Integer(102)]);
        let Value::Cons(cell) = &key else {
            unreachable!("list constructor")
        };
        let weak_key = Rc::downgrade(cell);
        let strong = crate::lisp::json::make_hash_table(
            &mut interp,
            test,
            vec![(key.clone(), value.clone())],
        );
        let Value::Record(strong_id) = strong else {
            unreachable!("hash table constructor")
        };
        // Include a cycle: an unreachable strong table must not retain its
        // payload merely because one of its values points back to itself.
        assert!(interp.equal_hash_put(strong_id, Value::T, strong.clone(), &Env::new()));
        let observer = crate::lisp::json::make_hash_table(
            &mut interp,
            "eq",
            vec![(key.clone(), Value::T), (value.clone(), Value::T)],
        );
        let Value::Record(observer_id) = observer else {
            unreachable!("hash table constructor")
        };
        interp.find_record_mut(observer_id).unwrap().slots[5] = Value::symbol("key");
        interp.set_global_binding("gc-strong-observer", observer);
        let slots_before = interp.equal_hash_tables[&strong_id].slot_indices.clone();
        let capacity_before = interp.gnu_hash_table_capacity(strong_id);

        let result = interp.weak_hash_reachability(&Env::new(), std::slice::from_ref(&strong));
        assert!(result.live_records.contains(&strong_id));
        assert!(result.tables.iter().all(|(id, _, _)| *id != strong_id));
        let (_, _, keep) = result
            .tables
            .iter()
            .find(|(id, _, _)| *id == observer_id)
            .unwrap();
        assert_eq!(keep, &[true, true]);
        for (id, entries, keep) in result.tables {
            interp.sweep_weak_hash_table(id, entries, &keep);
        }
        interp.install_gc_record_census(result.live_records);
        assert_eq!(
            interp.equal_hash_lookup(strong_id, &key, &Env::new()),
            Some(Some(value.clone()))
        );
        assert_eq!(
            interp.equal_hash_tables[&strong_id].slot_indices,
            slots_before
        );
        assert_eq!(interp.gnu_hash_table_capacity(strong_id), capacity_before);

        drop((key, value));
        assert!(weak_key.upgrade().is_some());
        let result = interp.weak_hash_reachability(&Env::new(), &[]);
        assert!(!result.live_records.contains(&strong_id));
        for table_id in [strong_id, observer_id] {
            let (_, entries, keep) = result
                .tables
                .iter()
                .find(|(id, _, _)| *id == table_id)
                .unwrap();
            assert_eq!(entries.len(), 2);
            assert_eq!(keep, &[false, false]);
        }
        for (id, entries, keep) in result.tables {
            interp.sweep_weak_hash_table(id, entries, &keep);
        }
        interp.install_gc_record_census(result.live_records);
        assert!(
            interp
                .hash_table_runtime_entries(strong_id)
                .unwrap()
                .is_empty()
        );
        assert!(
            interp
                .hash_table_runtime_entries(observer_id)
                .unwrap()
                .is_empty()
        );
        assert!(
            weak_key.upgrade().is_none(),
            "dead cyclic table must release its payload"
        );
    }
}

#[test]
fn gc_reachability_discovers_strong_tables_during_the_weak_fixed_point() {
    let mut interp = Interpreter::new();
    let root = Value::list([Value::Integer(201)]);
    let middle = Value::list([Value::Integer(202)]);
    let leaf = Value::list([Value::Integer(203)]);
    let strong =
        crate::lisp::json::make_hash_table(&mut interp, "eq", vec![(middle.clone(), Value::T)]);
    let Value::Record(strong_id) = strong else {
        unreachable!("hash table constructor")
    };
    let mut weak_ids = Vec::new();
    // Visit dependents before their sources: HEAD marks the strong table,
    // which marks MIDDLE; TAIL then marks LEAF on a later pass. Neither
    // dropping unmarked strong tables nor stopping after one pass is valid.
    for (name, key, value) in [
        ("gc-bridge-leaf", leaf.clone(), Value::T),
        ("gc-bridge-tail", middle, leaf),
        ("gc-bridge-head", root.clone(), strong),
    ] {
        let table = crate::lisp::json::make_hash_table(&mut interp, "eq", vec![(key, value)]);
        let Value::Record(id) = table else {
            unreachable!("hash table constructor")
        };
        interp.find_record_mut(id).unwrap().slots[5] = Value::symbol("key");
        interp.set_global_binding(name, table);
        weak_ids.push(id);
    }
    for rooted in [true, false, true] {
        let roots = if rooted {
            std::slice::from_ref(&root)
        } else {
            &[]
        };
        let result = interp.weak_hash_reachability(&Env::new(), roots);
        assert_eq!(result.live_records.contains(&strong_id), rooted);
        for id in std::iter::once(&strong_id).chain(&weak_ids) {
            let (_, entries, keep) = result
                .tables
                .iter()
                .find(|(table_id, _, _)| table_id == id)
                .unwrap();
            assert_eq!(entries.len(), 1);
            assert_eq!(keep, &[rooted]);
        }
        interp.install_gc_record_census(result.live_records);
    }
}

mod eval_01;
mod eval_02;
mod eval_03;
mod eval_04;
mod eval_05;
