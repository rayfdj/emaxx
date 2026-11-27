use super::*;
use crate::lisp::reader::Reader;
use std::path::PathBuf;
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

#[test]
fn file_notification_roots_follow_queue_dispatch_and_error_requeue() {
    // The queued inputs are made in a frame of their own, cleared before
    // each collection, so that no test-owned reference conceals an omitted
    // edge in the queue's root set: each is a weak table's key, and a
    // collection keeps exactly what the queue holds.
    #[inline(never)]
    fn queue(interp: &mut Interpreter, env: &mut Env) -> crate::lisp::types::HashTableRef {
        let callback = interp
            .eval(
                &Reader::new(
                    "(lambda (event) (garbage-collect)
                       (if (eq (car event) 'fail) (error \"notification control\")
                         (setq notification-control-result (length (cadr event)))))",
                )
                .read()
                .expect("read collecting callback")
                .expect("callback form"),
                env,
            )
            .expect("create collecting callback");
        let descriptor = Value::cons(Value::Integer(37), Value::Integer(4));
        let events = [
            Value::list([Value::symbol("fail")]),
            Value::list([Value::symbol("second"), Value::string(&"x".repeat(257))]),
            Value::list([Value::symbol("third"), Value::string(&"y".repeat(113))]),
        ];
        for event in events {
            interp
                .pending_file_notifications
                .push(PendingFileNotification {
                    path: String::new(),
                    secondary_path: None,
                    action: String::new(),
                    callbacks: vec![(descriptor, callback)],
                    raw_event: Some(event),
                });
        }
        crate::test_support::weak_witness_table(
            interp,
            "file-notification-witnesses",
            &[
                (events[0], Value::symbol("event-0")),
                (events[1], Value::symbol("event-1")),
                (events[2], Value::symbol("event-2")),
                (descriptor, Value::symbol("descriptor")),
                (callback, Value::symbol("callback")),
            ],
        )
    }
    let tags = |names: &[&str]| {
        names
            .iter()
            .map(|name| Value::symbol(name))
            .collect::<Vec<_>>()
    };
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let mut env = Env::new();
    let table = queue(&mut interp, &mut env);
    crate::lisp::alloc::clobber_stack();
    let queued = crate::test_support::weak_tags_after_collection(&mut interp, &env, table);
    for tag in tags(&["event-0", "event-1", "event-2", "descriptor", "callback"]) {
        assert!(queued.contains(&tag), "queued input lost a Lisp edge");
    }
    let error = interp
        .run_pending_file_notifications(&mut env)
        .expect_err("the collecting first callback must propagate its error");
    assert!(error.to_string().contains("notification control"));
    assert_eq!(interp.pending_file_notifications.len(), 2);
    crate::lisp::alloc::clobber_stack();
    let requeued = crate::test_support::weak_tags_after_collection(&mut interp, &env, table);
    assert!(
        !requeued.contains(&Value::symbol("event-0")),
        "consumed event is no longer a queue root"
    );
    assert!(
        requeued.contains(&Value::symbol("event-1"))
            && requeued.contains(&Value::symbol("event-2"))
    );
    assert!(
        interp
            .run_pending_file_notifications(&mut env)
            .expect("deliver the surviving tail")
    );
    assert_eq!(
        interp.symbol_value("notification-control-result"),
        Some(Value::Integer(113))
    );
    assert!(interp.pending_file_notifications.is_empty());
    crate::lisp::alloc::clobber_stack();
    let finished = crate::test_support::weak_tags_after_collection(&mut interp, &env, table);
    for tag in tags(&["event-0", "event-1", "event-2", "descriptor", "callback"]) {
        assert!(
            !finished.contains(&tag),
            "finished batch retained its Lisp input"
        );
    }
}

#[test]
fn reachability_marks_deep_cons_paths_and_cycles_without_recursive_stack_growth() {
    // A collection on a 2 MiB stack marks a 100,000-cons path, along the
    // cdrs and along the cars, and a cycle, to their end: the leaf at the
    // end, a weak table's key reached only through the path (made in a
    // frame of its own, cleared before the collection), is kept.
    #[inline(never)]
    fn hold(interp: &mut Interpreter, along_car: Option<bool>) -> crate::lisp::types::HashTableRef {
        let leaf = Value::String("deep reachable leaf".into());
        let root = match along_car {
            Some(along_car) => {
                let mut root = leaf;
                for _ in 0..100_000 {
                    root = if along_car {
                        Value::cons(root, Value::Nil)
                    } else {
                        Value::cons(Value::Nil, root)
                    };
                }
                root
            }
            None => {
                let cycle = Value::cons(Value::vector([leaf]), Value::Nil);
                let Kind::Cons(cell) = cycle.kind() else {
                    unreachable!("constructed cons");
                };
                cell.cdr.set(cycle);
                cycle
            }
        };
        interp.set_global_binding("deep-reachable-root", root);
        crate::test_support::weak_witness_table(
            interp,
            "deep-reachable-witnesses",
            &[(leaf, Value::symbol("leaf"))],
        )
    }
    // The frames `hold' left, cleared (`clobber_stack' is deeper than this
    // stack).
    #[inline(never)]
    fn clear_below() {
        let mut scratch = std::mem::MaybeUninit::<[usize; 1 << 14]>::uninit();
        // SAFETY: a write of every word of the array on this frame.
        unsafe { std::ptr::write_bytes(scratch.as_mut_ptr().cast::<u8>(), 1, 8 << 14) };
        std::hint::black_box(&scratch);
    }
    thread::Builder::new()
        .stack_size(2 * 1024 * 1024)
        .spawn(|| {
            let mut interp = Interpreter::new();
            let env = Env::new();
            for along_car in [Some(false), Some(true), None] {
                let table = hold(&mut interp, along_car);
                clear_below();
                assert_eq!(
                    crate::test_support::weak_tags_after_collection(&mut interp, &env, table),
                    [Value::symbol("leaf")],
                    "the leaf at the path's end is reached"
                );
                interp.set_global_binding("deep-reachable-root", Value::Nil);
            }
        })
        .expect("small-stack collector control")
        .join()
        .expect("the complete reachable graph is marked without overflowing");
}

#[test]
fn a_collection_leaves_every_mark_bit_clear_and_a_new_cons_unmarked() {
    // alloc.c's sweeps unmark what they keep (XUNMARK_CONS, the float
    // blocks' bits, the vectors' ARRAY_MARK_FLAG, the symbols'
    // gcmarkbit), and Fcons and make_float give a new object no mark:
    // after a collection no bit of a cons, float or vector block is set,
    // no large vector's header carries the flag and no symbol's gcmarkbit
    // is set, an object made between collections is unmarked until the
    // next one marks it, and that one leaves it unmarked again, kept.
    let mut interp = Interpreter::new();
    let mut env = Env::new();
    crate::test_support::eval_lisp(&mut interp, &mut env, "(garbage-collect)")
        .expect("a collection");
    assert!(crate::lisp::alloc::every_mark_bit_clear());
    let cons = Value::cons(Value::Integer(5), Value::Nil);
    let float = Value::Float(crate::lisp::alloc::allocate_float(0.25));
    let vector = Value::vector([cons, float]);
    let Kind::Cons(cell) = cons.kind() else {
        unreachable!("constructed cons");
    };
    let Kind::Float(cell_float) = float.kind() else {
        unreachable!("constructed float");
    };
    assert!(!cell.mark_bit().is_marked());
    assert!(!cell_float.mark_bit().is_marked());
    // A vector too large for a block, whose mark is its header's
    // ARRAY_MARK_FLAG: made without it, and unmarked again by the sweep
    // that keeps it (XUNMARK_VECTOR), so its length reads as it was made.
    let large = Value::vector(std::iter::repeat_n(cons, 600));
    let Kind::Vector(large_vector) = large.kind() else {
        unreachable!("constructed vector");
    };
    assert!(!large_vector.mark_bit().is_marked());
    // A symbol, whose mark is its gcmarkbit: Fmake_symbol makes it
    // unmarked (init_symbol's `gcmarkbit = 0'), and sweep_symbols clears
    // the bit of the one it keeps.
    let symbol = crate::lisp::primitives::call(
        &mut interp,
        "make-symbol",
        &[Value::String("zz-symbol-made-between".into())],
        &mut env,
    )
    .expect("a symbol");
    let Kind::Symbol(name) = symbol.kind() else {
        unreachable!("constructed symbol");
    };
    assert!(!name.is_marked());
    large_vector.set(0, symbol);
    interp.set_symbol_value_cell("zz-made-between", vector);
    interp.set_symbol_value_cell("zz-large-made-between", large);
    crate::test_support::eval_lisp(&mut interp, &mut env, "(garbage-collect)")
        .expect("the next collection");
    assert!(crate::lisp::alloc::every_mark_bit_clear());
    assert!(!cell.mark_bit().is_marked() && !cell_float.mark_bit().is_marked());
    assert!(!large_vector.mark_bit().is_marked());
    assert!(!name.is_marked());
    assert_eq!(large_vector.len(), 600);
    assert_eq!(
        crate::test_support::eval_lisp(
            &mut interp,
            &mut env,
            "(list (length zz-large-made-between)
                   (symbol-name (aref zz-large-made-between 0))
                   (aref zz-large-made-between 599))"
        )
        .expect("the large vector")
        .to_string(),
        "(600 \"zz-symbol-made-between\" (5))"
    );
    assert_eq!(
        crate::test_support::eval_lisp(&mut interp, &mut env, "zz-made-between")
            .expect("the vector")
            .to_string(),
        "[(5) 0.25]"
    );
}

// A record nothing holds, of a type the symbol in `zz-pending-type' names,
// made in a frame of its own so that no word of the test's names it.
#[inline(never)]
fn make_dead_record_of_pending_type(interp: &mut Interpreter) {
    let type_tag = interp
        .symbol_value_cell("zz-pending-type")
        .expect("the type symbol");
    let id = interp.alloc_record_id();
    std::hint::black_box(crate::lisp::types::RecordRef::allocate(RecordState {
        id,
        owner: 0,
        type_tag,
        slots: Vec::new(),
        kind: RecordKind::UserPointer,
    }));
}

#[test]
fn the_sweep_s_note_of_a_freed_record_reads_no_name_string() {
    // alloc.c's sweep of a dead vector reads nothing of the objects it
    // names.  The note the sweep here leaves of a freed record carries
    // the key of its type's symbol only when that key is already built:
    // it never builds one from the symbol's name, a string, which
    // gc_sweep's order (sweep_strings first) may already have freed.
    let _runtime = crate::Runtime::lock();
    let mut interp = Interpreter::new();
    let mut env = Env::new();
    let symbol = crate::lisp::primitives::call(
        &mut interp,
        "make-symbol",
        &[Value::String("zz-pending".into())],
        &mut env,
    )
    .expect("a symbol");
    let Kind::Symbol(name) = symbol.kind() else {
        unreachable!("constructed symbol");
    };
    assert!(name.host_text_if_built().is_none(), "the key is pending");
    interp.set_symbol_value_cell("zz-pending-type", symbol);
    make_dead_record_of_pending_type(&mut interp);
    crate::lisp::alloc::clobber_stack();
    crate::test_support::eval_lisp(&mut interp, &mut env, "(garbage-collect)")
        .expect("a collection");
    assert!(
        name.host_text_if_built().is_none(),
        "the sweep built the type symbol's key from its name"
    );
}

#[test]
fn the_purge_drops_an_id_it_was_given_no_name_for() {
    // The registry files a record's id under its type's name.  A freed
    // record whose note carries no name (its type symbol's key was not
    // built where the sweep read it) leaves the index all the same.
    let mut interp = Interpreter::new();
    let id = interp.alloc_record_id();
    let record = interp.install_record(RecordState {
        id,
        owner: 0,
        type_tag: Value::symbol("zz-purged-type"),
        slots: Vec::new(),
        kind: RecordKind::UserPointer,
    });
    assert_eq!(interp.record_ids_by_type("zz-purged-type"), vec![id]);
    interp.purge_freed_records(&[crate::lisp::alloc::FreedRecord {
        owner: record.owner,
        id,
        identity: record.identity(),
        type_name: None,
    }]);
    assert!(
        interp.record_ids_by_type("zz-purged-type").is_empty(),
        "the id is still filed under its type's name"
    );
}

fn panic_eval_error(interp: &mut Interpreter, error: LispError) -> ! {
    let rendered_error = match error.kind() {
        LispErrorKind::SignalValue(value) => crate::lisp::primitives::render_prin1_ephemeral(
            interp,
            value,
            &crate::lisp::types::Env::new(),
        )
        .unwrap_or_else(|_| error.to_string()),
        _ => error.to_string(),
    };
    panic!("evaluation failed: {rendered_error}")
}

fn eval_str_bare(src: &str) -> Value {
    let mut interp = Interpreter::new();
    let mut env: Env = crate::lisp::types::Env::new();
    // The interpreter's reader, as readevalloop's: each form read (its
    // symbols interned in the current obarray, its literals built) and
    // evaluated before the next is read.
    let mut offset = 0;
    let mut result = Value::Nil;
    while let Some(form) =
        crate::lisp::read_next_load_form(&mut interp, src, &mut offset, &mut env).unwrap()
    {
        result = interp
            .eval(&form, &mut env)
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
    interp.select_symbol_owner();
    let mut env: Env = crate::lisp::types::Env::new();
    // The interpreter's reader, as `eval_str_bare' reads.
    let mut offset = 0;
    let mut result = Value::Nil;
    while let Some(form) =
        crate::lisp::read_next_load_form(interp, src, &mut offset, &mut env).unwrap()
    {
        result = interp
            .eval(&form, &mut env)
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
    let continuation = thread::Builder::new()
        .stack_size(128 * 1024 * 1024)
        .spawn(move || {
            let _permit = permit.moved_to_this_thread();
            test();
        })
        .unwrap();
    // The test goes on on the large stack: lend it this thread's runtime
    // while waiting, as thread-join gives global_lock up.
    crate::lisp::runtime::release_while(|| continuation.join()).unwrap();
}

fn run_large_stack_test(test_fn: fn()) {
    let permit = crate::test_support::acquire_host_test_permit();
    let continuation = thread::Builder::new()
        .stack_size(16 * 1024 * 1024)
        .spawn(move || {
            let _permit = permit.moved_to_this_thread();
            test_fn();
        })
        .unwrap();
    // The test goes on on the large stack: lend it this thread's runtime
    // while waiting, as thread-join gives global_lock up.
    crate::lisp::runtime::release_while(|| continuation.join()).unwrap();
}

fn run_exclusive_with_large_stack(test: impl FnOnce() + Send + 'static) {
    let permit = crate::test_support::acquire_exclusive_host_test_permit();
    thread::Builder::new()
        .stack_size(128 * 1024 * 1024)
        .spawn(move || {
            let _permit = permit.moved_to_this_thread();
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
    let Kind::HashTable(id) =
        crate::lisp::json::make_hash_table_with_capacity(&mut interp, "eql", Vec::new(), 0).kind()
    else {
        unreachable!("hash-table constructor must return a hash table")
    };
    let env = crate::lisp::types::Env::new();
    assert_eq!(Some(id.capacity()), Some(0));

    for key in 0..6 {
        assert!(matches!(
            interp.equal_hash_put(id, Value::Integer(key), Value::Integer(key), &env,),
            Ok(true)
        ));
    }
    assert_eq!(Some(id.capacity()), Some(6));
    id.clear();
    assert_eq!(Some(id.capacity()), Some(6));

    for key in 0..7 {
        assert!(matches!(
            interp.equal_hash_put(id, Value::Integer(key), Value::Integer(key), &env,),
            Ok(true)
        ));
    }
    assert_eq!(Some(id.capacity()), Some(24));
}

mod eval_01;
mod eval_02;
mod eval_03;
mod eval_04;
mod eval_05;
mod uninterned_symbols;
