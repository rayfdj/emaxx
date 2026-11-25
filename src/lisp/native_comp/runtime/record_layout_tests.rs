use super::*;

#[test]
fn generic_record_slots_are_the_public_native_payload_and_gc_edges() {
    // For one slot, a collection traces the replacement a native store
    // wrote and not the child it replaced: both are a weak table's keys,
    // and the record the value of a global, made in a frame of its own
    // that is cleared before the collection.
    #[inline(never)]
    fn check(
        interpreter: &mut Interpreter,
        env: &mut Env,
        count: usize,
    ) -> Option<crate::lisp::types::HashTableRef> {
        let type_tag = Value::symbol(&format!("inline-record-type-{count}"));
        let old = Value::vector([Value::Integer(17)]);
        let object = interpreter.create_record_with_type(type_tag, vec![old; count]);
        let words = (object.word() & !TAG_MASK) as *mut usize;
        // alloc.c:allocate_record uses one type slot plus the data slots,
        // without a host pointer or allocation-id prefix in that payload.
        let header = unsafe { words.read() };
        assert_eq!((header >> 24) & 0x3f, 34, "GNU PVEC_RECORD");
        assert_eq!(
            header & 0xfff,
            count + 1,
            "all record slots are inline Lisp fields"
        );
        assert_eq!(
            (header >> 12) & 0xfff,
            0,
            "no host payload behind the Lisp slots"
        );
        assert_eq!(unsafe { words.add(1).read() }, type_tag.word());
        for index in 0..count {
            assert_eq!(unsafe { words.add(index + 2).read() }, old.word());
        }
        let mut heap = NativeHeap::new();
        assert_eq!(
            heap.encode(&object).expect("same public word"),
            object.word()
        );
        assert_eq!(heap.decode(object.word()).expect("checked record"), object);
        if count != 1 {
            return None;
        }
        let current = Value::vector([Value::Integer(43)]);
        // No Rust payload borrow or reconciliation step surrounds a
        // native store. The interpreter and tracer read these fields.
        unsafe { words.add(2).write(current.word()) };
        assert_eq!(
            crate::lisp::primitives::call(interpreter, "aref", &[object, Value::Integer(1)], env)
                .expect("read the native store"),
            current
        );
        interpreter.set_global_binding("inline-record-edges-object", object);
        Some(crate::test_support::weak_witness_table(
            interpreter,
            "inline-record-edges-witnesses",
            &[
                (current, Value::symbol("current")),
                (old, Value::symbol("old")),
            ],
        ))
    }
    let mut interpreter = Interpreter::new();
    let mut env = Env::new();
    for count in [0, 1, 9, 257, 4094] {
        let Some(table) = check(&mut interpreter, &mut env, count) else {
            continue;
        };
        crate::lisp::alloc::clobber_stack();
        let reached =
            crate::test_support::weak_tags_after_collection(&mut interpreter, &env, table);
        assert!(
            reached.contains(&Value::symbol("current")),
            "trace the replacement child"
        );
        assert!(
            !reached.contains(&Value::symbol("old")),
            "do not trace the replaced child"
        );
        let object = interpreter
            .symbol_value("inline-record-edges-object")
            .expect("the record");
        let words = (object.word() & !TAG_MASK) as *mut usize;
        crate::lisp::primitives::call(
            &mut interpreter,
            "aset",
            &[object, Value::Integer(1), Value::Integer(73)],
            &mut env,
        )
        .expect("Lisp writes the same field");
        assert_eq!(unsafe { words.add(2).read() }, Value::Integer(73).word());
    }
}

#[test]
fn generic_record_census_includes_header_and_physical_rounding() {
    let interpreter = Interpreter::new();
    let type_tag = Value::symbol("record-census");
    // GNU alloc.c:pseudovector_nbytes / sweep_vectors, with word alignment.
    // Block bitmap and large mark metadata are separate from Lisp vector slots.
    for (data_slots, physical_words) in [(0, 2), (1, 3), (2, 4), (9, 11), (257, 259), (4094, 4096)]
    {
        let before = interpreter.live_object_census();
        let record = crate::lisp::types::LispRecordRef::filled(type_tag, data_slots, Value::Nil);
        let after = interpreter.live_object_census();
        assert_eq!(after.vectors - before.vectors, 1);
        assert_eq!(after.vector_slots - before.vector_slots, physical_words);
        assert_eq!(record.len(), data_slots + 1);
    }
}

fn make_bytecode_layout_object(
    interpreter: &mut Interpreter,
    environment: &mut Env,
    count: usize,
) -> Value {
    let slots = [
        Value::Integer(0),
        Value::string("unexecuted-layout-code"),
        Value::vector([Value::Integer(37)]),
        Value::Integer(1),
        Value::string("layout documentation"),
        Value::Nil,
    ];
    crate::lisp::primitives::call(interpreter, "make-byte-code", &slots[..count], environment)
        .expect("ordinary byte-code constructor")
}

#[test]
fn bytecode_closure_header_and_words_are_the_native_payload() {
    // A collection traces the constants a native store wrote and not the
    // ones they replaced: both are a weak table's keys, and the closure
    // the value of a global, made in a frame of its own that is cleared
    // before the collection.
    #[inline(never)]
    fn check(
        interpreter: &mut Interpreter,
        environment: &mut Env,
        count: usize,
    ) -> crate::lisp::types::HashTableRef {
        let object = make_bytecode_layout_object(interpreter, environment, count);
        let words = (object.word() & !TAG_MASK) as *mut usize;
        // alloc.c:Fmake_byte_code allocates an ordinary vector, then sets
        // PVEC_CLOSURE. Its Lisp fields immediately follow the header.
        let header = unsafe { words.read() };
        assert_eq!((header >> 24) & 0x3f, 31, "GNU PVEC_CLOSURE");
        assert_eq!(header & 0xfff, count, "all closure fields are inline");
        assert_eq!((header >> 12) & 0xfff, 0, "no host state in the payload");
        let mut fields = Vec::new();
        for index in 0..count {
            let field = crate::lisp::primitives::call(
                interpreter,
                "aref",
                &[object, Value::Integer(index as i64)],
                environment,
            )
            .expect("ordinary closure field read");
            assert_eq!(unsafe { words.add(index + 1).read() }, field.word());
            fields.push(field);
        }
        let mut heap = NativeHeap::new();
        assert_eq!(heap.encode(&object).expect("native word"), object.word());
        assert_eq!(heap.decode(object.word()).expect("native closure"), object);
        let old_constants = fields[2];
        let current = Value::vector([Value::Integer(73)]);
        // GNU make_closure writes the real constant field of its fresh
        // closure. This models that internal native store, not Lisp aset:
        // GNU deliberately rejects aset on closure objects.
        unsafe { words.add(3).write(current.word()) };
        assert_eq!(
            crate::lisp::primitives::call(
                interpreter,
                "aref",
                &[object, Value::Integer(2)],
                environment,
            )
            .expect("read actual native field"),
            current
        );
        interpreter.set_global_binding("closure-constants-edges-object", object);
        crate::test_support::weak_witness_table(
            interpreter,
            "closure-constants-edges-witnesses",
            &[
                (current, Value::symbol("current")),
                (old_constants, Value::symbol("old")),
            ],
        )
    }
    let mut interpreter = Interpreter::new();
    let mut environment = Env::new();
    for count in 4..=6 {
        let table = check(&mut interpreter, &mut environment, count);
        crate::lisp::alloc::clobber_stack();
        let reached =
            crate::test_support::weak_tags_after_collection(&mut interpreter, &environment, table);
        assert!(
            reached.contains(&Value::symbol("current")),
            "trace current constants"
        );
        assert!(
            !reached.contains(&Value::symbol("old")),
            "do not trace replaced constants"
        );
    }
}

#[test]
fn bytecode_closure_allocation_is_header_and_actual_lisp_slots() {
    let mut interpreter = Interpreter::new();
    let mut environment = Env::new();
    for count in 4..=6 {
        let object = make_bytecode_layout_object(&mut interpreter, &mut environment, count);
        let header = unsafe { ((object.word() & !TAG_MASK) as *const usize).read() };
        let allocation_words = 1 + (header & 0xfff) + ((header >> 12) & 0xfff);
        let detached_words = match object.kind() {
            Kind::Record(record) => record.slots.capacity(),
            _ => 0,
        };
        // Include the current detached capacity, not merely the GNU size
        // reported by the census. Block marks remain separate metadata.
        assert_eq!(
            (allocation_words + detached_words) * std::mem::size_of::<Value>(),
            (count + 1) * std::mem::size_of::<Value>(),
            "byte-code closure must have one authoritative inline allocation"
        );
    }
}

#[test]
fn bytecode_closure_copy_counts_both_actual_allocations() {
    let mut interpreter = Interpreter::new();
    for constant_count in [0, 1, 3, 17, 257] {
        for closure_count in [4, 5, 6, 9] {
            let constants = Value::vector(vec![Value::Integer(73); constant_count]);
            let mut fields = vec![Value::Nil; closure_count];
            fields[..4].copy_from_slice(&[
                Value::Integer(0),
                Value::string("unexecuted clone census"),
                constants,
                Value::Integer(1),
            ]);
            let prototype = Value::allocated_closure(&fields);
            let before = interpreter.live_object_census();
            let copy = interpreter
                .make_closure(&prototype, &[])
                .expect("copy closure");
            let after = interpreter.live_object_census();
            // alloc.c:Fmake_closure allocates the copied constants and the
            // copied PVEC_CLOSURE. The zero-length vector is shared static
            // storage; every other vector contributes its header and slots.
            let constants_words = if constant_count == 0 {
                0
            } else {
                constant_count + 1
            };
            assert_eq!(
                after.vectors - before.vectors,
                1 + usize::from(constant_count != 0)
            );
            assert_eq!(
                after.vector_slots - before.vector_slots,
                closure_count + 1 + constants_words
            );
            let Kind::Closure(copy) = copy.kind() else {
                panic!("inline closure");
            };
            assert_eq!(copy.public_len(), closure_count);
            let copied_constants = copy.get(2).expect("constants field");
            assert_eq!(copied_constants, constants);
            assert_eq!(copied_constants.eq_value(constants), constant_count == 0);
            assert!(!Value::Closure(copy).eq_value(prototype));
        }
    }
}
