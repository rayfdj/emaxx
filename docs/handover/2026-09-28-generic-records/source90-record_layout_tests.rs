use super::*;

#[test]
fn generic_record_slots_are_the_public_native_payload_and_gc_edges() {
    let mut interpreter = Interpreter::new();
    let mut env = Env::new();
    for count in [0, 1, 9, 257, 4094] {
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
        if count == 1 {
            let current = Value::vector([Value::Integer(43)]);
            // No Rust payload borrow or reconciliation step surrounds a
            // native store. The interpreter and tracer read these fields.
            unsafe { words.add(2).write(current.word()) };
            assert_eq!(
                crate::lisp::primitives::call(
                    &mut interpreter,
                    "aref",
                    &[object, Value::Integer(1)],
                    &mut env
                )
                .expect("read the native store"),
                current
            );
            let mut reached = crate::lisp::eval::LispReachability::default();
            reached.mark(&interpreter, &object);
            let epoch = crate::lisp::types::current_mark_epoch();
            let (Kind::Vector(current), Kind::Vector(old)) = (current.kind(), old.kind()) else {
                unreachable!("vector children")
            };
            assert!(
                current.mark_bit().is_marked(epoch),
                "trace the replacement child"
            );
            assert!(
                !old.mark_bit().is_marked(epoch),
                "do not trace the replaced child"
            );
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
}
