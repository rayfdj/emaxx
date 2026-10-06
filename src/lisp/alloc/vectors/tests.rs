use super::*;
use crate::lisp::types::{Env, Kind};

#[test]
fn vector_allocations_have_word_sized_footprints() {
    // Check real allocated headers, including both sides of the small/large
    // boundary. These are physical footprints, not Lisp's census counters.
    let lengths = [1, 2, 3, 4, 5, 6, 7, 8, 249, 250, 251, 252, 511, 512, 4097];
    let measured: Vec<_> = lengths
        .into_iter()
        .map(|len| {
            let vector = VectorRef::allocate(vec![Value::Nil; len]);
            (len, vector.header().nbytes())
        })
        .collect();
    println!("vector footprints (slots, bytes): {measured:?}");
    println!(
        "vector metadata: block={} bitmap={} usable={} large_mark={} free_lists={}",
        VECTOR_BLOCK_SIZE,
        std::mem::size_of::<VectorBlockMarks>(),
        VECTOR_BLOCK_BYTES,
        LARGE_MARK_BYTES,
        std::mem::size_of_val(&FREE_LISTS),
    );
    for (len, bytes) in measured {
        assert_eq!(bytes, (len + 1) * WORD_SIZE, "vector length {len}");
    }
}

#[test]
fn vector_word_offsets_survive_mark_bitmap_boundaries() {
    let mut interpreter = crate::lisp::eval::Interpreter::new();
    let mut environment = Env::new();
    let mut roots = crate::lisp::alloc::RootedVec::new();
    // Repeated three-word allocations visit odd word offsets. Larger
    // vectors exercise split free lists and separate marks.
    let lengths = [2, 2, 2, 2, 4, 6, 1, 250, 251, 252, 511];
    let mixed_count = 660;
    // Two roughly half-block objects can leave no object start in the last
    // bitmap word. Free-list history decides whether the mixed population
    // reaches it. Preserve that entire population, then use one-slot vectors
    // until every bitmap word is represented. This upper bound could fill
    // every existing block, every block the mixed pass might add, and one
    // additional block; failure to cover all words still fails the test.
    let allocation_limit = mixed_count
        + (blocks_of(BlockKind::VectorBlock).len() + mixed_count + 1)
            * (VECTOR_BLOCK_BYTES / VBLOCK_BYTES_MIN);
    let length_at = |index: usize| {
        if index < mixed_count {
            lengths[index % lengths.len()]
        } else {
            1
        }
    };
    let mut odd_offsets = 0;
    let mut bitmap_words = std::collections::BTreeSet::new();
    let mut marks = std::collections::BTreeSet::new();
    for index in 0..allocation_limit {
        let len = length_at(index);
        let value = Value::vector(std::iter::repeat_n(Value::Integer(index as i64), len));
        let Kind::Vector(vector) = value.kind() else {
            unreachable!("constructed vector")
        };
        let address = vector.identity();
        assert_eq!(address % WORD_SIZE, 0);
        let bytes = vector.header().nbytes();
        match vector.mark_bit().0 {
            VectorMarkStorage::Block(block_marks, bit) => {
                let base = address & !(VECTOR_BLOCK_SIZE - 1);
                assert!(
                    marks.insert((base, bit)),
                    "distinct live objects need distinct mark bits"
                );
                assert_eq!(
                    std::ptr::from_ref(block_marks) as usize,
                    base + VECTOR_BLOCK_BYTES,
                );
                assert!(address + bytes <= base + VECTOR_BLOCK_BYTES);
                assert_eq!(
                    live_small_vector_holding(base, address + len * WORD_SIZE),
                    Some(vector.0.as_ptr()),
                );
                odd_offsets += usize::from((address / WORD_SIZE) % 2 == 1);
                bitmap_words.insert(bit / usize::BITS as usize);
            }
            VectorMarkStorage::Separate(mark) => {
                assert_eq!(std::ptr::from_ref(mark) as usize, address + bytes);
                assert_eq!(
                    live_large_vector_holding(address, address + len * WORD_SIZE),
                    Some(vector.0.as_ptr()),
                );
            }
        }
        vector.set(0, value);
        roots.push(value);
        if index + 1 >= mixed_count && bitmap_words.len() == VECTOR_MARK_WORDS {
            break;
        }
    }
    println!(
        "vector bitmap coverage: {} allocations, {} odd offsets, words {bitmap_words:?}",
        roots.len(),
        odd_offsets,
    );
    assert!(
        odd_offsets > 100,
        "many objects must start between 16-byte boundaries"
    );
    assert_eq!(bitmap_words.len(), VECTOR_MARK_WORDS);
    for _ in 0..3 {
        crate::lisp::alloc::clobber_stack();
        crate::lisp::primitives::call(&mut interpreter, "garbage-collect", &[], &mut environment)
            .expect("mark and sweep mixed vector sizes");
        for (index, value) in roots.iter().enumerate() {
            let Kind::Vector(vector) = value.kind() else {
                unreachable!("rooted vector")
            };
            assert_eq!(vector.len(), length_at(index));
            assert!(vector.get(0).expect("cycle").eq_value(*value));
            if vector.len() > 1 {
                assert_eq!(
                    vector.get(vector.len() - 1),
                    Some(Value::Integer(index as i64))
                );
            }
        }
    }
}

#[test]
fn native_subr_allocation_has_gnu_fields_and_direct_tagged_identity() {
    use native_functions::NativeFunctionSpec;
    use std::ffi::CString;

    extern "C" fn identity(word: usize) -> usize {
        word
    }
    let fields = [
        Value::Integer(17),
        Value::Integer(29),
        Value::Integer(41),
        Value::Nil,
        Value::T,
    ];
    let function = NativeFunctionRef::new(NativeFunctionSpec {
        target: identity as *const std::ffi::c_void,
        min_args: 0,
        max_args: 1,
        name: CString::new("native-layout-λ").expect("NUL-free fixture name"),
        c_name: CString::new("Fnative_layout_27").expect("NUL-free fixture name"),
        doc: 13,
        fields,
    });
    let value = Value::NativeFunction(function);
    let header = function.0.as_ptr();
    // These read the actual configured Lisp_Subr offsets, not a mirror or a
    // requested logical size. Static DEFUN subrs remain a distinct Rust type.
    unsafe {
        assert_eq!((*header).tag(), VectorTag::Subr);
        assert_eq!((*header).nbytes(), 88);
        assert_eq!((*header).size & PSEUDOVECTOR_SIZE_MASK, 0);
        assert!(subr_is_allocated(header));
        let bytes = header.cast::<u8>();
        assert_eq!(
            bytes.add(8).cast::<*const std::ffi::c_void>().read(),
            identity as *const _
        );
        assert_eq!(bytes.add(16).cast::<i16>().read(), 0);
        assert_eq!(bytes.add(18).cast::<i16>().read(), 1);
        assert_eq!(bytes.add(48).cast::<isize>().read(), 13);
        for (offset, field) in [32, 40, 56, 72, 80].into_iter().zip(fields) {
            assert_eq!(bytes.add(offset).cast::<usize>().read(), field.word());
        }
        assert_eq!(
            std::ffi::CStr::from_ptr(bytes.add(24).cast::<*const std::ffi::c_char>().read())
                .to_str()
                .expect("native object retains UTF-8 name"),
            "native-layout-λ"
        );
        assert_eq!(
            std::ffi::CStr::from_ptr(bytes.add(64).cast::<*const std::ffi::c_char>().read())
                .to_str()
                .expect("native object retains UTF-8 name"),
            "Fnative_layout_27"
        );
        assert!(value_of(header).eq_value(value));
    }
    assert_eq!(value.word(), function.identity() | 5);
    assert!(matches!(value.kind(), Kind::NativeFunction(same) if same.ptr_eq(&function)));
    assert_eq!(function.fields(), fields);
    assert_eq!(format!("{value}"), "#<subr native-layout-λ>");
}

#[test]
fn native_subr_marks_all_five_fields_and_releases_unreachable_cycles() {
    use native_functions::NativeFunctionSpec;
    use std::ffi::CString;
    const HIDE: usize = 0x5555_5555_5555_5555;

    #[inline(never)]
    fn allocate_cycle() -> (crate::lisp::alloc::RootedVec<Value>, [usize; 6]) {
        let function = NativeFunctionRef::new(NativeFunctionSpec {
            target: std::ptr::null(),
            min_args: 0,
            max_args: 0,
            name: CString::new("gc-native-object-π").expect("NUL-free fixture name"),
            c_name: CString::new("Fgc_native_object_91").expect("NUL-free fixture name"),
            doc: 7,
            fields: [Value::Nil; 5],
        });
        let value = Value::NativeFunction(function);
        let fields =
            std::array::from_fn(|index| Value::vector([Value::Integer(index as i64), value]));
        function.set_fields(fields);
        let mut hidden = [function.identity() ^ HIDE; 6];
        for (index, field) in fields.into_iter().enumerate() {
            hidden[index + 1] = (field.word() & !7) ^ HIDE;
        }
        let mut roots = crate::lisp::alloc::RootedVec::new();
        roots.push(value);
        (roots, hidden)
    }
    #[inline(never)]
    fn is_live(hidden: usize, tag: VectorTag) -> bool {
        let address = hidden ^ HIDE;
        matches!(unsafe { crate::lisp::alloc::mem_find(address) },
            Some(crate::lisp::alloc::Found::Vectorlike(header))
                if header as usize == address && unsafe { header_tag(header) } == tag)
    }
    #[inline(never)]
    fn check_fields(roots: &crate::lisp::alloc::RootedVec<Value>) {
        let value = roots[0];
        let Kind::NativeFunction(function) = value.kind() else {
            panic!("native object kind")
        };
        assert_eq!(function.name(), "gc-native-object-π");
        assert_eq!(function.c_name(), "Fgc_native_object_91");
        for (index, field) in function.fields().into_iter().enumerate() {
            let Kind::Vector(vector) = field.kind() else {
                panic!("traced native field")
            };
            assert_eq!(vector.get(0), Some(Value::Integer(index as i64)));
            assert!(vector.get(1).expect("cycle slot").eq_value(value));
        }
    }
    let mut interpreter = crate::lisp::eval::Interpreter::new();
    let environment = Env::new();
    let (roots, hidden) = allocate_cycle();
    for _ in 0..3 {
        crate::lisp::alloc::clobber_stack();
        crate::lisp::native_comp::begin_garbage_collection(&mut interpreter, &environment);
        assert!(is_live(hidden[0], VectorTag::Subr));
        for child in &hidden[1..] {
            assert!(is_live(*child, VectorTag::Normal));
        }
        check_fields(&roots);
    }
    drop(roots);
    crate::lisp::alloc::clobber_stack();
    crate::lisp::native_comp::begin_garbage_collection(&mut interpreter, &environment);
    assert!(
        !is_live(hidden[0], VectorTag::Subr),
        "unreachable native subr retained"
    );
    for child in &hidden[1..] {
        assert!(
            !is_live(*child, VectorTag::Normal),
            "unreachable native field retained"
        );
    }
}
