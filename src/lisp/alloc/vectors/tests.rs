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

#[test]
fn native_unit_allocation_has_actual_gnu_fields_and_direct_tagged_identity() {
    let unit = NativeUnitRef::new();
    let fields = std::array::from_fn(|index| Value::Integer(17 + index as i64 * 11));
    unit.set_fields(fields);
    unit.loaded_once.set(true);
    unit.load_ongoing.set(true);
    let value = Value::NativeCompUnit(unit);
    let header = unit.0.as_ptr();
    // Actual comp.h offsets, including the two one-byte flags and padding.
    unsafe {
        assert_eq!((*header).tag(), VectorTag::NativeCompUnit);
        assert_eq!((*header).nbytes(), 88);
        assert_eq!((*header).size & PSEUDOVECTOR_SIZE_MASK, 7);
        let bytes = header.cast::<u8>();
        for (index, field) in fields.into_iter().enumerate() {
            assert_eq!(
                bytes.add(8 + index * 8).cast::<usize>().read(),
                field.word()
            );
        }
        assert!(bytes.add(64).cast::<*mut usize>().read().is_null());
        assert_eq!(bytes.add(72).read(), 1);
        assert_eq!(bytes.add(73).read(), 1);
        assert!(
            bytes
                .add(80)
                .cast::<*mut std::ffi::c_void>()
                .read()
                .is_null()
        );
        // A store through the actual C field is immediately authoritative.
        bytes
            .add(16)
            .cast::<std::cell::Cell<Value>>()
            .as_ref()
            .expect("allocated optimization-quality field")
            .set(Value::Integer(313));
        assert_eq!(unit.field(1), Value::Integer(313));
        assert!(value_of(header).eq_value(value));
    }
    assert_eq!(value.word(), unit.identity() | 5);
    assert!(matches!(value.kind(), Kind::NativeCompUnit(same) if same.ptr_eq(&unit)));
}

#[test]
fn native_unit_traces_seven_fields_from_a_subr_and_collects_the_unreachable_cycle() {
    const HIDE: usize = 0x5555_5555_5555_5555;
    #[inline(never)]
    fn allocate_cycle() -> (crate::lisp::alloc::RootedVec<Value>, [usize; 9]) {
        let unit = NativeUnitRef::new();
        let value = Value::NativeCompUnit(unit);
        let function = NativeFunctionRef::blank();
        function.set_fields([Value::Nil, Value::Nil, value, Value::Nil, Value::Nil]);
        let subr = Value::NativeFunction(function);
        let fields =
            std::array::from_fn(|index| Value::vector([Value::Integer(index as i64), value, subr]));
        unit.set_fields(fields);
        let mut hidden = [0; 9];
        hidden[0] = unit.identity() ^ HIDE;
        hidden[1] = function.identity() ^ HIDE;
        for (index, field) in fields.into_iter().enumerate() {
            hidden[index + 2] = (field.word() & !7) ^ HIDE;
        }
        let mut roots = crate::lisp::alloc::RootedVec::new();
        roots.push(subr);
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
        let subr = roots[0];
        let Kind::NativeFunction(function) = subr.kind() else {
            panic!("native subr root")
        };
        let value = function.unit();
        let Kind::NativeCompUnit(unit) = value.kind() else {
            panic!("unit held by subr")
        };
        for (index, field) in unit.fields().into_iter().enumerate() {
            let Kind::Vector(vector) = field.kind() else {
                panic!("traced unit field")
            };
            assert_eq!(vector.get(0), Some(Value::Integer(index as i64)));
            assert!(vector.get(1).expect("unit cycle").eq_value(value));
            assert!(vector.get(2).expect("subr cycle").eq_value(subr));
        }
    }
    let mut interpreter = crate::lisp::eval::Interpreter::new();
    let (roots, hidden) = allocate_cycle();
    for _ in 0..3 {
        crate::lisp::alloc::clobber_stack();
        crate::lisp::native_comp::begin_garbage_collection(&mut interpreter, &Env::new());
        assert!(is_live(hidden[0], VectorTag::NativeCompUnit));
        assert!(is_live(hidden[1], VectorTag::Subr));
        for child in &hidden[2..] {
            assert!(is_live(*child, VectorTag::Normal));
        }
        check_fields(&roots);
    }
    drop(roots);
    crate::lisp::alloc::clobber_stack();
    crate::lisp::native_comp::begin_garbage_collection(&mut interpreter, &Env::new());
    assert!(
        !is_live(hidden[0], VectorTag::NativeCompUnit),
        "unreachable unit retained"
    );
    assert!(
        !is_live(hidden[1], VectorTag::Subr),
        "unreachable subr retained"
    );
    for child in &hidden[2..] {
        assert!(
            !is_live(*child, VectorTag::Normal),
            "unreachable unit field retained"
        );
    }
}

#[test]
fn window_object_has_the_pvec_window_header_and_its_slots_inline() {
    use crate::lisp::primitives::{WINDOW_FRAME_SLOT, WindowRef};
    use windows::{
        WINDOW_GNU_VECTOR_SLOTS, WINDOW_LISP_WORDS, WINDOW_REST_WORDS, WINDOW_SLOT_COUNT,
    };

    let mut interpreter = crate::lisp::eval::Interpreter::new();
    let before = live_record_census();
    let window = interpreter.make_window(vec![Value::Integer(17), Value::Integer(29)]);
    let after = live_record_census();
    let value = window.value();
    let Kind::Window(object) = value.kind() else {
        panic!("a window is a window object")
    };
    let header = object.0.as_ptr();
    // The real allocated header: lisp.h's PVEC_WINDOW, the Lisp words
    // first (the contents, then the slots) and the id, the owner and
    // held count, window.h's twenty-four integers from the use time to
    // window_end_pos and the bits in the twenty-nine words after them.
    unsafe {
        assert_eq!((*header).tag(), VectorTag::Window);
        assert_eq!(VectorTag::Window as usize, 11);
        assert_eq!((WINDOW_LISP_WORDS, WINDOW_REST_WORDS), (22, 29));
        assert_eq!(
            (*header).nbytes(),
            (1 + WINDOW_LISP_WORDS + WINDOW_REST_WORDS) * WORD_SIZE
        );
        assert_eq!((*header).size & PSEUDOVECTOR_SIZE_MASK, WINDOW_LISP_WORDS);
        assert_eq!(
            ((*header).size >> PSEUDOVECTOR_SIZE_BITS) & PSEUDOVECTOR_SIZE_MASK,
            WINDOW_REST_WORDS
        );
        assert!(value_of(header).eq_value(value));
        let words = header.cast::<usize>();
        // make_window gives a window no contents; the slots follow them.
        assert_eq!(words.add(1).read(), Value::Nil.word());
        assert_eq!(words.add(2).read(), Value::Integer(17).word());
        assert_eq!(words.add(3).read(), Value::Integer(29).word());
        assert_eq!(
            words.add(2 + WINDOW_FRAME_SLOT).read(),
            interpreter.selected_frame_value().word()
        );
        // A slot the window does not hold is a nil word of the object.
        for index in WINDOW_FRAME_SLOT + 1..WINDOW_SLOT_COUNT {
            assert_eq!(words.add(2 + index).read(), Value::Nil.word());
        }
        assert_eq!(words.add(1 + WINDOW_LISP_WORDS).read() as u64, window.id());
        // A store through the handle is the object's own word.
        window.set_slot(&mut interpreter, WINDOW_SLOT_COUNT - 1, Value::Integer(313));
        assert_eq!(
            words.add(WINDOW_LISP_WORDS).read(),
            Value::Integer(313).word()
        );
        window.set_contents(Value::Integer(7));
        assert_eq!(words.add(1).read(), Value::Integer(7).word());
        // The use time and the sequence number make_window gave it are
        // the two words after the owner and held count.
        let integers = words.add(1 + WINDOW_LISP_WORDS + 2).cast::<i64>();
        assert_eq!(integers.read(), 0);
        window.set_use_time(41);
        assert_eq!(integers.read(), 41);
        assert_eq!(integers.add(1).read(), interpreter.window_sequence_number);
        assert_eq!(integers.add(1).read(), window.sequence_number());
        // window.h's other integers follow, from change_stamp to
        // window_end_pos in its order: all zero in a new window, but for
        // the column number displayed, which is -1, none.
        let fields = integers.add(2);
        let read_fields = || std::array::from_fn::<i64, 22, _>(|index| fields.add(index).read());
        let mut expected = [0; 22];
        expected[18] = -1;
        assert_eq!(read_fields(), expected);
        assert_eq!(window.column_number_displayed(), -1);
        // The bits are bytes of the words after them, in window.h's
        // order from mini to suspend_auto_hscroll, then one for each
        // edge and size and for the window_end_pos the window has been
        // given.
        let bits = header
            .cast::<u8>()
            .add((1 + WINDOW_LISP_WORDS + 26) * WORD_SIZE);
        let read = || std::array::from_fn::<u8, 20, _>(|index| bits.add(index).read());
        assert_eq!(read(), [0; 20]);
        let stores: [fn(WindowRef, bool); 11] = [
            WindowRef::set_mini,
            WindowRef::set_horizontal,
            WindowRef::set_update_mode_line,
            WindowRef::set_last_had_star,
            WindowRef::set_start_at_line_beg,
            WindowRef::set_force_start,
            WindowRef::set_optional_new_start,
            WindowRef::set_cursor_off_p,
            WindowRef::set_window_end_valid,
            WindowRef::set_redisplay,
            WindowRef::set_suspend_auto_hscroll,
        ];
        for (index, store) in stores.into_iter().enumerate() {
            store(window, true);
            let mut expected = [0; 20];
            expected[..=index].fill(1);
            assert_eq!(read(), expected, "bit {index}");
        }
        // Each store through the handle is the object's own integer, and
        // an edge, a size or a window_end_pos given sets its bit.
        window.set_change_stamp(100);
        window.set_pixel_left(Some(101));
        window.set_pixel_top(Some(102));
        window.set_left_col(Some(103));
        window.set_top_line(Some(104));
        window.set_pixel_width(Some(105));
        window.set_pixel_height(Some(106));
        window.set_old_pixel_width(107);
        window.set_old_pixel_height(108);
        window.set_old_body_pixel_width(109);
        window.set_old_body_pixel_height(110);
        window.set_total_cols(Some(111));
        window.set_total_lines(Some(112));
        window.set_hscroll(113);
        window.set_min_hscroll(114);
        window.set_last_modified(115);
        window.set_last_overlay_modified(116);
        window.set_last_point(117);
        window.set_column_number_displayed(118);
        window.set_left_margin_cols(119);
        window.set_right_margin_cols(120);
        window.set_window_end_pos(Some(121));
        assert_eq!(
            read_fields(),
            std::array::from_fn::<i64, 22, _>(|index| 100 + index as i64)
        );
        assert_eq!(read(), [1; 20]);
        // One taken back is zero again, its bit clear.
        window.set_total_cols(None);
        window.set_window_end_pos(None);
        assert_eq!((fields.add(11).read(), fields.add(21).read()), (0, 0));
        let mut expected = [1; 20];
        expected[17] = 0;
        expected[19] = 0;
        assert_eq!(read(), expected);
        // No Lisp word of the object changed for any of them.
        for index in (2..WINDOW_SLOT_COUNT - 1).filter(|index| *index != WINDOW_FRAME_SLOT) {
            assert_eq!(words.add(2 + index).read(), Value::Nil.word());
        }
    }
    assert_eq!(
        std::mem::size_of::<windows::WindowState>(),
        (WINDOW_LISP_WORDS + WINDOW_REST_WORDS) * WORD_SIZE
    );
    assert_eq!(std::mem::size_of::<windows::WindowState>(), 408);
    assert_eq!(value.word(), object.identity() | 5);
    assert!(matches!(value.kind(), Kind::Window(same) if same.ptr_eq(&object)));
    assert!(WindowRef::from_value(&value).is_some_and(|same| same.id() == window.id()));
    // alloc.c counts a window as window.c's whole struct, one vector.
    assert_eq!(after.0 - before.0, 1);
    assert_eq!(after.1 - before.1, WINDOW_GNU_VECTOR_SLOTS);
}

#[test]
fn window_holds_its_first_slots_and_gains_the_rest_by_stores() {
    use crate::lisp::primitives::{WINDOW_BASE_LINE_SLOT, WINDOW_FRAME_SLOT, WINDOW_START_SLOT};
    use windows::WINDOW_SLOT_COUNT;

    let mut interpreter = crate::lisp::eval::Interpreter::new();
    // make_window keeps the slots up to the frame's, whatever it is given.
    let window = interpreter.make_window(vec![Value::T; WINDOW_SLOT_COUNT]);
    assert_eq!(window.slots().len(), WINDOW_FRAME_SLOT + 1);
    assert_eq!(window.slot_opt(WINDOW_START_SLOT), Some(Value::T));
    assert_eq!(
        window.slot_opt(WINDOW_FRAME_SLOT),
        Some(interpreter.selected_frame_value())
    );
    // The one slot past the frame's, the base line's, is absent, which
    // reads as nil but is not a held nil.
    assert_eq!(WINDOW_BASE_LINE_SLOT, WINDOW_FRAME_SLOT + 1);
    assert_eq!(WINDOW_SLOT_COUNT, WINDOW_BASE_LINE_SLOT + 1);
    assert_eq!(window.slot_opt(WINDOW_BASE_LINE_SLOT), None);
    assert_eq!(window.slot(WINDOW_BASE_LINE_SLOT), Value::Nil);
    // A store where the slot is held only: nothing for an absent one.
    window.set_slot_if_held(&mut interpreter, WINDOW_BASE_LINE_SLOT, Value::T);
    assert_eq!(window.slot_opt(WINDOW_BASE_LINE_SLOT), None);
    assert_eq!(window.slots().len(), WINDOW_FRAME_SLOT + 1);
    // ensure_slot grows the held slots with nil.
    window.ensure_slot(&mut interpreter, WINDOW_BASE_LINE_SLOT);
    assert_eq!(window.slots().len(), WINDOW_SLOT_COUNT);
    assert_eq!(window.slot_opt(WINDOW_BASE_LINE_SLOT), Some(Value::Nil));
    window.set_slot_if_held(&mut interpreter, WINDOW_BASE_LINE_SLOT, Value::T);
    assert_eq!(window.slot_opt(WINDOW_BASE_LINE_SLOT), Some(Value::T));
    // A window that holds fewer, as the dead window of a dump can: the
    // slots between its last and one it gains are held nils.
    let few = interpreter.install_dead_window(2);
    assert_eq!(few.slots().len(), 2);
    assert_eq!(few.slot_opt(2), None);
    few.set_slot_if_held(&mut interpreter, 4, Value::T);
    assert_eq!(few.slot_opt(4), None);
    assert_eq!(few.slots().len(), 2);
    few.ensure_slot(&mut interpreter, 4);
    assert_eq!(few.slots().len(), 5);
    assert_eq!(few.slot_opt(3), Some(Value::Nil));
    assert_eq!(few.slot_opt(4), Some(Value::Nil));
    few.set_slot_if_held(&mut interpreter, 4, Value::T);
    assert_eq!(few.slot_opt(4), Some(Value::T));
    few.set_slot(&mut interpreter, WINDOW_SLOT_COUNT - 1, Value::Integer(5));
    assert_eq!(few.slots().len(), WINDOW_SLOT_COUNT);
    assert_eq!(few.slot_opt(WINDOW_SLOT_COUNT - 1), Some(Value::Integer(5)));
    assert_eq!(few.slot_opt(WINDOW_SLOT_COUNT - 2), Some(Value::Nil));
    // An ensure of a held slot changes nothing.
    window.ensure_slot(&mut interpreter, WINDOW_START_SLOT);
    assert_eq!(window.slots().len(), WINDOW_SLOT_COUNT);
    assert_eq!(window.slot_opt(WINDOW_START_SLOT), Some(Value::T));
}

#[test]
fn window_traces_every_slot_and_is_reclaimed_from_the_registry_and_type_index() {
    use windows::{WINDOW_LISP_WORDS, WINDOW_SLOT_COUNT};
    const HIDE: usize = 0x5555_5555_5555_5555;

    // The window's address, its id, and each slot's vector, then its
    // contents'.
    type Hidden = (usize, u64, [usize; WINDOW_LISP_WORDS]);

    #[inline(never)]
    fn allocate(
        interpreter: &mut crate::lisp::eval::Interpreter,
    ) -> (crate::lisp::alloc::RootedVec<Value>, Hidden) {
        let window = interpreter.make_window(Vec::new());
        let value = window.value();
        let mut children = [0; WINDOW_LISP_WORDS];
        // A vector nothing but its slot holds, in every slot: the last
        // stores are of slots a new window does not yet hold.  And one
        // in the contents.
        for (index, child) in children.iter_mut().enumerate() {
            let vector = Value::vector([Value::Integer(index as i64), value]);
            if index < WINDOW_SLOT_COUNT {
                window.set_slot(interpreter, index, vector);
            } else {
                window.set_contents(vector);
            }
            *child = (vector.word() & !7) ^ HIDE;
        }
        let Kind::Window(object) = value.kind() else {
            panic!("a window is a window object")
        };
        let mut roots = crate::lisp::alloc::RootedVec::new();
        roots.push(value);
        (roots, (object.identity() ^ HIDE, window.id(), children))
    }
    #[inline(never)]
    fn is_live(hidden: usize, tag: VectorTag) -> bool {
        let address = hidden ^ HIDE;
        matches!(unsafe { crate::lisp::alloc::mem_find(address) },
            Some(crate::lisp::alloc::Found::Vectorlike(header))
                if header as usize == address && unsafe { header_tag(header) } == tag)
    }
    #[inline(never)]
    fn check_slots(
        interpreter: &crate::lisp::eval::Interpreter,
        roots: &crate::lisp::alloc::RootedVec<Value>,
        id: u64,
    ) {
        let value = roots[0];
        let Kind::Window(window) = value.kind() else {
            panic!("window kind")
        };
        assert_eq!(window.id(), id);
        assert_eq!(window.held(), WINDOW_SLOT_COUNT);
        for index in 0..WINDOW_LISP_WORDS {
            let word = if index < WINDOW_SLOT_COUNT {
                window.slot(index)
            } else {
                Some(window.contents())
            };
            let Some(Kind::Vector(vector)) = word.map(|slot| slot.kind()) else {
                panic!("traced window slot {index}")
            };
            assert_eq!(vector.get(0), Some(Value::Integer(index as i64)));
            assert!(vector.get(1).expect("cycle slot").eq_value(value));
        }
        // The registry and the type index name the same object.
        assert!(
            interpreter
                .window_ref(id)
                .is_some_and(|found| found.value().eq_value(value))
        );
        assert!(interpreter.record_ids_by_type("window").contains(&id));
    }
    let mut interpreter = crate::lisp::eval::Interpreter::new();
    let environment = Env::new();
    let windows_before = interpreter.record_ids_by_type("window");
    let (roots, (window, id, children)) = allocate(&mut interpreter);
    for _ in 0..3 {
        crate::lisp::alloc::clobber_stack();
        crate::lisp::native_comp::begin_garbage_collection(&mut interpreter, &environment);
        assert!(is_live(window, VectorTag::Window));
        for child in &children {
            assert!(is_live(*child, VectorTag::Normal));
        }
        check_slots(&interpreter, &roots, id);
    }
    drop(roots);
    crate::lisp::alloc::clobber_stack();
    crate::lisp::native_comp::begin_garbage_collection(&mut interpreter, &environment);
    assert!(
        !is_live(window, VectorTag::Window),
        "unreachable window retained"
    );
    for child in &children {
        assert!(
            !is_live(*child, VectorTag::Normal),
            "unreachable window slot retained"
        );
    }
    // The freed window's id names nothing: neither the registry nor the
    // type index keeps a window the sweep freed.
    assert!(interpreter.window_ref(id).is_none());
    assert_eq!(interpreter.record_ids_by_type("window"), windows_before);
    // The windows of the frame, which the frame holds, are as they were.
    for id in windows_before {
        assert!(interpreter.window_ref(id).is_some());
    }
}

#[test]
fn window_number_use_time_and_cursor_bit_live_and_die_with_the_window() {
    const HIDE: usize = 0x5555_5555_5555_5555;

    // The window's address, its id and its sequence number.
    type Hidden = (usize, u64, i64);

    // What the registry names under ID: its use time, 0 for no window,
    // and whether its cursor is on, as it is for no window.
    fn use_time(interpreter: &crate::lisp::eval::Interpreter, id: u64) -> i64 {
        interpreter
            .window_ref(id)
            .map_or(0, crate::lisp::primitives::WindowRef::use_time)
    }
    fn cursor_visible(interpreter: &crate::lisp::eval::Interpreter, id: u64) -> bool {
        interpreter
            .window_ref(id)
            .is_none_or(|window| !window.cursor_off_p())
    }

    #[inline(never)]
    fn allocate(
        interpreter: &mut crate::lisp::eval::Interpreter,
    ) -> (crate::lisp::alloc::RootedVec<Value>, Hidden) {
        // window.c:make_window's `w->sequence_number = ++sequence_number',
        // a use time of zero and the cursor on.
        let last = interpreter.window_sequence_number;
        let window = interpreter.make_window(Vec::new());
        let id = window.id();
        assert_eq!(window.sequence_number(), last + 1);
        assert_eq!(interpreter.window_sequence_number, last + 1);
        assert_eq!((window.use_time(), window.cursor_off_p()), (0, false));
        assert_eq!(use_time(interpreter, id), 0);
        assert!(cursor_visible(interpreter, id));
        // select_window's use time and Finternal_show_cursor's bit are
        // the window's own.
        interpreter.record_window_selection(window);
        window.set_cursor_off_p(true);
        assert!(window.use_time() > 0 && window.cursor_off_p());
        assert_eq!(use_time(interpreter, id), window.use_time());
        assert!(!cursor_visible(interpreter, id));
        let value = window.value();
        let Kind::Window(object) = value.kind() else {
            panic!("a window is a window object")
        };
        let mut roots = crate::lisp::alloc::RootedVec::new();
        roots.push(value);
        (
            roots,
            (object.identity() ^ HIDE, id, window.sequence_number()),
        )
    }
    #[inline(never)]
    fn is_live(hidden: usize) -> bool {
        let address = hidden ^ HIDE;
        matches!(unsafe { crate::lisp::alloc::mem_find(address) },
            Some(crate::lisp::alloc::Found::Vectorlike(header))
                if header as usize == address
                    && unsafe { header_tag(header) } == VectorTag::Window)
    }
    #[inline(never)]
    fn check(interpreter: &crate::lisp::eval::Interpreter, id: u64, number: i64) {
        let window = interpreter.window_ref(id).expect("a live window");
        assert_eq!(window.sequence_number(), number);
        assert!(window.use_time() > 0 && window.cursor_off_p());
        assert!(!cursor_visible(interpreter, id));
    }
    let mut interpreter = crate::lisp::eval::Interpreter::new();
    let environment = Env::new();
    let (roots, (window, id, number)) = allocate(&mut interpreter);
    for _ in 0..2 {
        crate::lisp::alloc::clobber_stack();
        crate::lisp::native_comp::begin_garbage_collection(&mut interpreter, &environment);
        assert!(is_live(window));
        check(&interpreter, id, number);
    }
    drop(roots);
    crate::lisp::alloc::clobber_stack();
    crate::lisp::native_comp::begin_garbage_collection(&mut interpreter, &environment);
    assert!(!is_live(window), "unreachable window retained");
    // Nothing is kept under the freed window's id: no use time, and no
    // cursor turned off.
    assert!(interpreter.window_ref(id).is_none());
    assert_eq!(use_time(&interpreter, id), 0);
    assert!(cursor_visible(&interpreter, id));
    // The counter is the interpreter's: the next window has the next
    // number, and starts as any new window does.
    assert_eq!(interpreter.window_sequence_number, number);
    let next = interpreter.make_window(Vec::new());
    assert_eq!(next.sequence_number(), number + 1);
    assert_eq!((next.use_time(), next.cursor_off_p()), (0, false));
}

#[test]
fn window_alone_holds_its_three_markers() {
    use crate::lisp::primitives::{WINDOW_MARKER_SLOTS, window_record_slots};
    const HIDE: usize = 0x5555_5555_5555_5555;

    #[inline(never)]
    fn allocate(
        interpreter: &mut crate::lisp::eval::Interpreter,
    ) -> (crate::lisp::alloc::RootedVec<Value>, [usize; 3]) {
        let buffer = interpreter.current_buffer_id();
        let slots = window_record_slots(interpreter, Some(buffer), 1);
        let window = interpreter.make_window(slots);
        let markers = WINDOW_MARKER_SLOTS.map(|slot| {
            let Kind::Marker(marker) = window.slot(slot).kind() else {
                panic!("a leaf window's marker")
            };
            assert!(marker.buffer().is_some(), "a marker of the window's buffer");
            marker.identity() ^ HIDE
        });
        let mut roots = crate::lisp::alloc::RootedVec::new();
        roots.push(window.value());
        (roots, markers)
    }
    #[inline(never)]
    fn is_live(hidden: usize) -> bool {
        let address = hidden ^ HIDE;
        matches!(unsafe { crate::lisp::alloc::mem_find(address) },
            Some(crate::lisp::alloc::Found::Vectorlike(header))
                if header as usize == address
                    && unsafe { header_tag(header) } == VectorTag::Marker)
    }
    #[inline(never)]
    fn chained(interpreter: &crate::lisp::eval::Interpreter) -> usize {
        interpreter.buffer.markers().count()
    }
    let mut interpreter = crate::lisp::eval::Interpreter::new();
    let environment = Env::new();
    crate::lisp::alloc::clobber_stack();
    crate::lisp::native_comp::begin_garbage_collection(&mut interpreter, &environment);
    let chained_before = chained(&interpreter);
    let (roots, markers) = allocate(&mut interpreter);
    assert_eq!(chained(&interpreter), chained_before + 3);
    for _ in 0..2 {
        crate::lisp::alloc::clobber_stack();
        crate::lisp::native_comp::begin_garbage_collection(&mut interpreter, &environment);
        // Nothing but the window's own slots holds the three markers.
        assert!(markers.iter().all(|marker| is_live(*marker)));
        assert_eq!(chained(&interpreter), chained_before + 3);
    }
    drop(roots);
    crate::lisp::alloc::clobber_stack();
    crate::lisp::native_comp::begin_garbage_collection(&mut interpreter, &environment);
    // With the window they go, off the buffer's chain first.
    assert!(!markers.iter().any(|marker| is_live(*marker)));
    assert_eq!(chained(&interpreter), chained_before);
}

#[test]
fn the_interpreters_window_holders_keep_their_windows() {
    // window.c staticpros selected_window, old_selected_window and
    // minibuf_selected_window, and minibuf.c minibuf_window: each holds
    // its window as the object, which lives while it is held and goes
    // when the holder lets go.
    const HIDE: usize = 0x5555_5555_5555_5555;
    const HOLDERS: usize = 4;

    #[inline(never)]
    fn allocate(interpreter: &mut crate::lisp::eval::Interpreter) -> [(usize, u64); HOLDERS] {
        // A window nothing else holds in each holder: the frame keeps
        // the window it had selected.
        let windows: [_; HOLDERS] = std::array::from_fn(|_| interpreter.make_window(Vec::new()));
        let frame_selected = interpreter.selected_frame_id.borrow().selected_window;
        interpreter.set_selected_window(Some(windows[0]));
        interpreter.selected_frame_id.borrow_mut().selected_window = frame_selected;
        interpreter.old_selected_window = Some(windows[1]);
        interpreter.replace_minibuffer_selected_window(Some(windows[2]));
        let buffer = interpreter.current_buffer_id();
        interpreter.begin_minibuffer_runtime(buffer, windows[3], String::new());
        windows.map(|window| {
            let Kind::Window(object) = window.value().kind() else {
                panic!("a window is a window object")
            };
            (object.identity() ^ HIDE, window.id())
        })
    }
    #[inline(never)]
    fn is_live(hidden: usize) -> bool {
        let address = hidden ^ HIDE;
        matches!(unsafe { crate::lisp::alloc::mem_find(address) },
            Some(crate::lisp::alloc::Found::Vectorlike(header))
                if header as usize == address
                    && unsafe { header_tag(header) } == VectorTag::Window)
    }
    // What each holder holds is the window made for it.
    #[inline(never)]
    fn check(interpreter: &crate::lisp::eval::Interpreter, held: &[(usize, u64)]) {
        let holders = [
            interpreter.selected_window(),
            interpreter.old_selected_window,
            interpreter.minibuffer_selected_window(),
            interpreter
                .active_minibuffer_window_value()
                .as_ref()
                .and_then(crate::lisp::primitives::WindowRef::from_value),
        ];
        for (holder, (hidden, id)) in holders.iter().skip(HOLDERS - held.len()).zip(held) {
            let window = holder.expect("the holder's window");
            let Kind::Window(object) = window.value().kind() else {
                panic!("a window is a window object")
            };
            assert_eq!(object.identity() ^ HIDE, *hidden);
            assert_eq!(window.id(), *id);
            assert!(
                interpreter
                    .window_ref(*id)
                    .is_some_and(|found| found == window)
            );
        }
    }
    // Holder INDEX lets go of its window.
    #[inline(never)]
    fn release(interpreter: &mut crate::lisp::eval::Interpreter, index: usize) {
        match index {
            0 => {
                let frame_selected = interpreter.selected_frame_id.borrow().selected_window;
                interpreter.set_selected_window(frame_selected);
            }
            1 => interpreter.old_selected_window = interpreter.selected_window(),
            2 => {
                interpreter.replace_minibuffer_selected_window(None);
            }
            _ => interpreter.restore_minibuffer_runtime(Default::default()),
        }
    }
    let mut interpreter = crate::lisp::eval::Interpreter::new();
    let environment = Env::new();
    let windows_before = interpreter.record_ids_by_type("window");
    let held = allocate(&mut interpreter);
    for _ in 0..2 {
        crate::lisp::alloc::clobber_stack();
        crate::lisp::native_comp::begin_garbage_collection(&mut interpreter, &environment);
        assert!(held.iter().all(|(window, _)| is_live(*window)));
        check(&interpreter, &held);
    }
    for index in 0..HOLDERS {
        release(&mut interpreter, index);
        crate::lisp::alloc::clobber_stack();
        crate::lisp::native_comp::begin_garbage_collection(&mut interpreter, &environment);
        // The window let go of is collected and its id names nothing;
        // the windows still held are as they were.
        let (window, id) = held[index];
        assert!(!is_live(window), "window of holder {index} retained");
        assert!(interpreter.window_ref(id).is_none());
        assert!(held[index + 1..].iter().all(|(window, _)| is_live(*window)));
        check(&interpreter, &held[index + 1..]);
    }
    assert_eq!(interpreter.record_ids_by_type("window"), windows_before);
}

#[test]
fn a_list_of_windows_keeps_them_for_the_collector() {
    // A walk's windows are held by the list while its user runs Lisp
    // (window.c's window_list is a Lisp list on the C stack): a window
    // of the list that nothing else names survives a collection, a
    // deleted window, and goes once the list does.
    use crate::lisp::primitives::{WindowList, WindowRef};
    const HIDE: usize = 0x5555_5555_5555_5555;

    #[inline(never)]
    fn listed(interpreter: &mut crate::lisp::eval::Interpreter) -> (WindowList, [(usize, u64); 3]) {
        let mut list = WindowList::new();
        let hidden = std::array::from_fn(|_| {
            let window = interpreter.make_window(Vec::new());
            list.push(window);
            let Kind::Window(object) = window.value().kind() else {
                panic!("a window is a window object")
            };
            (object.identity() ^ HIDE, window.id())
        });
        (list, hidden)
    }
    #[inline(never)]
    fn is_live(hidden: usize) -> bool {
        let address = hidden ^ HIDE;
        matches!(unsafe { crate::lisp::alloc::mem_find(address) },
            Some(crate::lisp::alloc::Found::Vectorlike(header))
                if header as usize == address
                    && unsafe { header_tag(header) } == VectorTag::Window)
    }
    #[inline(never)]
    fn check(
        interpreter: &crate::lisp::eval::Interpreter,
        list: &[WindowRef],
        hidden: &[(usize, u64)],
    ) {
        assert_eq!(list.len(), hidden.len());
        for (window, (address, id)) in list.iter().zip(hidden) {
            assert!(is_live(*address));
            // A window nothing shows: deleted, and still the registry's.
            assert!(!window.is_valid());
            assert_eq!(window.id(), *id);
            assert_eq!(interpreter.window_ref(*id), Some(*window));
        }
    }
    let mut interpreter = crate::lisp::eval::Interpreter::new();
    let environment = Env::new();
    let windows_before = interpreter.record_ids_by_type("window");
    let (mut list, hidden) = listed(&mut interpreter);
    for _ in 0..2 {
        crate::lisp::alloc::clobber_stack();
        crate::lisp::native_comp::begin_garbage_collection(&mut interpreter, &environment);
        check(&interpreter, &list, &hidden);
    }
    // A window taken off the list is collected; the others stay.
    #[inline(never)]
    fn take_second(list: &mut WindowList) {
        list.remove(1);
    }
    take_second(&mut list);
    crate::lisp::alloc::clobber_stack();
    crate::lisp::native_comp::begin_garbage_collection(&mut interpreter, &environment);
    assert!(!is_live(hidden[1].0), "a window off the list retained");
    assert!(interpreter.window_ref(hidden[1].1).is_none());
    check(&interpreter, &list, &[hidden[0], hidden[2]]);
    drop(list);
    crate::lisp::alloc::clobber_stack();
    crate::lisp::native_comp::begin_garbage_collection(&mut interpreter, &environment);
    assert!(hidden.iter().all(|(address, _)| !is_live(*address)));
    assert_eq!(interpreter.record_ids_by_type("window"), windows_before);
}
