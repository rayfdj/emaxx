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
            VectorMarkStorage::Dump(_) => panic!("a heap vector marked in the dump's bits"),
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
    use crate::lisp::primitives::{WindowMarker, WindowRef};
    use windows::{WINDOW_LISP_WORDS, WINDOW_VECTOR_WORDS};
    // A field's offset and its store, a bit's number and its store.
    type FieldStore = (usize, fn(WindowRef, Value));
    type BitStore = (u32, fn(WindowRef, bool));

    let mut interpreter = crate::lisp::eval::Interpreter::new();
    let before = live_vector_census();
    let window = interpreter.allocate_window();
    let after = live_vector_census();
    let other = interpreter.allocate_window();
    let value = window.value();
    let Kind::Window(object) = value.kind() else {
        panic!("a window is a window object")
    };
    let header = object.0.as_ptr();
    // The real allocated header: lisp.h's PVEC_WINDOW, window.h's
    // twenty-five Lisp fields, PSEUDOVECSIZE (struct window,
    // mode_line_help_echo), and the rest, 544 bytes in all as
    // sizeof (struct window) is in the configured GNU build.
    unsafe {
        assert_eq!((*header).tag(), VectorTag::Window);
        assert_eq!(VectorTag::Window as usize, 11);
        assert_eq!((WINDOW_LISP_WORDS, WINDOW_VECTOR_WORDS), (25, 67));
        assert_eq!((*header).nbytes(), 544);
        assert_eq!((*header).nbytes(), (1 + WINDOW_VECTOR_WORDS) * WORD_SIZE);
        assert_eq!((*header).size & PSEUDOVECTOR_SIZE_MASK, WINDOW_LISP_WORDS);
        assert_eq!(
            ((*header).size >> PSEUDOVECTOR_SIZE_BITS) & PSEUDOVECTOR_SIZE_MASK,
            WINDOW_VECTOR_WORDS - WINDOW_LISP_WORDS
        );
        assert!(value_of(header).eq_value(value));
        let bytes = header.cast::<u8>();
        let word = |offset: usize| bytes.add(offset).cast::<usize>().read();
        let int = |offset: usize| bytes.add(offset).cast::<i32>().read();
        let long = |offset: usize| bytes.add(offset).cast::<i64>().read();
        // allocate_window's window: every Lisp field nil, the glyph
        // matrices null, prev_buffers and next_buffers nil.
        for offset in (8..=200).step_by(8).chain([224, 232]) {
            assert_eq!(word(offset), Value::Nil.word(), "Lisp field at {offset}");
        }
        assert_eq!((word(208), word(216)), (0, 0));
        // Each store through the handle is the field at window.h's offset.
        let fields: [FieldStore; 16] = [
            (8, WindowRef::set_frame),
            (40, |w, v| w.set_normal_size(false, v)),
            (48, |w, v| w.set_normal_size(true, v)),
            (56, WindowRef::set_new_total),
            (64, WindowRef::set_new_normal),
            (72, WindowRef::set_new_pixel),
            (80, WindowRef::set_contents),
            (88, WindowRef::set_old_buffer),
            (96, |w, v| w.set_marker_field(WindowMarker::Start, v)),
            (104, |w, v| w.set_marker_field(WindowMarker::Pointm, v)),
            (112, |w, v| w.set_marker_field(WindowMarker::OldPointm, v)),
            (160, WindowRef::set_display_table),
            (168, WindowRef::set_dedicated),
            (176, WindowRef::set_combination_limit),
            (184, WindowRef::set_window_parameters),
            (192, WindowRef::set_cursor_type),
        ];
        for (index, (offset, store)) in fields.into_iter().enumerate() {
            store(window, Value::Integer(300 + index as i64));
            assert_eq!(word(offset), Value::Integer(300 + index as i64).word());
        }
        window.set_prev_buffers(Value::Integer(331));
        window.set_next_buffers(Value::Integer(332));
        assert_eq!(
            (word(224), word(232)),
            (Value::Integer(331).word(), Value::Integer(332).word())
        );
        // The links, which a window reads as the windows they are.
        window.set_next_sibling(Some(other));
        window.set_prev_sibling(Some(other));
        window.set_parent_window(Some(other));
        assert_eq!([word(16), word(24), word(32)], [other.value().word(); 3]);
        assert_eq!(window.parent_window(), Some(other));
        // The EMACS_INTs use_time and sequence_number, then window.h's
        // ints from change_stamp to total_lines, the given bits in the four
        // bytes after them, and the ptrdiff_ts from hscroll on.
        window.set_use_time(41);
        window.set_sequence_number(42);
        assert_eq!((long(240), long(248)), (41, 42));
        let ints = || std::array::from_fn::<i32, 13, _>(|index| int(256 + 4 * index));
        assert_eq!(ints(), [0; 13]);
        assert_eq!(int(308), 0);
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
        assert_eq!(ints(), std::array::from_fn(|index| 100 + index as i32));
        assert_eq!(int(308), 0xff);
        window.set_hscroll(113);
        window.set_min_hscroll(114);
        window.set_last_modified(115);
        window.set_last_overlay_modified(116);
        window.set_last_point(117);
        window.set_base_line_number(118);
        window.set_base_line_pos(119);
        window.set_column_number_displayed(120);
        let longs = [312, 320, 336, 344, 352, 376, 384, 392].map(long);
        assert_eq!(longs, [113, 114, 115, 116, 117, 118, 119, 120]);
        // The margins are ints, window_end_pos a ptrdiff_t with a given bit.
        window.set_left_margin_cols(121);
        window.set_right_margin_cols(122);
        window.set_window_end_pos(Some(123));
        assert_eq!((int(484), int(488), long(512)), (121, 122, 123));
        assert_eq!(int(308), 0x1ff);
        // One taken back is zero again, its bit clear.
        window.set_total_cols(None);
        window.set_window_end_pos(None);
        assert_eq!((int(300), long(512), int(308)), (0, 0, 0xbf));
        // A C int keeps the low 32 bits of what is stored in it.
        window.set_change_stamp(1 << 32 | 7);
        assert_eq!((int(256), window.change_stamp()), (7, 7));
        // The bit-fields are bits of the word at 524, each at the place
        // GCC gives window.h's bit of that name.
        let stores: [BitStore; 11] = [
            (0, WindowRef::set_mini),
            (1, WindowRef::set_horizontal),
            (2, WindowRef::set_update_mode_line),
            (3, WindowRef::set_last_had_star),
            (4, WindowRef::set_start_at_line_beg),
            (5, WindowRef::set_force_start),
            (6, WindowRef::set_optional_new_start),
            (8, WindowRef::set_cursor_off_p),
            (15, WindowRef::set_window_end_valid),
            (16, WindowRef::set_redisplay),
            (17, WindowRef::set_suspend_auto_hscroll),
        ];
        let bits = || bytes.add(524).cast::<u32>().read();
        assert_eq!(bits(), 0);
        for (bit, store) in stores {
            store(window, true);
            assert_eq!(bits() & (1 << bit), 1 << bit, "bit {bit}");
            store(window, false);
            assert_eq!(bits(), 0, "bit {bit} cleared");
        }
    }
    assert_eq!(std::mem::size_of::<windows::WindowState>(), 536);
    assert_eq!(value.word(), object.identity() | 5);
    assert!(matches!(value.kind(), Kind::Window(same) if same.ptr_eq(&object)));
    assert_eq!(WindowRef::from_value(&value), Some(window));
    // alloc.c counts a window as window.c's whole struct, one vector of
    // its 68 words, the header among them.
    assert_eq!(after.0 - before.0, 1);
    assert_eq!(after.1 - before.1, 68);
}

#[test]
fn window_has_every_field_from_make_window_on() {
    use crate::lisp::primitives::{WindowMarker, window_marker};

    let mut interpreter = crate::lisp::eval::Interpreter::new();
    // allocate_window: every Lisp field nil, the rest zero, no edge or
    // size given (and no column number displayed, -1, here already).
    let bare = interpreter.allocate_window();
    assert!(bare.lisp_values().iter().all(Value::is_nil));
    assert_eq!(bare.frame(), None);
    assert_eq!((bare.sequence_number(), bare.use_time()), (0, 0));
    assert_eq!((bare.base_line_number(), bare.base_line_pos()), (0, 0));
    assert_eq!(bare.column_number_displayed(), -1);
    assert_eq!((bare.pixel_width(), bare.total_lines()), (None, None));
    assert_eq!((bare.left_fringe_width(), bare.scroll_bar_width()), (0, 0));
    assert!(!bare.mini() && !bare.cursor_off_p());
    // make_window: the fields it gives values other than nil and zero,
    // on the selected frame, next in sequence.
    let last = interpreter.window_sequence_number;
    let window = interpreter.make_window();
    assert_eq!(window.sequence_number(), last + 1);
    assert_eq!(window.frame(), Some(interpreter.selected_frame_id));
    assert_eq!(window.normal_size(false), Value::float(1.0));
    assert_eq!(window.normal_size(true), Value::float(1.0));
    assert_eq!(
        [window.new_total(), window.new_normal(), window.new_pixel()],
        [Value::Integer(0); 3]
    );
    assert_eq!(window.cursor_type(), Value::T);
    assert_eq!(window.vertical_scroll_bar_type(), Value::T);
    assert_eq!(window.horizontal_scroll_bar_type(), Value::T);
    assert_eq!(
        (window.prev_buffers(), window.next_buffers()),
        (Value::Nil, Value::Nil)
    );
    assert_eq!(
        (window.left_fringe_width(), window.right_fringe_width()),
        (-1, -1)
    );
    assert_eq!(
        (window.scroll_bar_width(), window.scroll_bar_height()),
        (-1, -1)
    );
    assert_eq!((window.hscroll_whole(), window.vscroll()), (0, 0));
    assert_eq!(window.column_number_displayed(), -1);
    // Three markers of its own that point nowhere.
    let markers = WindowMarker::ALL.map(|which| window_marker(window, which).expect("a marker"));
    assert!(markers.iter().all(|marker| marker.buffer().is_none()));
    assert!(!markers[0].ptr_eq(&markers[1]) && !markers[1].ptr_eq(&markers[2]));
    assert!(!markers[0].ptr_eq(&markers[2]));
    // Every field is there to read and store: nothing is absent.
    assert_eq!(window.window_parameters(), Value::Nil);
    window.set_base_line_pos(-1);
    assert_eq!(window.base_line_pos(), -1);
    window.set_window_parameters(Value::T);
    assert_eq!(window.window_parameters(), Value::T);
    // The dead window of a dump is allocate_window's.
    let dead = interpreter.install_dead_window();
    assert!(dead.lisp_values().iter().all(Value::is_nil));
    assert_eq!((dead.frame(), dead.sequence_number()), (None, 0));
    assert!(!dead.is_valid());
}

#[test]
fn window_traces_every_slot_and_is_reclaimed_off_the_frames() {
    use crate::lisp::primitives::foreach_frame_window;
    use windows::WINDOW_LISP_WORDS;
    const HIDE: usize = 0x5555_5555_5555_5555;
    // window.h's Lisp fields and prev_buffers and next_buffers.
    const FIELDS: usize = WINDOW_LISP_WORDS + 2;

    // The window's address and each field's vector.
    type Hidden = (usize, [usize; FIELDS]);

    #[inline(never)]
    fn allocate(
        interpreter: &mut crate::lisp::eval::Interpreter,
    ) -> (crate::lisp::alloc::RootedVec<Value>, Hidden) {
        let window = interpreter.make_window();
        let value = window.value();
        let mut children = [0; FIELDS];
        // A vector nothing but its field holds, in every Lisp field,
        // those mark_window marks apart among them.
        let vectors: [Value; FIELDS] =
            std::array::from_fn(|index| Value::vector([Value::Integer(index as i64), value]));
        for (child, vector) in children.iter_mut().zip(&vectors) {
            *child = (vector.word() & !7) ^ HIDE;
        }
        window.set_lisp_values(vectors);
        let Kind::Window(object) = value.kind() else {
            panic!("a window is a window object")
        };
        let mut roots = crate::lisp::alloc::RootedVec::new();
        roots.push(value);
        (roots, (object.identity() ^ HIDE, children))
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
        hidden: usize,
    ) {
        let value = roots[0];
        let Kind::Window(window) = value.kind() else {
            panic!("window kind")
        };
        assert_eq!(window.identity() ^ HIDE, hidden);
        for (index, field) in window.lisp_values().into_iter().enumerate() {
            let Kind::Vector(vector) = field.kind() else {
                panic!("traced window field {index}")
            };
            assert_eq!(vector.get(0), Some(Value::Integer(index as i64)));
            assert!(vector.get(1).expect("cycle slot").eq_value(value));
        }
        // No frame's tree has the window: the root alone keeps it.
        let handle = crate::lisp::primitives::WindowRef::from_object(window);
        assert!(!foreach_frame_window(interpreter).contains(&handle));
    }
    let mut interpreter = crate::lisp::eval::Interpreter::new();
    let environment = Env::new();
    let windows_before = foreach_frame_window(&interpreter);
    let (roots, (window, children)) = allocate(&mut interpreter);
    for _ in 0..3 {
        crate::lisp::alloc::clobber_stack();
        crate::lisp::native_comp::begin_garbage_collection(&mut interpreter, &environment);
        assert!(is_live(window, VectorTag::Window));
        for child in &children {
            assert!(is_live(*child, VectorTag::Normal));
        }
        check_slots(&interpreter, &roots, window);
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
    // The frames' windows, which the frames hold, are as they were.
    assert_eq!(foreach_frame_window(&interpreter), windows_before);
    for kept in windows_before.iter() {
        assert!(kept.is_valid());
    }
}

#[test]
fn window_number_use_time_and_cursor_bit_live_and_die_with_the_window() {
    const HIDE: usize = 0x5555_5555_5555_5555;

    // The window's address, its sequence number and its use time.
    type Hidden = (usize, i64, i64);

    // The window the roots hold: its use time, and whether its cursor is
    // on.
    fn held(roots: &crate::lisp::alloc::RootedVec<Value>) -> crate::lisp::primitives::WindowRef {
        crate::lisp::primitives::WindowRef::from_value(&roots[0]).expect("a window")
    }
    fn use_time(roots: &crate::lisp::alloc::RootedVec<Value>) -> i64 {
        held(roots).use_time()
    }
    fn cursor_visible(roots: &crate::lisp::alloc::RootedVec<Value>) -> bool {
        !held(roots).cursor_off_p()
    }

    #[inline(never)]
    fn allocate(
        interpreter: &mut crate::lisp::eval::Interpreter,
    ) -> (crate::lisp::alloc::RootedVec<Value>, Hidden) {
        // window.c:make_window's `w->sequence_number = ++sequence_number',
        // a use time of zero and the cursor on.
        let last = interpreter.window_sequence_number;
        let window = interpreter.make_window();
        let value = window.value();
        let mut roots = crate::lisp::alloc::RootedVec::new();
        roots.push(value);
        assert_eq!(window.sequence_number(), last + 1);
        assert_eq!(interpreter.window_sequence_number, last + 1);
        assert_eq!((window.use_time(), window.cursor_off_p()), (0, false));
        assert_eq!(use_time(&roots), 0);
        assert!(cursor_visible(&roots));
        // select_window's use time and Finternal_show_cursor's bit are
        // the window's own.
        interpreter.record_window_selection(window);
        window.set_cursor_off_p(true);
        assert!(window.use_time() > 0 && window.cursor_off_p());
        assert_eq!(use_time(&roots), window.use_time());
        assert!(!cursor_visible(&roots));
        let Kind::Window(object) = value.kind() else {
            panic!("a window is a window object")
        };
        (
            roots,
            (
                object.identity() ^ HIDE,
                window.sequence_number(),
                window.use_time(),
            ),
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
    fn check(roots: &crate::lisp::alloc::RootedVec<Value>, hidden: usize, number: i64) {
        let window = held(roots);
        let Kind::Window(object) = window.value().kind() else {
            panic!("a window is a window object")
        };
        assert_eq!(object.identity() ^ HIDE, hidden);
        assert_eq!(window.sequence_number(), number);
        assert!(window.use_time() > 0 && window.cursor_off_p());
        assert!(!cursor_visible(roots));
    }
    let mut interpreter = crate::lisp::eval::Interpreter::new();
    let environment = Env::new();
    let frame_times_before = crate::lisp::primitives::foreach_frame_window(&interpreter)
        .iter()
        .map(|window| (window.use_time(), window.cursor_off_p()))
        .collect::<Vec<_>>();
    let (roots, (window, number, time)) = allocate(&mut interpreter);
    for _ in 0..2 {
        crate::lisp::alloc::clobber_stack();
        crate::lisp::native_comp::begin_garbage_collection(&mut interpreter, &environment);
        assert!(is_live(window));
        check(&roots, window, number);
    }
    drop(roots);
    crate::lisp::alloc::clobber_stack();
    crate::lisp::native_comp::begin_garbage_collection(&mut interpreter, &environment);
    assert!(!is_live(window), "unreachable window retained");
    // Nothing kept the freed window's use time or its cursor bit: the
    // frames' windows have theirs, as they had before it was made.
    let frame_times = crate::lisp::primitives::foreach_frame_window(&interpreter)
        .iter()
        .map(|window| (window.use_time(), window.cursor_off_p()))
        .collect::<Vec<_>>();
    assert_eq!(frame_times, frame_times_before);
    assert!(frame_times.iter().all(|(_, off)| !off));
    assert!(frame_times.iter().all(|(kept, _)| *kept != time));
    // The counter is the interpreter's: the next window has the next
    // number, and starts as any new window does.
    assert_eq!(interpreter.window_sequence_number, number);
    let next = interpreter.make_window();
    assert_eq!(next.sequence_number(), number + 1);
    assert_eq!((next.use_time(), next.cursor_off_p()), (0, false));
}

#[test]
fn window_alone_holds_its_three_markers() {
    use crate::lisp::primitives::{WindowMarker, make_leaf_window};
    const HIDE: usize = 0x5555_5555_5555_5555;

    #[inline(never)]
    fn allocate(
        interpreter: &mut crate::lisp::eval::Interpreter,
    ) -> (crate::lisp::alloc::RootedVec<Value>, [usize; 3]) {
        let buffer = interpreter.current_buffer_id();
        let window = make_leaf_window(interpreter, buffer, 1, false, (80, 10, 0, 0));
        let markers = WindowMarker::ALL.map(|which| {
            let Kind::Marker(marker) = window.marker_field(which).kind() else {
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
    fn allocate(interpreter: &mut crate::lisp::eval::Interpreter) -> [usize; HOLDERS] {
        // A window nothing else holds in each holder: the frame keeps
        // the window it had selected.
        let windows: [_; HOLDERS] = std::array::from_fn(|_| interpreter.make_window());
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
            object.identity() ^ HIDE
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
    // What each holder holds is the window made for it, which no frame's
    // tree has.
    #[inline(never)]
    fn check(interpreter: &crate::lisp::eval::Interpreter, held: &[usize]) {
        let holders = [
            interpreter.selected_window(),
            interpreter.old_selected_window,
            interpreter.minibuffer_selected_window(),
            interpreter
                .active_minibuffer_window_value()
                .as_ref()
                .and_then(crate::lisp::primitives::WindowRef::from_value),
        ];
        let frame_windows = crate::lisp::primitives::foreach_frame_window(interpreter);
        for (holder, hidden) in holders.iter().skip(HOLDERS - held.len()).zip(held) {
            let window = holder.expect("the holder's window");
            let Kind::Window(object) = window.value().kind() else {
                panic!("a window is a window object")
            };
            assert_eq!(object.identity() ^ HIDE, *hidden);
            assert!(!window.is_valid());
            assert!(!frame_windows.contains(&window));
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
    let windows_before = crate::lisp::primitives::foreach_frame_window(&interpreter);
    let held = allocate(&mut interpreter);
    for _ in 0..2 {
        crate::lisp::alloc::clobber_stack();
        crate::lisp::native_comp::begin_garbage_collection(&mut interpreter, &environment);
        assert!(held.iter().all(|window| is_live(*window)));
        check(&interpreter, &held);
    }
    for index in 0..HOLDERS {
        release(&mut interpreter, index);
        crate::lisp::alloc::clobber_stack();
        crate::lisp::native_comp::begin_garbage_collection(&mut interpreter, &environment);
        // The window let go of is collected; the windows still held
        // are as they were, and so are the frames' windows.
        let window = held[index];
        assert!(!is_live(window), "window of holder {index} retained");
        assert_eq!(
            crate::lisp::primitives::foreach_frame_window(&interpreter),
            windows_before
        );
        assert!(held[index + 1..].iter().all(|window| is_live(*window)));
        check(&interpreter, &held[index + 1..]);
    }
    assert_eq!(
        crate::lisp::primitives::foreach_frame_window(&interpreter),
        windows_before
    );
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
    fn listed(interpreter: &mut crate::lisp::eval::Interpreter) -> (WindowList, [usize; 3]) {
        let mut list = WindowList::new();
        let hidden = std::array::from_fn(|_| {
            let window = interpreter.make_window();
            list.push(window);
            let Kind::Window(object) = window.value().kind() else {
                panic!("a window is a window object")
            };
            object.identity() ^ HIDE
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
    fn check(interpreter: &crate::lisp::eval::Interpreter, list: &[WindowRef], hidden: &[usize]) {
        assert_eq!(list.len(), hidden.len());
        let frame_windows = crate::lisp::primitives::foreach_frame_window(interpreter);
        for (window, address) in list.iter().zip(hidden) {
            assert!(is_live(*address));
            // A window nothing shows: deleted, and on no frame's tree.
            assert!(!window.is_valid());
            let Kind::Window(object) = window.value().kind() else {
                panic!("a window is a window object")
            };
            assert_eq!(object.identity() ^ HIDE, *address);
            assert!(!frame_windows.contains(window));
        }
    }
    let mut interpreter = crate::lisp::eval::Interpreter::new();
    let environment = Env::new();
    let windows_before = crate::lisp::primitives::foreach_frame_window(&interpreter);
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
    assert!(!is_live(hidden[1]), "a window off the list retained");
    assert_eq!(
        crate::lisp::primitives::foreach_frame_window(&interpreter),
        windows_before
    );
    check(&interpreter, &list, &[hidden[0], hidden[2]]);
    drop(list);
    crate::lisp::alloc::clobber_stack();
    crate::lisp::native_comp::begin_garbage_collection(&mut interpreter, &environment);
    assert!(hidden.iter().all(|address| !is_live(*address)));
    assert_eq!(
        crate::lisp::primitives::foreach_frame_window(&interpreter),
        windows_before
    );
}

#[test]
fn a_buffer_marks_each_of_its_char_table_words_and_a_killed_one_lets_its_own_go() {
    // alloc.c:mark_buffer marks the buffer's Lisp words, the syntax,
    // category and four case tables among them: tables only the buffer
    // holds live through collections.  Fkill_buffer's
    // reset_buffer_local_variables gives the killed buffer the standard
    // tables, and its own are garbage.
    const HIDE: usize = 0x5555_5555_5555_5555;
    #[inline(never)]
    fn give_tables(buffer: crate::lisp::types::BufferRef) -> [usize; 6] {
        let table = || CharTableRef::new(Value::symbol("buffer-table-test"), Value::Nil, 0);
        let tables: [CharTableRef; 6] = std::array::from_fn(|_| table());
        buffer.set_syntax_table(tables[0]);
        buffer.set_category_table(Some(tables[1]));
        buffer.set_case_tables(Some(crate::lisp::eval::coding::CaseTables {
            down: tables[2],
            up: tables[3],
            canon: tables[4],
            eqv: tables[5],
        }));
        tables.map(|table| table.identity() ^ HIDE)
    }
    #[inline(never)]
    fn is_live(hidden: usize) -> bool {
        let address = hidden ^ HIDE;
        matches!(unsafe { crate::lisp::alloc::mem_find(address) },
            Some(crate::lisp::alloc::Found::Vectorlike(header))
                if header as usize == address
                    && unsafe { header_tag(header) } == VectorTag::CharTable)
    }
    #[inline(never)]
    fn words(buffer: crate::lisp::types::BufferRef) -> [usize; 6] {
        buffer.char_table_words().map(|word| match word.kind() {
            Kind::CharTable(table) => table.identity() ^ HIDE,
            other => panic!("a table word: {other:?}"),
        })
    }
    let mut interpreter = crate::lisp::eval::Interpreter::new();
    let environment = Env::new();
    // buffer.c's buffer_defaults holds a standard category table from
    // init_category_once; this interpreter makes it at its first use.
    interpreter.ensure_standard_category_table();
    let (id, _) = interpreter.create_buffer("buffer-table-test");
    let buffer = interpreter.buffer_object(id).expect("the new buffer");
    let standard = words(buffer);
    assert!(standard.iter().all(|word| is_live(*word)));
    let hidden = give_tables(buffer);
    for _ in 0..3 {
        crate::lisp::alloc::clobber_stack();
        crate::lisp::native_comp::begin_garbage_collection(&mut interpreter, &environment);
        assert!(hidden.iter().all(|table| is_live(*table)));
        assert_eq!(words(buffer), hidden);
    }
    interpreter.kill_buffer_id(id).expect("kill the buffer");
    assert_eq!(
        words(buffer),
        standard,
        "a killed buffer's are the standard tables"
    );
    crate::lisp::alloc::clobber_stack();
    crate::lisp::native_comp::begin_garbage_collection(&mut interpreter, &environment);
    assert!(hidden.iter().all(|table| !is_live(*table)));
    assert!(standard.iter().all(|word| is_live(*word)));
}
