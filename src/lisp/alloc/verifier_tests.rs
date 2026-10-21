use super::*;

#[test]
fn marked_cons_edges_to_positioned_symbols_are_checked() {
    let object = Value::positioned_symbol(Value::symbol("verifier-child"), Value::Integer(17));
    let parent = Value::cons(object, Value::Nil);
    let Kind::Cons(cell) = parent.kind() else {
        unreachable!();
    };
    // No collection runs here. Give this cons a separate mark epoch while
    // its child retains its real allocation mark, then invoke the actual
    // pre-sweep checker. Restore the entire block bitmap before asserting.
    let mark = unsafe { cons_mark(cell.as_ptr()) };
    let old_epoch = mark.block.epoch.get();
    let old_words: [usize; CONS_BITMAP_WORDS] =
        std::array::from_fn(|index| mark.block.marked[index].get());
    let probe_epoch = super::super::types::current_mark_epoch().wrapping_add(1);
    mark.mark(probe_epoch);
    let rejected = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        verify_marking(probe_epoch);
    }));
    cell.car.set(Value::Nil);
    let accepted = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        verify_marking(probe_epoch);
    }));
    mark.block.epoch.set(old_epoch);
    for (word, saved) in mark.block.marked.iter().zip(old_words) {
        word.set(saved);
    }
    assert!(
        rejected.is_err(),
        "checker accepted an unmarked positioned-symbol child"
    );
    assert!(
        accepted.is_ok(),
        "checker rejected the same cons with a nil field"
    );
    std::hint::black_box(object);
    std::hint::black_box(parent);
}

#[test]
fn a_block_address_names_no_cons_or_float() {
    // Fcons and make_float reach a cell's bitmaps through its block's
    // address, and copies of that address stay in stack slots; the
    // conservative scan must not take one for the block's first cell.
    let bound = free_conses() + 2 * CELLS_PER_BLOCK + 1;
    let mut conses = Vec::new();
    let first = loop {
        assert!(conses.len() < bound, "no cons took a block's first cell");
        let cons = Value::cons(Value::Nil, Value::Nil);
        conses.push(cons);
        let Kind::Cons(cell) = cons.kind() else {
            unreachable!();
        };
        let address = cell.as_ptr() as usize;
        if address == cons_cell(address & !(CONS_BLOCK_ALIGN - 1), 0) as usize {
            break cons;
        }
    };
    let address = first.word() & !TAG_BITS;
    assert_eq!(
        conservative_value(first.word()).map(Value::word),
        Some(first.word())
    );
    assert_eq!(
        conservative_value(address & !(CONS_BLOCK_ALIGN - 1)).map(Value::word),
        None
    );
    let mut floats = Vec::new();
    let first = loop {
        assert!(
            floats.len() < FREE_FLOATS.load(Ordering::Relaxed) + 2 * FLOATS_PER_BLOCK + 1,
            "no float took a block's first cell"
        );
        let float = allocate_float(0.5);
        floats.push(float);
        let address = float.identity_ptr();
        if address == float_cell(address & !(FLOAT_BLOCK_ALIGN - 1), 0) as usize {
            break float;
        }
    };
    let address = first.identity_ptr();
    assert!(matches!(
        conservative_value(address).map(|value| value.kind()),
        Some(Kind::Float(found)) if found.ptr_eq(&first)
    ));
    assert_eq!(
        conservative_value(address & !(FLOAT_BLOCK_ALIGN - 1)).map(Value::word),
        None
    );
    std::hint::black_box((conses, floats));
}
