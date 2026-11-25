use super::*;

#[test]
fn marked_cons_edges_to_positioned_symbols_are_checked() {
    let object = Value::positioned_symbol(Value::symbol("verifier-child"), Value::Integer(17));
    let parent = Value::cons(object, Value::Nil);
    let Kind::Cons(cell) = parent.kind() else {
        unreachable!();
    };
    // No collection runs here, and between collections every mark bit is
    // clear. Mark this cons in its block's bitmap while its child stays
    // unmarked, then invoke the actual pre-sweep checker with an epoch no
    // object carries. Restore the entire block bitmap before asserting.
    let probe_epoch = super::super::types::current_mark_epoch().wrapping_add(1);
    let mark = unsafe { cons_mark(cell.as_ptr()) };
    let old_words: [usize; CONS_BITMAP_WORDS] =
        std::array::from_fn(|index| mark.block.marked[index].get());
    mark.mark();
    let rejected = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        verify_marking(probe_epoch);
    }));
    cell.car.set(Value::Nil);
    let accepted = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        verify_marking(probe_epoch);
    }));
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

#[test]
fn a_cons_block_holds_its_metadata_and_2016_conses() {
    // The block's metadata is the mark and allocation bitmaps, a bit for
    // each 16 bytes of the 32 KiB block: 512 bytes, the first cell at 512.
    // GNU's block holds 2,030 (its mark bits alone); with a weak-reference
    // serial a cons this one held 1,344, and with the marks' epoch 2,015.
    assert_eq!(CONS_CELLS, 512);
    assert_eq!(CELLS_PER_BLOCK, 2016);
    assert_eq!(CONS_CELLS + CELLS_PER_BLOCK * CELL_SIZE, CONS_BLOCK_ALIGN);
    let [_, block, payload, rest] = cons_storage_for_test();
    assert_eq!((block, payload, rest), (32768, 2016 * 16, 512));
    // A cell's bits are its slot's: the first cell's at 32, the last
    // cell's in the bitmaps' last word.
    let last = Value::cons(Value::Nil, Value::Nil);
    let Kind::Cons(cell) = last.kind() else {
        unreachable!();
    };
    let (word, _) = unsafe { cons_mark(cell.as_ptr()) }.bit();
    assert!(word < CONS_BITMAP_WORDS);
    assert_eq!(CONS_FIRST_SLOT, 32);
    assert_eq!(
        (CONS_FIRST_SLOT + CELLS_PER_BLOCK - 1) / usize::BITS as usize,
        CONS_BITMAP_WORDS - 1
    );
}
