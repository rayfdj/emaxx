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
