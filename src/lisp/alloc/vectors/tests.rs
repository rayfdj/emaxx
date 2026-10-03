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
