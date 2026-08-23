use super::*;


pub(crate) fn normalize_random_seed(seed: u64) -> u64 {
    if seed == 0 {
        0x1234_5678_9abc_def0
    } else {
        seed
    }
}

pub(crate) fn set_random_seed(seed: u64) {
    RANDOM_STATE.store(normalize_random_seed(seed), AtomicOrdering::Relaxed);
}

pub(crate) fn next_random_u64() -> u64 {
    let mut state = normalize_random_seed(RANDOM_STATE.load(AtomicOrdering::Relaxed));
    state ^= state << 13;
    state ^= state >> 7;
    state ^= state << 17;
    state = normalize_random_seed(state);
    RANDOM_STATE.store(state, AtomicOrdering::Relaxed);
    state
}

pub(crate) fn random_seed_from_bytes(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in bytes {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    normalize_random_seed(hash)
}

pub(crate) fn nondeterministic_random_seed() -> u64 {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    let counter = RANDOM_SEED_COUNTER.fetch_add(1, AtomicOrdering::Relaxed);
    let pid = std::process::id() as u64;
    let mixed = nanos ^ nanos.rotate_left(17) ^ counter.rotate_left(29) ^ pid.rotate_left(41);
    random_seed_from_bytes(&mixed.to_le_bytes())
}

pub(crate) fn random_bigint_below(limit: &BigInt) -> BigInt {
    let chunks = limit.bits().max(1).div_ceil(64) as usize;
    let mut value = BigInt::zero();
    for _ in 0..chunks {
        value = (value << 64) + BigInt::from(next_random_u64());
    }
    value % limit
}

/// Simple pseudo-random number (xorshift64).
pub(crate) fn rand_simple() -> i64 {
    next_random_u64() as i64
}
