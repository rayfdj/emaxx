use super::*;
use std::cell::RefCell;

/// glibc's random(3) state of TYPE_3, the generator sysdep.c draws from
/// (HAVE_RANDOM, RAND_BITS 31): additive feedback over 31 words with the
/// front pointer 3 ahead of the rear one (random_r.c).
struct RandomState {
    state: [i32; 31],
    front: usize,
    rear: usize,
}

impl RandomState {
    /// random_r.c:__srandom_r.
    fn seeded(seed: u32) -> Self {
        let mut generator = RandomState {
            state: [0; 31],
            front: 3,
            rear: 0,
        };
        let mut word = if seed == 0 { 1 } else { seed } as i32;
        generator.state[0] = word;
        for slot in 1..31 {
            // state[i] = (16807 * state[i - 1]) % 2147483647, without
            // overflowing 31 bits.
            let hi = i64::from(word) / 127_773;
            let lo = i64::from(word) % 127_773;
            word = (16_807 * lo - 2_836 * hi) as i32;
            if word < 0 {
                word = word.wrapping_add(2_147_483_647);
            }
            generator.state[slot] = word;
        }
        for _ in 0..310 {
            generator.next();
        }
        generator
    }

    /// random_r.c:__random_r: 31 random bits.
    fn next(&mut self) -> u32 {
        let value = (self.state[self.front] as u32).wrapping_add(self.state[self.rear] as u32);
        self.state[self.front] = value as i32;
        self.front += 1;
        self.rear += 1;
        if self.front >= 31 {
            self.front = 0;
        } else if self.rear >= 31 {
            self.rear = 0;
        }
        value >> 1
    }
}

thread_local! {
    // emacs.c calls init_random before any Lisp runs.  Each thread owns its
    // generator, so an interpreter on another thread never draws from it.
    static RANDOM: RefCell<RandomState> = RefCell::new(RandomState::seeded(entropy_seed()));
}

/// sysdep.c:init_random's seed: getrandom's bytes, else the pid and the
/// time.
fn entropy_seed() -> u32 {
    let mut bytes = [0u8; 4];
    // SAFETY: getrandom writes at most the four bytes it is given.
    let read = unsafe { libc::getrandom(bytes.as_mut_ptr().cast(), bytes.len(), 0) };
    if read == bytes.len() as isize {
        return u32::from_ne_bytes(bytes);
    }
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    std::process::id() ^ now.as_secs() as u32 ^ now.subsec_nanos()
}

fn set_random_seed(seed: u32) {
    RANDOM.with(|random| *random.borrow_mut() = RandomState::seeded(seed));
}

fn random() -> u64 {
    RANDOM.with(|random| u64::from(random.borrow_mut().next()))
}

/// sysdep.c:init_random.
pub(crate) fn init_random() {
    set_random_seed(entropy_seed());
}

/// sysdep.c:seed_random: SEED's bytes XORed into an unsigned int, byte by
/// byte.
pub(crate) fn seed_random(seed: &[u8]) {
    let mut arg = [0u8; 4];
    for (index, byte) in seed.iter().enumerate() {
        arg[index % arg.len()] ^= byte;
    }
    set_random_seed(u32::from_ne_bytes(arg));
}

/// lisp.h's INTMASK: a fixnum's 62 bits.
const INTMASK: u64 = (1 << 62) - 1;

/// sysdep.c:get_random: two draws make a fixnum's 62 bits.
pub(crate) fn get_random() -> i64 {
    let mut value: u64 = 0;
    for _ in 0..2 {
        value = random() ^ (value << 31) ^ (value >> (64 - 31));
    }
    value ^= value >> (64 - 62);
    (value & INTMASK) as i64
}

/// sysdep.c:get_random_ulong.
fn get_random_ulong() -> u64 {
    let mut value: u64 = 0;
    for _ in 0..(64 + 31 - 1) / 31 {
        value = random() ^ (value << 31) ^ (value >> (64 - 31));
    }
    value
}

/// fns.c:Frandom's `make_ufixnum (get_random ())': bit 61 is the sign.
pub(crate) fn random_fixnum() -> i64 {
    (get_random() << 2) >> 2
}

/// fns.c:get_random_fixnum: a random integer in 0..LIM, redrawn when the
/// draw is so near INTMASK that its remainder would not be uniform.
pub(crate) fn get_random_fixnum(lim: i64) -> i64 {
    let difflim = INTMASK as i64 - lim + 1;
    loop {
        let r = get_random();
        let remainder = r % lim;
        if r - remainder <= difflim {
            return remainder;
        }
    }
}

/// bignum.c:get_random_limb_lim: a random limb below LIM, any limb for a
/// LIM of 0.
fn get_random_limb_lim(lim: u64) -> u64 {
    let difflim = lim.wrapping_neg();
    loop {
        let r = get_random_ulong();
        if lim == 0 {
            return r;
        }
        let remainder = r % lim;
        if r - remainder <= difflim {
            return remainder;
        }
    }
}

/// bignum.c:get_random_bignum: a random integer in 0..LIMIT made a limb at
/// a time, most significant first, redrawn when it reaches LIMIT.
pub(crate) fn get_random_bignum(limit: &BigInt) -> BigInt {
    let (_, limbs) = limit.to_u64_digits();
    let count = limbs.len();
    let high = limbs[count - 1];
    let mut result = vec![0u64; count];
    loop {
        let r_high = get_random_limb_lim(high.wrapping_add(u64::from(1 < count)));
        let mut edgy = r_high == high;
        result[count - 1] = r_high;
        for index in (0..count - 1).rev() {
            let limb = get_random_ulong();
            if edgy {
                if limbs[index] < limb {
                    break;
                }
                edgy = limbs[index] == limb;
            }
            result[index] = limb;
        }
        if !edgy {
            break;
        }
    }
    result.iter().rev().fold(BigInt::zero(), |value, limb| {
        (value << 64) + BigInt::from(*limb)
    })
}

/// A number unlike the last one asked for, for test file names that must
/// not collide.
#[cfg(test)]
pub(crate) fn nondeterministic_random_seed() -> u64 {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    let counter = COUNTER.fetch_add(1, AtomicOrdering::Relaxed);
    let pid = std::process::id() as u64;
    nanos ^ nanos.rotate_left(17) ^ counter.rotate_left(29) ^ pid.rotate_left(41)
}
