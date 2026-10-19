//! A one-time check of a byte-code function's stack discipline.
//!
//! bytecode.c trusts the byte compiler: with BYTE_CODE_SAFE off, as in
//! release builds, exec_byte_code checks neither the operand stack's
//! bounds, nor constant indices, nor where a jump lands. Emaxx must stay
//! memory-safe on any closure, so it checks a function's code once instead
//! of at every instruction: from the entry, every branch and every handler
//! target, each reachable instruction decodes within the code string, reads
//! a constant within the vector, finds the operands it consumes, leaves no
//! more than the declared depth, and is reached with the same operand count
//! on every path. The VM's fast loop then runs verified code without
//! per-op checks (`vm::run_verified').
//!
//! Each result belongs to the code string's current bytes. Any change to
//! the string makes its results stale (`CodeState'), and the string's
//! collection drops them. A result is checked again where control enters
//! the fast loop: the code must still be current and the actual operand
//! count must be the one verified for that instruction, so an op whose
//! stack effect is modeled wrongly here costs only the fast loop, never
//! safety.

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::mem::MaybeUninit;
use std::ptr::NonNull;

use super::{ArgSpec, ByteCodeObject, CodeBytes, Op, decode_instr};
use crate::lisp::alloc::CodeState;
use crate::lisp::types::{ClosureRef, IdentityBuildHasher, Kind, VectorRef};

/// No verified path reaches an instruction starting at this offset.
const UNREACHED: u16 = u16::MAX;

/// One function shape's verified code: the operand count at the start of
/// each reachable instruction, by code offset.
pub(crate) struct VerifiedCode {
    constants_len: usize,
    stack_depth: usize,
    entry_depth: usize,
    /// Empty when the code failed verification.
    depths: Box<[u16]>,
    /// False once the code string changed and was verified again.
    current: Cell<bool>,
}

impl std::fmt::Debug for VerifiedCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VerifiedCode")
            .field("code_len", &self.depths.len())
            .field("current", &self.current.get())
            .finish()
    }
}

impl VerifiedCode {
    /// Whether this is the current result for PROGRAM's shape.
    #[inline]
    fn fits(&self, program: &ByteCodeObject, entry_depth: usize) -> bool {
        self.current.get()
            && self.constants_len == program.constants.len()
            && self.stack_depth == program.stack_depth
            && self.entry_depth == entry_depth
    }

    /// Whether this is still the result for its code string's bytes.
    #[inline]
    pub(crate) fn is_current(&self) -> bool {
        self.current.get()
    }

    /// Whether the fast loop may run this code entering at PC with DEPTH
    /// operands on the stack.
    #[inline]
    pub(crate) fn admits(&self, pc: usize, depth: usize) -> bool {
        self.depths
            .get(pc)
            .is_some_and(|&verified| usize::from(verified) == depth)
            && self.current.get()
    }
}

const CACHE_SLOTS: usize = 256;

/// A direct-mapped cache in front of `VERIFIED', by code string identity.
#[inline]
fn cache_slot(identity: usize) -> usize {
    (identity.wrapping_mul(0x9e37_79b9_7f4a_7c15) >> 56) % CACHE_SLOTS
}

thread_local! {
    /// The result last found for a code string, by `cache_slot'.
    static CACHE: [Cell<(usize, *const VerifiedCode)>; CACHE_SLOTS] =
        const { [const { Cell::new((0, std::ptr::null())) }; CACHE_SLOTS] };
    /// Results by code string identity: the current one for each function
    /// shape sharing the string, and stale ones a running frame may still
    /// hold. All of them go when the string is collected. Each is boxed so
    /// its address survives the vector's growth.
    #[allow(clippy::vec_box)]
    static VERIFIED: RefCell<HashMap<usize, Vec<Box<VerifiedCode>>, IdentityBuildHasher>> =
        RefCell::new(HashMap::default());
}

/// The verified code for PROGRAM entered with ENTRY_DEPTH operands, if
/// its code passed verification. The result stays allocated while the code
/// string is reachable, which a running frame's function guarantees.
#[inline(always)]
pub(crate) fn verified_code(
    program: &ByteCodeObject,
    entry_depth: usize,
) -> Option<&'static VerifiedCode> {
    let string = program.code.string();
    let identity = string.identity();
    if string.code_state() == CodeState::Current {
        let (key, cached) = CACHE.with(|cache| cache[cache_slot(identity)].get());
        if key == identity {
            // SAFETY: `forget' clears a result's cache slot before it frees
            // the result.
            let version = unsafe { &*cached };
            if version.fits(program, entry_depth) {
                return (!version.depths.is_empty()).then_some(version);
            }
        }
    }
    verified_code_slow(program, string, entry_depth)
}

#[inline(never)]
fn verified_code_slow(
    program: &ByteCodeObject,
    string: crate::lisp::alloc::StringObjectRef,
    entry_depth: usize,
) -> Option<&'static VerifiedCode> {
    let identity = string.identity();
    let state = string.code_state();
    let version = VERIFIED.with_borrow_mut(|verified| {
        let versions = verified.entry(identity).or_default();
        match state {
            CodeState::Current => {
                if let Some(version) = versions
                    .iter()
                    .find(|version| version.fits(program, entry_depth))
                {
                    let version: &VerifiedCode = version;
                    return version as *const VerifiedCode;
                }
            }
            // Results left for a string whose bytes changed, or for an
            // earlier string at this address, are not this code's.
            CodeState::Stale | CodeState::Unverified => {
                for version in versions.iter() {
                    version.current.set(false);
                }
            }
        }
        let depths = program
            .code
            .with_bytes(|code| verify(code, program.constants, program.stack_depth, entry_depth));
        versions.push(Box::new(VerifiedCode {
            constants_len: program.constants.len(),
            stack_depth: program.stack_depth,
            entry_depth,
            depths: depths.unwrap_or_default(),
            current: Cell::new(true),
        }));
        string.set_code_current();
        let version: &VerifiedCode = versions.last().expect("just pushed");
        version as *const VerifiedCode
    });
    CACHE.with(|cache| cache[cache_slot(identity)].set((identity, version)));
    // SAFETY: boxed results stay at their address until the string's
    // collection removes them (see `forget').
    let version = unsafe { &*version };
    (!version.depths.is_empty()).then_some(version)
}

/// What a plain entry of a closure runs (`entry_program'): the fields
/// `ByteCodeObject::for_call' reads, the verified code for the entry, and
/// what setup_frame finds from the argument template.
#[derive(Clone, Copy)]
pub(crate) struct EntryProgram {
    pub(crate) code: CodeBytes,
    pub(crate) constants: VectorRef,
    pub(crate) verified: &'static VerifiedCode,
    pub(crate) stack_depth: usize,
    /// The frame's first slots: the arguments, the &optional ones' nils
    /// and the &rest list's.
    pub(crate) entry_depth: usize,
    /// Bit N set when the template takes N arguments with no &rest list
    /// to make (N from MANDATORY to NONREST) and the first slots fit the
    /// frame; none at or above 64.
    counts: u64,
}

impl EntryProgram {
    /// Whether a plain entry with ARGC arguments runs this program as it
    /// stands: setup_frame's count test and the frame's room, in one bit.
    #[inline(always)]
    pub(crate) fn takes(&self, argc: usize) -> bool {
        argc < 64 && self.counts & (1 << argc) != 0
    }
}

/// A closure's program as its last entry read it (see `entry_program').
struct ClosureEntry {
    /// The closure's identity; zero for an entry that holds nothing.
    identity: usize,
    /// `code_changes' when the entry was filled: no verified code has
    /// changed while it is the same.
    code_changes: usize,
    program: MaybeUninit<EntryProgram>,
}

const CLOSURE_SLOTS: usize = 1024;

const EMPTY_ENTRY: ClosureEntry = ClosureEntry {
    identity: 0,
    code_changes: 0,
    program: MaybeUninit::uninit(),
};

/// The table a thread reads until its first miss: every entry empty, and
/// never written.
struct EmptyClosures([ClosureEntry; CLOSURE_SLOTS]);

// SAFETY: the table is never written, and its entries hold no words.
unsafe impl Sync for EmptyClosures {}

static EMPTY_CLOSURES: EmptyClosures = EmptyClosures([EMPTY_ENTRY; CLOSURE_SLOTS]);

const fn empty_closures() -> *mut ClosureEntry {
    EMPTY_CLOSURES.0.as_ptr().cast_mut()
}

thread_local! {
    /// The programs of the closures last entered, direct-mapped by
    /// closure identity: a table on the heap, where the collector's
    /// conservative scan does not look. A thread's static TLS can lie
    /// inside the stack range the scan reads (glibc places it at the top
    /// of a spawned thread's stack), and the entries' words there kept
    /// their closures' code and constants alive.
    static CLOSURES: Cell<*mut ClosureEntry> = const { Cell::new(empty_closures()) };
    /// `vector_sweeps' when this thread's table last held no entry of a
    /// freed closure: a sweep this thread runs forgets the closures it
    /// frees (`forget_closure') and keeps the count; one another thread
    /// ran leaves it behind, and the next lookup empties the table.
    static CLOSURES_SWEPT: Cell<usize> = const { Cell::new(0) };
    /// The owner of this thread's table, which frees it at the thread's
    /// exit.
    static CLOSURE_TABLE: ClosureTable = const { ClosureTable(Cell::new(std::ptr::null_mut())) };
}

struct ClosureTable(Cell<*mut [ClosureEntry; CLOSURE_SLOTS]>);

impl Drop for ClosureTable {
    fn drop(&mut self) {
        let table = self.0.replace(std::ptr::null_mut());
        if !table.is_null() {
            let _ = CLOSURES.try_with(|closures| closures.set(empty_closures()));
            // SAFETY: the table came from `Box::into_raw' in `closure_table'
            // and nothing points into it now.
            drop(unsafe { Box::from_raw(table) });
        }
    }
}

/// This thread's own table, made at its first miss, which `CLOSURES'
/// then points to in place of the empty one; None once the thread is
/// exiting.
#[cold]
fn closure_table() -> Option<*mut ClosureEntry> {
    CLOSURE_TABLE
        .try_with(|owner| {
            if owner.0.get().is_null() {
                let table = Box::into_raw(Box::new([EMPTY_ENTRY; CLOSURE_SLOTS]));
                owner.0.set(table);
                CLOSURES.set(table.cast());
            }
            owner.0.get().cast::<ClosureEntry>()
        })
        .ok()
}

#[inline(always)]
fn closure_slot(identity: usize) -> usize {
    identity.wrapping_mul(0x9e37_79b9_7f4a_7c15) >> (usize::BITS - CLOSURE_SLOTS.trailing_zeros())
}

/// The program a plain entry of CLOSURE runs, with the verified code for
/// that entry: what `ByteCodeObject::for_call' reads and `verified_code'
/// finds, or None when either finds nothing.  A closure's slots never
/// change once it is published (Lisp cannot aset one), so what the last
/// entry found stands while the closure is the one it read -- the vector
/// sweep, which alone frees a closure and lets another take its address,
/// forgets the entry of each closure it frees -- and no string verified as
/// code has changed since, so the code string still holds the bytes that
/// were verified.
///
/// The program stays where the answer points until the next call here or
/// to `forget' on this thread.
#[inline(always)]
pub(crate) fn entry_program(closure: ClosureRef) -> Option<NonNull<EntryProgram>> {
    let identity = closure.identity();
    // SAFETY: the thread's table stays in place until its exit; nothing
    // else refers to this entry while this runs, and nothing here reenters.
    // The empty table is only read here.
    let entry = unsafe { &*CLOSURES.get().add(closure_slot(identity)) };
    if entry.identity == identity
        && entry.code_changes == crate::lisp::alloc::code_changes()
        && CLOSURES_SWEPT.get() == crate::lisp::alloc::vectors::vector_sweeps()
    {
        // SAFETY: an entry with an identity holds its program.
        return Some(NonNull::from(unsafe { entry.program.assume_init_ref() }));
    }
    entry_program_slow(closure, identity)
}

#[inline(never)]
fn entry_program_slow(closure: ClosureRef, identity: usize) -> Option<NonNull<EntryProgram>> {
    if closure.public_len() < 4 {
        return None;
    }
    let program = ByteCodeObject::for_call(closure)?;
    let ArgSpec::Packed {
        mandatory,
        nonrest,
        rest,
    } = program.argspec
    else {
        unreachable!("for_call reads packed argument lists")
    };
    let entry_depth = usize::from(nonrest) + usize::from(rest);
    let verified = verified_code(&program, entry_depth)?;
    // Bits MANDATORY to NONREST, as far as 63.
    let counts = if entry_depth <= program.stack_depth && mandatory <= nonrest.min(63) {
        (u64::MAX >> (63 - nonrest.min(63))) & (u64::MAX << mandatory)
    } else {
        0
    };
    let program = EntryProgram {
        code: program.code,
        constants: program.constants,
        verified,
        stack_depth: program.stack_depth,
        entry_depth,
        counts,
    };
    let table = closure_table()?;
    let sweeps = crate::lisp::alloc::vectors::vector_sweeps();
    if CLOSURES_SWEPT.get() != sweeps {
        // A sweep this thread did not run may have freed closures that
        // entries name.
        // SAFETY: as in `entry_program', the thread's own table.
        for entry in unsafe { std::slice::from_raw_parts_mut(table, CLOSURE_SLOTS) } {
            entry.identity = 0;
        }
        CLOSURES_SWEPT.set(sweeps);
    }
    // SAFETY: as in `entry_program', in the thread's own table.
    let entry = unsafe { &mut *table.add(closure_slot(identity)) };
    entry.identity = identity;
    // Read after the verification, which may have made its code current.
    entry.code_changes = crate::lisp::alloc::code_changes();
    Some(NonNull::from(entry.program.write(program)))
}

/// Whether this thread's table holds no entry of a closure freed by a
/// sweep so far: a sweep this thread runs keeps it so (`forget_closure',
/// `closures_swept').
pub(crate) fn closures_in_sweep() -> bool {
    CLOSURES_SWEPT
        .try_with(|swept| swept.get() == crate::lisp::alloc::vectors::vector_sweeps())
        .unwrap_or(false)
}

/// The sweep frees the closure at IDENTITY: this thread's entry of it goes.
pub(crate) fn forget_closure(identity: usize) {
    let _ = CLOSURES.try_with(|entries| {
        let table = entries.get();
        if table == empty_closures() {
            return;
        }
        // SAFETY: as in `entry_program', in the thread's own table.
        let entry = unsafe { &mut *table.add(closure_slot(identity)) };
        if entry.identity == identity {
            entry.identity = 0;
        }
    });
}

/// A sweep this thread ran is over, and IN_SWEEP was `closures_in_sweep'
/// before it began: its entries name no freed closure.
pub(crate) fn closures_swept(in_sweep: bool) {
    if in_sweep {
        let _ = CLOSURES_SWEPT
            .try_with(|swept| swept.set(crate::lisp::alloc::vectors::vector_sweeps()));
    }
}

/// The string at IDENTITY was collected: no frame can run its code.
pub(crate) fn forget(identity: usize) {
    let _ = CACHE.try_with(|cache| {
        let slot = &cache[cache_slot(identity)];
        if slot.get().0 == identity {
            slot.set((0, std::ptr::null()));
        }
    });
    // The entries whose verified code goes with the string's results.
    let _ = CLOSURES.try_with(|entries| {
        let table = entries.get();
        if table == empty_closures() {
            return;
        }
        // SAFETY: as in `entry_program', in the thread's own table.
        for entry in unsafe { std::slice::from_raw_parts_mut(table, CLOSURE_SLOTS) } {
            // SAFETY: an entry with an identity holds its program.
            if entry.identity != 0
                && unsafe { entry.program.assume_init_ref() }
                    .code
                    .string()
                    .identity()
                    == identity
            {
                entry.identity = 0;
            }
        }
    });
    let _ = VERIFIED.try_with(|verified| {
        if let Ok(mut verified) = verified.try_borrow_mut() {
            verified.remove(&identity);
        }
    });
}

/// What an instruction consumes and produces: the operands it reads
/// (NEED), how many of them it removes (POP), and how many it pushes.
struct Effect {
    need: usize,
    pop: usize,
    push: usize,
}

const fn effect(need: usize, pop: usize, push: usize) -> Option<Effect> {
    Some(Effect { need, pop, push })
}

/// bytecode.c's stack effect of each op.
fn stack_effect(op: Op) -> Option<Effect> {
    match op {
        Op::StackRef(n) => effect(usize::from(n) + 1, 0, 1),
        Op::VarRef(_) | Op::Constant(_) | Op::Constant2(_) => effect(0, 0, 1),
        Op::VarSet(_) | Op::VarBind(_) => effect(1, 1, 0),
        Op::Call(n) => effect(usize::from(n) + 1, usize::from(n) + 1, 1),
        Op::Unbind(_)
        | Op::PopHandler
        | Op::SaveCurrentBufferObsolete
        | Op::SaveCurrentBuffer
        | Op::SaveExcursion
        | Op::SaveRestriction
        | Op::Goto { .. } => effect(0, 0, 0),
        Op::PushConditionCase { .. }
        | Op::PushCatch { .. }
        | Op::GotoIfNil { .. }
        | Op::GotoIfNonNil { .. }
        | Op::Return
        | Op::Discard
        | Op::UnwindProtect => effect(1, 1, 0),
        // The value stays on the stack when the branch is taken; the
        // fallthrough pops it (see `verify').
        Op::GotoIfNilElsePop { .. } | Op::GotoIfNonNilElsePop { .. } => effect(1, 0, 0),
        Op::Switch => effect(2, 2, 0),
        Op::Dup => effect(1, 0, 1),
        Op::Point
        | Op::PointMax
        | Op::PointMin
        | Op::FollowingChar
        | Op::PrecedingChar
        | Op::CurrentColumn
        | Op::Eolp
        | Op::Eobp
        | Op::Bolp
        | Op::Bobp
        | Op::CurrentBuffer
        | Op::InteractiveP
        | Op::Widen => effect(0, 0, 1),
        Op::Symbolp
        | Op::Consp
        | Op::Stringp
        | Op::Listp
        | Op::Not
        | Op::Car
        | Op::Cdr
        | Op::List1
        | Op::Length
        | Op::SymbolValue
        | Op::SymbolFunction
        | Op::Sub1
        | Op::Add1
        | Op::Negate
        | Op::GotoChar
        | Op::Insert
        | Op::CharAfter
        | Op::IndentTo
        | Op::SetBuffer
        | Op::ForwardChar
        | Op::ForwardWord
        | Op::ForwardLine
        | Op::CharSyntax
        | Op::EndOfLine
        | Op::SaveWindowExcursion
        | Op::TempOutputBufferSetup
        | Op::MatchBeginning
        | Op::MatchEnd
        | Op::Upcase
        | Op::Downcase
        | Op::Nreverse
        | Op::CarSafe
        | Op::CdrSafe
        | Op::Numberp
        | Op::Integerp => effect(1, 1, 1),
        Op::Nth
        | Op::Eq
        | Op::Memq
        | Op::Cons
        | Op::List2
        | Op::Aref
        | Op::Set
        | Op::Fset
        | Op::Get
        | Op::Concat2
        | Op::Eqlsign
        | Op::Gtr
        | Op::Lss
        | Op::Leq
        | Op::Geq
        | Op::Diff
        | Op::Plus
        | Op::Max
        | Op::Min
        | Op::Mult
        | Op::SkipCharsForward
        | Op::SkipCharsBackward
        | Op::BufferSubstring
        | Op::DeleteRegion
        | Op::NarrowToRegion
        | Op::Catch
        | Op::TempOutputBufferShow
        | Op::StringEqlsign
        | Op::StringLss
        | Op::Equal
        | Op::Nthcdr
        | Op::Elt
        | Op::Member
        | Op::Assq
        | Op::Setcar
        | Op::Setcdr
        | Op::Nconc
        | Op::Quo
        | Op::Rem => effect(2, 2, 1),
        Op::List3 | Op::Aset | Op::Substring | Op::Concat3 | Op::ConditionCase | Op::SetMarker => {
            effect(3, 3, 1)
        }
        Op::List4 | Op::Concat4 => effect(4, 4, 1),
        Op::ListN(n) | Op::ConcatN(n) | Op::InsertN(n) if n > 0 => {
            effect(usize::from(n), usize::from(n), 1)
        }
        Op::ListN(_) | Op::ConcatN(_) | Op::InsertN(_) => None,
        // `*(top - N) = POP': N == 0 would store into the popped slot.
        Op::StackSet(n) if n > 0 => effect(usize::from(n) + 1, 1, 0),
        Op::StackSet(_) => None,
        Op::DiscardN {
            count,
            preserve_tos,
        } => effect(
            usize::from(count) + usize::from(preserve_tos),
            usize::from(count),
            0,
        ),
    }
}

/// The worklist of the depth propagation.
struct Flow {
    depths: Vec<u16>,
    pending: Vec<usize>,
}

impl Flow {
    /// Control reaches PC with DEPTH operands: PC must lie within the code,
    /// and every path to it must agree on the depth.
    fn reach(&mut self, pc: usize, depth: usize) -> Option<()> {
        let slot = self.depths.get_mut(pc)?;
        let depth = u16::try_from(depth)
            .ok()
            .filter(|depth| *depth != UNREACHED)?;
        if *slot == UNREACHED {
            *slot = depth;
            self.pending.push(pc);
        } else if *slot != depth {
            return None;
        }
        Some(())
    }
}

/// The operand count at each reachable instruction of CODE, entered with
/// ENTRY_DEPTH operands in a frame of STACK_DEPTH, or None if any check
/// fails.
fn verify(
    code: &[u8],
    constants: VectorRef,
    stack_depth: usize,
    entry_depth: usize,
) -> Option<Box<[u16]>> {
    if stack_depth >= usize::from(UNREACHED) || entry_depth > stack_depth {
        return None;
    }
    let constants_len = constants.len();
    let mut flow = Flow {
        depths: vec![UNREACHED; code.len()],
        pending: Vec::new(),
    };
    flow.reach(0, entry_depth)?;
    while let Some(pc) = flow.pending.pop() {
        let depth = usize::from(flow.depths[pc]);
        let instr = decode_instr(code, pc).ok()?;
        let next = pc + instr.len;
        let Effect { need, pop, push } = stack_effect(instr.op)?;
        if depth < need {
            return None;
        }
        let after = depth - pop + push;
        if after > stack_depth {
            return None;
        }
        match instr.op {
            Op::VarRef(index)
            | Op::VarSet(index)
            | Op::VarBind(index)
            | Op::Constant(index)
            | Op::Constant2(index) => {
                let index = usize::from(index);
                if index >= constants_len {
                    return None;
                }
                // Bswitch's jump table is the constant pushed just before
                // it; its targets are reached with both operands popped
                // (the table and the value under it).
                if decode_instr(code, next).is_ok_and(|switch| switch.op == Op::Switch)
                    && let Some(table) = constants.get(index)
                    && let Kind::HashTable(table) = table.kind()
                {
                    let target_depth = after.checked_sub(2)?;
                    let mut slot = 0;
                    while let Some((index, _, target)) = table.entry_at_or_after(slot) {
                        slot = index + 1;
                        if let Some(target) = target.fixnum() {
                            flow.reach(usize::try_from(target).ok()?, target_depth)?;
                        }
                    }
                }
                flow.reach(next, after)?;
            }
            Op::Goto { target } => flow.reach(usize::from(target), after)?,
            Op::Return => {}
            Op::GotoIfNil { target } | Op::GotoIfNonNil { target } => {
                flow.reach(usize::from(target), after)?;
                flow.reach(next, after)?;
            }
            Op::GotoIfNilElsePop { target } | Op::GotoIfNonNilElsePop { target } => {
                flow.reach(usize::from(target), depth)?;
                flow.reach(next, depth - 1)?;
            }
            // A throw or signal caught here resumes at TARGET with the
            // stack cut back to the handler's push, then the value pushed.
            Op::PushConditionCase { target } | Op::PushCatch { target } => {
                flow.reach(usize::from(target), after + 1)?;
                flow.reach(next, after)?;
            }
            _ => flow.reach(next, after)?,
        }
    }
    Some(flow.depths.into_boxed_slice())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lisp::types::Value;

    fn depths(code: &[u8], constants: usize, stack_depth: usize, entry: usize) -> Option<Vec<u16>> {
        let constants = VectorRef::allocate(vec![Value::Nil; constants]);
        verify(code, constants, stack_depth, entry).map(Vec::from)
    }

    const U: u16 = UNREACHED;

    #[test]
    fn records_the_depth_at_each_reachable_instruction() {
        // core-perf-byte-calls: (let ((i 0) (sum 0)) (while (< i n) ...) sum)
        let code = [
            0o300, 0o211, 0o001, 0o003, 0o127, 0o203, 21, 0, 0o301, 0o001, 0o003, 0o042, 0o262, 1,
            0o001, 0o124, 0o262, 2, 0o202, 2, 0, 0o207,
        ];
        assert_eq!(
            depths(&code, 2, 6, 1),
            Some(vec![
                1, 2, 3, 4, 5, 4, U, U, 3, 4, 5, 6, 4, U, 3, 4, 4, U, 3, U, U, 3
            ])
        );
    }

    #[test]
    fn rejects_code_whose_paths_disagree_or_overflow() {
        // Falls off the end of the code.
        assert_eq!(depths(&[0o300], 1, 1, 0), None);
        // Constant index past the vector.
        assert_eq!(depths(&[0o301, 0o207], 1, 1, 0), None);
        // Pops an empty stack.
        assert_eq!(depths(&[0o210, 0o207], 0, 1, 0), None);
        // Pushes past the declared depth.
        assert_eq!(depths(&[0o300, 0o300, 0o207], 1, 1, 0), None);
        // Jump target outside the code.
        assert_eq!(depths(&[0o202, 9, 0], 0, 1, 0), None);
        // A loop that grows the stack on every pass.
        assert_eq!(depths(&[0o300, 0o202, 0, 0], 1, 9, 0), None);
        // Truncated operand.
        assert_eq!(depths(&[0o202, 0], 0, 1, 0), None);
        // The entry pushes more than the frame holds.
        assert_eq!(depths(&[0o207], 0, 1, 2), None);
    }

    #[test]
    fn handler_targets_receive_the_caught_value() {
        // pushcatch (tag) to 6; constant; pophandler; return / 6: return.
        let code = [0o300, 0o062, 7, 0, 0o301, 0o060, 0o207, 0o207];
        assert_eq!(depths(&code, 2, 2, 0), Some(vec![0, 1, U, U, 0, 1, 1, 1]));
    }

    #[test]
    fn results_belong_to_the_code_strings_current_bytes() {
        let code = crate::lisp::primitives::coding::bytes_to_unibyte_value(&[0o300, 0o207]);
        let program = |code: Value| {
            ByteCodeObject::from_slots(&[
                Value::Integer(0),
                code,
                Value::vector([Value::Integer(1)]),
                Value::Integer(1),
            ])
            .expect("valid object")
            .expect("byte code")
        };
        let first = verified_code(&program(code), 0).expect("verified");
        assert!(first.admits(0, 0) && first.admits(1, 1) && !first.admits(1, 0));
        let Kind::StringObject(string) = code.kind() else {
            unreachable!("a string")
        };
        assert_eq!(string.code_state(), CodeState::Current);
        let again = verified_code(&program(code), 0).expect("cached");
        assert!(std::ptr::eq(first, again));
        // A function of another shape on the same string has its own.
        let deeper = ByteCodeObject {
            stack_depth: 2,
            ..program(code)
        };
        let other = verified_code(&deeper, 0).expect("verified");
        assert!(!std::ptr::eq(first, other) && first.admits(0, 0));
        // Any exclusive borrow of the string retires its results.
        drop(string.borrow_mut());
        assert_eq!(string.code_state(), CodeState::Stale);
        let second = verified_code(&program(code), 0).expect("verified again");
        assert!(!std::ptr::eq(first, second));
        assert!(!first.admits(0, 0) && !other.admits(0, 0) && second.admits(0, 0));
        // Code that fails verification has none.
        let underflow = crate::lisp::primitives::coding::bytes_to_unibyte_value(&[0o210, 0o207]);
        assert!(verified_code(&program(underflow), 0).is_none());
    }

    #[test]
    fn dead_code_is_not_verified() {
        // return; then an undefined opcode nothing reaches.
        assert_eq!(depths(&[0o300, 0o207, 0o063], 1, 1, 0), Some(vec![0, 1, U]));
    }
}
