//! Execution of GNU 30.2 bytecode from its authoritative string bytes (exec_byte_code port).
//!
//! Runs a validated [`ByteCodeObject`] against the interpreter: operand
//! stack, argument prologue, dynamic binds with a specpdl-style unwind
//! stack, and catch/condition-case handlers that unwind to an in-function
//! destination exactly like GNU's `pushhandler`/`sys_setjmp` pairs.
//! The dispatch match is exhaustive over the decoded opcode set, so the
//! compiler proves every GNU 30.2 opcode has an execution arm.

use super::super::eval::roots::{LispRootMarker, TraceLispRoots};
use super::super::eval::{Interpreter, LabeledRestriction};
use super::super::primitives;
use super::super::types::{Env, LispError, Value};
use super::{ArgSpec, ByteCodeObject, Op};
use crate::lisp::types::Kind;
use crate::lisp::types::LispErrorKind;

/// bytecode.c's `bc_thread_state': one stable 512K-word allocation containing
/// both operand slots and the four-word footer of every active frame. A callee
/// begins after its caller's declared depth, not after its current stack top.
/// Only initialized live slots are exposed as Rust values or traced precisely.
pub(crate) struct BcStack {
    words: Vec<std::mem::MaybeUninit<usize>>,
    frame: usize,
    base: usize,
    top: usize,
}

const BC_STACK_WORDS: usize = 512 * 1024;
const BC_HEADER_WORDS: usize = 4;

/// GNU's `bc_frame', using allocation-relative offsets instead of interior
/// pointers so a suspended stack can be moved with its owning thread state.
/// The saved top includes outgoing arguments, which remain roots and borrowed
/// backtrace arguments until the callee returns. The function is the actual
/// closure, so no parallel function-root vector is necessary.
#[repr(C)]
#[derive(Clone, Copy)]
struct BcHeader {
    saved_frame: usize,
    saved_top: usize,
    saved_pc: usize,
    function: Value,
}

const _: () = assert!(std::mem::size_of::<BcHeader>() == BC_HEADER_WORDS * 8);
const _: () = assert!(std::mem::size_of::<Value>() == 8);

impl BcStack {
    pub(crate) const fn new() -> Self {
        Self {
            words: Vec::new(),
            frame: 0,
            base: 0,
            top: 0,
        }
    }

    fn ensure_allocated(&mut self) {
        if self.words.capacity() == 0 {
            self.words.reserve_exact(BC_STACK_WORDS);
            // SAFETY: the allocation is word-aligned and has room for the
            // dummy footer. No operand slice refers to these first four words.
            unsafe {
                self.words.as_mut_ptr().cast::<BcHeader>().write(BcHeader {
                    saved_frame: 0,
                    saved_top: 0,
                    saved_pc: 0,
                    function: Value::Nil,
                });
            }
        }
    }

    #[inline]
    fn header(&self, frame: usize) -> BcHeader {
        // SAFETY: FRAME is zero or a footer installed by enter_frame and
        // retained in the live frame chain. Reading copies its four words.
        unsafe { self.words.as_ptr().add(frame).cast::<BcHeader>().read() }
    }

    fn enter_frame(&mut self, depth: usize, function: Value, pc: usize) -> Result<(), LispError> {
        let base = self.frame + BC_HEADER_WORDS;
        let frame = base
            .checked_add(depth)
            .filter(|end| *end <= BC_STACK_WORDS - BC_HEADER_WORDS)
            .ok_or_else(stack_overflow)?;
        // SAFETY: the checked footer lies after the caller's entire declared
        // frame. Its slots are disjoint from all caller/backtrace slices.
        unsafe {
            self.words
                .as_mut_ptr()
                .add(frame)
                .cast::<BcHeader>()
                .write(BcHeader {
                    saved_frame: self.frame,
                    saved_top: self.top,
                    saved_pc: pc,
                    function,
                });
        }
        self.frame = frame;
        self.base = base;
        self.top = base;
        Ok(())
    }

    fn leave_frame(&mut self) -> usize {
        assert_ne!(self.frame, 0, "cannot pop the dummy bytecode frame");
        let header = self.header(self.frame);
        self.frame = header.saved_frame;
        self.top = header.saved_top;
        self.base = if self.frame == 0 {
            0
        } else {
            self.header(self.frame).saved_frame + BC_HEADER_WORDS
        };
        header.saved_pc
    }

    #[cfg(test)]
    fn frame_count(&self) -> usize {
        let (mut frame, mut count) = (self.frame, 0);
        while frame != 0 {
            count += 1;
            frame = self.header(frame).saved_frame;
        }
        count
    }

    #[inline(always)]
    pub(crate) fn push(&mut self, value: Value) -> Result<(), LispError> {
        if self.top == self.frame {
            return Err(stack_overflow());
        }
        self.push_within_frame(value);
        Ok(())
    }

    #[inline(always)]
    fn push_within_frame(&mut self, value: Value) {
        assert!(
            self.top < self.frame,
            "byte code exceeded its declared stack depth"
        );
        // SAFETY: this free operand word precedes the active footer and is
        // disjoint from the live operand prefix and every caller's arguments.
        unsafe {
            self.words
                .as_mut_ptr()
                .add(self.top)
                .cast::<Value>()
                .write(value);
        }
        self.top += 1;
    }

    #[inline(always)]
    pub(crate) fn pop(&mut self) -> Option<Value> {
        if self.top == self.base {
            return None;
        }
        self.top -= 1;
        // SAFETY: push initialized this word; Value is Copy and has no drop.
        Some(unsafe { self.words.as_ptr().add(self.top).cast::<Value>().read() })
    }

    #[inline(always)]
    pub(crate) fn truncate(&mut self, len: usize) {
        self.top = self.base + len.min(self.top - self.base);
    }

    #[inline(always)]
    fn drain_from(&mut self, start: usize) -> impl Iterator<Item = Value> + '_ {
        let len = self.top - self.base;
        assert!(start <= len);
        self.top = self.base + start;
        // SAFETY: these removed slots are initialized and the iterator's
        // borrow prevents stack mutation until the caller finishes copying.
        unsafe {
            std::slice::from_raw_parts(
                self.words.as_ptr().add(self.top).cast::<Value>(),
                len - start,
            )
        }
        .iter()
        .copied()
    }

    /// # Safety
    /// The caller must keep these initialized caller slots intact until the
    /// returned slice is no longer used. Callee frames occupy disjoint storage.
    unsafe fn slice_from(&self, start: usize, len: usize) -> &'static [Value] {
        assert!(start <= self.len() && len <= self.len() - start);
        unsafe {
            std::slice::from_raw_parts(
                self.words.as_ptr().add(self.base + start).cast::<Value>(),
                len,
            )
        }
    }
}

impl std::ops::Deref for BcStack {
    type Target = [Value];
    #[inline(always)]
    fn deref(&self) -> &[Value] {
        // SAFETY: only the initialized prefix of the current frame is exposed;
        // reserved slots and raw footer words are never reinterpreted as values.
        unsafe {
            std::slice::from_raw_parts(
                self.words.as_ptr().add(self.base).cast::<Value>(),
                self.top - self.base,
            )
        }
    }
}

impl std::ops::DerefMut for BcStack {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut [Value] {
        // SAFETY: exclusive access to the active initialized operand prefix.
        // A borrowed caller's arguments are in an earlier, disjoint frame.
        unsafe {
            std::slice::from_raw_parts_mut(
                self.words.as_mut_ptr().add(self.base).cast::<Value>(),
                self.top - self.base,
            )
        }
    }
}

impl TraceLispRoots for BcStack {
    fn trace_lisp_roots(&self, marker: &mut LispRootMarker<'_>) {
        let (mut frame, mut top) = (self.frame, self.top);
        while frame != 0 {
            let header = self.header(frame);
            let base = header.saved_frame + BC_HEADER_WORDS;
            // SAFETY: TOP is the current top or the initialized caller prefix
            // recorded on entry. Reserved holes and footers are not scanned.
            let values = unsafe {
                std::slice::from_raw_parts(
                    self.words.as_ptr().add(base).cast::<Value>(),
                    top - base,
                )
            };
            for value in values {
                marker.value(value);
            }
            marker.value(&header.function);
            frame = header.saved_frame;
            top = header.saved_top;
        }
    }
}

impl Default for BcStack {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for BcStack {
    fn clone(&self) -> Self {
        let mut copy = Self::new();
        if self.words.capacity() != 0 {
            copy.ensure_allocated();
            // SAFETY: copying MaybeUninit words preserves initialized headers
            // and operands without reading unused storage as initialized data.
            // All links are offsets, not pointers into the original allocation.
            unsafe {
                std::ptr::copy_nonoverlapping(
                    self.words.as_ptr(),
                    copy.words.as_mut_ptr(),
                    BC_STACK_WORDS,
                );
            }
            copy.frame = self.frame;
            copy.base = self.base;
            copy.top = self.top;
        }
        copy
    }
}

impl std::fmt::Debug for BcStack {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "BcStack({} active values, footer at {})",
            self.len(),
            self.frame
        )
    }
}

fn stack_overflow() -> LispError {
    LispError::Signal("Bytecode stack overflow".into())
}

/// exec_byte_code's arity signal for a lexbound template: the data is
/// `((MANDATORY . NONREST) NARGS)'.
fn packed_arity_error(mandatory: usize, nonrest: usize, nargs: usize) -> LispError {
    LispError::SignalValue(Value::list([
        Value::Symbol("wrong-number-of-arguments".into()),
        Value::cons(
            Value::Integer(mandatory as i64),
            Value::Integer(nonrest as i64),
        ),
        Value::Integer(nargs as i64),
    ]))
}

/// funcall_lambda's arity signal for a dynamic arglist: the closure
/// itself and NARGS (the old spelling when the object is not a record).
fn legacy_arity_error(function: &Value, nargs: usize) -> LispError {
    match function.kind() {
        Kind::Closure(_) => LispError::SignalValue(Value::list([
            Value::Symbol("wrong-number-of-arguments".into()),
            *function,
            Value::Integer(nargs as i64),
        ])),
        _ => LispError::WrongNumberOfArgs("byte-code function".into(), nargs),
    }
}

/// One suspended byte-code frame of this activation (bytecode.c's
/// `bc_frame': the caller's program and pc, its stack top, and the
/// watermarks the callee's return or error restores).
struct BcFrame {
    program: ByteCodeObject,
    /// The caller's slot holding the callee (Bcall's TOP after DISCARD):
    /// the callee's arguments and frame lie above it, and the return
    /// value replaces it.
    base: usize,
    handlers_len: usize,
    unwinds_len: usize,
    backtrace_depth: usize,
    op_error_frames: usize,
}

/// Breturn's and the error path's leave of a frame: its specpdl entries
/// above the watermark undone, its backtrace frame popped, Ffuncall's
/// depth restored.
///
/// A signal from an unwind form replaces the one in flight (each signal
/// during unbind_to starts its own unwinding in C, the remaining forms
/// still run), so the last signal is the one that leaves.
fn leave_frame(interp: &mut Interpreter, env: &mut Env, frame: &BcFrame) -> Result<(), LispError> {
    let mut last_error = None;
    while interp.bc_unwinds.len() > frame.unwinds_len {
        let Some(entry) = interp.bc_unwinds.pop() else {
            break;
        };
        if let Err(error) = unwind_one(interp, entry, env) {
            last_error = Some(error);
        }
    }
    interp.truncate_backtrace_frames(frame.backtrace_depth);
    interp.end_funcall();
    last_error.map_or(Ok(()), Err)
}

/// One specpdl-style entry the VM must undo on Bunbind or error unwind.
#[derive(Clone, Debug)]
pub(crate) enum UnwindEntry {
    Binding(super::super::eval::SpecialBindingRestore),
    /// Bsave_excursion: restore buffer and (marker-tracked) point.
    Excursion {
        buffer_id: u64,
        marker_id: crate::lisp::types::MarkerRef,
        saved_pt: usize,
    },
    /// Bsave_current_buffer: restore the current buffer only.
    CurrentBuffer {
        buffer_id: u64,
    },
    /// Bsave_restriction on a wide buffer: re-widen on exit.
    RestrictionWide {
        buffer_id: u64,
        labeled: Vec<LabeledRestriction>,
    },
    /// Bsave_restriction on a narrowed buffer: marker-tracked bounds.
    Restriction {
        buffer_id: u64,
        beg_id: crate::lisp::types::MarkerRef,
        end_id: crate::lisp::types::MarkerRef,
        saved_begv: usize,
        saved_zv: usize,
        labeled: Vec<LabeledRestriction>,
    },
    /// Bunwind_protect: a handler function (24.4+) or list of forms.
    Protect(Value),
}

impl TraceLispRoots for UnwindEntry {
    fn trace_lisp_roots(&self, marker: &mut LispRootMarker<'_>) {
        match self {
            // These are restore tokens, not an alternate binding stack.
            // A thread switch updates the canonical interpreter specpdl.
            Self::Binding(_) | Self::CurrentBuffer { .. } => {}
            Self::Excursion { marker_id, .. } => marker.value(&Value::Marker(*marker_id)),
            Self::RestrictionWide { labeled, .. } | Self::Restriction { labeled, .. } => {
                if let Self::Restriction { beg_id, end_id, .. } = self {
                    marker.value(&Value::Marker(*beg_id));
                    marker.value(&Value::Marker(*end_id));
                }
                for restriction in labeled {
                    restriction.trace_lisp_roots(marker);
                }
            }
            Self::Protect(handler) => marker.value(handler),
        }
    }
}

/// Undo one specpdl-style entry (GNU's do_one_unbind).
fn unwind_one(
    interp: &mut Interpreter,
    entry: UnwindEntry,
    env: &mut Env,
) -> Result<(), LispError> {
    match entry {
        UnwindEntry::Binding(restore) => interp.restore_special_binding(restore, env),
        UnwindEntry::Excursion {
            buffer_id,
            marker_id,
            saved_pt,
        } => {
            if interp.has_buffer_id(buffer_id) {
                let _ = interp.set_current_buffer_id(buffer_id);
                let restore_pt = interp.marker_position(marker_id).unwrap_or(saved_pt).clamp(
                    interp.buffer.borrow().point_min(),
                    interp.buffer.borrow().point_max(),
                );
                interp.buffer.borrow_mut().goto_char(restore_pt);
            }
            let _ = interp.set_marker(marker_id, None, None);
            Ok(())
        }
        UnwindEntry::CurrentBuffer { buffer_id } => {
            if interp.has_buffer_id(buffer_id) {
                let _ = interp.set_current_buffer_id(buffer_id);
            }
            Ok(())
        }
        UnwindEntry::RestrictionWide { buffer_id, labeled } => {
            let final_buffer_id = interp.current_buffer_id();
            if interp.has_buffer_id(buffer_id) {
                if final_buffer_id != buffer_id {
                    let _ = interp.set_current_buffer_id(buffer_id);
                }
                let full_end = interp.buffer.borrow().size_total() + 1;
                interp.buffer.borrow_mut().restore_restriction(1, full_end);
                interp.restore_labeled_restrictions(buffer_id, labeled);
                if final_buffer_id != buffer_id && interp.has_buffer_id(final_buffer_id) {
                    let _ = interp.set_current_buffer_id(final_buffer_id);
                }
            }
            Ok(())
        }
        UnwindEntry::Restriction {
            buffer_id,
            beg_id,
            end_id,
            saved_begv,
            saved_zv,
            labeled,
        } => {
            let final_buffer_id = interp.current_buffer_id();
            let restore_begv = interp.marker_position(beg_id).unwrap_or(saved_begv);
            let restore_zv = interp.marker_position(end_id).unwrap_or(saved_zv);
            if interp.has_buffer_id(buffer_id) {
                if final_buffer_id != buffer_id {
                    let _ = interp.set_current_buffer_id(buffer_id);
                }
                interp
                    .buffer
                    .borrow_mut()
                    .restore_restriction(restore_begv, restore_zv);
                interp.restore_labeled_restrictions(buffer_id, labeled);
                if final_buffer_id != buffer_id && interp.has_buffer_id(final_buffer_id) {
                    let _ = interp.set_current_buffer_id(final_buffer_id);
                }
            }
            let _ = interp.set_marker(beg_id, None, None);
            let _ = interp.set_marker(end_id, None, None);
            Ok(())
        }
        UnwindEntry::Protect(handler) => {
            // GNU records bcall0 for functions (24.4+) and prog_ignore
            // for the obsolete forms-list shape.
            interp.with_lisp_stack_roots(&handler, |interp| {
                let is_function = prim(interp, "functionp", std::slice::from_ref(&handler), env)?;
                if is_function.is_truthy() {
                    interp.call_function_value(handler, None, &[], env)?;
                } else if let Ok(forms) = handler.to_vec() {
                    for form in &forms {
                        interp.eval(form, env)?;
                    }
                }
                Ok(())
            })
        }
    }
}

enum HandlerKind {
    /// Bpushcatch: TAG caught by `eq`.
    Catch(Value),
    /// Bpushconditioncase: the popped conditions clause head.
    ConditionCase(Value),
}

struct Handler {
    kind: HandlerKind,
    dest: usize,
    stack_len: usize,
    unwind_len: usize,
    /// Interpreter handler-registry watermark for a condition-case frame,
    /// so signal-time `handler-bind' dispatch sees VM frames like GNU's
    /// handlerlist.
    registry_start: Option<usize>,
}

/// Whether `EMAXX_TRACE_LOAD_ERRORS' asks for VM dispatch traces — the
/// process-wide latch, taken before main() scrubs the variable from the
/// environment so children and Lisp `getenv' stay oracle-clean.
fn trace_load_errors() -> bool {
    crate::lisp::trace_load_errors_enabled()
}

type PrimitiveFactBucket = Vec<(usize, crate::lisp::primitives::NameFacts)>;
type PrimitiveFactCache =
    std::collections::HashMap<usize, PrimitiveFactBucket, crate::lisp::types::IdentityBuildHasher>;

/// Call a named Emaxx primitive with stack values (the Bcar/Bplus-style
/// single-instruction function ops).
///
/// The names are a closed set of static literals naming GNU's dedicated
/// opcodes (`Bnth', `Bpoint', ...), so their dispatch facts are cached by
/// string address: the general string-keyed probe ran once per executed
/// opcode and was a measurable slice of `nth'-heavy bytecode.
fn prim(
    interp: &mut Interpreter,
    name: &'static str,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    thread_local! {
        static PRIM_FACTS: std::cell::RefCell<PrimitiveFactCache> =
            std::cell::RefCell::new(PrimitiveFactCache::default());
    }
    let pointer = name.as_ptr() as usize;
    let facts = PRIM_FACTS.with_borrow(|cache| {
        cache.get(&pointer).and_then(|entries| {
            entries
                .iter()
                .find_map(|(length, facts)| (*length == name.len()).then_some(*facts))
        })
    });
    let facts = match facts {
        Some(facts) => facts,
        None => {
            let facts = crate::lisp::primitives::name_facts(name);
            PRIM_FACTS.with_borrow_mut(|cache| {
                cache.entry(pointer).or_default().push((name.len(), facts));
            });
            facts
        }
    };
    interp.with_lisp_stack_roots(&args, |interp| {
        primitives::call_with_facts(interp, name, facts, args, env)
    })
}

/// Why the hot loop left: the next instruction needs the interpreter (or
/// signals), the function returned, or 256 backward jumps have passed
/// (exec_byte_code's `quitcounter': maybe_gc and maybe_quit are due).
enum FastExit {
    Slow(super::Instr),
    Error(super::ByteCodeError),
    Return(Value),
    QuitCheck,
}

/// exec_byte_code's dispatch loop for the ops that need nothing but the
/// operand stack and the program: stack shuffles, constants, jumps,
/// car/cdr, cons, eq, fixnum arithmetic and comparison, vector aref/aset.
/// Dispatches directly on the canonical opcode, as bytecode.c's FETCH/NEXT,
/// without first building an Instr and switching on its Op a second time.
/// The operand stack is held directly for the whole run. An op
/// that cannot complete here (a call, a variable, a signal, an operand of
/// another kind) is left untouched at PC for the full dispatch.
#[inline(never)]
fn run_fast(
    object: &ByteCodeObject,
    code: &[u8],
    ops: &mut BcStack,
    pc: &mut usize,
    quitcounter: &mut u8,
) -> FastExit {
    let mut at = *pc;
    let mut offset;
    macro_rules! operand {
        ($value:expr) => {
            match $value {
                Ok(value) => value,
                Err(error) => {
                    *pc = offset;
                    return FastExit::Error(error);
                }
            }
        };
    }
    macro_rules! slow {
        () => {{
            *pc = offset;
            return match super::fetch_instruction(code, offset, object.constants.len()) {
                Ok(instruction) => FastExit::Slow(instruction),
                Err(error) => FastExit::Error(error),
            };
        }};
    }
    macro_rules! jump {
        ($target:expr) => {{
            let destination = $target as usize;
            if destination < at {
                *quitcounter = quitcounter.wrapping_add(1);
                if *quitcounter == 0 {
                    *quitcounter = 1;
                    *pc = destination;
                    return FastExit::QuitCheck;
                }
            }
            at = destination;
        }};
    }
    macro_rules! pop {
        () => {
            ops.pop().expect("validated bytecode never underflows")
        };
    }
    loop {
        offset = at;
        let Some(&byte) = code.get(at) else {
            *pc = at;
            return FastExit::Error(super::ByteCodeError::EndOfCode { offset: at });
        };
        at += 1;
        // bytecode.c's FETCH/NEXT dispatches the actual opcode directly.
        // Only the interpreter fallback needs a decoded Instr. Neither
        // representation nor an instruction cache survives this borrow.
        let mut reader = super::OperandReader {
            code,
            cursor: at,
            offset,
            byte,
        };
        match byte {
            // Bcall's argument count uses the same checked operand reader
            // as the other packed families. Keep the instruction needed
            // by the full call/error path without decoding the opcode again.
            super::B_CALL..=0o047 => {
                let count = operand!(reader.family_operand(super::B_CALL));
                *pc = offset;
                return FastExit::Slow(super::Instr {
                    offset,
                    len: reader.cursor - offset,
                    op: Op::Call(count),
                });
            }
            0o001..=0o007 => {
                let n = operand!(reader.family_operand(super::B_STACK_REF));
                at = reader.cursor;
                let index = ops.len() - 1 - n as usize;
                let value = ops[index];
                ops.push_within_frame(value);
            }
            super::B_STACK_SET | super::B_STACK_SET2 => {
                let n = operand!(if byte == super::B_STACK_SET {
                    reader.fetch()
                } else {
                    reader.fetch2()
                });
                at = reader.cursor;
                let value = pop!();
                // stack-set N stores relative to the pre-pop top.
                let slot = ops.len() - 1 - (n as usize - 1);
                std::mem::replace(&mut ops[slot], value).discard();
            }
            // Bdup.
            0o211 => {
                let top = *ops.last().expect("validated bytecode");
                ops.push_within_frame(top);
            }
            // Bdiscard.
            0o210 => {
                pop!().discard();
            }
            super::B_DISCARDN => {
                let raw = operand!(reader.fetch()) as u8;
                at = reader.cursor;
                let count = raw & 0x7f;
                let preserve_tos = raw & 0x80 != 0;
                if preserve_tos {
                    let top = pop!();
                    for _ in 0..count {
                        pop!().discard();
                    }
                    ops.push_within_frame(top);
                } else {
                    for _ in 0..count {
                        pop!().discard();
                    }
                }
            }
            super::B_CONSTANT2 | 0o300..=0o377 => {
                let index = if byte == super::B_CONSTANT2 {
                    let index = operand!(reader.fetch2());
                    at = reader.cursor;
                    index
                } else {
                    u16::from(byte - super::B_CONSTANT)
                };
                operand!(super::check_constant_index(
                    offset,
                    index,
                    object.constants.len()
                ));
                ops.push_within_frame(object.constant(index));
            }
            0o202 => {
                let target = operand!(reader.fetch2());
                at = reader.cursor;
                jump!(target);
            }
            0o203 => {
                let target = operand!(reader.fetch2());
                at = reader.cursor;
                let value = pop!();
                let is_nil = value.is_nil();
                value.discard();
                if is_nil {
                    jump!(target);
                }
            }
            0o204 => {
                let target = operand!(reader.fetch2());
                at = reader.cursor;
                let value = pop!();
                let is_nil = value.is_nil();
                value.discard();
                if !is_nil {
                    jump!(target);
                }
            }
            0o205 => {
                let target = operand!(reader.fetch2());
                at = reader.cursor;
                if ops.last().expect("validated bytecode").is_nil() {
                    jump!(target);
                } else {
                    pop!().discard();
                }
            }
            0o206 => {
                let target = operand!(reader.fetch2());
                at = reader.cursor;
                if !ops.last().expect("validated bytecode").is_nil() {
                    jump!(target);
                } else {
                    pop!().discard();
                }
            }
            // Breturn.
            0o207 => {
                return FastExit::Return(pop!());
            }
            // Bnot.
            0o77 => {
                let top = ops.last_mut().expect("validated bytecode");
                *top = if top.is_nil() { Value::T } else { Value::Nil };
            }
            // Bcons.
            0o102 => {
                let b = pop!();
                let top = ops.last_mut().expect("validated bytecode");
                *top = Value::cons(*top, b);
            }
            // Beq.
            0o75 => {
                let len = ops.len();
                if matches!(ops[len - 1].kind(), Kind::SymbolWithPos(_))
                    || matches!(ops[len - 2].kind(), Kind::SymbolWithPos(_))
                {
                    slow!();
                }
                let equal = crate::lisp::primitives::values_eq_plain(&ops[len - 2], &ops[len - 1]);
                ops[len - 2] = if equal { Value::T } else { Value::Nil };
                ops.truncate(len - 1);
            }
            // Bconsp.
            0o72 => {
                let is_cons = match ops.last().expect("validated bytecode").kind() {
                    Kind::Cons(_) => true,
                    // A keymap record reads as a cons; the full arm asks.
                    Kind::Record(_) => slow!(),
                    _ => false,
                };
                *ops.last_mut().expect("validated bytecode") =
                    if is_cons { Value::T } else { Value::Nil };
            }
            0o134 | 0o132 | 0o137 | 0o245 | 0o246 => {
                let len = ops.len();
                let (Kind::Integer(x), Kind::Integer(y)) =
                    (ops[len - 2].kind(), ops[len - 1].kind())
                else {
                    slow!();
                };
                // checked_div/checked_rem refuse y == 0 and the MIN/-1
                // overflow, which fall through to the full arithmetic
                // (and its arith-error); overflow falls through to bignums.
                let fast = match byte {
                    0o134 => x.checked_add(y),
                    0o132 => x.checked_sub(y),
                    0o137 => x.checked_mul(y),
                    0o245 => x.checked_div(y),
                    _ => x.checked_rem(y),
                };
                let Some(n) = fast else { slow!() };
                // bytecode.c pops the right operand and overwrites TOP.
                // The result occupies an existing slot: no push, capacity
                // check or extra length update is needed.
                ops[len - 2] = Value::Integer(n);
                ops.truncate(len - 1);
            }
            0o125..=0o131 => {
                let len = ops.len();
                let (Kind::Integer(x), Kind::Integer(y)) =
                    (ops[len - 2].kind(), ops[len - 1].kind())
                else {
                    slow!();
                };
                let holds = match byte {
                    0o125 => x == y,
                    0o126 => x > y,
                    0o127 => x < y,
                    0o130 => x <= y,
                    _ => x >= y,
                };
                ops[len - 2] = if holds { Value::T } else { Value::Nil };
                ops.truncate(len - 1);
            }
            0o124 | 0o123 | 0o133 => {
                let Kind::Integer(x) = ops.last().expect("validated bytecode").kind() else {
                    slow!();
                };
                let fast = match byte {
                    0o124 => x.checked_add(1),
                    0o123 => x.checked_sub(1),
                    _ => x.checked_neg(),
                };
                let Some(n) = fast else { slow!() };
                *ops.last_mut().expect("validated bytecode") = Value::Integer(n);
            }
            // Baref.
            0o110 => {
                let len = ops.len();
                let Kind::Integer(index) = ops[len - 1].kind() else {
                    slow!()
                };
                if index < 0 {
                    slow!();
                }
                let Some(value) =
                    crate::lisp::primitives::vector_aref_fast(&ops[len - 2], index as usize)
                else {
                    slow!();
                };
                ops[len - 2] = value;
                ops.truncate(len - 1);
            }
            // Baset.
            0o111 => {
                // Stack: [.. vector index value]; aset returns the value.
                let len = ops.len();
                let Kind::Integer(index) = ops[len - 2].kind() else {
                    slow!()
                };
                if index < 0
                    || crate::lisp::primitives::vector_aset_fast(
                        &ops[len - 3],
                        index as usize,
                        &ops[len - 1],
                    )
                    .is_none()
                {
                    slow!();
                }
                ops[len - 3] = ops[len - 1];
                ops.truncate(len - 2);
            }
            0o100 | 0o101 | 0o242 | 0o243 => {
                // bytecode.c reads the car or cdr of the object on the stack
                // top and stores it there: the operand is read in place,
                // never copied first.
                let replacement = match ops.last().expect("validated bytecode").kind() {
                    Kind::Cons(cell) => {
                        if matches!(byte, 0o100 | 0o242) {
                            cell.car.get()
                        } else {
                            cell.cdr.get()
                        }
                    }
                    Kind::Nil => continue,
                    _ if matches!(byte, 0o242 | 0o243) => Value::Nil,
                    // The full arm signals wrong-type-argument.
                    _ => slow!(),
                };
                let top = ops.last_mut().expect("validated bytecode");
                std::mem::replace(top, replacement).discard();
            }
            _ => slow!(),
        }
    }
}

impl TraceLispRoots for ByteCodeObject {
    fn trace_lisp_roots(&self, marker: &mut LispRootMarker<'_>) {
        marker.value(&self.code.original());
        marker.value(&Value::Vector(self.constants));
        if let ArgSpec::Legacy(arguments) = &self.argspec {
            marker.value(arguments);
        }
    }
}

impl ByteCodeObject {
    #[inline]
    fn constant(&self, index: u16) -> Value {
        // bytecode.c reads vectorp[index] at the instruction, not a copy
        // captured during decoding. End the borrow before Lisp can run.
        self.constants
            .get(usize::from(index))
            .expect("validated constant index")
    }
}

pub(crate) fn closure_program(
    closure: crate::lisp::types::ClosureRef,
) -> Result<ByteCodeObject, LispError> {
    ByteCodeObject::from_closure(closure)
        .map_err(|error| LispError::Signal(error.to_string()))?
        .ok_or_else(|| {
            LispError::SignalValue(Value::list([
                Value::symbol("invalid-function"),
                Value::Closure(closure),
            ]))
        })
}

/// Read the actual PVEC_CLOSURE fields and dispatch from its code bytes.
#[inline(always)]
pub fn execute_closure(
    interp: &mut Interpreter,
    closure: crate::lisp::types::ClosureRef,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    let program = closure_program(closure)?;
    run(interp, program, Value::Closure(closure), args, env)
}

/// Execute OBJECT with ARGS, returning the value of Breturn.
#[cfg(test)]
pub fn execute(
    interp: &mut Interpreter,
    object: &ByteCodeObject,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    let program = object.clone();
    run(interp, program, Value::Nil, args, env)
}

/// One activation entered from Rust (Ffuncall's exec_byte_code call):
/// the arguments pushed onto the thread's bytecode stack, the frames of
/// the byte-code functions it calls laid out above them in the loop,
/// and the stack cut back to the entry mark on the way out.
fn run(
    interp: &mut Interpreter,
    program: ByteCodeObject,
    function: Value,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    interp.bc_stack.ensure_allocated();
    let stack_at_entry = (
        interp.bc_stack.frame,
        interp.bc_stack.base,
        interp.bc_stack.top,
    );
    let unwinds_at_entry = interp.bc_unwinds.len();
    let frames_at_entry = interp.backtrace_frames_len();
    // The activation's program is a root while it runs (alloc.c marks the
    // thread's bytecode stack, whose frames hold their functions).
    let result = if function.is_nil() {
        // Direct Rust execution also roots the code and legacy arglist; a
        // constants-only root cannot keep them alive across a callback.
        interp.with_lisp_stack_roots(&program, |interp| {
            run_frames(interp, program.clone(), function, args, env)
        })
    } else {
        run_frames(interp, program, function, args, env)
    };
    // A signaling byte op recorded itself as a backtrace frame
    // (bytecode.c's record_in_backtrace) so handler-bind handlers saw it;
    // the handlers have run by now, so unwind it like GNU's specpdl does.
    interp.truncate_backtrace_frames(frames_at_entry);
    // Balance any entries left by an abnormal exit (GNU unbind_to on the
    // frame's specpdl watermark), running unwind-protect handlers and
    // restoring saved buffer state on the way out.
    // A signal from an unwind form replaces the one in flight, as in
    // leave_frame; the remaining forms still run.
    let result = if interp.bc_unwinds.len() > unwinds_at_entry {
        let balance = interp.with_lisp_stack_roots(&result, |interp| {
            let mut last_error = None;
            while interp.bc_unwinds.len() > unwinds_at_entry {
                let Some(entry) = interp.bc_unwinds.pop() else {
                    break;
                };
                if let Err(error) = unwind_one(interp, entry, env) {
                    last_error = Some(error);
                }
            }
            last_error.map_or(Ok::<_, LispError>(()), Err)
        });
        match (result, balance) {
            (_, Err(error)) => Err(error),
            (result, Ok(())) => result,
        }
    } else {
        result
    };
    (
        interp.bc_stack.frame,
        interp.bc_stack.base,
        interp.bc_stack.top,
    ) = stack_at_entry;
    result
}

/// bytecode.c:exec_byte_code.  The operand stack and the specpdl-style
/// unwind list are the thread's (`interp.bc_stack', `interp.bc_unwinds');
/// a call to a byte-code function pushes a frame and continues in this
/// loop (`goto setup_frame'), its return pops it.
fn run_frames(
    interp: &mut Interpreter,
    mut program: ByteCodeObject,
    function: Value,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    interp.bc_stack.enter_frame(
        program.stack_depth,
        if function.is_nil() {
            Value::Vector(program.constants)
        } else {
            function
        },
        0,
    )?;
    // Argument prologue (exec_byte_code's ARGS_TEMPLATE handling).
    match &program.argspec {
        ArgSpec::Packed {
            mandatory,
            nonrest,
            rest,
        } => {
            let mandatory = *mandatory as usize;
            let nonrest = *nonrest as usize;
            if args.len() < mandatory || (!rest && args.len() > nonrest) {
                return Err(packed_arity_error(mandatory, nonrest, args.len()));
            }
            let pushed = args.len().min(nonrest);
            // bytecode.c:exec_byte_code pushes the original Lisp objects.
            // Allocation belongs to their producers; copying a string here
            // splits aliases and shared compiler constants.
            for value in &args[..pushed] {
                interp.bc_stack.push(*value)?;
            }
            if args.len() > nonrest {
                interp
                    .bc_stack
                    .push(Value::list(args[nonrest..].iter().cloned()))?;
            } else {
                for _ in args.len()..nonrest {
                    interp.bc_stack.push(Value::Nil)?;
                }
                if *rest {
                    interp.bc_stack.push(Value::Nil)?;
                }
            }
        }
        // Old-style dynamic bytecode: funcall_lambda specbinds each
        // formal (honoring &optional/&rest) and runs the body with an
        // empty stack; the exit balance below is GNU's unbind_to.
        ArgSpec::Legacy(arglist) => {
            let formals = arglist.to_vec().unwrap_or_default();
            let mut index = 0usize;
            let mut optional = false;
            let mut rest = false;
            for formal in &formals {
                let name = match formal.kind() {
                    Kind::Symbol(name) => name,
                    other => {
                        let error = LispError::SignalValue(Value::list([
                            Value::Symbol("invalid-function".into()),
                            other.value(),
                        ]));
                        return Err(error);
                    }
                };
                if name == "&optional" {
                    optional = true;
                    continue;
                }
                if name == "&rest" {
                    rest = true;
                    continue;
                }
                let value = if rest {
                    Value::list(args[index.min(args.len())..].iter().cloned())
                } else if index < args.len() {
                    args[index]
                } else if optional {
                    Value::Nil
                } else {
                    return Err(legacy_arity_error(&function, args.len()));
                };
                if rest {
                    index = args.len();
                } else {
                    index += 1;
                }
                let restore = interp.bind_special_variable(&name, value, env)?;
                interp.bc_unwinds.push(UnwindEntry::Binding(restore));
                if rest {
                    break;
                }
            }
            if !rest && index < args.len() {
                return Err(legacy_arity_error(&function, args.len()));
            }
        }
    }
    let mut handlers: Vec<Handler> = Vec::new();
    let mut frames: Vec<BcFrame> = Vec::new();
    let mut pc = 0usize;
    let mut last_instruction = None;
    let trace_errors = trace_load_errors();
    // Frames pushed by signaling byte ops (record_in_backtrace); an
    // in-frame condition-case that catches must unwind them, and the
    // caller (`run') balances whatever is left after handler dispatch.
    let mut op_error_frames = 0usize;
    // bytecode.c's `quitcounter': backward branches increment the byte
    // counter; wrapping it to zero calls maybe_gc and maybe_quit before
    // resetting it to one, so a loop without a call can be interrupted.
    let mut quitcounter: u8 = 1;

    macro_rules! pop {
        () => {
            interp
                .bc_stack
                .pop()
                .expect("validated bytecode never underflows")
        };
    }
    macro_rules! push {
        ($value:expr) => {{
            let value = $value;
            interp.bc_stack.push_within_frame(value)
        }};
    }

    // exec_byte_code's dispatch loop runs inside one closure, so an op's
    // failure (`?') leaves the loop for the handler search below, which
    // resumes the loop at the handler's target; a normal return leaves
    // it with the value.
    macro_rules! branch {
        ($target:expr) => {{
            let destination = $target as usize;
            if destination < pc {
                quitcounter = quitcounter.wrapping_add(1);
                if quitcounter == 0 {
                    quitcounter = 1;
                    crate::lisp::native_comp::maybe_gc(interp, env);
                    interp.maybe_quit(env)?;
                }
            }
            pc = destination;
        }};
    }

    // Breturn: with a frame below, the value replaces the callee's slot
    // and the caller's program and pc resume; at the entry frame it
    // leaves the activation.
    macro_rules! breturn {
        ($value:expr) => {{
            let value = $value;
            match frames.pop() {
                Some(frame) => {
                    if let Err(error) = leave_frame(interp, env, &frame) {
                        frames.push(frame);
                        return Err(error);
                    }
                    pc = interp.bc_stack.leave_frame();
                    interp.bc_stack.truncate(frame.base);
                    push!(value);
                    program = frame.program;
                    op_error_frames = frame.op_error_frames;
                    handlers.truncate(frame.handlers_len);
                    continue;
                }
                None => return Ok(value),
            }
        }};
    }

    let result = 'run: loop {
        let step: Result<Value, LispError> = (|| loop {
            // The hot loop first; it leaves PC at the instruction it could
            // not run, which the full dispatch below runs once.
            // No Lisp callback, collection, or string store occurs in the
            // fast loop. Release its byte borrow before the full dispatch.
            let exit = program.code.with_bytes(|code| {
                run_fast(
                    &program,
                    code,
                    &mut interp.bc_stack,
                    &mut pc,
                    &mut quitcounter,
                )
            });
            last_instruction = None;
            // Carry the already fetched instruction into the slow dispatch.
            // No callback runs between these loops, so fetching it again
            // would add work without observing another Lisp mutation.
            let instr = match exit {
                FastExit::Return(value) => breturn!(value),
                FastExit::QuitCheck => {
                    crate::lisp::native_comp::maybe_gc(interp, env);
                    interp.maybe_quit(env)?;
                    continue;
                }
                FastExit::Slow(instr) => instr,
                FastExit::Error(error) => return Err(LispError::Signal(error.to_string())),
            };
            last_instruction = Some(instr);
            let op = instr.op;
            let offset = instr.offset;
            pc += instr.len;

            // run_fast already handled the ordinary stack/value cases.
            // A fallback goes directly to its complete operation; repeating
            // the same fast predicates here only adds a third dispatch.
            // Every fallible operation funnels through the closure's result so
            // handler unwinding (GNU's sys_setjmp arm) is applied uniformly.
            match op {
                Op::StackRef(n) => {
                    let value = interp.bc_stack[interp.bc_stack.len() - 1 - n as usize];
                    push!(value);
                }
                Op::StackSet(n) => {
                    let value = pop!();
                    let slot = interp.bc_stack.len() - 1 - (n as usize - 1);
                    // stack-set N stores relative to the *pre-pop* top.
                    interp.bc_stack[slot] = value;
                }
                Op::DiscardN {
                    count,
                    preserve_tos,
                } => {
                    if preserve_tos {
                        let top = pop!();
                        for _ in 0..count {
                            pop!();
                        }
                        push!(top);
                    } else {
                        for _ in 0..count {
                            pop!();
                        }
                    }
                }
                Op::Discard => {
                    pop!();
                }
                Op::Dup => {
                    let top = *interp.bc_stack.last().expect("validated bytecode");
                    push!(top);
                }
                Op::Constant(index) | Op::Constant2(index) => {
                    push!(program.constant(index));
                }
                Op::VarRef(index) => {
                    // bytecode.c:Bvarref is find_symbol_value on the constant,
                    // signalling void-variable with that very object; a bare
                    // symbol reaches the cell directly, anything else takes
                    // Fsymbol_value's CHECK_SYMBOL path.
                    let name = program.constant(index);
                    let value = match name.kind() {
                        Kind::Symbol(symbol) => match interp.symbol_value_cell_symbol(&symbol) {
                            Ok(value) => value,
                            Err(error) if matches!(error.kind(), LispErrorKind::Void(_)) => {
                                return Err(LispError::SignalValue(Value::list([
                                    Value::symbol("void-variable"),
                                    name,
                                ])));
                            }
                            Err(error) => return Err(error),
                        },
                        _ => prim(interp, "symbol-value", &[name], env)?,
                    };
                    push!(value);
                }
                Op::VarSet(index) => {
                    // bytecode.c:Bvarset is set_internal on the constant.
                    let name = program.constant(index);
                    let value = pop!();
                    match name.kind() {
                        Kind::Symbol(symbol) => {
                            crate::lisp::primitives::set_internal_symbol(
                                interp, &symbol, value, env,
                            )?;
                        }
                        _ => {
                            prim(interp, "set", &[name, value], env)?;
                        }
                    }
                }
                Op::VarBind(index) => {
                    // bytecode.c:Bvarbind is specbind on the constant.
                    let name = program.constant(index);
                    let name = name.as_symbol().map_err(|_| {
                        LispError::Signal("varbind constant must be a symbol".into())
                    })?;
                    let value = pop!();
                    let restore = interp.bind_special_variable(name, value, env)?;
                    interp.bc_unwinds.push(UnwindEntry::Binding(restore));
                }
                Op::Unbind(count) => {
                    for _ in 0..count {
                        match interp.bc_unwinds.pop() {
                            Some(entry) => unwind_one(interp, entry, env)?,
                            None => {
                                return Err(LispError::Signal("byte code unbind underflow".into()));
                            }
                        }
                    }
                }
                Op::SaveExcursion => {
                    let buffer_id = interp.current_buffer_id();
                    let saved_pt = interp.buffer.borrow().point();
                    let Kind::Marker(marker_id) = interp.make_marker().kind() else {
                        unreachable!("make_marker returns a marker")
                    };
                    interp.set_marker(marker_id, Some(saved_pt), Some(buffer_id))?;
                    interp.bc_unwinds.push(UnwindEntry::Excursion {
                        buffer_id,
                        marker_id,
                        saved_pt,
                    });
                }
                Op::SaveCurrentBuffer | Op::SaveCurrentBufferObsolete => {
                    let buffer_id = interp.current_buffer_id();
                    interp
                        .bc_unwinds
                        .push(UnwindEntry::CurrentBuffer { buffer_id });
                }
                Op::SaveRestriction => {
                    let buffer_id = interp.current_buffer_id();
                    let saved_begv = interp.buffer.borrow().point_min();
                    let saved_zv = interp.buffer.borrow().point_max();
                    let labeled = interp.labeled_restrictions_snapshot(buffer_id);
                    // Mirrors sf_save_restriction: a wide buffer records "no
                    // restriction" (marker-tracking would spuriously
                    // re-narrow after edits at BEGV).
                    if saved_begv == 1 && saved_zv == interp.buffer.borrow().size_total() + 1 {
                        interp
                            .bc_unwinds
                            .push(UnwindEntry::RestrictionWide { buffer_id, labeled });
                    } else {
                        let Kind::Marker(beg_id) = interp.make_marker().kind() else {
                            unreachable!("make_marker returns a marker")
                        };
                        let Kind::Marker(end_id) = interp.make_marker().kind() else {
                            unreachable!("make_marker returns a marker")
                        };
                        let _ = interp.set_marker(beg_id, Some(saved_begv), Some(buffer_id));
                        let _ = interp.set_marker(end_id, Some(saved_zv), Some(buffer_id));
                        interp.set_marker_insertion_type(end_id, true);
                        interp.bc_unwinds.push(UnwindEntry::Restriction {
                            buffer_id,
                            beg_id,
                            end_id,
                            saved_begv,
                            saved_zv,
                            labeled,
                        });
                    }
                }
                Op::UnwindProtect => {
                    let handler = pop!();
                    interp.bc_unwinds.push(UnwindEntry::Protect(handler));
                }
                Op::SaveWindowExcursion => {
                    // Obsolete since 24.1: TOP is a list of body forms;
                    // evaluate them inside a window-configuration save.
                    let body = pop!();
                    let snapshot = interp.snapshot_window_configuration();
                    let mut value = Value::Nil;
                    let result: Result<(), LispError> =
                        interp.with_lisp_stack_roots(&(&body, &snapshot), |interp| {
                            for form in &body.to_vec()? {
                                value = interp.eval(form, env)?;
                            }
                            Ok(())
                        });
                    let _ = interp.restore_window_configuration(snapshot);
                    result?;
                    push!(value);
                }
                Op::Catch => {
                    // Obsolete since 25: TAG below an unevaluated body form.
                    let body = pop!();
                    let tag = pop!();
                    let value =
                        interp.with_lisp_stack_roots(&(&body, &tag), |interp| {
                            match interp.eval(&body, env) {
                                Err(error) => match error.kind() {
                                    LispErrorKind::Throw(thrown, thrown_value) => {
                                        let matched = interp
                                            .with_lisp_stack_roots(&error, |interp| {
                                                prim(interp, "eq", &[tag, *thrown], env)
                                            })?;
                                        if matched.is_truthy() {
                                            Ok(*thrown_value)
                                        } else {
                                            Err(error)
                                        }
                                    }
                                    _ => Err(error),
                                },
                                ok => ok,
                            }
                        })?;
                    push!(value);
                }
                Op::ConditionCase => {
                    // Obsolete since 25: VAR, BODY form, HANDLERS list —
                    // exactly `(condition-case VAR BODY . HANDLERS)`.
                    let handlers = pop!();
                    let body = pop!();
                    let var = pop!();
                    let mut form = vec![Value::symbol("condition-case"), var, body];
                    form.extend(handlers.to_vec()?);
                    let form = Value::list(form);
                    let value =
                        interp.with_lisp_stack_roots(&form, |interp| interp.eval(&form, env))?;
                    push!(value);
                }
                Op::TempOutputBufferSetup => {
                    // Obsolete since 24.1: create/erase the temp buffer and
                    // bind standard-output to it (GNU temp_output_buffer_setup
                    // specbinds, so Bunbind pops it).
                    let name = pop!();
                    let buffer = prim(interp, "get-buffer-create", &[name], env)?;
                    let saved = interp.current_buffer_id();
                    let buffer_id = interp.resolve_buffer_id(&buffer)?;
                    let _ = interp.set_current_buffer_id(buffer_id);
                    prim(interp, "erase-buffer", &[], env)?;
                    let _ = interp.set_current_buffer_id(saved);
                    let restore = interp.bind_special_variable("standard-output", buffer, env)?;
                    interp.bc_unwinds.push(UnwindEntry::Binding(restore));
                    push!(buffer);
                }
                Op::TempOutputBufferShow => {
                    // Obsolete since 24.1: show the buffer at TOP, replace it
                    // with the saved value below, pop the standard-output bind.
                    let value = pop!();
                    let buffer = pop!();
                    let show = interp
                        .lookup_var("temp-buffer-show-function", env)
                        .unwrap_or(Value::Nil);
                    interp.with_lisp_stack_roots(&value, |interp| {
                        if show.is_truthy() {
                            interp.call_function_value(
                                show,
                                None,
                                std::slice::from_ref(&buffer),
                                env,
                            )?;
                        } else {
                            prim(interp, "display-buffer", &[buffer], env)?;
                        }
                        Ok::<_, LispError>(())
                    })?;
                    push!(value);
                    match interp.bc_unwinds.pop() {
                        Some(entry) => unwind_one(interp, entry, env)?,
                        None => {
                            return Err(LispError::Signal("byte code unbind underflow".into()));
                        }
                    }
                }
                Op::InteractiveP => {
                    // Obsolete since 24.1: GNU call0s the Lisp function.
                    let value = interp.call_function_value(
                        Value::symbol("interactive-p"),
                        Some("interactive-p"),
                        &[],
                        env,
                    )?;
                    push!(value);
                }
                // Zero-argument push ops.
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
                | Op::Widen => {
                    let name = match op {
                        Op::Point => "point",
                        Op::PointMax => "point-max",
                        Op::PointMin => "point-min",
                        Op::FollowingChar => "following-char",
                        Op::PrecedingChar => "preceding-char",
                        Op::CurrentColumn => "current-column",
                        Op::Eolp => "eolp",
                        Op::Eobp => "eobp",
                        Op::Bolp => "bolp",
                        Op::Bobp => "bobp",
                        Op::CurrentBuffer => "current-buffer",
                        _ => "widen",
                    };
                    let value = prim(interp, name, &[], env)?;
                    push!(value);
                }
                // One-argument buffer/navigation ops (TOP = F(TOP)).
                Op::GotoChar
                | Op::Insert
                | Op::CharAfter
                | Op::SetBuffer
                | Op::ForwardChar
                | Op::ForwardWord
                | Op::ForwardLine
                | Op::CharSyntax
                | Op::EndOfLine
                | Op::MatchBeginning
                | Op::MatchEnd => {
                    let a = pop!();
                    let name = match op {
                        Op::GotoChar => "goto-char",
                        Op::Insert => "insert",
                        Op::CharAfter => "char-after",
                        Op::SetBuffer => "set-buffer",
                        Op::ForwardChar => "forward-char",
                        Op::ForwardWord => "forward-word",
                        Op::ForwardLine => "forward-line",
                        Op::CharSyntax => "char-syntax",
                        Op::EndOfLine => "end-of-line",
                        Op::MatchBeginning => "match-beginning",
                        _ => "match-end",
                    };
                    let value = prim(interp, name, &[a], env)?;
                    push!(value);
                }
                Op::IndentTo => {
                    // GNU passes an explicit nil MINIMUM.
                    let column = pop!();
                    let value = prim(interp, "indent-to", &[column, Value::Nil], env)?;
                    push!(value);
                }
                // Two-argument region/motion ops.
                Op::SkipCharsForward
                | Op::SkipCharsBackward
                | Op::BufferSubstring
                | Op::DeleteRegion
                | Op::NarrowToRegion => {
                    let b = pop!();
                    let a = pop!();
                    let name = match op {
                        Op::SkipCharsForward => "skip-chars-forward",
                        Op::SkipCharsBackward => "skip-chars-backward",
                        Op::BufferSubstring => "buffer-substring",
                        Op::DeleteRegion => "delete-region",
                        _ => "narrow-to-region",
                    };
                    let value = prim(interp, name, &[a, b], env)?;
                    push!(value);
                }
                Op::SetMarker => {
                    let position_buffer = pop!();
                    let position = pop!();
                    let marker = pop!();
                    let value = prim(
                        interp,
                        "set-marker",
                        &[marker, position, position_buffer],
                        env,
                    )?;
                    push!(value);
                }
                Op::InsertN(n) => {
                    let start = interp.bc_stack.len() - n as usize;
                    let items: Vec<Value> = interp.bc_stack.drain_from(start).collect();
                    let value = prim(interp, "insert", &items, env)?;
                    push!(value);
                }
                Op::Call(argc) => {
                    let argc = argc as usize;
                    let args_start = interp.bc_stack.len() - argc;
                    // Bcall: the function is TOP below the arguments; both
                    // stay on the stack until the call returns (GNU keeps
                    // them rooted the same way).
                    let func = interp.bc_stack[args_start - 1];
                    interp.begin_funcall(env)?;
                    if let Some(termination) = interp.pending_termination().cloned() {
                        interp.end_funcall();
                        return Err(LispError::Terminate(termination));
                    }
                    let backtrace_depth = interp.backtrace_frames_len();
                    // Bcall records the unresolved function and rooted arguments
                    // before maybe_gc, which may redefine the function cell.
                    // SAFETY: the stack buffer never moves and these slots stay
                    // below the callee's disjoint storage until return/unwind.
                    let call_args = unsafe { interp.bc_stack.slice_from(args_start, argc) };
                    interp.push_backtrace_frame_borrowed(func, call_args);
                    interp.capture_current_backtrace_context(
                        match func.kind() {
                            Kind::Symbol(_) => func.as_symbol().ok(),
                            _ => None,
                        },
                        env,
                        None,
                    );
                    crate::lisp::native_comp::maybe_gc(interp, env);
                    // Only now inspect the live cell/code slots, just as GNU's
                    // packed-closure setup_frame branch does after maybe_gc.
                    if let Some((callee, callee_function)) = interp.bytecode_callee(&func) {
                        if let Err(error) =
                            interp
                                .bc_stack
                                .enter_frame(callee.stack_depth, callee_function, pc)
                        {
                            let result = interp.settle_frame_result(Err(error), env);
                            interp.truncate_backtrace_frames(backtrace_depth);
                            interp.end_funcall();
                            match result {
                                Ok(value) => {
                                    interp.bc_stack.truncate(args_start - 1);
                                    push!(value);
                                    continue;
                                }
                                Err(error) => return Err(error),
                            }
                        }
                        frames.push(BcFrame {
                            program: program.clone(),
                            base: args_start - 1,
                            handlers_len: handlers.len(),
                            unwinds_len: interp.bc_unwinds.len(),
                            backtrace_depth,
                            op_error_frames,
                        });
                        program = callee;
                        pc = 0;
                        op_error_frames = 0;
                        // setup_frame: the callee's frame starts above the
                        // caller's top; the arguments are copied into it
                        // (`PUSH (*args)'), the optionals padded, the rest
                        // gathered.  The caller's own slots -- the function
                        // and the arguments the backtrace frame borrows --
                        // stay as they are until Breturn cuts back to them.
                        let ArgSpec::Packed {
                            mandatory,
                            nonrest,
                            rest,
                        } = &program.argspec
                        else {
                            unreachable!("bytecode_callee selects packed argument lists")
                        };
                        let mandatory = *mandatory as usize;
                        let nonrest = *nonrest as usize;
                        if argc < mandatory || (!rest && argc > nonrest) {
                            return Err(packed_arity_error(mandatory, nonrest, argc));
                        }
                        let pushed = argc.min(nonrest);
                        for argument in &call_args[..pushed] {
                            push!(*argument);
                        }
                        if argc > nonrest {
                            let rest_list = Value::list(call_args[nonrest..].iter().cloned());
                            push!(rest_list);
                        } else {
                            for _ in argc..nonrest {
                                push!(Value::Nil);
                            }
                            if *rest {
                                push!(Value::Nil);
                            }
                        }
                        continue;
                    }
                    let trace_call = trace_errors.then(|| func.to_string());
                    let result = interp.funcall_body(func, call_args, env);
                    let result = interp.settle_frame_result(result, env);
                    interp.truncate_backtrace_frames(backtrace_depth);
                    interp.end_funcall();
                    let value = match result {
                        Ok(value) => value,
                        Err(error) => {
                            // Expected conditions such as `scan-error' are
                            // routinely caught by compiled Lisp.  Logging
                            // every one makes a real VM dispatch failure
                            // disappear in megabytes of noise.
                            if matches!(error.kind(), LispErrorKind::WrongNumberOfArgs(_, _))
                                && let Some(func) = trace_call
                            {
                                eprintln!(
                                    "bytecode call {func} failed at byte offset {offset}: {error:?}"
                                );
                            }
                            return Err(error);
                        }
                    };
                    interp.bc_stack.truncate(args_start - 1);
                    push!(value);
                }
                Op::Goto { target } => {
                    branch!(target);
                }
                Op::GotoIfNil { target } => {
                    if pop!().is_nil() {
                        branch!(target);
                    }
                }
                Op::GotoIfNonNil { target } => {
                    if !pop!().is_nil() {
                        branch!(target);
                    }
                }
                Op::GotoIfNilElsePop { target } => {
                    if interp.bc_stack.last().expect("validated bytecode").is_nil() {
                        branch!(target);
                    } else {
                        pop!();
                    }
                }
                Op::GotoIfNonNilElsePop { target } => {
                    if !interp.bc_stack.last().expect("validated bytecode").is_nil() {
                        branch!(target);
                    } else {
                        pop!();
                    }
                }
                Op::Return => {
                    breturn!(pop!());
                }
                Op::PushCatch { target } => {
                    let tag = pop!();
                    // `throw' consults the interpreter's active-catch
                    // registry before unwinding; a VM catch frame must be
                    // visible there like any `sf_catch' frame.
                    interp.push_active_catch_tag(tag);
                    handlers.push(Handler {
                        kind: HandlerKind::Catch(tag),
                        dest: target as usize,
                        stack_len: interp.bc_stack.len(),
                        unwind_len: interp.bc_unwinds.len(),
                        registry_start: None,
                    });
                }
                Op::PushConditionCase { target } => {
                    let conditions = pop!();
                    // Register the clause head with the interpreter so
                    // signal-time `handler-bind' dispatch at native-call
                    // boundaries sees this frame before any outer handler,
                    // exactly like an interpreted `condition-case'.
                    let registry_start = interp.push_condition_case_handler(vec![conditions]);
                    handlers.push(Handler {
                        kind: HandlerKind::ConditionCase(conditions),
                        dest: target as usize,
                        stack_len: interp.bc_stack.len(),
                        unwind_len: interp.bc_unwinds.len(),
                        registry_start: Some(registry_start),
                    });
                }
                Op::PopHandler => {
                    if let Some(handler) = handlers.pop() {
                        if matches!(handler.kind, HandlerKind::Catch(_)) {
                            interp.pop_active_catch_tag();
                        }
                        if let Some(start) = handler.registry_start {
                            interp.pop_handler_bindings(start);
                        }
                    }
                }
                Op::Switch => {
                    let table = pop!();
                    let value = pop!();
                    let dest = prim(interp, "gethash", &[value, table, Value::Nil], env)?;
                    if let Kind::Integer(dest) = dest.kind() {
                        pc = dest as usize;
                    }
                }
                Op::ListN(n) => {
                    let start = interp.bc_stack.len() - n as usize;
                    let items: Vec<Value> = interp.bc_stack.drain_from(start).collect();
                    push!(Value::list(items));
                }
                Op::ConcatN(n) => {
                    let start = interp.bc_stack.len() - n as usize;
                    let items: Vec<Value> = interp.bc_stack.drain_from(start).collect();
                    let value = prim(interp, "concat", &items, env)?;
                    push!(value);
                }
                Op::List1 | Op::List2 | Op::List3 | Op::List4 => {
                    let n = match op {
                        Op::List1 => 1,
                        Op::List2 => 2,
                        Op::List3 => 3,
                        _ => 4,
                    };
                    let start = interp.bc_stack.len() - n;
                    let items: Vec<Value> = interp.bc_stack.drain_from(start).collect();
                    push!(Value::list(items));
                }
                Op::Not => {
                    let value = pop!();
                    push!(if value.is_nil() { Value::T } else { Value::Nil });
                }
                // Two-argument ops with inline fast paths.
                Op::Eq => {
                    let b = pop!();
                    let a = pop!();
                    let equal = crate::lisp::primitives::values_eq_in_env(interp, &a, &b, env);
                    push!(if equal { Value::T } else { Value::Nil });
                }
                Op::Cons => {
                    let b = pop!();
                    let a = pop!();
                    push!(Value::cons(a, b));
                }
                Op::Eqlsign | Op::Gtr | Op::Lss | Op::Leq | Op::Geq => {
                    let b = pop!();
                    let a = pop!();
                    if let (Kind::Integer(x), Kind::Integer(y)) = (a.kind(), b.kind()) {
                        let holds = match op {
                            Op::Eqlsign => x == y,
                            Op::Gtr => x > y,
                            Op::Lss => x < y,
                            Op::Leq => x <= y,
                            _ => x >= y,
                        };
                        push!(if holds { Value::T } else { Value::Nil });
                        continue;
                    }
                    let name = match op {
                        Op::Eqlsign => "=",
                        Op::Gtr => ">",
                        Op::Lss => "<",
                        Op::Leq => "<=",
                        _ => ">=",
                    };
                    let value = prim(interp, name, &[a, b], env)?;
                    push!(value);
                }
                Op::Plus | Op::Diff | Op::Mult => {
                    let b = pop!();
                    let a = pop!();
                    if let (Kind::Integer(x), Kind::Integer(y)) = (a.kind(), b.kind()) {
                        let fast = match op {
                            Op::Plus => x.checked_add(y),
                            Op::Diff => x.checked_sub(y),
                            _ => x.checked_mul(y),
                        };
                        if let Some(n) = fast {
                            push!(Value::Integer(n));
                            continue;
                        }
                    }
                    let name = match op {
                        Op::Plus => "+",
                        Op::Diff => "-",
                        _ => "*",
                    };
                    let value = prim(interp, name, &[a, b], env)?;
                    push!(value);
                }
                Op::Max | Op::Min => {
                    let b = pop!();
                    let a = pop!();
                    if let (Kind::Integer(x), Kind::Integer(y)) = (a.kind(), b.kind()) {
                        let n = if matches!(op, Op::Max) {
                            (x).max(y)
                        } else {
                            (x).min(y)
                        };
                        push!(Value::Integer(n));
                        continue;
                    }
                    let name = if matches!(op, Op::Max) { "max" } else { "min" };
                    let value = prim(interp, name, &[a, b], env)?;
                    push!(value);
                }
                // Two-argument primitive ops.
                Op::Memq
                | Op::Nth
                | Op::Aref
                | Op::Setcar
                | Op::Setcdr
                | Op::Nthcdr
                | Op::Elt
                | Op::Member
                | Op::Assq
                | Op::Equal
                | Op::Get
                | Op::Quo
                | Op::Rem
                | Op::StringEqlsign
                | Op::StringLss
                | Op::Concat2
                | Op::Set
                | Op::Fset
                | Op::Nconc => {
                    let b = pop!();
                    let a = pop!();
                    let name = match op {
                        Op::Memq => "memq",
                        Op::Nth => "nth",
                        Op::Aref => "aref",
                        Op::Setcar => "setcar",
                        Op::Setcdr => "setcdr",
                        Op::Nthcdr => "nthcdr",
                        Op::Elt => "elt",
                        Op::Member => "member",
                        Op::Assq => "assq",
                        Op::Equal => "equal",
                        Op::Get => "get",
                        Op::Quo => "/",
                        Op::Rem => "%",
                        Op::StringEqlsign => "string-equal",
                        Op::StringLss => "string-lessp",
                        Op::Concat2 => "concat",
                        Op::Set => "set",
                        Op::Fset => "fset",
                        _ => "nconc",
                    };
                    let call_args = [a, b];
                    let value = match prim(interp, name, &call_args, env) {
                        Ok(value) => value,
                        Err(error) => {
                            // bytecode.c's record_in_backtrace covers
                            // exactly nth/elt/aref/setcar/setcdr here.
                            if matches!(op, Op::Nth | Op::Elt | Op::Aref | Op::Setcar | Op::Setcdr)
                                && !matches!(
                                    error.kind(),
                                    LispErrorKind::Throw(_, _) | LispErrorKind::Terminate(_)
                                )
                            {
                                interp.push_backtrace_frame_with_evald(
                                    Value::Symbol(name.into()),
                                    call_args.to_vec(),
                                    true,
                                );
                                op_error_frames += 1;
                            }
                            return Err(error);
                        }
                    };
                    push!(value);
                }
                // One-argument ops with inline fast paths.
                Op::Car | Op::Cdr | Op::CarSafe | Op::CdrSafe => {
                    let a = pop!();
                    match (a.kind(), op) {
                        (Kind::Cons(cell), Op::Car | Op::CarSafe) => {
                            let value = cell.car.get();
                            push!(value);
                        }
                        (Kind::Cons(cell), _) => {
                            let value = cell.cdr.get();
                            push!(value);
                        }
                        (Kind::Nil, _) | (_, Op::CarSafe | Op::CdrSafe) => {
                            push!(Value::Nil)
                        }
                        _ => {
                            let name = if matches!(op, Op::Car) { "car" } else { "cdr" };
                            let call_args = [a];
                            let value = match prim(interp, name, &call_args, env) {
                                Ok(value) => value,
                                Err(error) => {
                                    // bytecode.c records the signaling op
                                    // itself as a frame so handlers see
                                    // e.g. `(car a)' innermost.
                                    if !matches!(
                                        error.kind(),
                                        LispErrorKind::Throw(_, _) | LispErrorKind::Terminate(_)
                                    ) {
                                        interp.push_backtrace_frame_with_evald(
                                            Value::Symbol(name.into()),
                                            call_args.to_vec(),
                                            true,
                                        );
                                        op_error_frames += 1;
                                    }
                                    return Err(error);
                                }
                            };
                            push!(value);
                        }
                    }
                }
                Op::Add1 | Op::Sub1 | Op::Negate => {
                    let a = pop!();
                    if let Kind::Integer(x) = a.kind() {
                        let fast = match op {
                            Op::Add1 => x.checked_add(1),
                            Op::Sub1 => x.checked_sub(1),
                            _ => x.checked_neg(),
                        };
                        if let Some(n) = fast {
                            push!(Value::Integer(n));
                            continue;
                        }
                    }
                    let name = match op {
                        Op::Add1 => "1+",
                        Op::Sub1 => "1-",
                        _ => "-",
                    };
                    let value = prim(interp, name, &[a], env)?;
                    push!(value);
                }
                Op::Consp => {
                    let a = pop!();
                    push!(if primitives::is_cons_value(interp, &a) {
                        Value::T
                    } else {
                        Value::Nil
                    });
                }
                Op::Length => {
                    let sequence = pop!();
                    push!(Value::Integer(primitives::sequence_length_value(
                        interp, &sequence,
                    )?));
                }
                // One-argument primitive ops.
                Op::Symbolp
                | Op::Stringp
                | Op::Listp
                | Op::SymbolValue
                | Op::SymbolFunction
                | Op::Nreverse
                | Op::Numberp
                | Op::Integerp
                | Op::Upcase
                | Op::Downcase => {
                    let a = pop!();
                    let name = match op {
                        Op::Symbolp => "symbolp",
                        Op::Stringp => "stringp",
                        Op::Listp => "listp",
                        Op::SymbolValue => "symbol-value",
                        Op::SymbolFunction => "symbol-function",
                        Op::Nreverse => "nreverse",
                        Op::Numberp => "numberp",
                        Op::Integerp => "integerp",
                        Op::Upcase => "upcase",
                        _ => "downcase",
                    };
                    let value = prim(interp, name, &[a], env)?;
                    push!(value);
                }
                // Three-argument primitive ops.
                Op::Aset | Op::Substring | Op::Concat3 => {
                    let c = pop!();
                    let b = pop!();
                    let a = pop!();
                    let name = match op {
                        Op::Aset => "aset",
                        Op::Substring => "substring",
                        _ => "concat",
                    };
                    let call_args = [a, b, c];
                    let value = match prim(interp, name, &call_args, env) {
                        Ok(value) => value,
                        Err(error) => {
                            if matches!(op, Op::Aset)
                                && !matches!(
                                    error.kind(),
                                    LispErrorKind::Throw(_, _) | LispErrorKind::Terminate(_)
                                )
                            {
                                interp.push_backtrace_frame_with_evald(
                                    Value::Symbol(name.into()),
                                    call_args.to_vec(),
                                    true,
                                );
                                op_error_frames += 1;
                            }
                            return Err(error);
                        }
                    };
                    push!(value);
                }
                Op::Concat4 => {
                    let d = pop!();
                    let c = pop!();
                    let b = pop!();
                    let a = pop!();
                    let value = prim(interp, "concat", &[a, b, c, d], env)?;
                    push!(value);
                }
            }
        })();

        match step {
            Ok(value) => {
                break 'run Ok(value);
            }
            Err(mut error) => {
                // GNU's handler search: innermost first; Bpushcatch
                // handles `throw' by tag eq, Bpushconditioncase handles
                // signals whose condition list matches the clause head.
                // A frame without a matching handler is popped (its
                // unwinds run, its backtrace frame goes, the error settles
                // at the boundary as a Rust-level return would) and the
                // search continues in its caller's handlers.
                let mut handled = false;
                'frames: loop {
                    let floor = frames.last().map_or(0, |frame| frame.handlers_len);
                    while handlers.len() > floor {
                        let Some(handler) = handlers.pop() else {
                            break;
                        };
                        if matches!(handler.kind, HandlerKind::Catch(_)) {
                            interp.pop_active_catch_tag();
                        }
                        if let Some(start) = handler.registry_start {
                            interp.pop_handler_bindings(start);
                        }
                        let matched_value = match (&handler.kind, error.kind()) {
                            (HandlerKind::Catch(tag), LispErrorKind::Throw(thrown, value)) => {
                                let same = prim(interp, "eq", &[*tag, *thrown], env)?;
                                if same.is_truthy() { Some(*value) } else { None }
                            }
                            (HandlerKind::ConditionCase(_), LispErrorKind::Throw(_, _))
                            | (HandlerKind::ConditionCase(_), LispErrorKind::Terminate(_))
                            | (HandlerKind::Catch(_), _) => None,
                            (HandlerKind::ConditionCase(clause), kind) => {
                                let condition = kind.condition_type();
                                let condition_list = interp.error_condition_names(&condition);
                                if Interpreter::clause_head_matches(
                                    clause,
                                    &condition,
                                    &condition_list,
                                ) {
                                    Some(super::super::eval::error_condition_value(&error))
                                } else {
                                    None
                                }
                            }
                        };
                        if let Some(value) = matched_value {
                            if matches!(handler.kind, HandlerKind::ConditionCase(_)) {
                                interp.clear_batch_error_backtrace();
                            }
                            // A caught signal unwinds the frame its byte op
                            // recorded (GNU unbinds specpdl to the handler).
                            while op_error_frames > 0 {
                                interp.pop_backtrace_frame();
                                op_error_frames -= 1;
                            }
                            while interp.bc_unwinds.len() > handler.unwind_len {
                                match interp.bc_unwinds.pop() {
                                    Some(entry) => interp
                                        .with_lisp_stack_roots(&(&error, &value), |interp| {
                                            unwind_one(interp, entry, env)
                                        })?,
                                    None => break,
                                }
                            }
                            interp.bc_stack.truncate(handler.stack_len);
                            push!(value);
                            pc = handler.dest;
                            handled = true;
                            break 'frames;
                        }
                    }
                    let Some(frame) = frames.pop() else {
                        break 'frames;
                    };
                    // The error leaves this frame: the frames its
                    // signaling ops pushed, then the settling every call
                    // boundary does (handler-bind, the batch backtrace),
                    // then the frame itself.
                    while op_error_frames > 0 {
                        interp.pop_backtrace_frame();
                        op_error_frames -= 1;
                    }
                    error = match interp.settle_frame_result(Err(error), env) {
                        Ok(value) => {
                            if let Err(error) = leave_frame(interp, env, &frame) {
                                break 'run Err(error);
                            }
                            pc = interp.bc_stack.leave_frame();
                            interp.bc_stack.truncate(frame.base);
                            push!(value);
                            program = frame.program;
                            op_error_frames = frame.op_error_frames;
                            handlers.truncate(frame.handlers_len);
                            continue 'run;
                        }
                        Err(error) => error,
                    };
                    if let Err(unwind_error) = interp
                        .with_lisp_stack_roots(&error, |interp| leave_frame(interp, env, &frame))
                    {
                        error = unwind_error;
                    }
                    pc = interp.bc_stack.leave_frame();
                    interp.bc_stack.truncate(frame.base);
                    program = frame.program;
                    op_error_frames = frame.op_error_frames;
                }
                if !handled {
                    // Trace only genuine errors, and render them bounded: a
                    // `throw' unwinding to an outer catch is control flow
                    // (seq-some's `seq--break' fires constantly), and the
                    // derived Debug rendering recursed without limit -- a
                    // cyclic widget graph in the payload overflowed the
                    // stack after gigabytes of trace (custom-tests.el under
                    // the harness), killing the child with no report.
                    // An error a live condition-case/handler-bind frame is
                    // about to absorb is invisible in GNU, so the trace
                    // speaks only for errors headed to the toplevel report
                    // (cl-generic's bootstrap raises and handles one on
                    // every boot; a child process spawned by a test must
                    // not leak that into its measured stderr).
                    if trace_errors
                        && !matches!(error.kind(), LispErrorKind::Throw(_, _))
                        && !interp.some_active_handler_matches(&error)
                        && let Some(instr) = last_instruction
                    {
                        eprintln!(
                            "bytecode operation {:?} failed at byte offset {}: {}",
                            instr.op,
                            instr.offset,
                            crate::lisp::types::bounded_error_debug(&error)
                        );
                    }
                    break 'run Err(error);
                }
            }
        }
    };

    // Balance any entries left by an abnormal exit (GNU unbind_to on
    // the frame's specpdl watermark), running unwind-protect handlers
    // and restoring saved buffer state on the way out; catch tags of
    // still-pushed handlers leave the registry with their frame.
    for handler in handlers.into_iter().rev() {
        if matches!(handler.kind, HandlerKind::Catch(_)) {
            interp.pop_active_catch_tag();
        }
        if let Some(start) = handler.registry_start {
            interp.pop_handler_bindings(start);
        }
    }
    // GNU runs `handler-bind' handlers from `signal' itself, so an error
    // raised inside byte-code reaches them exactly as one raised by the
    // interpreter does.  Emaxx dispatches at each native-call boundary
    // (eval/core.rs); without the same dispatch here, an error escaping the VM
    // skipped every enclosing handler-bind — which meant ert could not turn a
    // failing compiled test body into a result, and only ever showed up once
    // the subject started executing GNU's compiled Lisp.  A condition-case
    // inside this frame has already had its chance above, so anything still
    // propagating belongs to an outer handler.
    // Keep the error in its box. Unpacking LispErrorKind reserves its large
    // payload in this long-lived VM frame even on successful bytecode paths;
    // inactive stack words can then retain Lisp objects under conservative GC.
    // GNU dispatches the error without introducing this extra payload copy.
    match result {
        Err(error)
            if matches!(
                error.kind(),
                LispErrorKind::Throw(_, _) | LispErrorKind::Terminate(_)
            ) =>
        {
            Err(error)
        }
        Err(error) => interp.dispatch_handler_bindings(error, env),
        Ok(value) => Ok(value),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::super::tests::fixture_objects;
    use super::*;

    fn run(name: &str, args: &[Value]) -> Result<Value, LispError> {
        let mut interp = Interpreter::new();
        let mut env = Env::new();
        let object = fixture_objects()
            .remove(name)
            .unwrap_or_else(|| panic!("fixture {name} missing"));
        // The decoder fixture helper returns reader syntax. Complete the
        // existing C-owned reader step before handing live objects to the VM.
        interp.materialize_read_object_literals(Value::Vector(object.constants), &mut env)?;
        execute(&mut interp, &object, args, &mut env)
    }

    #[test]
    fn executes_arithmetic_fixture() {
        // (defun emaxx-fx-add (a b) (+ a b 1))
        let value = run("emaxx-fx-add", &[Value::Integer(2), Value::Integer(3)]).unwrap();
        assert_eq!(value, Value::Integer(6));
    }

    #[test]
    fn materializes_reader_labels_shared_across_nested_closures() {
        let form = super::super::super::reader::Reader::new(
            r##"#[0 "\301\207"
                   [nil
                    #[0 "\300\207" [nil] 1 #1=""]
                    #[0 "\300\207" [nil] 1 #1#]]
                   1]"##,
        )
        .read()
        .expect("read byte-code object with shared reader label")
        .expect("byte-code object form");
        let mut interp = Interpreter::new();
        let mut env = Env::new();
        let closure = interp
            .eval(&form, &mut env)
            .expect("materialize outer byte-code closure");

        assert!(matches!(
            interp
                .call_function_value(closure, None, &[], &mut env)
                .map(|v| v.kind()),
            Ok(Kind::Closure(_))
        ));
    }

    #[test]
    fn materializes_bytecode_records_nested_in_list_constants() {
        let constant =
            super::super::super::reader::Reader::new(r##"(wrapper #[0 "\300\207" [nil] 1])"##)
                .read()
                .expect("read list-valued bytecode constant")
                .expect("constant form");
        let mut interp = Interpreter::new();
        let mut env = Env::new();
        let materialized = interp
            .materialize_read_object_literals(constant, &mut env)
            .expect("materialize nested byte-code function");
        let items = materialized.to_vec().expect("proper list constant");

        assert!(matches!(
            items.get(1).map(|v| v.kind()),
            Some(Kind::Closure(_))
        ));
    }

    #[test]
    fn executes_branching_fixture() {
        // (defun emaxx-fx-branch (x) (if (consp x) (car x) (list x 'tagged)))
        let value = run(
            "emaxx-fx-branch",
            &[Value::list([Value::Integer(7), Value::Integer(8)])],
        )
        .unwrap();
        assert_eq!(value, Value::Integer(7));
        let value = run("emaxx-fx-branch", &[Value::Integer(5)]).unwrap();
        assert_eq!(
            format!("{value}"),
            format!(
                "{}",
                Value::list([Value::Integer(5), Value::symbol("tagged")])
            )
        );

        let vector = Value::list([Value::symbol("vector-literal"), Value::Integer(9)]);
        let value = run("emaxx-fx-branch", std::slice::from_ref(&vector)).unwrap();
        assert_eq!(value, Value::list([vector, Value::symbol("tagged")]));
    }

    #[test]
    fn executes_loop_fixture() {
        // (defun emaxx-fx-loop (n) (let ((acc nil)) (while (> n 0) (push n acc) (setq n (1- n))) acc))
        let value = run("emaxx-fx-loop", &[Value::Integer(3)]).unwrap();
        assert_eq!(
            format!("{value}"),
            format!(
                "{}",
                Value::list([Value::Integer(1), Value::Integer(2), Value::Integer(3)])
            )
        );
    }

    #[test]
    fn executes_rest_args_fixture() {
        // (defun emaxx-fx-rest (head &rest tail) (cons head (length tail)))
        let value = run(
            "emaxx-fx-rest",
            &[Value::symbol("a"), Value::symbol("b"), Value::symbol("c")],
        )
        .unwrap();
        assert_eq!(format!("{value}"), "(a . 2)");
        let value = run("emaxx-fx-rest", &[Value::symbol("solo")]).unwrap();
        assert_eq!(format!("{value}"), "(solo . 0)");
    }

    #[test]
    fn executes_condition_case_fixture() {
        // (defun emaxx-fx-catch (f) (condition-case err (funcall f) (error (cons 'caught err))))
        let mut interp = Interpreter::new();
        let mut env = Env::new();
        let object = fixture_objects().remove("emaxx-fx-catch").unwrap();
        interp
            .materialize_read_object_literals(Value::Vector(object.constants), &mut env)
            .expect("materialize unchanged fixture reader objects");
        // Non-signaling: (lambda () 42) via a builtin-friendly stand-in.
        let ok = execute(
            &mut interp,
            &object,
            &[Value::lambda(
                Vec::new(),
                vec![Value::Integer(42)],
                Value::Nil,
            )],
            &mut env,
        )
        .unwrap();
        assert_eq!(ok, Value::Integer(42));
        // Signaling: `car` of a non-list signals wrong-type-argument,
        // which the (error ...) clause catches.
        let caught = execute(
            &mut interp,
            &object,
            &[Value::lambda(
                Vec::new(),
                vec![Value::list([Value::symbol("car"), Value::Integer(9)])],
                Value::Nil,
            )],
            &mut env,
        )
        .unwrap();
        assert!(format!("{caught}").starts_with("(caught wrong-type-argument"));
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod phase_c_tests {
    use super::super::tests::{ORACLE_ELC2, named_objects};
    use super::*;

    fn run2(name: &str, args: &[Value]) -> Result<Value, LispError> {
        let mut interp = Interpreter::new();
        let mut env = Env::new();
        let object = named_objects(ORACLE_ELC2)
            .remove(name)
            .unwrap_or_else(|| panic!("fixture {name} missing"));
        interp.materialize_read_object_literals(Value::Vector(object.constants), &mut env)?;
        execute(&mut interp, &object, args, &mut env)
    }

    fn run2_with_gnu_early_lisp(name: &str, args: &[Value]) -> Result<Value, LispError> {
        let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
        let mut env = Env::new();
        let object = named_objects(ORACLE_ELC2)
            .remove(name)
            .unwrap_or_else(|| panic!("fixture {name} missing"));
        interp.materialize_read_object_literals(Value::Vector(object.constants), &mut env)?;
        execute(&mut interp, &object, args, &mut env)
    }

    #[test]
    fn executes_buffer_ops_fixture() {
        // (with-temp-buffer (insert "hello world") (goto-char (point-min))
        //   (forward-word 1) (list (point) (buffer-substring ...) (point-max)))
        let value = run2_with_gnu_early_lisp("emaxx-fx2-buffer", &[]).unwrap();
        assert_eq!(format!("{value}"), "(6 \"hello\" 12)");
    }

    #[test]
    fn executes_save_excursion_fixture() {
        // Point restored to 3 after save-excursion moves to point-max.
        let value = run2_with_gnu_early_lisp("emaxx-fx2-excursion", &[]).unwrap();
        assert_eq!(value, Value::Integer(3));
    }

    #[test]
    fn executes_unwind_protect_fixture() {
        // Normal exit: handler still runs (setcdr log 'cleaned).
        let mut interp = Interpreter::new();
        let mut env = Env::new();
        let object = named_objects(ORACLE_ELC2)
            .remove("emaxx-fx2-unwind")
            .unwrap();
        interp
            .materialize_read_object_literals(Value::Vector(object.constants), &mut env)
            .expect("materialize unchanged fixture reader objects");
        let log = Value::list([Value::symbol("payload"), Value::symbol("untouched")]);
        let value = execute(&mut interp, &object, std::slice::from_ref(&log), &mut env).unwrap();
        assert_eq!(format!("{value}"), "payload");
        assert_eq!(format!("{log}"), "(payload . cleaned)");
    }

    #[test]
    fn executes_unwind_protect_through_throw() {
        // The handler must run while the throw unwinds to the catch.
        let value = run2("emaxx-fx2-unwind-throw", &[Value::symbol("tag")]).unwrap();
        assert_eq!(format!("{value}"), "(cleaned)");
    }

    #[test]
    fn executes_dynamic_binding_fixture() {
        // (let ((emaxx-fx2-dyn value)) (symbol-value 'emaxx-fx2-dyn))
        let mut interp = Interpreter::new();
        let mut env = Env::new();
        // The defvar lives in the fixture file, not the test interp:
        // declare it as GNU load would.
        interp
            .eval(
                &Value::list([
                    Value::symbol("defvar"),
                    Value::symbol("emaxx-fx2-dyn"),
                    Value::list([Value::symbol("quote"), Value::symbol("unset")]),
                ]),
                &mut env,
            )
            .unwrap();
        let object = named_objects(ORACLE_ELC2)
            .remove("emaxx-fx2-dynbind")
            .unwrap();
        interp
            .materialize_read_object_literals(Value::Vector(object.constants), &mut env)
            .expect("materialize unchanged fixture reader objects");
        let value = execute(&mut interp, &object, &[Value::symbol("bound")], &mut env).unwrap();
        assert_eq!(format!("{value}"), "bound");
        // The binding must be undone after the call.
        let after = interp
            .eval(&Value::symbol("emaxx-fx2-dyn"), &mut env)
            .unwrap();
        assert_eq!(format!("{after}"), "unset");
    }

    #[test]
    fn executes_legacy_dynamic_argspec_fixture() {
        // (defun emaxx-fx3-legacy (a &optional b &rest c)
        //   (list a b (length c) (symbol-value 'a))) — dynamic binding:
        // formals are specbound, so symbol-value sees `a'.
        let mut interp = Interpreter::new();
        let mut env = Env::new();
        let object = super::super::tests::named_objects(super::super::tests::ORACLE_ELC3)
            .remove("emaxx-fx3-legacy")
            .unwrap();
        interp
            .materialize_read_object_literals(Value::Vector(object.constants), &mut env)
            .expect("materialize unchanged fixture reader objects");
        assert!(matches!(object.argspec, ArgSpec::Legacy(_)));
        let value = execute(
            &mut interp,
            &object,
            &[
                Value::Integer(1),
                Value::Integer(2),
                Value::Integer(3),
                Value::Integer(4),
            ],
            &mut env,
        )
        .unwrap();
        assert_eq!(format!("{value}"), "(1 2 2 1)");
        // Optional/rest defaults, and bindings must not leak out.
        let value = execute(&mut interp, &object, &[Value::Integer(9)], &mut env).unwrap();
        assert_eq!(format!("{value}"), "(9 nil 0 9)");
        assert!(interp.eval(&Value::symbol("a"), &mut env).is_err());
        // Too few arguments signals like GNU.
        assert!(execute(&mut interp, &object, &[], &mut env).is_err());
    }

    #[test]
    fn executes_switch_fixture() {
        for (key, expected) in [
            ("alpha", "1"),
            ("gamma", "3"),
            ("zeta", "6"),
            ("nomatch", "default"),
        ] {
            let value = run2("emaxx-fx2-switch", &[Value::symbol(key)]).unwrap();
            assert_eq!(format!("{value}"), expected, "case {key}");
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod native_surface_tests {
    use super::*;

    #[test]
    fn bytecode_reads_original_constants_after_vector_mutation() {
        let mut interp = Interpreter::new();
        let mut env = Env::new();
        let constants = Value::vector([Value::Integer(11)]);
        let code = crate::lisp::primitives::make_shared_string_value_with_multibyte(
            "\u{c0}\u{87}".to_owned(),
            Vec::new(),
            false,
        );
        let function = prim(
            &mut interp,
            "make-byte-code",
            &[Value::Integer(0), code, constants, Value::Integer(1)],
            &mut env,
        )
        .expect("alloc.c:Fmake_byte_code retains the supplied constants vector");
        let stored = prim(
            &mut interp,
            "aref",
            &[function, Value::Integer(2)],
            &mut env,
        )
        .expect("CLOSURE_CONSTANTS");
        assert_eq!(
            prim(&mut interp, "eq", &[constants, stored], &mut env)
                .expect("original vector identity"),
            Value::T,
        );
        assert_eq!(
            interp
                .call_function_value(function, None, &[], &mut env)
                .expect("first execution"),
            Value::Integer(11),
        );
        prim(
            &mut interp,
            "aset",
            &[constants, Value::Integer(0), Value::Integer(29)],
            &mut env,
        )
        .expect("data.c:Faset changes the original vector slot");
        assert_eq!(
            interp
                .call_function_value(function, None, &[], &mut env)
                .expect("execution after constants-vector mutation"),
            Value::Integer(29),
            "bytecode.c:Bconstant reads the original vector, not a snapshot",
        );
    }

    #[test]
    fn bytecode_reads_original_constants_during_vector_mutation() {
        let mut interp = Interpreter::new();
        let mut env = Env::new();
        let constants = Value::vector([Value::Integer(11), Value::Integer(77), Value::Integer(0)]);
        // Argument: the constants vector itself. Push index 0 and value 77,
        // Baset, discard its result, then read constant 0 and return it.
        // GNU's saved vectorp sees a mutation made during the same frame.
        let code = crate::lisp::primitives::make_shared_string_value_with_multibyte(
            "\u{c2}\u{c1}\u{49}\u{88}\u{c0}\u{87}".to_owned(),
            Vec::new(),
            false,
        );
        let function = prim(
            &mut interp,
            "make-byte-code",
            &[Value::Integer(257), code, constants, Value::Integer(3)],
            &mut env,
        )
        .expect("bytecode function with one stack argument");
        assert_eq!(
            interp
                .call_function_value(function, None, &[constants], &mut env)
                .expect("mutation during execution"),
            Value::Integer(77),
            "bytecode.c:Bconstant sees the preceding Baset store",
        );
    }

    #[test]
    fn bytecode_constant_storage_does_not_retain_removed_values() {
        let mut interp = Interpreter::new();
        let mut env = Env::new();
        let removed = crate::lisp::primitives::make_shared_string_value_with_multibyte(
            "original constant".to_owned(),
            Vec::new(),
            false,
        );
        let constants = Value::vector([removed]);
        let code = crate::lisp::primitives::make_shared_string_value_with_multibyte(
            "\u{c0}\u{87}".to_owned(),
            Vec::new(),
            false,
        );
        let slots = [Value::Integer(0), code, constants, Value::Integer(1)];
        let object = ByteCodeObject::from_slots(&slots)
            .expect("valid bytecode")
            .expect("bytecode slots");
        let program = object.clone();
        let Kind::Vector(vector) = constants.kind() else {
            panic!("constants vector")
        };
        assert!(vector.ptr_eq(&object.constants));
        assert!(vector.ptr_eq(&program.constants));
        prim(
            &mut interp,
            "aset",
            &[constants, Value::Integer(0), Value::Nil],
            &mut env,
        )
        .expect("replace the sole reference to the old constant");
        // The program's constants are the vector itself (asserted above),
        // read in place: no snapshot of the old constant exists to keep it
        // (the collector frees the string object once nothing names it).
        assert_eq!(program.constant(0), Value::Nil);
    }

    /// `make-byte-code` output must execute through `byte-code`-adjacent
    /// dispatch: build (lambda (x) (1+ x)) by hand and funcall it.
    #[test]
    fn make_byte_code_object_is_callable() {
        let mut interp = Interpreter::new();
        let mut env = Env::new();
        // argspec 257: one mandatory arg; code: dup; add1; return.
        // alloc.c:Fmake_byte_code requires an unibyte string and a real
        // constants vector, including when called through the Rust API.
        let code = crate::lisp::primitives::make_shared_string_value_with_multibyte(
            "\u{89}\u{54}\u{87}".to_owned(),
            Vec::new(),
            false,
        );
        let object = prim(
            &mut interp,
            "make-byte-code",
            &[
                Value::Integer(257),
                code,
                Value::vector([]),
                Value::Integer(3),
            ],
            &mut env,
        )
        .unwrap();
        let value = interp
            .call_function_value(object, None, &[Value::Integer(41)], &mut env)
            .unwrap();
        assert_eq!(value, Value::Integer(42));
    }

    #[test]
    fn byte_code_argument_prologue_preserves_string_identity() {
        let mut interp = Interpreter::new();
        let mut env = Env::new();
        let code = crate::lisp::primitives::make_shared_string_value_with_multibyte(
            "\u{87}".to_owned(),
            Vec::new(),
            false,
        );
        let object = prim(
            &mut interp,
            "make-byte-code",
            &[
                Value::Integer(257),
                code,
                Value::vector([]),
                Value::Integer(1),
            ],
            &mut env,
        )
        .expect("make a one-argument return function");
        // bytecode.c:exec_byte_code pushes *args, not a copy of its
        // pointed-to object. This includes shared immutable constants.
        let argument = Value::String("shared constant".into());
        for _ in 0..2 {
            let returned = interp
                .call_function_value(object, None, std::slice::from_ref(&argument), &mut env)
                .expect("return the original argument");
            assert_eq!(
                prim(&mut interp, "eq", &[argument, returned], &mut env,)
                    .expect("compare Lisp object identity"),
                Value::T,
                "the argument prologue must not allocate a different string",
            );
        }
    }

    #[test]
    fn byte_code_direct_string_arguments_are_mutable_lisp_objects() {
        let mut interp = Interpreter::new();
        let mut env = Env::new();
        // argspec 257: one mandatory arg; code: return that argument.
        let code = crate::lisp::primitives::make_shared_string_value_with_multibyte(
            "\u{87}".to_owned(),
            Vec::new(),
            false,
        );
        let object = prim(
            &mut interp,
            "make-byte-code",
            &[
                Value::Integer(257),
                code,
                Value::vector([]),
                Value::Integer(1),
            ],
            &mut env,
        )
        .unwrap();
        // Even compact strings retain their original identity across calls.
        // A per-call promotion passed the mutation-only assertion below but
        // silently duplicated shared compiler constants.
        let compact = Value::String("native diagnostic".into());
        for _ in 0..2 {
            let returned = interp
                .call_function_value(object, None, std::slice::from_ref(&compact), &mut env)
                .expect("bytecode returns the original argument");
            assert_eq!(
                prim(&mut interp, "eq", &[compact, returned], &mut env).expect("string identity"),
                Value::T,
            );
        }
        // print.c:Ferror_message_string's general path returns Fbuffer_string:
        // the diagnostic is already a mutable Lisp string before the VM sees it.
        let argument = prim(
            &mut interp,
            "error-message-string",
            &[Value::list([
                Value::symbol("wrong-type-argument"),
                Value::symbol("listp"),
                Value::T,
            ])],
            &mut env,
        )
        .expect("C-owned diagnostic producer");
        let value = interp
            .call_function_value(object, None, std::slice::from_ref(&argument), &mut env)
            .unwrap();
        assert_eq!(
            prim(&mut interp, "eq", &[argument, value], &mut env).expect("same diagnostic object"),
            Value::T,
        );
        assert_eq!(
            prim(
                &mut interp,
                "multibyte-string-p",
                std::slice::from_ref(&argument),
                &mut env
            )
            .expect("print buffer is multibyte"),
            Value::T,
        );

        prim(
            &mut interp,
            "put-text-property",
            &[
                Value::Integer(0),
                Value::Integer(1),
                Value::Symbol("field".into()),
                Value::Symbol("command-output".into()),
                value,
            ],
            &mut env,
        )
        .unwrap();
        assert_eq!(
            prim(
                &mut interp,
                "get-text-property",
                &[Value::Integer(0), Value::Symbol("field".into()), argument,],
                &mut env,
            )
            .unwrap(),
            Value::Symbol("command-output".into())
        );
        // Ferror_message_string's allocation-free (error STRING) branch
        // returns that exact string, including its properties.
        let plain_error = Value::list([Value::symbol("error"), argument]);
        let returned = prim(
            &mut interp,
            "error-message-string",
            &[plain_error],
            &mut env,
        )
        .expect("original error string");
        assert_eq!(
            prim(&mut interp, "eq", &[argument, returned], &mut env).expect("fast-return identity"),
            Value::T,
        );
    }

    /// GNU's `byte-code' executes a bare program against a constants
    /// vector; `internal-stack-stats' is a nil-returning telemetry stub.
    #[test]
    fn byte_code_primitive_executes_program() {
        let mut interp = Interpreter::new();
        let mut env = Env::new();
        // constant0; constant1; plus; return with constants [40 2].
        // bytecode.c:Fbyte_code converts multibyte input with
        // string-as-unibyte. Actual Unicode U+00C0/U+00C1 encode as
        // C3 80/C3 81, not the C0/C1 constant opcodes required here.
        let value = prim(
            &mut interp,
            "byte-code",
            &[
                primitives::bytes_to_shared_unibyte_value(&[192, 193, 92, 135]),
                Value::vector([Value::Integer(40), Value::Integer(2)]),
                Value::Integer(4),
            ],
            &mut env,
        )
        .unwrap();
        assert_eq!(value, Value::Integer(42));
        assert_eq!(
            prim(&mut interp, "internal-stack-stats", &[], &mut env).unwrap(),
            Value::Nil
        );
    }
}

#[cfg(test)]
#[test]
fn canonical_bytecode_entry_borrows_the_actual_bytes_without_predecoding() {
    let _interp = Interpreter::new();
    let code = primitives::bytes_to_shared_unibyte_value(&[192, 135, 0]);
    let Kind::StringObject(state) = code.kind() else {
        panic!("canonical code string");
    };
    let function = Value::allocated_closure(&[
        Value::Integer(0),
        code,
        Value::vector([Value::Integer(73)]),
        Value::Integer(1),
    ]);
    let Kind::Closure(closure) = function.kind() else {
        panic!("actual closure");
    };
    // The invalid dead byte must not be decoded when an ordinary activation
    // starts. The separate diagnostic parser still rejects corrupt streams.
    let program = closure_program(closure).expect("entry reads fields, not dead instructions");
    assert_eq!(program.code.original().word(), code.word());
    assert_eq!(
        std::mem::size_of::<crate::lisp::bytecode::CodeBytes>(),
        std::mem::size_of::<Value>()
    );
    program.code.with_bytes(|bytes| {
        let state = state.borrow();
        assert_eq!(bytes.as_ptr(), state.bytes().as_ptr());
        assert_eq!(bytes, &[192, 135, 0]);
    });
}

#[cfg(test)]
#[test]
fn taken_bytecode_branch_outside_storage_errors_without_leaking_a_frame() {
    let mut interp = Interpreter::new();
    let mut env = Env::new();
    let code = primitives::bytes_to_shared_unibyte_value(&[130, 255, 255]);
    let function = Value::allocated_closure(&[
        Value::Integer(0),
        code,
        Value::vector([]),
        Value::Integer(0),
    ]);
    let Kind::Closure(closure) = function.kind() else {
        panic!("actual closure");
    };
    let stack_before = interp.bc_stack.len();
    let roots_before = interp.bc_stack.frame_count();
    let error = execute_closure(&mut interp, closure, &[], &mut env)
        .expect_err("the actual next fetch must check the string boundary");
    assert!(matches!(error.kind(), LispErrorKind::Signal(message)
        if message == "byte code ran off the end of its program"));
    assert_eq!(interp.bc_stack.len(), stack_before);
    assert_eq!(interp.bc_stack.frame_count(), roots_before);
}

#[cfg(test)]
#[test]
fn bytecode_stack_footers_share_storage_and_preserve_borrowed_caller_arguments() {
    let mut stack = BcStack::new();
    stack.ensure_allocated();
    assert_eq!(stack.words.capacity(), BC_STACK_WORDS);
    assert_eq!(std::mem::size_of::<BcHeader>(), 32);
    stack
        .enter_frame(5, Value::Integer(11), 0)
        .expect("caller reservation");
    stack.push(Value::Integer(17)).expect("caller argument");
    // This is the same borrow retained by an ordinary backtrace while the
    // callee writes its own operands and while the owning stack moves.
    let arguments = unsafe { stack.slice_from(0, 1) };
    assert_eq!(arguments.as_ptr().cast::<usize>(), unsafe {
        stack.words.as_ptr().add(4).cast::<usize>()
    });
    assert_eq!(stack.frame, 9);
    stack
        .enter_frame(7, Value::Integer(23), 91)
        .expect("callee reservation");
    stack.push(Value::Integer(29)).expect("callee operand");
    assert_eq!(stack.frame, 20);
    assert_eq!(stack.header(stack.frame).function, Value::Integer(23));
    let caller = stack.header(stack.frame).saved_frame;
    assert_eq!(stack.header(caller).function, Value::Integer(11));
    assert_eq!(arguments, &[Value::Integer(17)]);
    assert_eq!(&*stack, &[Value::Integer(29)]);
    let mut moved = stack;
    assert_eq!(moved.leave_frame(), 91);
    assert_eq!(&*moved, arguments);
    assert_eq!(moved.leave_frame(), 0);
    assert!(moved.is_empty());
    assert_eq!(moved.frame_count(), 0);
    moved
        .enter_frame(BC_STACK_WORDS - 8, Value::Integer(31), 0)
        .expect("GNU's largest single frame includes both footers");
    assert!(moved.enter_frame(0, Value::Nil, 0).is_err());
    assert_eq!(moved.frame_count(), 1);
    moved.leave_frame();
    assert!(
        moved
            .enter_frame(BC_STACK_WORDS - 7, Value::Nil, 0)
            .is_err()
    );
    assert_eq!(moved.frame_count(), 0);
}
