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

    /// A new frame's first ENTRY_DEPTH slots: ARGS, then nils.
    #[inline(always)]
    fn push_arguments(&mut self, args: &[Value], entry_depth: usize) {
        assert!(
            args.len() <= entry_depth && self.top + entry_depth <= self.frame,
            "byte code exceeded its declared stack depth"
        );
        // SAFETY: these free operand words precede the active footer; ARGS
        // lie in the caller's frame, disjoint from them.
        unsafe {
            let slots = self.words.as_mut_ptr().add(self.top).cast::<Value>();
            std::ptr::copy_nonoverlapping(args.as_ptr(), slots, args.len());
            for index in args.len()..entry_depth {
                slots.add(index).write(Value::Nil);
            }
        }
        self.top += entry_depth;
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

/// What a frame switch in the dispatch loop changes: the thread's state
/// (its byte-code stack, backtrace and depth), this activation's program
/// and suspended frames, its handlers, and the frames its signaling ops
/// recorded.
struct VmFrames<'a> {
    interp: &'a mut Interpreter,
    env: &'a mut Env,
    program: &'a mut ByteCodeObject,
    frames: &'a mut Vec<BcFrame>,
    handlers: &'a mut Vec<Handler>,
    op_error_frames: &'a mut usize,
    trace_errors: bool,
}

/// What a full Bcall left: a new byte-code frame to run from pc 0, or the
/// callee's value on the caller's stack; the stack's top and the frame's
/// depth.
enum CallOutcome {
    Entered(usize, usize),
    Returned(usize, usize),
}

/// bytecode.c's Bcall (`docall'): Ffuncall's entry, the frame record,
/// maybe_gc, then setup_frame for a byte-code callee or the call itself.
/// ARGC arguments lie above the function on the stack, whose top is TOP
/// (the verified loop keeps it in a register); the caller resumes
/// at RETURN_PC, the instruction's end, and OFFSET is its start (for the
/// error trace).
#[inline(never)]
fn bytecode_call(
    vm: &mut VmFrames<'_>,
    top: usize,
    argc: usize,
    return_pc: usize,
    offset: usize,
) -> Result<CallOutcome, LispError> {
    let VmFrames {
        interp,
        env,
        program,
        frames,
        handlers,
        op_error_frames,
        trace_errors,
    } = vm;
    interp.bc_stack.top = top;
    let args_start = interp.bc_stack.len() - argc;
    // Bcall: the function is TOP below the arguments; both stay on the
    // stack until the call returns (GNU keeps them rooted the same way).
    let func = interp.bc_stack[args_start - 1];
    interp.begin_bytecode_call(env)?;
    if let Some(termination) = interp.pending_termination().cloned() {
        interp.end_funcall();
        return Err(LispError::Terminate(termination));
    }
    let backtrace_depth = interp.backtrace_frames_len();
    // Bcall records the unresolved function and rooted arguments before
    // maybe_gc, which may redefine the function cell.
    // SAFETY: the stack buffer never moves and these slots stay below the
    // callee's disjoint storage until return/unwind.
    let call_args = unsafe { interp.bc_stack.slice_from(args_start, argc) };
    interp.push_backtrace_frame_borrowed(func, call_args);
    let callee_name = match func.kind() {
        Kind::Symbol(name) => Some(name),
        _ => None,
    };
    interp.capture_current_backtrace_context(
        callee_name.as_ref().map(|name| name.as_str()),
        env,
        None,
    );
    crate::lisp::native_comp::maybe_gc(interp, env);
    // Only now inspect the live cell/code slots, just as GNU's
    // packed-closure setup_frame branch does after maybe_gc.
    let callee = match interp.bcall_callee(&func) {
        crate::lisp::eval::BcallCallee::ByteCode(callee, callee_function) => {
            if let Err(error) =
                interp
                    .bc_stack
                    .enter_frame(callee.stack_depth, callee_function, return_pc)
            {
                let result = interp.settle_frame_result(Err(error), env);
                interp.truncate_backtrace_frames(backtrace_depth);
                interp.end_funcall();
                let value = result?;
                interp.bc_stack.truncate(args_start - 1);
                interp.bc_stack.push_within_frame(value);
                return Ok(CallOutcome::Returned(
                    interp.bc_stack.top,
                    interp.bc_stack.len(),
                ));
            }
            frames.push(BcFrame {
                program: std::mem::replace(*program, callee),
                base: args_start - 1,
                handlers_len: handlers.len(),
                unwinds_len: interp.bc_unwinds.len(),
                backtrace_depth,
                op_error_frames: std::mem::take(*op_error_frames),
            });
            // setup_frame: the callee's frame starts above the caller's
            // top; the arguments are copied into it (`PUSH (*args)'), the
            // optionals padded, the rest gathered.  The caller's own slots
            // -- the function and the arguments the backtrace frame borrows
            // -- stay as they are until Breturn cuts back to them.
            let ArgSpec::Packed {
                mandatory,
                nonrest,
                rest,
            } = program.argspec
            else {
                unreachable!("bytecode_callee selects packed argument lists")
            };
            let mandatory = usize::from(mandatory);
            let nonrest = usize::from(nonrest);
            if argc < mandatory || (!rest && argc > nonrest) {
                return Err(packed_arity_error(mandatory, nonrest, argc));
            }
            let pushed = argc.min(nonrest);
            for argument in &call_args[..pushed] {
                interp.bc_stack.push_within_frame(*argument);
            }
            if argc > nonrest {
                let rest_list = Value::list(call_args[nonrest..].iter().cloned());
                interp.bc_stack.push_within_frame(rest_list);
            } else {
                for _ in argc..nonrest {
                    interp.bc_stack.push_within_frame(Value::Nil);
                }
                if rest {
                    interp.bc_stack.push_within_frame(Value::Nil);
                }
            }
            program.verified = super::verify::verified_code(program, nonrest + usize::from(rest));
            return Ok(CallOutcome::Entered(
                interp.bc_stack.top,
                interp.bc_stack.len(),
            ));
        }
        callee => callee,
    };
    let trace_call = trace_errors.then(|| func.to_string());
    let result = match callee {
        crate::lisp::eval::BcallCallee::Native(native) => {
            crate::lisp::native_comp::call_function(interp, env, native, call_args)
        }
        crate::lisp::eval::BcallCallee::Subr(subr) => interp.call_subr(subr, call_args, env, true),
        _ => interp.funcall_body(func, call_args, env),
    };
    let result = interp.settle_frame_result(result, env);
    interp.truncate_backtrace_frames(backtrace_depth);
    interp.end_funcall();
    let value = match result {
        Ok(value) => value,
        Err(error) => {
            // Expected conditions such as `scan-error' are routinely
            // caught by compiled Lisp.  Logging every one makes a real VM
            // dispatch failure disappear in megabytes of noise.
            if matches!(error.kind(), LispErrorKind::WrongNumberOfArgs(_, _))
                && let Some(func) = trace_call
            {
                eprintln!("bytecode call {func} failed at byte offset {offset}: {error:?}");
            }
            return Err(error);
        }
    };
    interp.bc_stack.truncate(args_start - 1);
    interp.bc_stack.push_within_frame(value);
    Ok(CallOutcome::Returned(
        interp.bc_stack.top,
        interp.bc_stack.len(),
    ))
}

/// Bcall's `goto setup_frame' when the call does nothing else: the
/// evaluator finds Ffuncall's entry plain (`plain_bytecode_callee'),
/// maybe_gc would not collect, and the callee is a byte-code function
/// that takes these ARGC arguments without a &rest list and whose frame
/// fits.  The callee's frame starts at pc 0 and the caller resumes at
/// RETURN_PC.  Otherwise nothing changes and the full Call arm runs the
/// instruction, signals and collections included.
#[inline(always)]
fn enter_bytecode_frame(vm: &mut VmFrames<'_>, return_pc: usize, argc: usize) -> bool {
    let top = vm.interp.bc_stack.top;
    let func = vm.interp.bc_stack[vm.interp.bc_stack.len() - argc - 1];
    callee_is_closure(func) && enter_closure_frame(vm, top, return_pc, argc, func).is_some()
}

/// Whether Bcall's FUNC (its live function cell, for a symbol) is a
/// closure: the test a subr or native callee pays before the full Call
/// arm.
#[inline(always)]
fn callee_is_closure(func: Value) -> bool {
    let function = match func.symbol_cell() {
        Some(name) => name.cell().function.get(),
        None => func,
    };
    matches!(function.kind(), Kind::Closure(_))
}

#[inline(never)]
fn enter_closure_frame(
    vm: &mut VmFrames<'_>,
    top: usize,
    return_pc: usize,
    argc: usize,
    func: Value,
) -> Option<(usize, usize)> {
    let state: &mut crate::lisp::eval::InterpreterState = vm.interp;
    state.bc_stack.top = top;
    let args_start = state.bc_stack.len() - argc;
    let (function, closure) = state.plain_bytecode_callee(func)?;
    let mut callee = ByteCodeObject::for_call(closure)?;
    let ArgSpec::Packed {
        mandatory,
        nonrest,
        rest,
    } = callee.argspec
    else {
        unreachable!("bytecode_callee selects packed argument lists")
    };
    let nonrest = usize::from(nonrest);
    let entry_depth = nonrest + usize::from(rest);
    if argc < usize::from(mandatory)
        || argc > nonrest
        || entry_depth > callee.stack_depth
        || !crate::lisp::native_comp::collection_cannot_be_due(state)
    {
        return None;
    }
    // SAFETY: as the Call arm's: the caller's argument slots stay as they
    // are below the callee's frame until it returns or unwinds.
    let call_args = unsafe { state.bc_stack.slice_from(args_start, argc) };
    if state
        .bc_stack
        .enter_frame(callee.stack_depth, function, return_pc)
        .is_err()
    {
        return None;
    }
    let backtrace_depth = state.enter_plain_funcall(func, call_args);
    callee.verified = super::verify::verified_code(&callee, entry_depth);
    vm.frames.push(BcFrame {
        program: std::mem::replace(vm.program, callee),
        base: args_start - 1,
        handlers_len: vm.handlers.len(),
        unwinds_len: state.bc_unwinds.len(),
        backtrace_depth,
        op_error_frames: std::mem::take(vm.op_error_frames),
    });
    // setup_frame's `PUSH (*args)' and the optionals' nils.
    state.bc_stack.push_arguments(call_args, entry_depth);
    Some((state.bc_stack.top, entry_depth))
}

/// Breturn into a byte-code caller when the callee left no specpdl
/// entries above its watermark: the frame and its backtrace record are
/// popped and the caller gets VALUE; the answer is the pc it resumes at,
/// the stack's top and the frame's depth.  Otherwise the full return path runs the
/// unwinding; nothing changes but the stack's TOP, which is stored.
#[inline(never)]
fn return_to_bytecode_frame(
    vm: &mut VmFrames<'_>,
    top: usize,
    value: Value,
) -> Option<(usize, usize, usize)> {
    let state: &mut crate::lisp::eval::InterpreterState = vm.interp;
    state.bc_stack.top = top;
    let unwinds = state.bc_unwinds.len();
    let frame = vm.frames.pop_if(|frame| unwinds <= frame.unwinds_len)?;
    state.leave_plain_funcall(frame.backtrace_depth);
    let resume = state.bc_stack.leave_frame();
    state.bc_stack.truncate(frame.base);
    state.bc_stack.push_within_frame(value);
    *vm.program = frame.program;
    *vm.op_error_frames = frame.op_error_frames;
    vm.handlers.truncate(frame.handlers_len);
    Some((resume, state.bc_stack.top, state.bc_stack.len()))
}

/// Breturn's and the error path's leave of a frame: its specpdl entries
/// above the watermark undone, its backtrace frame popped, Ffuncall's
/// depth restored.
///
/// A signal from an unwind form replaces the one in flight (each signal
/// during unbind_to starts its own unwinding in C, the remaining forms
/// still run), so the last signal is the one that leaves.
#[inline]
fn leave_frame(interp: &mut Interpreter, env: &mut Env, frame: &BcFrame) -> Result<(), LispError> {
    if interp.bc_unwinds.len() > frame.unwinds_len {
        return leave_frame_unwinding(interp, env, frame);
    }
    interp.truncate_backtrace_frames(frame.backtrace_depth);
    interp.end_funcall();
    Ok(())
}

#[inline(never)]
fn leave_frame_unwinding(
    interp: &mut Interpreter,
    env: &mut Env,
    frame: &BcFrame,
) -> Result<(), LispError> {
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
        primitives::call_with_facts(interp, name, &facts, args, env)
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
    /// A frame switch left a frame the verified loop cannot run at its
    /// pc; the dispatch resumes it.
    Resume,
    /// Inside the verified loop only: a call or return switched frames,
    /// or a call ran Lisp, which may have changed the frame's code; the
    /// stack's top and the frame's depth.
    Switched(usize, usize),
    /// The instruction's signal, raised after its own work (a Bcall's).
    Raised(super::Instr, LispError),
}

/// exec_byte_code's dispatch loop for the ops that need nothing but the
/// operand stack and the program: stack shuffles, constants, jumps,
/// car/cdr, cons, eq, fixnum arithmetic and comparison, vector aref/aset.
/// run_fast's arm for each opcode byte. A dense class lets one load and
/// one jump select the arm, where matching the byte itself left LLVM
/// testing the call, stack-ref and constant ranges ahead of its table.
#[derive(Clone, Copy)]
enum FastOp {
    Slow,
    Call,
    StackRef,
    StackSet,
    Dup,
    Discard,
    DiscardN,
    Constant,
    Goto,
    GotoIfNil,
    GotoIfNonNil,
    GotoIfNilElsePop,
    GotoIfNonNilElsePop,
    Return,
    Not,
    Cons,
    Eq,
    Consp,
    Arithmetic,
    Comparison,
    Unary,
    Aref,
    Aset,
    CarCdr,
}

const FAST_OPS: [FastOp; 256] = {
    let mut table = [FastOp::Slow; 256];
    let mut byte = 0;
    while byte < 256 {
        table[byte] = match byte as u8 {
            super::B_CALL..=0o047 => FastOp::Call,
            0o001..=0o007 => FastOp::StackRef,
            super::B_STACK_SET | super::B_STACK_SET2 => FastOp::StackSet,
            0o211 => FastOp::Dup,
            0o210 => FastOp::Discard,
            super::B_DISCARDN => FastOp::DiscardN,
            super::B_CONSTANT2 | 0o300..=0o377 => FastOp::Constant,
            0o202 => FastOp::Goto,
            0o203 => FastOp::GotoIfNil,
            0o204 => FastOp::GotoIfNonNil,
            0o205 => FastOp::GotoIfNilElsePop,
            0o206 => FastOp::GotoIfNonNilElsePop,
            0o207 => FastOp::Return,
            0o77 => FastOp::Not,
            0o102 => FastOp::Cons,
            0o75 => FastOp::Eq,
            0o72 => FastOp::Consp,
            0o134 | 0o132 | 0o137 | 0o245 | 0o246 => FastOp::Arithmetic,
            0o125..=0o131 => FastOp::Comparison,
            0o124 | 0o123 | 0o133 => FastOp::Unary,
            0o110 => FastOp::Aref,
            0o111 => FastOp::Aset,
            0o100 | 0o101 | 0o242 | 0o243 => FastOp::CarCdr,
            _ => FastOp::Slow,
        };
        byte += 1;
    }
    table
};

/// Dispatches directly on the canonical opcode, as bytecode.c's FETCH/NEXT,
/// without first building an Instr and switching on its Op a second time.
/// The operand stack is held directly for the whole run. An op
/// that cannot complete here (a call, a variable, a signal, an operand of
/// another kind) is left untouched at PC for the full dispatch.
#[inline(always)]
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
        match FAST_OPS[usize::from(byte)] {
            // Bcall's argument count uses the same checked operand reader
            // as the other packed families. Keep the instruction needed
            // by the full call/error path without decoding the opcode again.
            FastOp::Call => {
                let count = operand!(reader.family_operand(super::B_CALL));
                *pc = offset;
                return FastExit::Slow(super::Instr {
                    offset,
                    len: reader.cursor - offset,
                    op: Op::Call(count),
                });
            }
            FastOp::StackRef => {
                let n = operand!(reader.family_operand(super::B_STACK_REF));
                at = reader.cursor;
                let index = ops.len() - 1 - n as usize;
                let value = ops[index];
                ops.push_within_frame(value);
            }
            FastOp::StackSet => {
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
            FastOp::Dup => {
                let top = *ops.last().expect("validated bytecode");
                ops.push_within_frame(top);
            }
            // Bdiscard.
            FastOp::Discard => {
                pop!().discard();
            }
            FastOp::DiscardN => {
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
            FastOp::Constant => {
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
            FastOp::Goto => {
                let target = operand!(reader.fetch2());
                at = reader.cursor;
                jump!(target);
            }
            FastOp::GotoIfNil => {
                let target = operand!(reader.fetch2());
                at = reader.cursor;
                let value = pop!();
                let is_nil = value.is_nil();
                value.discard();
                if is_nil {
                    jump!(target);
                }
            }
            FastOp::GotoIfNonNil => {
                let target = operand!(reader.fetch2());
                at = reader.cursor;
                let value = pop!();
                let is_nil = value.is_nil();
                value.discard();
                if !is_nil {
                    jump!(target);
                }
            }
            FastOp::GotoIfNilElsePop => {
                let target = operand!(reader.fetch2());
                at = reader.cursor;
                if ops.last().expect("validated bytecode").is_nil() {
                    jump!(target);
                } else {
                    pop!().discard();
                }
            }
            FastOp::GotoIfNonNilElsePop => {
                let target = operand!(reader.fetch2());
                at = reader.cursor;
                if !ops.last().expect("validated bytecode").is_nil() {
                    jump!(target);
                } else {
                    pop!().discard();
                }
            }
            // Breturn.
            FastOp::Return => {
                return FastExit::Return(pop!());
            }
            // Bnot.
            FastOp::Not => {
                let top = ops.last_mut().expect("validated bytecode");
                *top = if top.is_nil() { Value::T } else { Value::Nil };
            }
            // Bcons.
            FastOp::Cons => {
                let b = pop!();
                let top = ops.last_mut().expect("validated bytecode");
                *top = Value::cons(*top, b);
            }
            // Beq.
            FastOp::Eq => {
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
            FastOp::Consp => {
                let is_cons = match ops.last().expect("validated bytecode").kind() {
                    Kind::Cons(_) => true,
                    // A keymap record reads as a cons; the full arm asks.
                    Kind::Record(_) => slow!(),
                    _ => false,
                };
                *ops.last_mut().expect("validated bytecode") =
                    if is_cons { Value::T } else { Value::Nil };
            }
            FastOp::Arithmetic => {
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
            FastOp::Comparison => {
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
            FastOp::Unary => {
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
            FastOp::Aref => {
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
            FastOp::Aset => {
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
            FastOp::CarCdr => {
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
            FastOp::Slow => slow!(),
        }
    }
}

/// `run_fast' for verified code, entered at an instruction whose verified
/// operand count is the stack's (see `verify::VerifiedCode::admits').
/// Like bytecode.c with BYTE_CODE_SAFE off, it checks neither the code's
/// bounds, nor the stack's, nor constant indices: verification proved
/// that every instruction reachable from here lies within the code, finds
/// its operands, stays within the frame's declared depth and reads a
/// constant within the vector. The stack top lives in a local pointer and
/// is stored back on the way out; nothing here runs Lisp or collects.
/// The ops and their exits are run_fast's.
#[inline(never)]
fn run_verified(
    vm: &mut VmFrames<'_>,
    code: &[u8],
    words: *mut Value,
    top: usize,
    pc: &mut usize,
    quitcounter: &mut u8,
) -> (FastExit, Option<usize>) {
    let mut code = code;
    let mut top = top;
    // One pass of the inner loop runs one frame; a call or return that
    // switches frames starts the next pass on the new frame's program.
    loop {
        let constants = vm.program.constants;
        // SAFETY: top lies within the active frame.
        let mut sp = unsafe { words.add(top) };
        let mut at = *pc;
        let exit = 'run: loop {
            let offset = at;
            // SAFETY (all code reads below): verified instructions, operands
            // included, lie within the code.
            let byte = unsafe { *code.get_unchecked(at) };
            at += 1;
            macro_rules! fetch {
                () => {{
                    let value = u16::from(unsafe { *code.get_unchecked(at) });
                    at += 1;
                    value
                }};
            }
            macro_rules! fetch2 {
                () => {{
                    let value = u16::from_le_bytes(unsafe {
                        [*code.get_unchecked(at), *code.get_unchecked(at + 1)]
                    });
                    at += 2;
                    value
                }};
            }
            // SAFETY (stack accesses below): verification found at least the
            // op's operands on the stack and room for its pushes.
            macro_rules! slot {
                ($n:expr) => {
                    unsafe { &mut *sp.sub($n + 1) }
                };
            }
            macro_rules! push {
                ($value:expr) => {{
                    let value = $value;
                    unsafe {
                        sp.write(value);
                        sp = sp.add(1);
                    }
                }};
            }
            macro_rules! drop_values {
                ($count:expr) => {
                    sp = unsafe { sp.sub($count) }
                };
            }
            macro_rules! slow {
                () => {{
                    *pc = offset;
                    break 'run match super::fetch_instruction(code, offset, constants.len()) {
                        Ok(instruction) => FastExit::Slow(instruction),
                        Err(error) => FastExit::Error(error),
                    };
                }};
            }
            macro_rules! jump {
                ($target:expr) => {{
                    let destination = usize::from($target);
                    if destination < at {
                        *quitcounter = quitcounter.wrapping_add(1);
                        if *quitcounter == 0 {
                            *quitcounter = 1;
                            *pc = destination;
                            break 'run FastExit::QuitCheck;
                        }
                    }
                    at = destination;
                }};
            }
            // One arm per opcode, so the dispatch is a single jump table and an
            // operand packed in the opcode is a constant in its arm.
            macro_rules! stack_ref {
                ($n:expr) => {{
                    let n: usize = $n;
                    let value = *slot!(n);
                    push!(value);
                }};
            }
            macro_rules! stack_set {
                ($n:expr) => {{
                    // `*(top - N) = POP'.
                    let n = usize::from($n);
                    *slot!(n) = *slot!(0);
                    drop_values!(1);
                }};
            }
            // Bcall: a plain call into byte code switches frames here; any
            // other runs the full sequence and resumes after it.
            macro_rules! call {
                ($count:expr) => {{
                    let count = $count;
                    let argc = usize::from(count);
                    let func = *slot!(argc);
                    // SAFETY: sp lies within the active frame.
                    let top = unsafe { sp.offset_from(words) as usize };
                    if callee_is_closure(func)
                        && let Some((top, depth)) = enter_closure_frame(vm, top, at, argc, func)
                    {
                        *pc = 0;
                        break 'run FastExit::Switched(top, depth);
                    }
                    match bytecode_call(vm, top, argc, at, offset) {
                        Ok(CallOutcome::Entered(top, depth)) => {
                            *pc = 0;
                            break 'run FastExit::Switched(top, depth);
                        }
                        Ok(CallOutcome::Returned(top, depth)) => {
                            *pc = at;
                            break 'run FastExit::Switched(top, depth);
                        }
                        Err(error) => {
                            *pc = at;
                            break 'run FastExit::Raised(
                                super::Instr {
                                    offset,
                                    len: at - offset,
                                    op: Op::Call(count),
                                },
                                error,
                            );
                        }
                    }
                }};
            }
            // Bvarref's SYMBOL_PLAINVAL case; any other variable, or a void
            // one, takes the full arm.
            macro_rules! varref {
                ($index:expr) => {{
                    let index = usize::from($index);
                    // SAFETY: verified against this vector's length.
                    let symbol = unsafe { constants.get_unchecked(index) };
                    let Kind::Symbol(name) = symbol.kind() else {
                        slow!()
                    };
                    let Some(value) = name.cell().plain_value() else {
                        slow!()
                    };
                    push!(value);
                }};
            }
            // Bvarset's SYMBOL_PLAINVAL, untrapped case: the store alone.
            macro_rules! varset {
                ($index:expr) => {{
                    let index = usize::from($index);
                    // SAFETY: verified against this vector's length.
                    let symbol = unsafe { constants.get_unchecked(index) };
                    let Kind::Symbol(name) = symbol.kind() else {
                        slow!()
                    };
                    if !name.cell().store_plain_value(*slot!(0)) {
                        slow!();
                    }
                    drop_values!(1);
                }};
            }
            macro_rules! constant {
                ($index:expr) => {{
                    let index = usize::from($index);
                    // SAFETY: verified against this vector's length.
                    push!(unsafe { constants.get_unchecked(index) });
                }};
            }
            macro_rules! goto_if {
                ($jump_when_nil:expr) => {{
                    let target = fetch2!();
                    let is_nil = slot!(0).is_nil();
                    drop_values!(1);
                    if is_nil == $jump_when_nil {
                        jump!(target);
                    }
                }};
            }
            macro_rules! goto_if_else_pop {
                ($jump_when_nil:expr) => {{
                    let target = fetch2!();
                    if slot!(0).is_nil() == $jump_when_nil {
                        jump!(target);
                    } else {
                        drop_values!(1);
                    }
                }};
            }
            // A fixnum operation on the two top operands; anything else (or an
            // overflow, or a zero divisor) goes to the full arm.
            macro_rules! arithmetic {
                ($op:ident) => {{
                    let (Some(x), Some(y)) = (slot!(1).fixnum(), slot!(0).fixnum()) else {
                        slow!();
                    };
                    let Some(n) = x.$op(y) else { slow!() };
                    *slot!(1) = Value::Integer(n);
                    drop_values!(1);
                }};
            }
            macro_rules! comparison {
            ($op:tt) => {{
                let (Some(x), Some(y)) = (slot!(1).fixnum(), slot!(0).fixnum()) else {
                    slow!();
                };
                *slot!(1) = if x $op y { Value::T } else { Value::Nil };
                drop_values!(1);
            }};
        }
            macro_rules! unary {
                ($op:expr) => {{
                    let Some(x) = slot!(0).fixnum() else { slow!() };
                    let Some(n) = $op(x) else { slow!() };
                    *slot!(0) = Value::Integer(n);
                }};
            }
            macro_rules! car_cdr {
                ($car:expr, $safe:expr) => {{
                    let replacement = match slot!(0).kind() {
                        Kind::Cons(cell) => {
                            if $car {
                                cell.car.get()
                            } else {
                                cell.cdr.get()
                            }
                        }
                        Kind::Nil => continue,
                        _ if $safe => Value::Nil,
                        // The full arm signals wrong-type-argument.
                        _ => slow!(),
                    };
                    *slot!(0) = replacement;
                }};
            }
            match byte {
                0o001 => stack_ref!(1),
                0o002 => stack_ref!(2),
                0o003 => stack_ref!(3),
                0o004 => stack_ref!(4),
                0o005 => stack_ref!(5),
                0o006 => stack_ref!(usize::from(fetch!())),
                0o007 => stack_ref!(usize::from(fetch2!())),
                0o040..=0o045 => call!(u16::from(byte - super::B_CALL)),
                0o046 => call!(fetch!()),
                0o047 => call!(fetch2!()),
                super::B_STACK_SET => stack_set!(fetch!()),
                super::B_STACK_SET2 => stack_set!(fetch2!()),
                // Bdup.
                0o211 => {
                    let top = *slot!(0);
                    push!(top);
                }
                // Bdiscard.
                0o210 => drop_values!(1),
                super::B_DISCARDN => {
                    let raw = fetch!();
                    let count = usize::from(raw & 0x7f);
                    if raw & 0x80 != 0 {
                        let top = *slot!(0);
                        drop_values!(count);
                        *slot!(0) = top;
                    } else {
                        drop_values!(count);
                    }
                }
                super::B_CONSTANT.. => constant!(byte - super::B_CONSTANT),
                super::B_CONSTANT2 => constant!(fetch2!()),
                // Bgoto.
                0o202 => {
                    let target = fetch2!();
                    jump!(target);
                }
                0o203 => goto_if!(true),
                0o204 => goto_if!(false),
                0o205 => goto_if_else_pop!(true),
                0o206 => goto_if_else_pop!(false),
                // Breturn.
                0o207 => {
                    let value = *slot!(0);
                    drop_values!(1);
                    if !vm.frames.is_empty() {
                        // SAFETY: sp lies within the active frame.
                        let top = unsafe { sp.offset_from(words) as usize };
                        if let Some((resume, top, depth)) = return_to_bytecode_frame(vm, top, value)
                        {
                            *pc = resume;
                            break 'run FastExit::Switched(top, depth);
                        }
                    }
                    break 'run FastExit::Return(value);
                }
                // Bnot.
                0o077 => {
                    let top = slot!(0);
                    *top = if top.is_nil() { Value::T } else { Value::Nil };
                }
                // Bcons.
                0o102 => {
                    let cdr = *slot!(0);
                    let car = *slot!(1);
                    *slot!(1) = Value::cons(car, cdr);
                    drop_values!(1);
                }
                // Beq.
                0o075 => {
                    let (left, right) = (*slot!(1), *slot!(0));
                    if matches!(left.kind(), Kind::SymbolWithPos(_))
                        || matches!(right.kind(), Kind::SymbolWithPos(_))
                    {
                        slow!();
                    }
                    let equal = crate::lisp::primitives::values_eq_plain(&left, &right);
                    *slot!(1) = if equal { Value::T } else { Value::Nil };
                    drop_values!(1);
                }
                // Bconsp.
                0o072 => {
                    let is_cons = match slot!(0).kind() {
                        Kind::Cons(_) => true,
                        // A keymap record reads as a cons; the full arm asks.
                        Kind::Record(_) => slow!(),
                        _ => false,
                    };
                    *slot!(0) = if is_cons { Value::T } else { Value::Nil };
                }
                0o134 => arithmetic!(checked_add),
                0o132 => arithmetic!(checked_sub),
                0o137 => arithmetic!(checked_mul),
                0o245 => arithmetic!(checked_div),
                0o246 => arithmetic!(checked_rem),
                0o125 => comparison!(==),
                0o126 => comparison!(>),
                0o127 => comparison!(<),
                0o130 => comparison!(<=),
                0o131 => comparison!(>=),
                0o124 => unary!(|x: i64| x.checked_add(1)),
                0o123 => unary!(|x: i64| x.checked_sub(1)),
                0o133 => unary!(i64::checked_neg),
                // Baref.
                0o110 => {
                    let Some(index) = slot!(0).fixnum().filter(|index| *index >= 0) else {
                        slow!()
                    };
                    let Some(value) =
                        crate::lisp::primitives::vector_aref_fast(slot!(1), index as usize)
                    else {
                        slow!();
                    };
                    *slot!(1) = value;
                    drop_values!(1);
                }
                // Baset: [.. vector index value]; aset returns the value.
                0o111 => {
                    let Some(index) = slot!(1).fixnum().filter(|index| *index >= 0) else {
                        slow!()
                    };
                    let value = *slot!(0);
                    if crate::lisp::primitives::vector_aset_fast(slot!(2), index as usize, &value)
                        .is_none()
                    {
                        slow!();
                    }
                    *slot!(2) = value;
                    drop_values!(2);
                }
                0o100 => car_cdr!(true, false),
                0o101 => car_cdr!(false, false),
                0o242 => car_cdr!(true, true),
                0o243 => car_cdr!(false, true),
                0o010..=0o015 => varref!(u16::from(byte - 0o010)),
                0o016 => varref!(fetch!()),
                0o017 => varref!(fetch2!()),
                0o020..=0o025 => varset!(u16::from(byte - 0o020)),
                0o026 => varset!(fetch!()),
                0o027 => varset!(fetch2!()),
                // Every other opcode, named so that the match covers all 256
                // bytes and its jump table needs no range check.
                0o000
                | 0o030..=0o037
                | 0o050..=0o071
                | 0o073..=0o074
                | 0o076
                | 0o103..=0o107
                | 0o112..=0o122
                | 0o135..=0o136
                | 0o140..=0o200
                | 0o212..=0o241
                | 0o244
                | 0o247..=0o261
                | 0o264..=0o265
                | 0o267..=0o277 => slow!(),
            }
        };
        if let FastExit::Raised(..) = exit {
            // The call left the stack as its signal found it.
            return (exit, None);
        }
        if let FastExit::Switched(new_top, depth) = exit {
            // The new frame's program, stack (DEPTH operands) and pc are in
            // place.
            // SAFETY: as the caller's slice: no Lisp runs, nothing is
            // collected and no string is stored into until this loop returns.
            let (bytes, current) = unsafe { vm.program.code.bytes_unguarded() };
            code = bytes;
            if current
                && vm
                    .program
                    .verified
                    .is_some_and(|verified| verified.admits(*pc, depth))
            {
                top = new_top;
                continue;
            }
            return (FastExit::Resume, None);
        }
        // SAFETY: sp stayed within the active frame.
        return (exit, Some(unsafe { sp.offset_from(words) as usize }));
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
    // The verifier's result for this code lets the fast loop run it
    // without per-op checks.
    let entry_depth = match program.argspec {
        ArgSpec::Packed { nonrest, rest, .. } => usize::from(nonrest) + usize::from(rest),
        ArgSpec::Legacy(_) => 0,
    };
    program.verified = super::verify::verified_code(&program, entry_depth);
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
            let exit = loop {
                // SAFETY: run_verified and run_fast run no Lisp, collect
                // nothing and store into no string; the bytes stay as they
                // are until they return.
                let (code, current) = unsafe { program.code.bytes_unguarded() };
                let verified_run = current
                    && program
                        .verified
                        .is_some_and(|verified| verified.admits(pc, interp.bc_stack.len()));
                let exit = match program.verified {
                    Some(_) if verified_run => {
                        let words = interp.bc_stack.words.as_mut_ptr().cast::<Value>();
                        let top = interp.bc_stack.top;
                        let (exit, top) = run_verified(
                            &mut VmFrames {
                                interp,
                                env,
                                program: &mut program,
                                frames: &mut frames,
                                handlers: &mut handlers,
                                op_error_frames: &mut op_error_frames,
                                trace_errors,
                            },
                            code,
                            words,
                            top,
                            &mut pc,
                            &mut quitcounter,
                        );
                        if let Some(top) = top {
                            interp.bc_stack.top = top;
                        }
                        exit
                    }
                    _ => run_fast(
                        &program,
                        code,
                        &mut interp.bc_stack,
                        &mut pc,
                        &mut quitcounter,
                    ),
                };
                // The verified loop switches frames itself, and a call or
                // return it left is not plain; one the unverified loop left
                // switches them here when nothing else happens on the way.
                // The full arms below take every other case.
                if verified_run {
                    if matches!(exit, FastExit::Resume) {
                        continue;
                    }
                    break exit;
                }
                let mut vm = VmFrames {
                    interp,
                    env,
                    program: &mut program,
                    frames: &mut frames,
                    handlers: &mut handlers,
                    op_error_frames: &mut op_error_frames,
                    trace_errors,
                };
                match exit {
                    FastExit::Resume => {}
                    FastExit::Slow(super::Instr {
                        op: Op::Call(argc),
                        len,
                        ..
                    }) if enter_bytecode_frame(&mut vm, pc + len, usize::from(argc)) => pc = 0,
                    FastExit::Return(value) if !vm.frames.is_empty() => {
                        let top = vm.interp.bc_stack.top;
                        match return_to_bytecode_frame(&mut vm, top, value) {
                            Some((resume, ..)) => pc = resume,
                            None => break exit,
                        }
                    }
                    exit => break exit,
                }
            };
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
                FastExit::Raised(instr, error) => {
                    last_instruction = Some(instr);
                    return Err(error);
                }
                FastExit::Resume | FastExit::Switched(..) => {
                    unreachable!("a frame switch resumes above")
                }
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
                    // A symbol constant is bound as that very object, as
                    // Bvarref and Bvarset read and set it, uninterned or not.
                    let name = program.constant(index);
                    let value = pop!();
                    let restore = match name.kind() {
                        Kind::Symbol(symbol) => interp.bind_special_symbol(&symbol, value, env)?,
                        _ => {
                            let name = name.as_symbol().map_err(|_| {
                                LispError::Signal("varbind constant must be a symbol".into())
                            })?;
                            interp.bind_special_variable(name, value, env)?
                        }
                    };
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
                    let top = interp.bc_stack.top;
                    let mut vm = VmFrames {
                        interp,
                        env,
                        program: &mut program,
                        frames: &mut frames,
                        handlers: &mut handlers,
                        op_error_frames: &mut op_error_frames,
                        trace_errors,
                    };
                    if let CallOutcome::Entered(..) =
                        bytecode_call(&mut vm, top, usize::from(argc), pc, offset)?
                    {
                        pc = 0;
                    }
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
    fn switch_targets_are_verified_with_both_operands_popped() {
        // (lambda (x) (cond ((eq x 'a) 1) ((eq x 'b) 2) (t 3))) compiled to
        // a jump table: dup; constant table; switch; constant 3; return /
        // 5: constant 1; return / 7: constant 2; return. The editor exists
        // before the reader interns the table's keys.
        let mut interp = Interpreter::new();
        let mut env = Env::new();
        let form = super::super::super::reader::Reader::new(
            r##"#[257 "\211\300\267\303\207\301\207\302\207"
                   [#s(hash-table test eq data (a 5 b 7)) 1 2 3] 3]"##,
        )
        .read()
        .expect("read byte-code object")
        .expect("byte-code object form");
        let closure = interp.eval(&form, &mut env).expect("byte-code closure");
        let Kind::Closure(closure_ref) = closure.kind() else {
            panic!("a byte-code closure")
        };
        let program = ByteCodeObject::for_call(closure_ref).expect("packed byte code");
        let verified = super::super::verify::verified_code(&program, 1).expect("verified");
        // Bswitch pops the table and the value: each target, like the
        // fall-through, starts with the argument alone.
        assert!(verified.admits(3, 1) && verified.admits(5, 1) && verified.admits(7, 1));
        for (argument, expected) in [("a", 1), ("b", 2), ("c", 3)] {
            let value = interp
                .call_function_value(closure, None, &[Value::symbol(argument)], &mut env)
                .expect("call");
            assert_eq!(value.fixnum(), Some(expected));
        }
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
        run2_in(&mut interp, name, args)
    }

    fn run2_in(interp: &mut Interpreter, name: &str, args: &[Value]) -> Result<Value, LispError> {
        let mut env = Env::new();
        let object = named_objects(ORACLE_ELC2)
            .remove(name)
            .unwrap_or_else(|| panic!("fixture {name} missing"));
        interp.materialize_read_object_literals(Value::Vector(object.constants), &mut env)?;
        execute(interp, &object, args, &mut env)
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
            let mut interp = Interpreter::new();
            let value = run2_in(&mut interp, "emaxx-fx2-switch", &[Value::symbol(key)]).unwrap();
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
