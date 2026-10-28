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
use super::super::types::{Env, LispError, SymbolName, Value};
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
        // The room above BASE bounds DEPTH, with no addition to overflow.
        const LIMIT: usize = BC_STACK_WORDS - BC_HEADER_WORDS;
        if base > LIMIT || depth > LIMIT - base {
            return Err(stack_overflow());
        }
        let frame = base + depth;
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
        // SAFETY: the frame is no dummy, as asserted.
        unsafe { self.leave_entered_frame() }
    }

    /// `leave_frame' of a frame `enter_frame' installed.
    ///
    /// # Safety
    /// The active frame is not the dummy one.
    #[inline(always)]
    unsafe fn leave_entered_frame(&mut self) -> usize {
        debug_assert_ne!(self.frame, 0, "cannot pop the dummy bytecode frame");
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
        // SAFETY: as asserted.
        unsafe { self.push_arguments_within(args, entry_depth) }
    }

    /// `push_arguments' into a frame known to hold them.
    ///
    /// # Safety
    /// ARGS are at most ENTRY_DEPTH, and ENTRY_DEPTH slots fit below the
    /// active footer.
    #[inline(always)]
    unsafe fn push_arguments_within(&mut self, args: &[Value], entry_depth: usize) {
        debug_assert!(
            args.len() <= entry_depth && self.top + entry_depth <= self.frame,
            "byte code exceeded its declared stack depth"
        );
        // SAFETY: these free operand words precede the active footer; ARGS
        // lie in the caller's frame, disjoint from them.
        unsafe {
            let slots = self.words.as_mut_ptr().add(self.top).cast::<Value>();
            // A store for each of a few arguments, as setup_frame's PUSH
            // loop: cheaper than a `memcpy' call, which a copy loop becomes.
            match args {
                [] => {}
                [a] => slots.write(*a),
                [a, b] => {
                    slots.write(*a);
                    slots.add(1).write(*b);
                }
                [a, b, c] => {
                    slots.write(*a);
                    slots.add(1).write(*b);
                    slots.add(2).write(*c);
                }
                args => std::ptr::copy_nonoverlapping(args.as_ptr(), slots, args.len()),
            }
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

    /// A call's end: VALUE takes the function's slot, LEN operands up the
    /// frame, and the arguments above it go (`truncate' and a push).
    #[inline(always)]
    fn replace_call(&mut self, len: usize, value: Value) {
        assert!(
            self.base + len < self.top,
            "a call's function lies below the top"
        );
        // SAFETY: as asserted.
        unsafe { self.replace_call_below_top(len, value) }
    }

    /// `replace_call' of a function slot known to lie below the top.
    ///
    /// # Safety
    /// The slot LEN operands up the frame lies below the top.
    #[inline(always)]
    unsafe fn replace_call_below_top(&mut self, len: usize, value: Value) {
        let slot = self.base + len;
        debug_assert!(slot < self.top, "a call's function lies below the top");
        // SAFETY: the slot is an initialized operand of the current frame.
        unsafe {
            self.words
                .as_mut_ptr()
                .add(slot)
                .cast::<Value>()
                .write(value)
        };
        self.top = slot + 1;
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
        // SAFETY: just checked, and as the caller's contract.
        unsafe { self.slice_from_unchecked(start, len) }
    }

    /// `slice_from' where the caller knows the slots are there: a verified
    /// Bcall's, or ones it has just indexed.
    ///
    /// # Safety
    /// As `slice_from's, and the frame holds at least START + LEN operands.
    #[inline(always)]
    unsafe fn slice_from_unchecked(&self, start: usize, len: usize) -> &'static [Value] {
        debug_assert!(start <= self.len() && len <= self.len() - start);
        unsafe {
            std::slice::from_raw_parts(
                self.words.as_ptr().add(self.base + start).cast::<Value>(),
                len,
            )
        }
    }

    /// The LEN operands below TOP, the frame's top as the verified loop
    /// holds it: `slice_from_unchecked' of the last LEN without the
    /// frame's base.
    ///
    /// # Safety
    /// As `slice_from_unchecked's, TOP being the stack's top.
    #[inline(always)]
    unsafe fn below_top_unchecked(&self, top: usize, len: usize) -> &'static [Value] {
        debug_assert!(top == self.top && len <= self.len());
        unsafe {
            std::slice::from_raw_parts(self.words.as_ptr().add(top - len).cast::<Value>(), len)
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

/// What a running frame reads of its program, as bytecode.c re-reads
/// `bytestr_data' and `vectorp' from the function at a return: the code,
/// the constants and the verifier's result.  The argument template and
/// the stack depth are read once, at the frame's entry, from the whole
/// program (`ByteCodeObject').  Three words: a call saves the caller's
/// and installs the callee's, a return restores the caller's.
#[derive(Clone, Copy)]
struct Running {
    code: super::CodeBytes,
    constants: crate::lisp::types::VectorRef,
    verified: Option<&'static super::verify::VerifiedCode>,
}

impl Running {
    #[inline(always)]
    fn of(program: &ByteCodeObject) -> Self {
        Self {
            code: program.code,
            constants: program.constants,
            verified: program.verified,
        }
    }

    #[inline(always)]
    fn entered(program: &super::verify::EntryProgram) -> Self {
        Self {
            code: program.code,
            constants: program.constants,
            verified: Some(program.verified),
        }
    }

    #[inline]
    fn constant(&self, index: u16) -> Value {
        // bytecode.c reads vectorp[index] at the instruction, not a copy
        // captured during decoding. End the borrow before Lisp can run.
        self.constants
            .get(usize::from(index))
            .expect("validated constant index")
    }
}

/// One suspended byte-code frame of this activation (bytecode.c's
/// `bc_frame': the caller's program and pc, its stack top, and the
/// watermarks the callee's return or error restores).
#[derive(Clone)]
pub(crate) struct BcFrame {
    program: Running,
    /// The caller's slot holding the callee (Bcall's TOP after DISCARD):
    /// the callee's arguments and frame lie above it, and the return
    /// value replaces it.
    base: usize,
    handlers_len: usize,
    unwinds_len: usize,
    backtrace_depth: usize,
    op_error_frames: usize,
    /// `code_changes' when the verified loop called from the frame, which
    /// verification found the call's successor in with the operands the
    /// return leaves: while it is the same, the caller's code and its
    /// verification still hold and the return resumes without testing
    /// them.  `usize::MAX' for a call made anywhere else.
    code_changes: usize,
}

/// What a frame switch in the dispatch loop changes: the thread's state
/// (its byte-code stack, backtrace and depth, suspended frames and
/// handlers), this activation's program, and the frames its signaling ops
/// recorded.
struct VmFrames<'a> {
    interp: &'a mut Interpreter,
    env: &'a mut Env,
    program: &'a mut Running,
    /// Where this activation's suspended frames and handlers begin in the
    /// thread's `bc_frames' and `bc_handlers'.
    frames_base: usize,
    handlers_base: usize,
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
        op_error_frames,
        trace_errors,
        ..
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
            let frame = BcFrame {
                program: std::mem::replace(*program, Running::of(&callee)),
                base: args_start - 1,
                handlers_len: interp.bc_handlers.len(),
                unwinds_len: interp.bc_unwinds.len(),
                backtrace_depth,
                op_error_frames: std::mem::take(*op_error_frames),
                code_changes: usize::MAX,
            };
            interp.bc_frames.push(frame);
            // setup_frame: the callee's frame starts above the caller's
            // top; the arguments are copied into it (`PUSH (*args)'), the
            // optionals padded, the rest gathered.  The caller's own slots
            // -- the function and the arguments the backtrace frame borrows
            // -- stay as they are until Breturn cuts back to them.
            let ArgSpec::Packed {
                mandatory,
                nonrest,
                rest,
            } = callee.argspec
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
                let rest_list = Value::list_of(&call_args[nonrest..]);
                interp.bc_stack.push_within_frame(rest_list);
            } else {
                for _ in argc..nonrest {
                    interp.bc_stack.push_within_frame(Value::Nil);
                }
                if rest {
                    interp.bc_stack.push_within_frame(Value::Nil);
                }
            }
            program.verified = super::verify::verified_code(&callee, nonrest + usize::from(rest));
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
/// evaluator finds Ffuncall's entry plain (`plain_bcall_entry'),
/// maybe_gc would not collect, and the callee is a byte-code function
/// whose code verifies (`verify::entry_program'), that takes these ARGC
/// arguments without a &rest list and whose frame fits.  The callee's
/// frame starts at pc 0 and the caller resumes at RETURN_PC.  Otherwise
/// nothing changes and the full Call arm runs the instruction, signals
/// and collections included.
#[inline(always)]
fn enter_bytecode_frame(vm: &mut VmFrames<'_>, return_pc: usize, argc: usize) -> bool {
    let top = vm.interp.bc_stack.top;
    let func = vm.interp.bc_stack[vm.interp.bc_stack.len() - argc - 1];
    callee_function(func).closure_ref().is_some_and(|closure| {
        enter_closure_frame(vm, top, return_pc, argc, func, closure, false).is_some()
    })
}

/// Bcall's FUNC as its live function cell, for a symbol: what the call
/// arm tests for a closure or a native function before anything else.
#[inline(always)]
fn callee_function(func: Value) -> Value {
    match func.symbol_cell() {
        Some(name) => name.cell().function.get(),
        None => func,
    }
}

/// `call_plain' of NATIVE, FUNC's function, a lexical native function of
/// fixed arity whose arguments all travel in registers (at most six),
/// when the call nests in the active call of this interpreter
/// (`NestedCall'): the same tests, Ffuncall's depth and frame, and the
/// call made by `invoke_nested_fixed' in this frame, without the
/// general native entry's layers.  The answer is the call's value, which
/// the caller puts in place of the function and its arguments, or its
/// signal; None, with nothing changed but the stack's TOP, for
/// `call_plain' to take.
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
#[inline(always)]
fn call_native_nested(
    vm: &mut VmFrames<'_>,
    top: usize,
    argc: usize,
    func: Value,
    native: crate::lisp::types::NativeFunctionRef,
) -> Option<Result<Value, LispError>> {
    // The interpreter and environment held here, not read again through
    // the loop's record after each step the call's stores may change.
    let interp: &mut Interpreter = vm.interp;
    let env: &mut Env = vm.env;
    interp.bc_stack.top = top;
    if vm.trace_errors {
        return None;
    }
    // `call_fixed_function's tests, for at most six arguments, in the
    // function's register counts (a target, lexical binding, the arity):
    // the general path takes any other call.
    if argc > 6 || native.register_counts() & (1 << argc) == 0 {
        return None;
    }
    let target = native.target();
    if !interp.plain_bcall_entry(func.symbol_cell()) {
        return None;
    }
    let nested = crate::lisp::native_comp::NestedCall::of(interp)?;
    // The active call's runtime is NESTED's: maybe_gc's test on the
    // active call's trigger first.
    if !crate::lisp::native_comp::collection_cannot_be_due(interp) {
        return None;
    }
    // SAFETY: the active call lends its runtime for this synchronous
    // call, as `current_runtime' does.
    let runtime = unsafe { &mut *nested.runtime() };
    // SAFETY: as the Call arm's: the arguments stay on the caller's stack
    // until the call returns, and verification found them there.
    let call_args = unsafe { interp.bc_stack.below_top_unchecked(top, argc) };
    let (backtrace_depth, lisp_eval_depth) = interp.enter_plain_funcall_at(func, call_args);
    let depths = (lisp_eval_depth, backtrace_depth + 1);
    // SAFETY: NESTED is the active call of this runtime and interpreter,
    // and TARGET takes at most six arguments, among them CALL_ARGS.
    let result =
        unsafe { runtime.invoke_nested_fixed(nested, interp, env, target, call_args, depths) };
    let result = interp.settle_frame_result(result, env);
    interp.leave_plain_funcall(backtrace_depth);
    Some(result)
}

/// `call_native_nested' for ARGC arguments, a constant in the opcodes
/// that pack the count: the registers are the arguments' loads and
/// zeros, with no dispatch on the count.
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
#[inline(always)]
fn call_native_nested_fixed<const ARGC: usize>(
    vm: &mut VmFrames<'_>,
    top: usize,
    func: Value,
    native: crate::lisp::types::NativeFunctionRef,
) -> Option<Result<Value, LispError>> {
    call_native_nested(vm, top, ARGC, func, native)
}

/// `call_native_nested' for a count in the operand (Bcall6, Bcall7).
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
#[inline(never)]
fn call_native_nested_counted(
    vm: &mut VmFrames<'_>,
    top: usize,
    argc: usize,
    func: Value,
    native: crate::lisp::types::NativeFunctionRef,
) -> Option<Result<Value, LispError>> {
    call_native_nested(vm, top, argc, func, native)
}

/// `call_native_nested' by ARGC: one of the fixed counts' copies, which an
/// arm whose count is a constant calls directly.
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
#[inline(always)]
fn call_native_nested_by_count(
    vm: &mut VmFrames<'_>,
    top: usize,
    argc: usize,
    func: Value,
    native: crate::lisp::types::NativeFunctionRef,
) -> Option<Result<Value, LispError>> {
    match argc {
        0 => call_native_nested_fixed::<0>(vm, top, func, native),
        1 => call_native_nested_fixed::<1>(vm, top, func, native),
        2 => call_native_nested_fixed::<2>(vm, top, func, native),
        3 => call_native_nested_fixed::<3>(vm, top, func, native),
        _ => call_native_nested_counted(vm, top, argc, func, native),
    }
}

/// Bcall into a native-compiled function or a subr when nothing else
/// happens on the way in (`plain_bcall_callee'; maybe_gc would not
/// collect; no error trace is asked for): Ffuncall's depth and frame
/// record, the call, the frame's settling and pop, and the value replacing
/// the function and its arguments, as the full Call arm does them for
/// that case. The answer is the stack's top and the frame's depth after
/// the call, or the call's signal; None, with nothing changed but the
/// stack's TOP, for the full arm.
#[inline(never)]
fn call_plain(
    vm: &mut VmFrames<'_>,
    top: usize,
    argc: usize,
    func: Value,
    function: Value,
) -> Option<Result<(usize, usize), LispError>> {
    let state: &mut crate::lisp::eval::InterpreterState = vm.interp;
    state.bc_stack.top = top;
    if vm.trace_errors {
        return None;
    }
    let callee = state.plain_bcall_callee(func, function)?;
    if !crate::lisp::native_comp::collection_cannot_be_due(state) {
        return None;
    }
    let args_start = state.bc_stack.len() - argc;
    // SAFETY: as the Call arm's: the arguments stay on the caller's stack
    // until the call returns, and verification found them there.
    let call_args = unsafe { state.bc_stack.slice_from_unchecked(args_start, argc) };
    let backtrace_depth = state.enter_plain_funcall(func, call_args);
    let result = match callee {
        crate::lisp::eval::PlainCallee::Native(native) => {
            crate::lisp::native_comp::call_function(vm.interp, vm.env, native, call_args)
        }
        crate::lisp::eval::PlainCallee::Subr(subr) => {
            vm.interp.call_subr(subr, call_args, vm.env, true)
        }
    };
    let result = vm.interp.settle_frame_result(result, vm.env);
    let state: &mut crate::lisp::eval::InterpreterState = vm.interp;
    state.leave_plain_funcall(backtrace_depth);
    Some(result.map(|value| {
        state.bc_stack.replace_call(args_start - 1, value);
        (state.bc_stack.top, state.bc_stack.len())
    }))
}

/// Bcall's `goto setup_frame' into CLOSURE, FUNC's function, when the call
/// does nothing else (see `enter_bytecode_frame'): the callee's frame and
/// backtrace record pushed, its arguments and optionals' nils in place.
/// The answer is the stack's top and the frame's depth; None, with nothing
/// changed but the stack's TOP, for the full Call arm.  Inline in the
/// verified loop, as setup_frame is in exec_byte_code.
#[inline(always)]
fn enter_closure_frame(
    vm: &mut VmFrames<'_>,
    top: usize,
    return_pc: usize,
    argc: usize,
    func: Value,
    closure: crate::lisp::types::ClosureRef,
    verified_return: bool,
) -> Option<(usize, usize)> {
    let state: &mut crate::lisp::eval::InterpreterState = vm.interp;
    state.bc_stack.top = top;
    let args_start = state.bc_stack.len() - argc;
    // The entry tests (`plain_bcall_entry') for CLOSURE, which the caller
    // found in FUNC's cell (or as FUNC itself).
    if !state.plain_bcall_entry(func.symbol_cell()) {
        return None;
    }
    let function = Value::Closure(closure);
    // SAFETY: nothing below enters a closure or collects a string before
    // the program is copied into the frame.
    let callee = unsafe { super::verify::entry_program(closure)?.as_ref() };
    if !callee.takes(argc) || !crate::lisp::native_comp::collection_cannot_be_due(state) {
        return None;
    }
    let entry_depth = callee.entry_depth;
    // SAFETY: as the Call arm's: the caller's argument slots stay as they
    // are below the callee's frame until it returns or unwinds. Both
    // callers found the function and the arguments on the stack: verified
    // code, or `enter_bytecode_frame's index of the function.
    let call_args = unsafe { state.bc_stack.slice_from_unchecked(args_start, argc) };
    if state
        .bc_stack
        .enter_frame(callee.stack_depth, function, return_pc)
        .is_err()
    {
        return None;
    }
    let backtrace_depth = state.enter_plain_funcall(func, call_args);
    // The caller's program copied into the frame's slot, then the callee's
    // over it: `mem::replace' moved the caller's through a temporary.
    let frame = BcFrame {
        program: *vm.program,
        base: args_start - 1,
        handlers_len: state.bc_handlers.len(),
        unwinds_len: state.bc_unwinds.len(),
        backtrace_depth,
        op_error_frames: std::mem::take(vm.op_error_frames),
        code_changes: if verified_return {
            crate::lisp::alloc::code_changes()
        } else {
            usize::MAX
        },
    };
    state.bc_frames.push(frame);
    *vm.program = Running::entered(callee);
    // setup_frame's `PUSH (*args)' and the optionals' nils.
    // SAFETY: the program takes ARGC arguments (`takes'), at most its entry
    // depth, and the frame entered above holds its stack depth, at least
    // that (`EntryProgram::counts').
    unsafe { state.bc_stack.push_arguments_within(call_args, entry_depth) };
    Some((state.bc_stack.top, entry_depth))
}

/// Breturn into a byte-code caller when the callee left no specpdl
/// entries above its watermark: the frame and its backtrace record are
/// popped and the caller gets VALUE; the answer is the pc it resumes at,
/// the stack's top, the frame's depth and whether the verified loop's
/// call is still current (`BcFrame::code_changes').  Otherwise the full return path runs the
/// unwinding; nothing changes but the stack's TOP, which is stored.
#[inline(always)]
fn return_to_bytecode_frame(
    vm: &mut VmFrames<'_>,
    top: usize,
    value: Value,
) -> Option<(usize, usize, usize, bool)> {
    let state: &mut crate::lisp::eval::InterpreterState = vm.interp;
    state.bc_stack.top = top;
    let unwinds = state.bc_unwinds.len();
    // The frame is read in place and its slot released by the length. A
    // frame moved out whole (`pop', an Option) was copied in pieces offset
    // by its argument list's tag byte, and the loop's next read of the
    // caller's constants spanned two of those stores.
    let frame = state
        .bc_frames
        .last_above(vm.frames_base)
        .filter(|frame| unwinds <= frame.unwinds_len)?;
    let (backtrace_depth, base, program, op_error_frames, handlers_len, code_changes) = (
        frame.backtrace_depth,
        frame.base,
        frame.program,
        frame.op_error_frames,
        frame.handlers_len,
        frame.code_changes,
    );
    state.leave_plain_funcall(backtrace_depth);
    // SAFETY: the frame's call entered the stack frame this leaves, and
    // the caller's function slot lies below the top the call found.
    let resume = unsafe {
        let resume = state.bc_stack.leave_entered_frame();
        state.bc_stack.replace_call_below_top(base, value);
        resume
    };
    *vm.program = program;
    *vm.op_error_frames = op_error_frames;
    state.bc_handlers.truncate(handlers_len);
    let unchanged = code_changes == crate::lisp::alloc::code_changes();
    // SAFETY: FRAME is the last, which needs no drop.
    unsafe { state.bc_frames.0.set_len(state.bc_frames.len() - 1) };
    Some((resume, state.bc_stack.top, state.bc_stack.len(), unchanged))
}

// A frame's slot is released by the length alone, and neither list's
// truncation runs any drop.
const _: () = assert!(!std::mem::needs_drop::<BcFrame>());
const _: () = assert!(!std::mem::needs_drop::<Handler>());

/// One of the thread's lists of its byte-code activations' state,
/// innermost last: the suspended frames (`BcFrames', the bookkeeping beside
/// the bc_frame footers on `bc_stack') and the catch and condition-case
/// handlers (`BcHandlers', eval.c's handlerlist entries), each kept for the
/// thread as the footers are.  Each activation's entries lie above the
/// length it began at (its base), to which it returns however it ends.
#[derive(Clone)]
pub(crate) struct Suspended<T>(Vec<T>);

pub(crate) type BcFrames = Suspended<BcFrame>;
pub(crate) type BcHandlers = Suspended<Handler>;

impl<T> Default for Suspended<T> {
    fn default() -> Self {
        Self(Vec::new())
    }
}

impl<T> std::fmt::Debug for Suspended<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Suspended({} entries)", self.0.len())
    }
}

impl<T> Suspended<T> {
    #[inline(always)]
    fn len(&self) -> usize {
        self.0.len()
    }

    #[inline(always)]
    fn push(&mut self, entry: T) {
        self.0.push(entry);
    }

    /// The innermost entry of the activation whose entries begin at BASE.
    #[inline(always)]
    fn last_above(&self, base: usize) -> Option<&T> {
        self.0.last().filter(|_| self.0.len() > base)
    }

    /// The innermost entry of the activation whose entries begin at BASE,
    /// taken off.
    #[inline(always)]
    fn pop_above(&mut self, base: usize) -> Option<T> {
        if self.0.len() > base {
            self.0.pop()
        } else {
            None
        }
    }

    /// The entries above LEN dropped: an activation, or a frame within
    /// one, ended.
    #[inline(always)]
    fn truncate(&mut self, len: usize) {
        self.0.truncate(len);
    }
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
    /// A binding with its restore record in hand (the rare bindings of a
    /// name that is not a symbol constant): boxed, so the common entries
    /// stay a few words.
    Binding(Box<super::super::eval::SpecialBindingRestore>),
    /// Bvarbind of a symbol: the record stays on the interpreter's binding
    /// stack alone, unbound by its id (`Interpreter::unbind_special_id').
    BindingId(u64),
    /// Bsave_excursion: restore buffer and (marker-tracked) point.
    Excursion {
        buffer_id: u64,
        marker_id: crate::lisp::types::MarkerRef,
        saved_pt: usize,
    },
    /// Bsave_current_buffer: restore the current buffer only.
    CurrentBuffer { buffer_id: u64 },
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
            Self::Binding(_) | Self::BindingId(_) | Self::CurrentBuffer { .. } => {}
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
        UnwindEntry::Binding(restore) => interp.restore_special_binding(*restore, env),
        UnwindEntry::BindingId(binding_id) => interp.unbind_special_id(binding_id, env),
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
                let is_function = prim(interp, "functionp", [handler], env)?;
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

#[derive(Clone)]
enum HandlerKind {
    /// Bpushcatch: TAG caught by `eq`.
    Catch(Value),
    /// Bpushconditioncase: the popped conditions clause head.
    ConditionCase(Value),
}

#[derive(Clone)]
pub(crate) struct Handler {
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

/// `prim's dispatch facts by the name literal's address and length: an
/// open-addressed table whose slots, once claimed, keep their name. The
/// facts depend on the name alone, so every thread shares them.
static PRIM_FACTS: [std::sync::OnceLock<(usize, usize, crate::lisp::primitives::NameFacts)>; 256] =
    [const { std::sync::OnceLock::new() }; 256];

/// NAME's facts from `PRIM_FACTS', claiming the first free slot from its
/// address's hash on the first call.
#[inline]
fn prim_facts(name: &'static str) -> crate::lisp::primitives::NameFacts {
    let pointer = name.as_ptr() as usize;
    let mut index = (pointer.wrapping_mul(0x9e37_79b9_7f4a_7c15) >> 56) % PRIM_FACTS.len();
    for _ in 0..PRIM_FACTS.len() {
        let slot = &PRIM_FACTS[index];
        let (address, length, facts) = match slot.get() {
            Some(entry) => *entry,
            None => {
                let facts = crate::lisp::primitives::name_facts(name);
                // Another thread may claim the slot first; its entry is
                // read below like any other.
                let _ = slot.set((pointer, name.len(), facts));
                *slot.get().expect("a claimed slot")
            }
        };
        if address == pointer && length == name.len() {
            return facts;
        }
        index = (index + 1) % PRIM_FACTS.len();
    }
    crate::lisp::primitives::name_facts(name)
}

/// Call a named Emaxx primitive with stack values (the Bcar/Bplus-style
/// single-instruction function ops).
///
/// The names are a closed set of static literals naming GNU's dedicated
/// opcodes (`Bnth', `Bpoint', ...), so their dispatch facts are cached by
/// string address: the general string-keyed probe ran once per executed
/// opcode and was a measurable slice of `nth'-heavy bytecode.
///
/// ARGS are passed by value, so they lie in this frame (or its caller's)
/// for the call, where the conservative stack scan finds them as it finds
/// a C function's locals (alloc.c:mark_stack); `prim_rooted' takes an
/// argument vector held elsewhere.
fn prim<const N: usize>(
    interp: &mut Interpreter,
    name: &'static str,
    args: [Value; N],
    env: &mut Env,
) -> Result<Value, LispError> {
    let facts = prim_facts(name);
    primitives::call_with_facts(interp, name, &facts, &args, env)
}

/// `prim' with arguments in a heap vector, rooted for the call as
/// SAFE_ALLOCA_LISP roots a C function's allocated argument array.
fn prim_rooted(
    interp: &mut Interpreter,
    name: &'static str,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    let facts = prim_facts(name);
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
    /// Inside the verified loop only: an op run in place went on to the
    /// next instruction and stored into no code string (`op_in_place');
    /// the stack's top.
    Continued(usize),
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
    object: &Running,
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
                // A symbol with position is vectorlike.
                if (ops[len - 1].is_vectorlike() || ops[len - 2].is_vectorlike())
                    && (matches!(ops[len - 1].kind(), Kind::SymbolWithPos(_))
                        || matches!(ops[len - 2].kind(), Kind::SymbolWithPos(_)))
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
/// is stored back on the way out. The ops' own arms run no Lisp and
/// collect nothing; a call, or an op whose full arm runs in place
/// (`op_in_place'), may do both, and the loop then re-reads the frame's
/// code and its verification, as after a frame switch.
// The opcode match names its bytes one by one where a range would read
// better: see the comment on its catch-all arm.
#[allow(clippy::manual_range_patterns)]
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
        let mut constants = vm.program.constants;
        // SP addresses the top operand, as bytecode.c's `top' does, so the
        // top is `*sp' and no op needs SP's neighbor first. With no operand
        // it addresses the word below the frame's first slot: the last word
        // of the header beneath the frame, which no op reads.
        // SAFETY: top lies within the active frame, above that header.
        let mut sp = unsafe { words.add(top - 1) };
        let mut at = *pc;
        let exit = 'run: loop {
            // AT stays at the op until its arm moves past it (by the op's
            // length) or jumps, so the head reads the byte and dispatches,
            // and the op's offset is AT itself.
            let offset = at;
            // SAFETY (all code reads below): verified instructions, operands
            // included, lie within the code.
            let byte = unsafe { *code.get_unchecked(at) };
            macro_rules! fetch {
                () => {
                    u16::from(unsafe { *code.get_unchecked(at + 1) })
                };
            }
            macro_rules! fetch2 {
                () => {
                    u16::from_le_bytes(unsafe {
                        [*code.get_unchecked(at + 1), *code.get_unchecked(at + 2)]
                    })
                };
            }
            // SAFETY (stack accesses below): verification found at least the
            // op's operands on the stack and room for its pushes.
            macro_rules! slot {
                ($n:expr) => {
                    unsafe { &mut *sp.sub($n) }
                };
            }
            macro_rules! push {
                ($value:expr) => {{
                    let value = $value;
                    unsafe {
                        sp = sp.add(1);
                        sp.write(value);
                    }
                }};
            }
            macro_rules! drop_values {
                ($count:expr) => {
                    sp = unsafe { sp.sub($count) }
                };
            }
            // An op without an arm here, or with operands its arm does
            // not take, runs its full arm in place.
            macro_rules! slow {
                () => {{
                    // SAFETY: sp lies within the active frame.
                    let top = unsafe { sp.offset_from(words) as usize } + 1;
                    break 'run op_in_place(vm, offset, top, pc);
                }};
            }
            // A jump op is three bytes; one to before its end is backward.
            macro_rules! jump {
                ($target:expr) => {{
                    let destination = usize::from($target);
                    if destination < at + 3 {
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
            // Bcall: a plain call into byte code switches frames here, and
            // a plain call into native code or a subr runs from here; any
            // other runs the full sequence and resumes after it.
            macro_rules! call {
                ($count:expr, $len:expr) => {{
                    let count = $count;
                    let next = at + $len;
                    let argc = usize::from(count);
                    let func = *slot!(argc);
                    // SAFETY: sp lies within the active frame.
                    let top = unsafe { sp.offset_from(words) as usize } + 1;
                    let function = callee_function(func);
                    if let Some(closure) = function.closure_ref() {
                        if let Some((top, _)) =
                            enter_closure_frame(vm, top, next, argc, func, closure, true)
                        {
                            // The callee's frame runs on from pc 0, for which
                            // `entry_program' found its code current and
                            // verified with these operands.
                            // SAFETY: as the outer loop's slice.
                            code = unsafe { vm.program.code.bytes_unguarded() }.0;
                            constants = vm.program.constants;
                            // SAFETY: TOP lies within the new frame.
                            sp = unsafe { words.add(top - 1) };
                            at = 0;
                            continue 'run;
                        }
                    } else if function.is_vectorlike() {
                        // The call ran in this frame, which goes on after
                        // it: its code is still the bytes verified unless
                        // the call stored into the string, and verification
                        // found the call's successor with the operands it
                        // left.
                        // A store into a code string counts a change: an
                        // unchanged count after the call leaves these bytes
                        // and their verification current.
                        let changes = crate::lisp::alloc::code_changes();
                        macro_rules! resume_after_call {
                            ($top:expr, $depth:expr) => {{
                                let top = $top;
                                if crate::lisp::alloc::code_changes() == changes {
                                    // SAFETY: as the outer loop's slice; the
                                    // bytes were current before the call.
                                    code = unsafe { vm.program.code.bytes_unchanged() };
                                    // SAFETY: TOP lies within the frame.
                                    sp = unsafe { words.add(top - 1) };
                                    at = next;
                                    continue 'run;
                                }
                                // SAFETY: as the outer loop's slice.
                                let (bytes, current) = unsafe { vm.program.code.bytes_unguarded() };
                                if current
                                    && vm
                                        .program
                                        .verified
                                        .is_some_and(super::verify::VerifiedCode::is_current)
                                {
                                    code = bytes;
                                    // SAFETY: TOP lies within the frame.
                                    sp = unsafe { words.add(top - 1) };
                                    at = next;
                                    continue 'run;
                                }
                                *pc = next;
                                break 'run FastExit::Switched(top, $depth);
                            }};
                        }
                        macro_rules! raise_from_call {
                            ($error:expr) => {{
                                *pc = next;
                                break 'run FastExit::Raised(
                                    super::Instr {
                                        offset,
                                        len: $len,
                                        op: Op::Call(count),
                                    },
                                    $error,
                                );
                            }};
                        }
                        #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
                        if let Some(native) = function.native_function_ref()
                            && let Some(result) =
                                call_native_nested_by_count(vm, top, argc, func, native)
                        {
                            match result {
                                Ok(value) => {
                                    // The value replaces the function and its
                                    // arguments, the stack's top left as
                                    // `replace_call' leaves it.
                                    let top = top - argc;
                                    // SAFETY: the function's slot lies within
                                    // the frame, below its arguments.
                                    unsafe { *words.add(top - 1) = value };
                                    vm.interp.bc_stack.top = top;
                                    resume_after_call!(top, vm.interp.bc_stack.len())
                                }
                                Err(error) => raise_from_call!(error),
                            }
                        }
                        if let Some(result) = call_plain(vm, top, argc, func, function) {
                            match result {
                                Ok((top, depth)) => resume_after_call!(top, depth),
                                Err(error) => raise_from_call!(error),
                            }
                        }
                    }
                    match bytecode_call(vm, top, argc, next, offset) {
                        Ok(CallOutcome::Entered(top, depth)) => {
                            *pc = 0;
                            break 'run FastExit::Switched(top, depth);
                        }
                        Ok(CallOutcome::Returned(top, depth)) => {
                            *pc = next;
                            break 'run FastExit::Switched(top, depth);
                        }
                        Err(error) => {
                            *pc = next;
                            break 'run FastExit::Raised(
                                super::Instr {
                                    offset,
                                    len: $len,
                                    op: Op::Call(count),
                                },
                                error,
                            );
                        }
                    }
                }};
            }
            // Bvarref of a variable whose cell holds its value: the
            // SYMBOL_PLAINVAL case, and SYMBOL_FORWARDED to an object slot
            // (`unlocalized_value'); any other variable's read in
            // `varref_redirected', and a void one's signal in the full arm.
            macro_rules! varref {
                ($index:expr) => {{
                    let index = usize::from($index);
                    // SAFETY: verified against this vector's length.
                    let symbol = unsafe { constants.get_unchecked(index) };
                    let Kind::Symbol(name) = symbol.kind() else {
                        slow!()
                    };
                    let value = match name.cell().unlocalized_value() {
                        Some(value) => value,
                        None => match varref_redirected(vm.interp, &name) {
                            Some(value) => value,
                            None => slow!(),
                        },
                    };
                    push!(value);
                }};
            }
            // Bvarset's SYMBOL_PLAINVAL, untrapped case: the store alone;
            // a C variable whose cell holds its value in `assign_plain_global'.
            macro_rules! varset {
                ($index:expr) => {{
                    let index = usize::from($index);
                    // SAFETY: verified against this vector's length.
                    let symbol = unsafe { constants.get_unchecked(index) };
                    let Kind::Symbol(name) = symbol.kind() else {
                        slow!()
                    };
                    if !name.cell().store_plain_value(*slot!(0))
                        && !varset_plain(vm.interp, &name, *slot!(0))
                    {
                        slow!();
                    }
                    drop_values!(1);
                }};
            }
            // Bvarbind of a symbol whose let is a plain store
            // (`varbind_plain'); any other, in the full arm.
            macro_rules! varbind {
                ($index:expr) => {{
                    let index = usize::from($index);
                    // SAFETY: verified against this vector's length.
                    let symbol = unsafe { constants.get_unchecked(index) };
                    let Kind::Symbol(name) = symbol.kind() else {
                        slow!()
                    };
                    if !varbind_plain(vm.interp, &name, *slot!(0)) {
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
            // Bconstant: the index is in the opcode.  Each such op has its
            // own arm, and so do the calls and variable ops below with a
            // count or an index in their opcode, so that no arm reads the
            // opcode again: the dispatch's register then holds it alone.
            macro_rules! constant1 {
                ($index:expr) => {{
                    constant!($index);
                    at += 1;
                }};
            }
            // Bsetcar (CAR) or Bsetcdr.
            macro_rules! set_car_cdr {
                ($car:expr) => {{
                    let Some(cell) = slot!(1).cons_ref() else {
                        slow!()
                    };
                    let value = *slot!(0);
                    if $car {
                        cell.car.set(value);
                    } else {
                        cell.cdr.set(value);
                    }
                    *slot!(1) = value;
                    drop_values!(1);
                    at += 1;
                }};
            }
            macro_rules! goto_if {
                ($jump_when_nil:expr) => {{
                    let target = fetch2!();
                    let is_nil = slot!(0).is_nil();
                    drop_values!(1);
                    if is_nil == $jump_when_nil {
                        jump!(target);
                    } else {
                        at += 3;
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
                        at += 3;
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
                    at += 1;
                }};
            }
            macro_rules! comparison {
            ($op:tt) => {{
                let (Some(x), Some(y)) = (slot!(1).fixnum(), slot!(0).fixnum()) else {
                    slow!();
                };
                *slot!(1) = if x $op y { Value::T } else { Value::Nil };
                drop_values!(1);
                at += 1;
            }};
        }
            macro_rules! unary {
                ($op:expr) => {{
                    let Some(x) = slot!(0).fixnum() else { slow!() };
                    let Some(n) = $op(x) else { slow!() };
                    *slot!(0) = Value::Integer(n);
                    at += 1;
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
                        Kind::Nil => {
                            at += 1;
                            continue;
                        }
                        _ if $safe => Value::Nil,
                        // The full arm signals wrong-type-argument.
                        _ => slow!(),
                    };
                    *slot!(0) = replacement;
                    at += 1;
                }};
            }
            match byte {
                0o001 => {
                    stack_ref!(1);
                    at += 1;
                }
                0o002 => {
                    stack_ref!(2);
                    at += 1;
                }
                0o003 => {
                    stack_ref!(3);
                    at += 1;
                }
                0o004 => {
                    stack_ref!(4);
                    at += 1;
                }
                0o005 => {
                    stack_ref!(5);
                    at += 1;
                }
                0o006 => {
                    stack_ref!(usize::from(fetch!()));
                    at += 2;
                }
                0o007 => {
                    stack_ref!(usize::from(fetch2!()));
                    at += 3;
                }
                // Bcall, Bcall1 and Bcall2, the common counts, each with its
                // own copy of the call's path, the count a constant in it.
                0o040 => call!(0u16, 1),
                0o041 => call!(1u16, 1),
                0o042 => call!(2u16, 1),
                // Bcall3 to Bcall7 in one arm, as exec_byte_code's
                // `dofuncall': the count is read here, from the byte or its
                // operand, so that the call's path is one copy of code for
                // these counts. Each count's own copy was 2 to 6 KB of this
                // loop, and a workload calling with several counts kept
                // several of them in the instruction cache; a constant
                // count saves about twenty instructions a call.
                0o043 | 0o044 | 0o045 | 0o046 | 0o047 => {
                    let (count, len) = match byte {
                        0o046 => (fetch!(), 2),
                        0o047 => (fetch2!(), 3),
                        _ => (u16::from(byte - 0o040), 1),
                    };
                    call!(count, len)
                }
                super::B_STACK_SET => {
                    stack_set!(fetch!());
                    at += 2;
                }
                super::B_STACK_SET2 => {
                    stack_set!(fetch2!());
                    at += 3;
                }
                // Bdup.
                0o211 => {
                    let top = *slot!(0);
                    push!(top);
                    at += 1;
                }
                // Bdiscard.
                0o210 => {
                    drop_values!(1);
                    at += 1;
                }
                // Bpoint, Bpoint_max, Bpoint_min and Bcurrent_buffer: the
                // current buffer's PT, ZV and BEGV and the buffer itself,
                // as `exec_op's arms push them, with no call out of the
                // loop (nothing here runs Lisp).
                0o140 => {
                    let point = vm.interp.buffer.borrow().point();
                    push!(Value::Integer(point as i64));
                    at += 1;
                }
                0o144 => {
                    let zv = vm.interp.buffer.borrow().point_max();
                    push!(Value::Integer(zv as i64));
                    at += 1;
                }
                0o145 => {
                    let begv = vm.interp.buffer.borrow().point_min();
                    push!(Value::Integer(begv as i64));
                    at += 1;
                }
                0o160 => {
                    push!(Value::Buffer(vm.interp.buffer));
                    at += 1;
                }
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
                    at += 2;
                }
                0o300 => constant1!(0u16),
                0o301 => constant1!(1u16),
                0o302 => constant1!(2u16),
                0o303 => constant1!(3u16),
                0o304 => constant1!(4u16),
                0o305 => constant1!(5u16),
                0o306 => constant1!(6u16),
                0o307 => constant1!(7u16),
                0o310 => constant1!(8u16),
                0o311 => constant1!(9u16),
                0o312 => constant1!(10u16),
                0o313 => constant1!(11u16),
                0o314 => constant1!(12u16),
                0o315 => constant1!(13u16),
                0o316 => constant1!(14u16),
                0o317 => constant1!(15u16),
                0o320 => constant1!(16u16),
                0o321 => constant1!(17u16),
                0o322 => constant1!(18u16),
                0o323 => constant1!(19u16),
                0o324 => constant1!(20u16),
                0o325 => constant1!(21u16),
                0o326 => constant1!(22u16),
                0o327 => constant1!(23u16),
                0o330 => constant1!(24u16),
                0o331 => constant1!(25u16),
                0o332 => constant1!(26u16),
                0o333 => constant1!(27u16),
                0o334 => constant1!(28u16),
                0o335 => constant1!(29u16),
                0o336 => constant1!(30u16),
                0o337 => constant1!(31u16),
                0o340 => constant1!(32u16),
                0o341 => constant1!(33u16),
                0o342 => constant1!(34u16),
                0o343 => constant1!(35u16),
                0o344 => constant1!(36u16),
                0o345 => constant1!(37u16),
                0o346 => constant1!(38u16),
                0o347 => constant1!(39u16),
                0o350 => constant1!(40u16),
                0o351 => constant1!(41u16),
                0o352 => constant1!(42u16),
                0o353 => constant1!(43u16),
                0o354 => constant1!(44u16),
                0o355 => constant1!(45u16),
                0o356 => constant1!(46u16),
                0o357 => constant1!(47u16),
                0o360 => constant1!(48u16),
                0o361 => constant1!(49u16),
                0o362 => constant1!(50u16),
                0o363 => constant1!(51u16),
                0o364 => constant1!(52u16),
                0o365 => constant1!(53u16),
                0o366 => constant1!(54u16),
                0o367 => constant1!(55u16),
                0o370 => constant1!(56u16),
                0o371 => constant1!(57u16),
                0o372 => constant1!(58u16),
                0o373 => constant1!(59u16),
                0o374 => constant1!(60u16),
                0o375 => constant1!(61u16),
                0o376 => constant1!(62u16),
                0o377 => constant1!(63u16),
                super::B_CONSTANT2 => {
                    constant!(fetch2!());
                    at += 3;
                }
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
                    // SAFETY: sp lies within the active frame.
                    let top = unsafe { sp.offset_from(words) as usize } + 1;
                    // With no caller frame here (or unwinding to do), the
                    // return leaves the loop, the top stored as any exit's.
                    if let Some((resume, top, depth, unchanged)) =
                        return_to_bytecode_frame(vm, top, value)
                    {
                        // The caller's frame runs on here if its code is
                        // still the bytes verified and verification found
                        // the call's successor with these operands: as
                        // the verified loop's call found them, when no
                        // verified code has changed since.
                        // SAFETY: as the outer loop's slice.
                        let (bytes, current) = unsafe { vm.program.code.bytes_unguarded() };
                        if unchanged
                            || current
                                && vm
                                    .program
                                    .verified
                                    .is_some_and(|verified| verified.admits(resume, depth))
                        {
                            code = bytes;
                            constants = vm.program.constants;
                            // SAFETY: TOP lies within the caller's frame.
                            sp = unsafe { words.add(top - 1) };
                            at = resume;
                            continue 'run;
                        }
                        *pc = resume;
                        break 'run FastExit::Switched(top, depth);
                    }
                    break 'run FastExit::Return(value);
                }
                // Bnot.
                0o077 => {
                    let top = slot!(0);
                    *top = if top.is_nil() { Value::T } else { Value::Nil };
                    at += 1;
                }
                // Bcons.
                0o102 => {
                    let cdr = *slot!(0);
                    let car = *slot!(1);
                    *slot!(1) = Value::cons(car, cdr);
                    drop_values!(1);
                    at += 1;
                }
                // Beq.
                0o075 => {
                    let (left, right) = (*slot!(1), *slot!(0));
                    // A symbol with position is vectorlike.
                    if (left.is_vectorlike() || right.is_vectorlike())
                        && (matches!(left.kind(), Kind::SymbolWithPos(_))
                            || matches!(right.kind(), Kind::SymbolWithPos(_)))
                    {
                        slow!();
                    }
                    let equal = crate::lisp::primitives::values_eq_plain(&left, &right);
                    *slot!(1) = if equal { Value::T } else { Value::Nil };
                    drop_values!(1);
                    at += 1;
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
                    at += 1;
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
                    at += 1;
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
                    at += 1;
                }
                0o100 => car_cdr!(true, false),
                0o101 => car_cdr!(false, false),
                0o242 => car_cdr!(true, true),
                0o243 => car_cdr!(false, true),
                // Bsymbolp; a symbol with position answers by
                // `symbols-with-pos-enabled' in the full arm.
                0o071 => {
                    let value = *slot!(0);
                    let symbolp = if value.is_symbol() {
                        true
                    } else if value.is_vectorlike()
                        && matches!(value.kind(), Kind::SymbolWithPos(_))
                    {
                        slow!()
                    } else {
                        false
                    };
                    *slot!(0) = if symbolp { Value::T } else { Value::Nil };
                    at += 1;
                }
                // Bstringp.
                0o073 => {
                    let stringp = slot!(0).is_string();
                    *slot!(0) = if stringp { Value::T } else { Value::Nil };
                    at += 1;
                }
                // Blistp.
                0o074 => {
                    let value = *slot!(0);
                    let listp = value.is_nil() || value.is_cons();
                    *slot!(0) = if listp { Value::T } else { Value::Nil };
                    at += 1;
                }
                // Bnumberp.
                0o247 => {
                    let value = *slot!(0);
                    let numberp = value.fixnum().is_some()
                        || value.is_float()
                        || (value.is_vectorlike() && value.is_integer());
                    *slot!(0) = if numberp { Value::T } else { Value::Nil };
                    at += 1;
                }
                // Bintegerp.
                0o250 => {
                    let value = *slot!(0);
                    let integerp =
                        value.fixnum().is_some() || (value.is_vectorlike() && value.is_integer());
                    *slot!(0) = if integerp { Value::T } else { Value::Nil };
                    at += 1;
                }
                // Bmemq: each element compared by lisp.h:EQ, which is
                // BASE_EQ (`values_eq_plain') while `symbols-with-pos-enabled'
                // is nil; the full arm takes the variable's other value.
                // FOR_EACH_TAIL's first quit check comes after the second
                // element: nothing here can set `quit-flag', so one test
                // there stands for every later one. A long or improper list
                // takes the full arm, which checks for cycles and the list's
                // end.
                0o076 => {
                    if vm.interp.symbols_with_positions_enabled() {
                        slow!();
                    }
                    let element = *slot!(1);
                    let mut tail = *slot!(0);
                    let mut steps = 0u32;
                    let found = loop {
                        let Some(cell) = tail.cons_ref() else {
                            if tail.is_nil() {
                                break Value::Nil;
                            }
                            slow!()
                        };
                        if crate::lisp::primitives::values_eq_plain(&cell.car.get(), &element) {
                            break tail;
                        }
                        steps += 1;
                        if steps == 64 || (steps == 2 && !vm.interp.quit_flag_is_nil()) {
                            slow!();
                        }
                        tail = cell.cdr.get();
                    };
                    *slot!(1) = found;
                    drop_values!(1);
                    at += 1;
                }
                // Bget of two bare symbols (`get_plain').
                0o116 => {
                    let Some(value) = get_plain(vm.interp, *slot!(1), *slot!(0)) else {
                        slow!()
                    };
                    *slot!(1) = value;
                    drop_values!(1);
                    at += 1;
                }
                // Blist1 to Blist4: the operands consed as the full arm
                // conses them.
                0o103 => {
                    *slot!(0) = Value::list_of(&[*slot!(0)]);
                    at += 1;
                }
                0o104 => {
                    let list = Value::list_of(&[*slot!(1), *slot!(0)]);
                    drop_values!(1);
                    *slot!(0) = list;
                    at += 1;
                }
                0o105 => {
                    let list = Value::list_of(&[*slot!(2), *slot!(1), *slot!(0)]);
                    drop_values!(2);
                    *slot!(0) = list;
                    at += 1;
                }
                0o106 => {
                    let list = Value::list_of(&[*slot!(3), *slot!(2), *slot!(1), *slot!(0)]);
                    drop_values!(3);
                    *slot!(0) = list;
                    at += 1;
                }
                // Bunbind 1 of an entry whose undoing runs nothing
                // (`unbind_plain').
                0o051 => {
                    if !unbind_plain(vm.interp) {
                        slow!();
                    }
                    at += 1;
                }
                // Bsave_current_buffer (and its obsolete opcode): the
                // current buffer on the unwind list.
                0o162 | 0o141 => {
                    save_current_buffer(vm.interp);
                    at += 1;
                }
                // Bset_buffer of a live buffer object (`set_buffer_object').
                0o161 => {
                    let Some(buffer) = set_buffer_object(vm.interp, *slot!(0)) else {
                        slow!()
                    };
                    *slot!(0) = buffer;
                    at += 1;
                }
                // Bgoto_char to a fixnum position; GNU's Fgoto_char returns
                // its argument.
                0o142 => {
                    let Some(position) = slot!(0).fixnum() else {
                        slow!()
                    };
                    goto_char_fixnum(vm.interp, position);
                    at += 1;
                }
                // Bswitch on a table with a standard test
                // (`switch_destination'): a hit jumps, through the frame
                // switch's admission test, since the table, not the
                // verifier, names the destination; a miss goes on.
                0o267 => {
                    let Some(destination) =
                        switch_destination(vm.interp, vm.env, *slot!(0), *slot!(1))
                    else {
                        slow!()
                    };
                    drop_values!(2);
                    match destination {
                        Some(destination) => {
                            *pc = destination;
                            // SAFETY: sp lies within the active frame.
                            let top = unsafe { sp.offset_from(words) as usize } + 1;
                            vm.interp.bc_stack.top = top;
                            break 'run FastExit::Switched(top, vm.interp.bc_stack.len());
                        }
                        None => at += 1,
                    }
                }
                // Bsetcar and Bsetcdr on a cons; anything else signals in the
                // full arm.
                0o240 => set_car_cdr!(true),
                0o241 => set_car_cdr!(false),
                0o010 => {
                    varref!(0u16);
                    at += 1;
                }
                0o011 => {
                    varref!(1u16);
                    at += 1;
                }
                0o012 => {
                    varref!(2u16);
                    at += 1;
                }
                0o013 => {
                    varref!(3u16);
                    at += 1;
                }
                0o014 => {
                    varref!(4u16);
                    at += 1;
                }
                0o015 => {
                    varref!(5u16);
                    at += 1;
                }
                0o016 => {
                    varref!(fetch!());
                    at += 2;
                }
                0o017 => {
                    varref!(fetch2!());
                    at += 3;
                }
                0o020 => {
                    varset!(0u16);
                    at += 1;
                }
                0o021 => {
                    varset!(1u16);
                    at += 1;
                }
                0o022 => {
                    varset!(2u16);
                    at += 1;
                }
                0o023 => {
                    varset!(3u16);
                    at += 1;
                }
                0o024 => {
                    varset!(4u16);
                    at += 1;
                }
                0o025 => {
                    varset!(5u16);
                    at += 1;
                }
                0o026 => {
                    varset!(fetch!());
                    at += 2;
                }
                0o027 => {
                    varset!(fetch2!());
                    at += 3;
                }
                0o030 => {
                    varbind!(0u16);
                    at += 1;
                }
                0o031 => {
                    varbind!(1u16);
                    at += 1;
                }
                0o032 => {
                    varbind!(2u16);
                    at += 1;
                }
                0o033 => {
                    varbind!(3u16);
                    at += 1;
                }
                0o034 => {
                    varbind!(4u16);
                    at += 1;
                }
                0o035 => {
                    varbind!(5u16);
                    at += 1;
                }
                0o036 => {
                    varbind!(fetch!());
                    at += 2;
                }
                0o037 => {
                    varbind!(fetch2!());
                    at += 3;
                }
                // Every other opcode, named so that the match covers all 256
                // bytes and its jump table needs no range check.  Each byte
                // in this match is its own case: rustc tests a range
                // pattern with comparisons ahead of the switch, which
                // brought back the range check and sent constants and
                // calls through branches before the table.
                0o000 | 0o050 | 0o052 | 0o053 | 0o054 | 0o055 | 0o056 | 0o057 | 0o060 | 0o061
                | 0o062 | 0o063 | 0o064 | 0o065 | 0o066 | 0o067 | 0o070 | 0o107 | 0o112 | 0o113
                | 0o114 | 0o115 | 0o117 | 0o120 | 0o121 | 0o122 | 0o135 | 0o136 | 0o143 | 0o146
                | 0o147 | 0o150 | 0o151 | 0o152 | 0o153 | 0o154 | 0o155 | 0o156 | 0o157 | 0o163
                | 0o164 | 0o165 | 0o166 | 0o167 | 0o170 | 0o171 | 0o172 | 0o173 | 0o174 | 0o175
                | 0o176 | 0o177 | 0o200 | 0o212 | 0o213 | 0o214 | 0o215 | 0o216 | 0o217 | 0o220
                | 0o221 | 0o222 | 0o223 | 0o224 | 0o225 | 0o226 | 0o227 | 0o230 | 0o231 | 0o232
                | 0o233 | 0o234 | 0o235 | 0o236 | 0o237 | 0o244 | 0o251 | 0o252 | 0o253 | 0o254
                | 0o255 | 0o256 | 0o257 | 0o260 | 0o261 | 0o264 | 0o265 | 0o270 | 0o271 | 0o272
                | 0o273 | 0o274 | 0o275 | 0o276 | 0o277 => slow!(),
            }
        };
        if let FastExit::Continued(new_top) = exit {
            // The frame goes on at the op's successor, which verification
            // found with the operands the op left.
            // SAFETY: as the caller's slice; the bytes were current before
            // the op and it stored into no code string.
            code = unsafe { vm.program.code.bytes_unchanged() };
            top = new_top;
            continue;
        }
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
        return (exit, Some(unsafe { sp.offset_from(words) as usize } + 1));
    }
}

// The verified loop's helpers for ops whose usual case reads or changes
// the interpreter's state but runs no Lisp, collects nothing and stores
// into no string, so that the loop goes on after them with its bytes: each
// does nothing and answers false (or None) for any other case, which the
// op's full arm in `op_in_place' takes from the start, its signals
// included.

/// Bvarref of a variable whose cell does not hold its value (localized,
/// held by the buffer, a counter or a KBOARD variable, an alias): the
/// rest of data.c:find_symbol_value, which reads and runs nothing else.
/// None for a void variable.
#[inline(never)]
fn varref_redirected(interp: &Interpreter, symbol: &SymbolName) -> Option<Value> {
    interp.symbol_value_cell_symbol(symbol).ok()
}

/// Bvarset of a C variable whose cell holds its value
/// (`assign_plain_global').
#[inline(never)]
fn varset_plain(interp: &mut Interpreter, symbol: &SymbolName, value: Value) -> bool {
    interp.assign_plain_global(symbol, value)
}

/// Bvarbind of a symbol whose let is a plain store
/// (`Interpreter::specbind_plain'), its record on the unwind list.
#[inline(never)]
fn varbind_plain(interp: &mut Interpreter, symbol: &SymbolName, value: Value) -> bool {
    let Some(binding_id) = interp.specbind_plain(symbol, value) else {
        return false;
    };
    interp.bc_unwinds.push(UnwindEntry::BindingId(binding_id));
    true
}

/// Bunbind 1 of the unwind list's last entry when undoing it runs nothing:
/// a plain binding's restore (`Interpreter::unbind_plain_special_id') or a
/// saved current buffer's (`unwind_one's).
#[inline(never)]
fn unbind_plain(interp: &mut Interpreter) -> bool {
    match interp.bc_unwinds.last() {
        Some(&UnwindEntry::BindingId(binding_id)) => {
            if !interp.unbind_plain_special_id(binding_id) {
                return false;
            }
        }
        Some(&UnwindEntry::CurrentBuffer { buffer_id }) => {
            if interp.has_buffer_id(buffer_id) {
                let _ = interp.set_current_buffer_id(buffer_id);
            }
        }
        _ => return false,
    }
    interp.bc_unwinds.pop();
    true
}

/// Bsave_current_buffer: the current buffer on the unwind list.
#[inline(never)]
fn save_current_buffer(interp: &mut Interpreter) {
    let buffer_id = interp.current_buffer_id();
    interp
        .bc_unwinds
        .push(UnwindEntry::CurrentBuffer { buffer_id });
}

/// Bset_buffer of a live buffer object: buffer.c:Fset_buffer's
/// set_buffer_internal, the buffer its value.  None for a name or a
/// killed buffer.
#[inline(never)]
fn set_buffer_object(interp: &mut Interpreter, buffer: Value) -> Option<Value> {
    if !matches!(buffer.kind(), Kind::Buffer(_)) {
        return None;
    }
    let id = interp.resolve_buffer_id(&buffer).ok()?;
    interp.set_current_buffer_id(id).ok()?;
    interp.buffer_value(id)
}

/// Bgoto_char to POSITION, clamped as `position_from_value' and the
/// buffer's motion clamp it.
#[inline(never)]
fn goto_char_fixnum(interp: &mut Interpreter, position: i64) {
    interp
        .buffer
        .borrow_mut()
        .goto_char(position.max(0) as usize);
}

/// Bget of two bare symbols with no overriding plist environment: fns.c:
/// Fget's plist_get on the symbol's plist (`direct_get's first case).
#[inline(never)]
fn get_plain(interp: &Interpreter, symbol: Value, property: Value) -> Option<Value> {
    let (Kind::Symbol(symbol), Kind::Symbol(property)) = (symbol.kind(), property.kind()) else {
        return None;
    };
    if !interp.overriding_plist_environment_is_nil() {
        return None;
    }
    Some(
        interp
            .get_symbol_property_of(&symbol, &property)
            .unwrap_or(Value::Nil),
    )
}

/// Bswitch's lookup of VALUE in TABLE, a hash table with a standard test
/// (eq, eql or equal, whose hashing and comparison run no Lisp): the
/// destination an entry's fixnum names, Some(None) for no entry or
/// another value, as the full arm's `gethash' and its fixnum test.
#[inline(never)]
fn switch_destination(
    interp: &mut Interpreter,
    env: &mut Env,
    table: Value,
    value: Value,
) -> Option<Option<usize>> {
    let Kind::HashTable(table) = table.kind() else {
        return None;
    };
    table.test().standard_test()?;
    let (_, slot) = crate::lisp::primitives::hash_table_lookup(interp, table, &value, env).ok()?;
    Some(
        slot.and_then(|slot| table.entry(slot))
            .and_then(|(_, destination)| match destination.kind() {
                Kind::Integer(destination) => Some(destination as usize),
                _ => None,
            }),
    )
}

/// The verified loop's way to an op it has no arm for (or whose operands
/// its arm does not take): `exec_op' runs the op at OFFSET in place, on
/// the stack below TOP, as exec_byte_code runs every op in one loop. An op
/// that goes on to the next instruction and stored into no code string
/// answers the stack's top (`Continued'), and the loop resumes there as
/// after a call. After a jump or a store into a code string the answer is
/// the stack's top and the frame's depth, as after a frame switch, since
/// the op may have run Lisp; after a signal, the signal. An op that does
/// not decode leaves the loop with the stack as it was.
#[inline(never)]
fn op_in_place(vm: &mut VmFrames<'_>, offset: usize, top: usize, pc: &mut usize) -> FastExit {
    let changes = crate::lisp::alloc::code_changes();
    // The frame's code, which the loop reads only by its verified offsets
    // and so holds without its length.
    // SAFETY: as the verified loop's: nothing runs Lisp, collects or
    // stores into a string before the instruction is decoded.
    let code = unsafe { vm.program.code.bytes_unguarded() }.0;
    let constants = vm.program.constants.len();
    // SAFETY: a verified instruction lies within the code.
    let byte = unsafe { *code.get_unchecked(offset) };
    let instr = match super::operandless_op(byte) {
        Some(op) => super::Instr { offset, len: 1, op },
        None => match super::fetch_family_instruction(code, offset, constants) {
            Some(instr) => instr,
            None => match super::fetch_instruction(code, offset, constants) {
                Ok(instr) => instr,
                Err(error) => {
                    *pc = offset;
                    return FastExit::Error(error);
                }
            },
        },
    };
    // The verified loop runs these itself, and never leaves one to its
    // full arm.
    debug_assert!(!matches!(
        instr.op,
        Op::StackRef(_)
            | Op::StackSet(_)
            | Op::DiscardN { .. }
            | Op::Discard
            | Op::Dup
            | Op::Constant(_)
            | Op::Constant2(_)
            | Op::Call(_)
            | Op::Goto { .. }
            | Op::GotoIfNil { .. }
            | Op::GotoIfNonNil { .. }
            | Op::GotoIfNilElsePop { .. }
            | Op::GotoIfNonNilElsePop { .. }
            | Op::Return
    ));
    let state: &mut crate::lisp::eval::InterpreterState = vm.interp;
    state.bc_stack.top = top;
    let next = offset + instr.len;
    *pc = next;
    match exec_op_inline(vm, instr.op, pc) {
        Ok(()) => {
            let state: &mut crate::lisp::eval::InterpreterState = vm.interp;
            if *pc == next && crate::lisp::alloc::code_changes() == changes {
                debug_assert!(
                    vm.program
                        .verified
                        .is_some_and(|verified| verified.admits(next, state.bc_stack.len())),
                    "an op left other operands than verification found"
                );
                return FastExit::Continued(state.bc_stack.top);
            }
            FastExit::Switched(state.bc_stack.top, state.bc_stack.len())
        }
        Err(error) => FastExit::Raised(instr, error),
    }
}

/// exec_byte_code's arm for every op the fast loops leave to the
/// interpreter: variables, bindings and unwinding, handlers, buffer and
/// primitive ops, and each fast op's operands of another kind. PC is
/// already past the instruction; Bswitch moves it. The verified loop runs
/// these in place and the full dispatch runs them after the unverified
/// loop.
#[inline(never)]
fn exec_op(vm: &mut VmFrames<'_>, op: Op, pc: &mut usize) -> Result<(), LispError> {
    exec_op_inline(vm, op, pc)
}

/// `exec_op's body, inline in `op_in_place', the verified loop's way to
/// these ops (a call and a second prologue for each of them).
#[inline(always)]
fn exec_op_inline(vm: &mut VmFrames<'_>, op: Op, pc: &mut usize) -> Result<(), LispError> {
    let VmFrames {
        interp,
        env,
        program,
        handlers_base,
        op_error_frames,
        ..
    } = vm;
    let interp: &mut Interpreter = interp;
    let env: &mut Env = env;
    let program: &mut Running = program;
    let handlers_base: usize = *handlers_base;
    let op_error_frames: &mut usize = op_error_frames;
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
    match op {
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
                _ => prim(interp, "symbol-value", [name], env)?,
            };
            push!(value);
        }
        Op::VarSet(index) => {
            // bytecode.c:Bvarset is set_internal on the constant.
            let name = program.constant(index);
            let value = pop!();
            match name.kind() {
                Kind::Symbol(symbol) => {
                    crate::lisp::primitives::set_internal_symbol(interp, &symbol, value, env)?;
                }
                _ => {
                    prim(interp, "set", [name, value], env)?;
                }
            }
        }
        Op::VarBind(index) => {
            // bytecode.c:Bvarbind is specbind on the constant.
            // A symbol constant is bound as that very object, as
            // Bvarref and Bvarset read and set it, uninterned or not.
            let name = program.constant(index);
            let value = pop!();
            let entry = match name.symbol_cell() {
                Some(symbol) => {
                    UnwindEntry::BindingId(interp.specbind_symbol_id(&symbol, value, env)?)
                }
                None => {
                    let name = name.as_symbol().map_err(|_| {
                        LispError::Signal("varbind constant must be a symbol".into())
                    })?;
                    UnwindEntry::Binding(Box::new(interp.bind_special_variable(name, value, env)?))
                }
            };
            interp.bc_unwinds.push(entry);
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
            let value = interp.with_lisp_stack_roots(&(&body, &tag), |interp| {
                match interp.eval(&body, env) {
                    Err(error) => match error.kind() {
                        LispErrorKind::Throw(thrown, thrown_value) => {
                            let matched = interp.with_lisp_stack_roots(&error, |interp| {
                                prim(interp, "eq", [tag, *thrown], env)
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
            let value = interp.with_lisp_stack_roots(&form, |interp| interp.eval(&form, env))?;
            push!(value);
        }
        Op::TempOutputBufferSetup => {
            // Obsolete since 24.1: create/erase the temp buffer and
            // bind standard-output to it (GNU temp_output_buffer_setup
            // specbinds, so Bunbind pops it).
            let name = pop!();
            let buffer = prim(interp, "get-buffer-create", [name], env)?;
            let saved = interp.current_buffer_id();
            let buffer_id = interp.resolve_buffer_id(&buffer)?;
            let _ = interp.set_current_buffer_id(buffer_id);
            prim(interp, "erase-buffer", [], env)?;
            let _ = interp.set_current_buffer_id(saved);
            let restore = interp.bind_special_variable("standard-output", buffer, env)?;
            interp
                .bc_unwinds
                .push(UnwindEntry::Binding(Box::new(restore)));
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
                    interp.call_function_value(show, None, std::slice::from_ref(&buffer), env)?;
                } else {
                    prim(interp, "display-buffer", [buffer], env)?;
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
        // Bpoint, Bpoint_max, Bpoint_min and Bcurrent_buffer push the
        // buffer's PT, ZV, BEGV and the buffer itself, as the primitives
        // answer, without looking them up by name.
        Op::Point => {
            let point = interp.buffer.borrow().point();
            push!(Value::Integer(point as i64));
        }
        Op::PointMax => {
            let zv = interp.buffer.borrow().point_max();
            push!(Value::Integer(zv as i64));
        }
        Op::PointMin => {
            let begv = interp.buffer.borrow().point_min();
            push!(Value::Integer(begv as i64));
        }
        Op::CurrentBuffer => {
            let buffer = interp.buffer;
            push!(Value::Buffer(buffer));
        }
        Op::FollowingChar
        | Op::PrecedingChar
        | Op::CurrentColumn
        | Op::Eolp
        | Op::Eobp
        | Op::Bolp
        | Op::Bobp
        | Op::Widen => {
            let name = match op {
                Op::FollowingChar => "following-char",
                Op::PrecedingChar => "preceding-char",
                Op::CurrentColumn => "current-column",
                Op::Eolp => "eolp",
                Op::Eobp => "eobp",
                Op::Bolp => "bolp",
                Op::Bobp => "bobp",
                _ => "widen",
            };
            let value = prim(interp, name, [], env)?;
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
            let value = prim(interp, name, [a], env)?;
            push!(value);
        }
        Op::IndentTo => {
            // GNU passes an explicit nil MINIMUM.
            let column = pop!();
            let value = prim(interp, "indent-to", [column, Value::Nil], env)?;
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
            let value = prim(interp, name, [a, b], env)?;
            push!(value);
        }
        Op::SetMarker => {
            let position_buffer = pop!();
            let position = pop!();
            let marker = pop!();
            let value = prim(
                interp,
                "set-marker",
                [marker, position, position_buffer],
                env,
            )?;
            push!(value);
        }
        Op::InsertN(n) => {
            let start = interp.bc_stack.len() - n as usize;
            let items: Vec<Value> = interp.bc_stack.drain_from(start).collect();
            let value = prim_rooted(interp, "insert", &items, env)?;
            push!(value);
        }
        Op::PushCatch { target } => {
            let tag = pop!();
            // `throw' consults the interpreter's active-catch
            // registry before unwinding; a VM catch frame must be
            // visible there like any `sf_catch' frame.
            interp.push_active_catch_tag(tag);
            let handler = Handler {
                kind: HandlerKind::Catch(tag),
                dest: target as usize,
                stack_len: interp.bc_stack.len(),
                unwind_len: interp.bc_unwinds.len(),
                registry_start: None,
            };
            interp.bc_handlers.push(handler);
        }
        Op::PushConditionCase { target } => {
            let conditions = pop!();
            // Register the clause head with the interpreter so
            // signal-time `handler-bind' dispatch at native-call
            // boundaries sees this frame before any outer handler,
            // exactly like an interpreted `condition-case'.
            let registry_start = interp.push_condition_case_handler(vec![conditions]);
            let handler = Handler {
                kind: HandlerKind::ConditionCase(conditions),
                dest: target as usize,
                stack_len: interp.bc_stack.len(),
                unwind_len: interp.bc_unwinds.len(),
                registry_start: Some(registry_start),
            };
            interp.bc_handlers.push(handler);
        }
        Op::PopHandler => {
            if let Some(handler) = interp.bc_handlers.pop_above(handlers_base) {
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
            let dest = prim(interp, "gethash", [value, table, Value::Nil], env)?;
            if let Kind::Integer(dest) = dest.kind() {
                *pc = dest as usize;
            }
        }
        // bytecode.c:Blist and BlistN: the operands consed from the stack,
        // as Flist does.
        Op::ListN(n) => {
            let start = interp.bc_stack.len() - n as usize;
            let list = Value::list_of(&interp.bc_stack[start..]);
            interp.bc_stack.truncate(start);
            push!(list);
        }
        Op::ConcatN(n) => {
            let start = interp.bc_stack.len() - n as usize;
            let items: Vec<Value> = interp.bc_stack.drain_from(start).collect();
            let value = prim_rooted(interp, "concat", &items, env)?;
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
            let list = Value::list_of(&interp.bc_stack[start..]);
            interp.bc_stack.truncate(start);
            push!(list);
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
                return Ok(());
            }
            let name = match op {
                Op::Eqlsign => "=",
                Op::Gtr => ">",
                Op::Lss => "<",
                Op::Leq => "<=",
                _ => ">=",
            };
            let value = prim(interp, name, [a, b], env)?;
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
                    return Ok(());
                }
            }
            let name = match op {
                Op::Plus => "+",
                Op::Diff => "-",
                _ => "*",
            };
            let value = prim(interp, name, [a, b], env)?;
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
                return Ok(());
            }
            let name = if matches!(op, Op::Max) { "max" } else { "min" };
            let value = prim(interp, name, [a, b], env)?;
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
            let value = match prim(interp, name, call_args, env) {
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
                        *op_error_frames += 1;
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
                    let value = match prim(interp, name, call_args, env) {
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
                                *op_error_frames += 1;
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
                    return Ok(());
                }
            }
            let name = match op {
                Op::Add1 => "1+",
                Op::Sub1 => "1-",
                _ => "-",
            };
            let value = prim(interp, name, [a], env)?;
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
                &sequence,
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
            let value = prim(interp, name, [a], env)?;
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
            let value = match prim(interp, name, call_args, env) {
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
                        *op_error_frames += 1;
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
            let value = prim(interp, "concat", [a, b, c, d], env)?;
            push!(value);
        }
        Op::StackRef(_)
        | Op::StackSet(_)
        | Op::DiscardN { .. }
        | Op::Discard
        | Op::Dup
        | Op::Constant(_)
        | Op::Constant2(_)
        | Op::Call(_)
        | Op::Goto { .. }
        | Op::GotoIfNil { .. }
        | Op::GotoIfNonNil { .. }
        | Op::GotoIfNilElsePop { .. }
        | Op::GotoIfNonNilElsePop { .. }
        | Op::Return => unreachable!("the dispatch loop runs {op:?} itself"),
    }
    Ok(())
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

#[cfg(test)]
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
    // The fields as Bcall's frame entry decoded and verified them (see
    // `entry_program'), when its packed list takes the arguments.
    if let Some(program) = super::verify::entry_program(closure) {
        // SAFETY: nothing runs Lisp or collects before the copy below.
        let program = unsafe { program.as_ref() };
        if program.takes(args.len()) {
            return run_entered(interp, *program, Value::Closure(closure), args, env);
        }
    }
    let program = closure_program(closure)?;
    run(interp, program, Value::Closure(closure), args, env)
}

/// Ffuncall of a byte-code closure from Rust (`mapatoms', `mapcar', a
/// hook's function): `execute_closure' under the nil environment frame
/// byte code runs in, with the entered frame set up in this one function
/// rather than in `run_entered' below it.
#[inline(never)]
pub(crate) fn funcall_closure(
    interp: &mut Interpreter,
    closure: crate::lisp::types::ClosureRef,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    let depth = env.len();
    env.push(crate::lisp::types::EnvFrame::dynamic());
    let result = match super::verify::entry_program(closure) {
        // SAFETY: nothing runs Lisp or collects before the copy.
        Some(program) if unsafe { program.as_ref() }.takes(args.len()) => {
            let program = unsafe { *program.as_ref() };
            run_entered_inline(interp, program, Value::Closure(closure), args, env)
        }
        _ => closure_program(closure)
            .and_then(|program| run(interp, program, Value::Closure(closure), args, env)),
    };
    env.truncate(depth);
    result
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
    let marks = EntryMarks::of(interp);
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
    finish_activation(interp, env, result, marks)
}

/// `run' for a closure whose fields and verification the frame entry
/// cache holds (`entry_program'), with arguments its packed list takes:
/// setup_frame's pushes and then the verified loop at once, as
/// exec_byte_code goes from its prologue to the first op. A return from
/// the entry frame finishes here; any other exit goes on in `run_loop'.
#[inline(never)]
fn run_entered(
    interp: &mut Interpreter,
    program: super::verify::EntryProgram,
    function: Value,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    run_entered_inline(interp, program, function, args, env)
}

#[inline(always)]
fn run_entered_inline(
    interp: &mut Interpreter,
    program: super::verify::EntryProgram,
    function: Value,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    let entry_depth = program.entry_depth;
    interp.bc_stack.ensure_allocated();
    let marks = EntryMarks::of(interp);
    let (frames_base, handlers_base) = (marks.suspended, marks.handlers);
    let result = 'entry: {
        if let Err(error) = interp
            .bc_stack
            .enter_frame(program.stack_depth, function, 0)
        {
            break 'entry Err(error);
        }
        // The caller checked the count against the list and the list's
        // slots against the frame's depth.
        interp.bc_stack.push_arguments(args, entry_depth);
        let mut program = Running::entered(&program);
        let mut op_error_frames = 0;
        let mut pc = 0;
        let mut quitcounter = 1;
        // SAFETY: as in `run_loop': the verified loop runs no Lisp, collects
        // nothing and stores into no string while it reads the bytes.
        let (code, current) = unsafe { program.code.bytes_unguarded() };
        let exit = if current
            && program
                .verified
                .is_some_and(|verified| verified.admits(0, entry_depth))
        {
            let words = interp.bc_stack.words.as_mut_ptr().cast::<Value>();
            let top = interp.bc_stack.top;
            let (verified_exit, top) = run_verified(
                &mut VmFrames {
                    interp,
                    env,
                    program: &mut program,
                    frames_base,
                    handlers_base,
                    op_error_frames: &mut op_error_frames,
                    trace_errors: trace_load_errors(),
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
            match verified_exit {
                FastExit::Return(value)
                    if interp.bc_frames.len() == frames_base
                        && interp.bc_handlers.len() == handlers_base =>
                {
                    // The return owns nothing to drop.
                    std::mem::forget(verified_exit);
                    break 'entry Ok(value);
                }
                FastExit::Resume => None,
                verified_exit => Some(verified_exit),
            }
        } else {
            None
        };
        run_loop(
            interp,
            env,
            Activation {
                program,
                pc,
                frames_base,
                handlers_base,
                op_error_frames,
                quitcounter,
            },
            exit,
        )
    };
    finish_activation(interp, env, result, marks)
}

/// The thread's lengths an activation entered from Rust found on entry and
/// leaves behind: its byte-code stack's frame, base and top, its specpdl
/// entries, backtrace frames, suspended byte-code frames and handlers.
#[derive(Clone, Copy)]
struct EntryMarks {
    stack: (usize, usize, usize),
    unwinds: usize,
    backtrace: usize,
    suspended: usize,
    handlers: usize,
}

impl EntryMarks {
    #[inline(always)]
    fn of(interp: &Interpreter) -> Self {
        Self {
            stack: (
                interp.bc_stack.frame,
                interp.bc_stack.base,
                interp.bc_stack.top,
            ),
            unwinds: interp.bc_unwinds.len(),
            backtrace: interp.backtrace_frames_len(),
            suspended: interp.bc_frames.len(),
            handlers: interp.bc_handlers.len(),
        }
    }
}

/// The end of an activation entered from Rust: the frames its signaling
/// byte ops recorded and the specpdl-style entries an abnormal exit left
/// above the MARKS undone, the suspended frames it left dropped, any
/// handler it left taken off (`pop_handlers_above'), and the stack cut
/// back to its entry mark.
#[inline(always)]
fn finish_activation(
    interp: &mut Interpreter,
    env: &mut Env,
    result: Result<Value, LispError>,
    marks: EntryMarks,
) -> Result<Value, LispError> {
    // A signaling byte op recorded itself as a backtrace frame
    // (bytecode.c's record_in_backtrace) so handler-bind handlers saw it;
    // the handlers have run by now, so unwind it like GNU's specpdl does.
    interp.truncate_backtrace_frames(marks.backtrace);
    // Balance any entries left by an abnormal exit (GNU unbind_to on the
    // frame's specpdl watermark), running unwind-protect handlers and
    // restoring saved buffer state on the way out.
    let result = if interp.bc_unwinds.len() > marks.unwinds {
        unwind_activation(interp, env, result, marks.unwinds)
    } else {
        result
    };
    interp.bc_frames.truncate(marks.suspended);
    if interp.bc_handlers.len() > marks.handlers {
        pop_handlers_above(interp, marks.handlers);
    }
    (
        interp.bc_stack.frame,
        interp.bc_stack.base,
        interp.bc_stack.top,
    ) = marks.stack;
    result
}

/// `finish_activation's unbind_to. A signal from an unwind form replaces
/// the one in flight, as in leave_frame; the remaining forms still run.
#[inline(never)]
fn unwind_activation(
    interp: &mut Interpreter,
    env: &mut Env,
    result: Result<Value, LispError>,
    unwinds_at_entry: usize,
) -> Result<Value, LispError> {
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
                interp.bc_stack.push(Value::list_of(&args[nonrest..]))?;
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
                    Value::list_of(&args[index.min(args.len())..])
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
                interp
                    .bc_unwinds
                    .push(UnwindEntry::Binding(Box::new(restore)));
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
    let frames_base = interp.bc_frames.len();
    let handlers_base = interp.bc_handlers.len();
    run_loop(
        interp,
        env,
        Activation::new(Running::of(&program), frames_base, handlers_base),
        None,
    )
}

/// eval.c's handlerlist entries of the byte-code activations above
/// BASE taken off innermost first: a catch's tag leaves the registry of
/// active catches, a condition-case's clause the handler registry.
fn pop_handlers_above(interp: &mut Interpreter, base: usize) {
    while let Some(handler) = interp.bc_handlers.pop_above(base) {
        if matches!(handler.kind, HandlerKind::Catch(_)) {
            interp.pop_active_catch_tag();
        }
        if let Some(start) = handler.registry_start {
            interp.pop_handler_bindings(start);
        }
    }
}

/// An activation's loop state, handed from its entry to `run_loop'.
struct Activation {
    program: Running,
    pc: usize,
    /// Where its suspended frames and handlers begin in the thread's
    /// `bc_frames' and `bc_handlers'.
    frames_base: usize,
    handlers_base: usize,
    /// Frames pushed by signaling byte ops (record_in_backtrace); an
    /// in-frame condition-case that catches must unwind them, and the
    /// caller (`run') balances whatever is left after handler dispatch.
    op_error_frames: usize,
    /// bytecode.c's `quitcounter': backward branches increment the byte
    /// counter; wrapping it to zero calls maybe_gc and maybe_quit before
    /// resetting it to one, so a loop without a call can be interrupted.
    quitcounter: u8,
}

impl Activation {
    fn new(program: Running, frames_base: usize, handlers_base: usize) -> Self {
        Self {
            program,
            pc: 0,
            frames_base,
            handlers_base,
            op_error_frames: 0,
            quitcounter: 1,
        }
    }
}

/// exec_byte_code's loop for an activation whose frame and arguments are
/// in place, from ACTIVATION's state; ENTRY_EXIT is an exit the verified
/// loop took before this began, which the loop takes up first.
#[inline(never)]
fn run_loop(
    interp: &mut Interpreter,
    env: &mut Env,
    activation: Activation,
    mut entry_exit: Option<FastExit>,
) -> Result<Value, LispError> {
    let Activation {
        mut program,
        mut pc,
        frames_base,
        handlers_base,
        mut op_error_frames,
        mut quitcounter,
    } = activation;
    let mut last_instruction = None;
    let trace_errors = trace_load_errors();

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
            match interp.bc_frames.pop_above(frames_base) {
                Some(frame) => {
                    if let Err(error) = leave_frame(interp, env, &frame) {
                        interp.bc_frames.push(frame);
                        return Err(error);
                    }
                    pc = interp.bc_stack.leave_frame();
                    interp.bc_stack.truncate(frame.base);
                    push!(value);
                    program = frame.program;
                    op_error_frames = frame.op_error_frames;
                    interp.bc_handlers.truncate(frame.handlers_len);
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
                if let Some(exit) = entry_exit.take() {
                    break exit;
                }
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
                                frames_base,
                                handlers_base,
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
                    frames_base,
                    handlers_base,
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
                    FastExit::Return(value) if vm.interp.bc_frames.len() > vm.frames_base => {
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
                FastExit::Resume | FastExit::Switched(..) | FastExit::Continued(_) => {
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
                Op::Call(argc) => {
                    let top = interp.bc_stack.top;
                    let mut vm = VmFrames {
                        interp,
                        env,
                        program: &mut program,
                        frames_base,
                        handlers_base,
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
                op => {
                    exec_op(
                        &mut VmFrames {
                            interp,
                            env,
                            program: &mut program,
                            frames_base,
                            handlers_base,
                            op_error_frames: &mut op_error_frames,
                            trace_errors,
                        },
                        op,
                        &mut pc,
                    )?;
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
                    let floor = interp
                        .bc_frames
                        .last_above(frames_base)
                        .map_or(handlers_base, |frame| frame.handlers_len);
                    while let Some(handler) = interp.bc_handlers.pop_above(floor) {
                        if matches!(handler.kind, HandlerKind::Catch(_)) {
                            interp.pop_active_catch_tag();
                        }
                        if let Some(start) = handler.registry_start {
                            interp.pop_handler_bindings(start);
                        }
                        let matched_value = match (&handler.kind, error.kind()) {
                            (HandlerKind::Catch(tag), LispErrorKind::Throw(thrown, value)) => {
                                let same = prim(interp, "eq", [*tag, *thrown], env)?;
                                if same.is_truthy() { Some(*value) } else { None }
                            }
                            (HandlerKind::ConditionCase(_), LispErrorKind::Throw(_, _))
                            | (HandlerKind::ConditionCase(_), LispErrorKind::Terminate(_))
                            | (HandlerKind::Catch(_), _) => None,
                            (HandlerKind::ConditionCase(clause), kind) => {
                                let condition = kind.condition_symbol();
                                let condition_list = interp.error_condition_names_of(&condition);
                                if Interpreter::clause_head_matches(
                                    clause,
                                    condition.as_str(),
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
                    let Some(frame) = interp.bc_frames.pop_above(frames_base) else {
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
                            interp.bc_handlers.truncate(frame.handlers_len);
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
    pop_handlers_above(interp, handlers_base);
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
    fn byte_code_calls_follow_the_callees_code_string_mutations() {
        // G calls F from byte code (constant F; call 0; return), entering
        // F's frame in the verified loop; F's code (constant 0 or 1;
        // return) changes between the calls, and a direct funcall verifies
        // the restored bytes before G calls it again: no frame may run on
        // the verification of bytes the string no longer holds. GNU answers
        // (1 1 2 1 1 1).
        let mut interp = Interpreter::new();
        let mut env = Env::new();
        let form = super::super::super::reader::Reader::new(
            r#"(let* ((code (unibyte-string 192 135))
                      (f (make-byte-code 0 code [1 2] 1))
                      (g (make-byte-code 0 (unibyte-string 192 32 135) (vector f) 1)))
                 (list (funcall g) (funcall g)
                       (progn (aset code 0 193) (funcall g))
                       (progn (aset code 0 192) (funcall f))
                       (funcall g) (funcall g)))"#,
        )
        .read()
        .expect("read form")
        .expect("a form");
        let values = interp
            .eval(&form, &mut env)
            .expect("calls")
            .to_vec()
            .expect("a list");
        let values: Vec<_> = values.into_iter().map(Value::fixnum).collect();
        assert_eq!(values, [1, 1, 2, 1, 1, 1].map(Some));
    }

    #[test]
    fn a_closure_at_a_swept_closures_address_runs_its_own_program() {
        // The entry cache answers for a closure's address until a vector
        // sweep, which alone frees a closure: a closure allocated where a
        // swept one was runs its own constants, not the program the swept
        // one's entry read.  Doomed closures, each called once, are
        // interleaved with kept ones, so that the sweep frees each alone
        // (it joins neighbouring free vectors) and the next closures of
        // their size take their places; their addresses are kept hidden
        // from the conservative scan.
        const HIDE: usize = 0x5555_5555_5555_5555;
        fn eval(interp: &mut Interpreter, env: &mut Env, text: &str) -> Value {
            let form = super::super::super::reader::Reader::new(text)
                .read()
                .expect("read form")
                .expect("a form");
            interp.eval(&form, env).expect("eval")
        }
        fn closure(code: Value, constants: Value) -> Value {
            Value::Closure(crate::lisp::types::ClosureRef::allocate(&[
                Value::Integer(0),
                code,
                constants,
                Value::Integer(1),
            ]))
        }
        #[inline(never)]
        fn call_doomed(interp: &mut Interpreter, env: &mut Env, code: Value) -> Vec<usize> {
            let ones = eval(interp, env, "[1]");
            let mut kept = Value::Nil;
            let mut doomed = Vec::new();
            for _ in 0..500 {
                kept = Value::cons(closure(code, ones), kept);
                let callee = closure(code, ones);
                let value = interp
                    .call_function_value(callee, None, &[], env)
                    .expect("call");
                assert_eq!(value.fixnum(), Some(1));
                doomed.push(callee.closure_ref().expect("a closure").identity() ^ HIDE);
            }
            kept = Value::cons(closure(code, ones), kept);
            interp.set_variable("emaxx-test-kept-closures", kept, env);
            doomed
        }
        let mut interp = Interpreter::new();
        let mut env = Env::new();
        let code = eval(&mut interp, &mut env, "(unibyte-string 192 135)");
        interp.set_variable("emaxx-test-code", code, &mut env);
        let doomed = call_doomed(&mut interp, &mut env, code);
        crate::lisp::alloc::clobber_stack();
        crate::lisp::primitives::call(&mut interp, "garbage-collect", &[], &mut env)
            .expect("collect");
        let twos = eval(&mut interp, &mut env, "[2]");
        let mut fresh = Value::Nil;
        let mut taken = 0;
        for _ in 0..20_000 {
            let callee = closure(code, twos);
            fresh = Value::cons(callee, fresh);
            let identity = callee.closure_ref().expect("a closure").identity();
            if doomed.contains(&(identity ^ HIDE)) {
                let value = interp
                    .call_function_value(callee, None, &[], &mut env)
                    .expect("call");
                assert_eq!(value.fixnum(), Some(2), "the new closure's own constant");
                taken += 1;
            }
        }
        assert!(taken > 0, "closures take swept closures' addresses");
        assert!(fresh.is_cons());
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

        let vector = Value::vector([Value::Integer(9)]);
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
    fn verified_loop_ops_without_lisp_match_their_full_arms() {
        // GNU 30.2's byte code for (with lexical-binding)
        //   (defun vm-light (buffer key a b)
        //     (list
        //      (let ((vm-light-plain a))
        //        (list vm-light-plain (setq vm-light-plain b)
        //              vm-light-plain))
        //      vm-light-plain
        //      (let ((vm-light-watched a))
        //        (list vm-light-watched (setq vm-light-watched b)))
        //      vm-light-watched
        //      (let ((print-length a)) (setq print-length b) (list print-length))
        //      print-length
        //      (with-current-buffer buffer
        //        (list vm-light-local (goto-char 3) (point) (goto-char -5)
        //              (point)
        //              (let ((vm-light-local key)) (list vm-light-local))
        //              vm-light-local))
        //      vm-light-local
        //      (save-current-buffer (set-buffer (buffer-name buffer))
        //        (goto-char (point-max-marker)) (point))
        //      (condition-case nil vm-light-void (void-variable 'void))
        //      (list (memq key '(x y c)) (memq b '(1 2 3)) (memq key '(p q)))
        //      (list (get 'vm-light-plain 'vm-light-prop)
        //            (get key 'vm-light-prop))
        //      (list (list a) (list a b) (list a b key) (list a b key a))
        //      (cond ((eq key 'x) 'is-x) ((eq key 'c) 'is-c) ((eq key 'z) 'is-z)
        //            (t 'other))
        //      (cond ((equal a "s") 's) ((equal a 1) 'one) ((equal a 2) 'two)
        //            (t 'none))
        //      (cond ((eq b 'x) 'is-x) ((eq b 'c) 'is-c) ((eq b 'z) 'is-z)
        //            (t 'other))))
        // The verified loop's arms run each op's usual case (a plain
        // special's and a C variable's let, set and unbind, a buffer-local
        // read, save-current-buffer and set-buffer of a buffer, goto-char
        // of a fixnum, memq, get, list1 to list4, switch's hits and misses
        // on eq and equal tables) and leave the others to the full arms (a
        // watched variable's let, set and unlet, a buffer-local let,
        // set-buffer of a name, goto-char of a marker, a void variable).
        // GNU answers the list asserted below.
        let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
        let mut env = Env::new();
        let object = concat!(
            r#"#[1028 "\1\211\30\1\211\20)\211E\10\3\211\31\3\211\21)D\11\5\32\4\211\22"#,
            r#")C\nr\6\11q\210\13\305b`\306b`\6\15C\13)\257\7\13r\307\6\f!q\210\310 b\2"#,
            r#"10`)\3111E\0\f0\202G\0\210\312\6\f\313>\6\13\314>\6\16\315>E\300\316N\6\"#,
            r#"16\316ND\6\15C\6\16\6\16D\6\17\6\17\6\22E\6\20\6\20\6\23\6\23FF\6\17\317"#,
            r#"\267\202\210\0\320\202\211\0\321\202\211\0\322\202\211\0\323\6\17\324\26"#,
            r#"7\202\234\0\325\202\235\0\326\202\235\0\327\202\235\0\330\6\17\331\267\2"#,
            r#"02\260\0\320\202\261\0\321\202\261\0\322\202\261\0\323\257\20\207" [vm-l"#,
            r#"ight-plain vm-light-watched print-length vm-light-local vm-light-void 3 "#,
            r#"-5 buffer-name point-max-marker (void-variable) void (x y c) (1 2 3) (p "#,
            r#"q) vm-light-prop #s(hash-table test eq purecopy t data (x 124 c 128 z 13"#,
            r#"2)) is-x is-c is-z other #s(hash-table test equal purecopy t data ("s" 1"#,
            r#"44 1 148 2 152)) s one two none #s(hash-table test eq purecopy t data (x"#,
            r#" 164 c 168 z 172))] 24 "\n\n(fn BUFFER KEY A B)"]"#,
        );
        let source = format!(
            r#"(progn
                 (defvar vm-light-plain 0)
                 (defvar vm-light-watched 0)
                 (defvar vm-light-local 'default)
                 (defvar vm-light-void)
                 (defvar vm-light-log nil)
                 (fset 'vm-light {object})
                 (put 'vm-light-plain 'vm-light-prop 'plain-prop)
                 (add-variable-watcher
                  'vm-light-watched
                  (lambda (s n o w)
                    (setq vm-light-log
                          (cons (list s n o w) vm-light-log))))
                 (let ((buffer (get-buffer-create "light-buffer")))
                   (with-current-buffer buffer
                     (insert "hello world")
                     (goto-char 5)
                     (set (make-local-variable 'vm-light-local) 'local))
                   (list (vm-light buffer 'c 1 2)
                         (vm-light buffer 'q 2 'z)
                         (reverse vm-light-log)
                         vm-light-plain
                         (with-current-buffer buffer (point)))))"#
        );
        let form = super::super::super::reader::Reader::new(&source)
            .read()
            .expect("read form")
            .expect("a form");
        let value = interp.eval(&form, &mut env).expect("calls");
        assert_eq!(
            format!("{value}"),
            concat!(
                "(((1 2 2) 0 (1 2) 0 (2) nil (local 3 3 -5 1 (c) local) default 12 void ",
                "((c) (2 3) nil) (plain-prop nil) ((1) (1 2) (1 2 c) (1 2 c 1)) is-c one ",
                "other) ((2 z z) 0 (2 z) 0 (z) nil (local 3 3 -5 1 (q) local) default 12 ",
                "void (nil nil (q)) (plain-prop nil) ((2) (2 z) (2 z q) (2 z q 2)) other ",
                "two is-z) ((vm-light-watched 1 let nil) (vm-light-watched 2 set ",
                "nil) (vm-light-watched 0 unlet nil) (vm-light-watched 2 let nil) ",
                "(vm-light-watched z set nil) (vm-light-watched 0 unlet nil)) 0 12)"
            )
        );
    }

    #[test]
    fn verified_loop_aref_and_aset_take_records_as_vectors() {
        // GNU 30.2's byte code for (with lexical-binding)
        //   (defun vm-record-slots (r v i)
        //     (list (aref r 0) (aref r 2) (aset r 1 'x) (aref r 1)
        //           (aset r i v) (aref r i)
        //           (condition-case e (aref r 3) (args-out-of-range (cdr e)))
        //           (condition-case e (aset r 3 0) (args-out-of-range (cdr e)))
        //           (condition-case e (aref r -1) (args-out-of-range (cdr e)))
        //           (aref v 1) (aset v 0 'y) v))
        // bytecode.c's Baref and Baset take a record's slots (its type
        // in slot zero) as a vector's; an index past them, or a negative
        // one, signals from the full arms. GNU answers the list below.
        let mut interp = Interpreter::new();
        let mut env = Env::new();
        let object = concat!(
            r#"#[771 "\2\300H\3\301H\4\302\303I\5\302H\6\6\5\6\7I\6\7\6\6H\3041$\0\6\10"#,
            r#"\305H0\202%\0A\30612\0\6\11\305\300I0\2023\0A\3071?\0\6\n\310H0\202@\0A\"#,
            r#"6\n\302H\6\13\300\311I\6\f\257\f\207" [0 2 1 x (args-out-of-range) 3 (ar"#,
            r#"gs-out-of-range) (args-out-of-range) -1 y] 16 "\n\n(fn R V I)"]"#,
        );
        let source = format!(
            r#"(progn
                 (fset 'vm-record-slots {object})
                 (prin1-to-string
                  (vm-record-slots (record 'vm-rec 1 2) (vector 'a 'b) 2)))"#
        );
        let form = super::super::super::reader::Reader::new(&source)
            .read()
            .expect("read form")
            .expect("a form");
        let value = interp.eval(&form, &mut env).expect("calls");
        assert_eq!(
            value.as_string().expect("printed"),
            concat!(
                "(vm-rec 2 x x [y b] [y b] (#s(vm-rec x [y b]) 3) ",
                "(#s(vm-rec x [y b]) 3) (#s(vm-rec x [y b]) -1) b y [y b])"
            )
        );
    }

    #[test]
    fn verified_loop_calls_with_three_to_seven_arguments_share_one_arm() {
        // GNU 30.2's byte code for (with lexical-binding)
        //   (defun vm-call-3 (a b c) (list 'three a b c))
        //   (defun vm-call-5 (a b c d &optional e) (list 'five a b c d e))
        //   (defun vm-call-rest (a &rest more) (list 'rest a more))
        //   (defun vm-call-many (a b c d e f g)
        //     (list (vm-call-3 a b c) (vm-call-5 a b c d) (vm-call-5 a b c d e)
        //           (vm-call-rest a b c d e f) (vm-call-rest a b c d e f g)
        //           (vector a b c) (vector a b c d e f g)
        //           (condition-case err (vm-call-3 a b c d)
        //             (wrong-number-of-arguments (car err)))
        //           (funcall (lambda (x y z) (list x y z)) e f g)))
        // whose calls are Bcall3, Bcall4, Bcall5 and Bcall6 with 6 and 7,
        // into byte code (optional and rest arguments, an arity error)
        // and a subr; and Bcall7 with 300 arguments. GNU answers the
        // list below.
        let mut interp = Interpreter::new();
        let mut env = Env::new();
        let vm_call_3 = r#"#[771 "\300\3\3\3F\207" [three] 7 "\n\n(fn A B C)"]"#;
        let vm_call_5 = concat!(
            r#"#[1284 "\300\5\5\5\5\5\257\6\207" [five] 11 "\n\n(fn A B C D &optional E"#,
            r#")"]"#,
        );
        let vm_call_rest = r#"#[385 "\300\2\2E\207" [rest] 5 "\n\n(fn A &rest MORE)"]"#;
        let vm_call_many = concat!(
            r#"#[1799 "\300\6\7\6\7\6\7#\301\6\10\6\10\6\10\6\10$\301\6\11\6\11\6\11\6\"#,
            r#"11\6\11%\302\6\n\6\n\6\n\6\n\6\n\6\n&\6\302\6\13\6\13\6\13\6\13\6\13\6\1"#,
            r#"3\6\13&\7\303\6\f\6\f\6\f#\303\6\15\6\15\6\15\6\15\6\15\6\15\6\15&\7\304"#,
            r#"1i\0\300\6\16\6\16\6\16\6\16$0\202j\0@\6\n\6\n\6\nE\257\11\207" [vm-call"#,
            r#"-3 vm-call-5 vm-call-rest vector (wrong-number-of-arguments)] 21 "\n\n(f"#,
            r#"n A B C D E F G)"]"#,
        );
        let source = format!(
            r#"(progn
                 (fset 'vm-call-3 {vm_call_3})
                 (fset 'vm-call-5 {vm_call_5})
                 (fset 'vm-call-rest {vm_call_rest})
                 (fset 'vm-call-many {vm_call_many})
                 (prin1-to-string
                  (list (vm-call-many 1 2 3 4 5 6 7)
                        (let* ((code (apply #'unibyte-string
                                            (append '(192) (make-list 300 193)
                                                    '(39 44 1 135))))
                               (f (make-byte-code 0 code [list x] 301)))
                          (list (length (funcall f)) (car (funcall f))))))))"#
        );
        let form = super::super::super::reader::Reader::new(&source)
            .read()
            .expect("read form")
            .expect("a form");
        let value = interp.eval(&form, &mut env).expect("calls");
        assert_eq!(
            value.as_string().expect("printed"),
            concat!(
                "(((three 1 2 3) (five 1 2 3 4 nil) (five 1 2 3 4 5) (rest 1 (2 3 4 5 6)) ",
                "(rest 1 (2 3 4 5 6 7)) [1 2 3] [1 2 3 4 5 6 7] wrong-number-of-arguments ",
                "(5 6 7)) (300 x))"
            )
        );
    }

    #[test]
    fn byte_code_calls_stop_at_a_lowered_depth_or_a_quit_and_resume_after() {
        // GNU 30.2's byte code for (with lexical-binding)
        //   (defun vm-gate-down (n) (if (> n 0) (1+ (vm-gate-down (1- n))) 0))
        //   (defun vm-gate-probe ()
        //     (list (vm-gate-down 500)
        //           (condition-case err
        //               (let ((max-lisp-eval-depth 200)) (vm-gate-down 500))
        //             (error err))
        //           max-lisp-eval-depth (vm-gate-down 500)
        //           (condition-case nil (progn (setq quit-flag t) (vm-gate-down 3))
        //             (quit 'quit))
        //           (condition-case nil
        //               (progn (setq quit-flag t) (string-to-number "3"))
        //             (quit 'quit))
        //           (condition-case nil
        //               (eval '(progn (setq quit-flag t) (+ 1 2)) t)
        //             (quit 'quit))
        //           quit-flag (vm-gate-down 3) (string-to-number "3")))
        // A binding of max-lisp-eval-depth and a store of quit-flag each
        // stop the next call into byte code, a subr or an interpreted form
        // (Ffuncall's and eval_sub's tests), and their end lets calls run
        // again. GNU answers the list below.
        let mut interp = Interpreter::new();
        let mut env = Env::new();
        let vm_gate_down = concat!(
            r#"#[257 "\211\300V\203\f\0\301\1S!T\207\300\207" [0 vm-gate-down] 3 "\n\n("#,
            r#"fn N)"]"#,
        );
        let vm_gate_probe = concat!(
            r#"#[0 "\302\303!\3041\16\0\305\30\302\303!)0\10\302\303!\3061\37\0\307\21\"#,
            r#"302\310!0\202!\0\210\311\3121.\0\307\21\313\314!0\2020\0\210\311\3151<\0"#,
            r#"\316\317\307\"0\202>\0\210\311\11\302\310!\313\314!\257\n\207" [max-lisp"#,
            r#"-eval-depth quit-flag vm-gate-down 500 (error) 200 (quit) t 3 quit (quit"#,
            r#") string-to-number "3" (quit) eval (progn (setq quit-flag t) (+ 1 2))] 1"#,
            r#"1]"#,
        );
        let source = format!(
            r#"(progn
                 (fset 'vm-gate-down {vm_gate_down})
                 (fset 'vm-gate-probe {vm_gate_probe})
                 (prin1-to-string (vm-gate-probe)))"#
        );
        let form = super::super::super::reader::Reader::new(&source)
            .read()
            .expect("read form")
            .expect("a form");
        let value = interp.eval(&form, &mut env).expect("probe");
        assert_eq!(
            value.as_string().expect("printed"),
            concat!(
                "(500 (error \"Lisp nesting exceeds `max-lisp-eval-depth'\") 1600 500 ",
                "quit quit quit nil 3 3)"
            )
        );
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
            [Value::Integer(0), code, constants, Value::Integer(1)],
            &mut env,
        )
        .expect("alloc.c:Fmake_byte_code retains the supplied constants vector");
        let stored = prim(&mut interp, "aref", [function, Value::Integer(2)], &mut env)
            .expect("CLOSURE_CONSTANTS");
        assert_eq!(
            prim(&mut interp, "eq", [constants, stored], &mut env)
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
            [constants, Value::Integer(0), Value::Integer(29)],
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
            [Value::Integer(257), code, constants, Value::Integer(3)],
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
            [constants, Value::Integer(0), Value::Nil],
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
            [
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
            [
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
                prim(&mut interp, "eq", [argument, returned], &mut env,)
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
            [
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
                prim(&mut interp, "eq", [compact, returned], &mut env).expect("string identity"),
                Value::T,
            );
        }
        // print.c:Ferror_message_string's general path returns Fbuffer_string:
        // the diagnostic is already a mutable Lisp string before the VM sees it.
        let argument = prim(
            &mut interp,
            "error-message-string",
            [Value::list([
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
            prim(&mut interp, "eq", [argument, value], &mut env).expect("same diagnostic object"),
            Value::T,
        );
        assert_eq!(
            prim(&mut interp, "multibyte-string-p", [argument], &mut env)
                .expect("print buffer is multibyte"),
            Value::T,
        );

        prim(
            &mut interp,
            "put-text-property",
            [
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
                [Value::Integer(0), Value::Symbol("field".into()), argument,],
                &mut env,
            )
            .unwrap(),
            Value::Symbol("command-output".into())
        );
        // Ferror_message_string's allocation-free (error STRING) branch
        // returns that exact string, including its properties.
        let plain_error = Value::list([Value::symbol("error"), argument]);
        let returned = prim(&mut interp, "error-message-string", [plain_error], &mut env)
            .expect("original error string");
        assert_eq!(
            prim(&mut interp, "eq", [argument, returned], &mut env).expect("fast-return identity"),
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
            [
                primitives::bytes_to_shared_unibyte_value(&[192, 193, 92, 135]),
                Value::vector([Value::Integer(40), Value::Integer(2)]),
                Value::Integer(4),
            ],
            &mut env,
        )
        .unwrap();
        assert_eq!(value, Value::Integer(42));
        assert_eq!(
            prim(&mut interp, "internal-stack-stats", [], &mut env).unwrap(),
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
