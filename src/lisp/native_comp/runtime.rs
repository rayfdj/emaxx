//! GNU-compatible one-word values at the generated-code boundary.
//!
//! Emaxx's interpreter deliberately owns richer Rust values.  Native code,
//! however, reads GNU's tagged word ABI directly.  This heap gives every
//! object crossing that boundary a stable tagged identity and gives conses
//! the exact two-word memory prefix generated code reads and writes.  It is
//! an in-process Rust runtime representation; it never calls GNU Emacs.

use super::abi::{
    HANDLER_JMP_OFFSET, HANDLER_NEXT_OFFSET, HANDLER_SIZE, HANDLER_VALUE_OFFSET, SYS_JMP_BUF_SIZE,
    THREAD_HANDLERLIST_OFFSET, THREAD_STATE_SIZE,
};
use crate::lisp::eval::{SavedExcursion, SavedRestriction, SpecialBindingRestore};
use crate::lisp::primitives::{symbol_with_pos_parts, wrong_type_argument};
use crate::lisp::types::{
    ConsCell, ConsMutationQueue, ConsMutationSnapshot, IdentityBuildHasher, SharedCons, Value,
};
use crate::lisp::{
    eval::Interpreter,
    types::{Env, LispError},
};
use std::cell::Cell;
use std::collections::{HashMap, HashSet};
use std::ffi::c_void;
use std::mem::MaybeUninit;
use std::rc::Rc;
use std::sync::{
    Mutex,
    atomic::{AtomicPtr, Ordering},
};

pub(crate) type NativeWord = usize;
type IdentityMap<T> = HashMap<usize, T, IdentityBuildHasher>;
type IdentitySet = HashSet<usize, IdentityBuildHasher>;

#[repr(align(8))]
struct NativeTrueAnchor;

// Qt is process-global in GNU.  A stable aligned Rust address gives generated
// code the same immediate symbol identity and lets hot boolean primitives
// return it without consulting interpreter state or thread-local call data.
static NATIVE_TRUE_ANCHOR: NativeTrueAnchor = NativeTrueAnchor;

#[inline]
fn native_boolean(value: bool) -> NativeWord {
    if value {
        (&NATIVE_TRUE_ANCHOR as *const NativeTrueAnchor) as NativeWord
    } else {
        0
    }
}

#[derive(Clone, Copy)]
pub(crate) enum NativeCallingConvention {
    Fixed,
    Many,
}

#[repr(C, align(16))]
struct NativeInvocation {
    target: *const c_void,
    arguments: [NativeWord; 8],
    result: NativeWord,
    jump_buffer: [u8; SYS_JMP_BUF_SIZE],
}

impl NativeInvocation {
    fn new(target: *const c_void) -> Self {
        Self {
            target,
            arguments: [0; 8],
            result: 0,
            jump_buffer: [0; SYS_JMP_BUF_SIZE],
        }
    }

    fn jump_buffer(&mut self) -> *mut c_void {
        self.jump_buffer.as_mut_ptr().cast()
    }
}

#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
core::arch::global_asm!(
    r#"
    .section __TEXT,__text,regular,pure_instructions
    .p2align 2
    .private_extern _native_call_trampoline
_native_call_trampoline:
    stp x29, x30, [sp, #-32]!
    stp x19, x20, [sp, #16]
    mov x29, sp
    mov x19, x0
    add x0, x19, #{jump_buffer}
    bl __setjmp
    cbnz w0, 1f
    ldr x8, [x19, #{target}]
    ldp x0, x1, [x19, #{arguments}]
    ldp x2, x3, [x19, #{arguments} + 16]
    ldp x4, x5, [x19, #{arguments} + 32]
    ldp x6, x7, [x19, #{arguments} + 48]
    blr x8
    str x0, [x19, #{result}]
    mov w0, #0
    b 2f
1:
    mov w0, #1
2:
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #32
    ret
    "#,
    target = const std::mem::offset_of!(NativeInvocation, target),
    arguments = const std::mem::offset_of!(NativeInvocation, arguments),
    result = const std::mem::offset_of!(NativeInvocation, result),
    jump_buffer = const std::mem::offset_of!(NativeInvocation, jump_buffer),
);

#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
unsafe extern "C" {
    fn native_call_trampoline(invocation: *mut NativeInvocation) -> std::ffi::c_int;
}

#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
unsafe fn invoke_platform(invocation: *mut NativeInvocation) -> Result<bool, String> {
    Ok(unsafe { native_call_trampoline(invocation) } != 0)
}

#[cfg(not(all(target_os = "macos", target_arch = "aarch64")))]
unsafe fn invoke_platform(_invocation: *mut NativeInvocation) -> Result<bool, String> {
    Err("native call trampoline is not implemented for this platform".to_string())
}

struct ActiveCall {
    interpreter: *mut Interpreter,
    environment: *mut Env,
    runtime: *mut NativeRuntime,
}

thread_local! {
    static ACTIVE_CALL: Cell<*mut ActiveCall> = const { Cell::new(std::ptr::null_mut()) };
}

// A Lisp interpreter is single-threaded, as reflected by its Rc-owned values.
// Serialize the outermost native activation so allocation helpers can use one
// direct atomic heap pointer instead of paying macOS's dynamic TLS lookup on
// every cons.  Nested native calls replace and restore the pointer normally.
static NATIVE_EXECUTION_LOCK: Mutex<()> = Mutex::new(());
static ACTIVE_NATIVE_HEAP: AtomicPtr<NativeHeap> = AtomicPtr::new(std::ptr::null_mut());

struct ActiveCallGuard {
    previous: *mut ActiveCall,
    previous_heap: *mut NativeHeap,
}

impl Drop for ActiveCallGuard {
    fn drop(&mut self) {
        ACTIVE_CALL.set(self.previous);
        ACTIVE_NATIVE_HEAP.store(self.previous_heap, Ordering::Relaxed);
    }
}

pub(crate) fn with_active_call<R>(
    interpreter: &mut Interpreter,
    environment: &mut Env,
    runtime: &mut NativeRuntime,
    body: impl FnOnce() -> R,
) -> R {
    let outermost = ACTIVE_CALL.with(|active| active.get().is_null());
    let _execution_guard = outermost.then(|| {
        NATIVE_EXECUTION_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    });
    let mut active = ActiveCall {
        interpreter,
        environment,
        runtime,
    };
    let previous = ACTIVE_CALL.replace(&mut active);
    let previous_heap =
        ACTIVE_NATIVE_HEAP.swap((&mut runtime.heap) as *mut NativeHeap, Ordering::Relaxed);
    let _guard = ActiveCallGuard {
        previous,
        previous_heap,
    };
    body()
}

#[inline(always)]
fn with_active_heap<R>(body: impl FnOnce(&mut NativeHeap) -> R) -> R {
    let heap = ACTIVE_NATIVE_HEAP.load(Ordering::Relaxed);
    if heap.is_null() {
        std::process::abort();
    }
    body(unsafe { &mut *heap })
}

fn with_active<R>(body: impl FnOnce(&mut ActiveCall) -> R) -> R {
    ACTIVE_CALL.with(|active| {
        let active = active.get();
        if active.is_null() {
            std::process::abort();
        }
        // SAFETY: `with_active_call` installs this pointer only for the
        // synchronous extent in which all pointees remain alive.
        body(unsafe { &mut *active })
    })
}

pub(crate) fn with_current_runtime<R>(body: impl FnOnce(&mut NativeRuntime) -> R) -> Option<R> {
    ACTIVE_CALL.with(|active| {
        let active = active.get();
        if active.is_null() {
            None
        } else {
            // SAFETY: The active call owns the runtime for this synchronous
            // callback. Nested native invocations use the same owner.
            Some(body(unsafe { &mut *(*active).runtime }))
        }
    })
}

#[repr(C, align(16))]
struct NativeThreadState {
    bytes: [u8; THREAD_STATE_SIZE],
}

impl Default for NativeThreadState {
    fn default() -> Self {
        Self {
            bytes: [0; THREAD_STATE_SIZE],
        }
    }
}

impl NativeThreadState {
    fn handler(&self) -> *mut NativeHandler {
        // GNU's field is naturally pointer-aligned at the pinned offset.
        unsafe {
            std::ptr::read(
                self.bytes
                    .as_ptr()
                    .add(THREAD_HANDLERLIST_OFFSET)
                    .cast::<*mut NativeHandler>(),
            )
        }
    }

    fn set_handler(&mut self, handler: *mut NativeHandler) {
        unsafe {
            std::ptr::write(
                self.bytes
                    .as_mut_ptr()
                    .add(THREAD_HANDLERLIST_OFFSET)
                    .cast::<*mut NativeHandler>(),
                handler,
            );
        }
    }
}

#[repr(C, align(16))]
struct NativeHandler {
    bytes: [u8; HANDLER_SIZE],
}

impl NativeHandler {
    fn new(next: *mut NativeHandler) -> Self {
        let mut handler = Self {
            bytes: [0; HANDLER_SIZE],
        };
        handler.set_next(next);
        handler
    }

    fn set_word(&mut self, offset: usize, value: NativeWord) {
        unsafe {
            std::ptr::write_unaligned(
                self.bytes.as_mut_ptr().add(offset).cast::<NativeWord>(),
                value,
            );
        }
    }

    fn set_next(&mut self, next: *mut NativeHandler) {
        self.set_word(HANDLER_NEXT_OFFSET, next as NativeWord);
    }

    fn set_value(&mut self, value: NativeWord) {
        self.set_word(HANDLER_VALUE_OFFSET, value);
    }
}

enum HandlerRegistration {
    Catch,
    ConditionCase(usize),
}

struct HandlerEntry {
    storage: Box<NativeHandler>,
    kind: i32,
    match_value: Value,
    unwind_depth: usize,
    registration: HandlerRegistration,
}

impl HandlerEntry {
    fn address(&self) -> *mut NativeHandler {
        (&*self.storage as *const NativeHandler).cast_mut()
    }
}

enum UnwindAction {
    Special(SpecialBindingRestore),
    Excursion(SavedExcursion),
    Restriction(SavedRestriction),
    CurrentBuffer(u64),
    Cleanup { function: bool, value: Value },
}

/// Per-interpreter state used by loaded native code.  All objects are Rust
/// owned; the boxed layouts merely expose the stable C ABI that GNU-generated
/// machine code expects to read directly.
pub(crate) struct NativeRuntime {
    heap: NativeHeap,
    thread: Box<NativeThreadState>,
    thread_pointer: Box<*mut NativeThreadState>,
    symbols_with_positions_enabled: Box<bool>,
    link_table: Box<[*mut c_void]>,
    handlers: Vec<HandlerEntry>,
    unwind: Vec<UnwindAction>,
    calls: Vec<NativeCallFrame>,
}

struct NativeCallFrame {
    handler_depth: usize,
    unwind_depth: usize,
    pending_error: Option<LispError>,
    escape_buffer: *mut c_void,
}

impl Default for NativeRuntime {
    fn default() -> Self {
        let mut thread = Box::<NativeThreadState>::default();
        let thread_pointer = Box::new((&mut *thread) as *mut NativeThreadState);
        Self {
            heap: NativeHeap::default(),
            thread,
            thread_pointer,
            symbols_with_positions_enabled: Box::new(false),
            link_table: runtime_link_table().into_boxed_slice(),
            handlers: Vec::new(),
            unwind: Vec::new(),
            calls: Vec::new(),
        }
    }
}

impl NativeRuntime {
    pub(crate) fn is_pristine(&self) -> bool {
        self.handlers.is_empty()
            && self.unwind.is_empty()
            && self.calls.is_empty()
            && self.thread.handler().is_null()
            && self.heap.is_empty()
    }

    pub(crate) fn begin_call(&mut self, escape_buffer: *mut c_void) {
        self.heap.begin_call();
        self.calls.push(NativeCallFrame {
            handler_depth: self.handlers.len(),
            unwind_depth: self.unwind.len(),
            pending_error: None,
            escape_buffer,
        });
    }

    pub(crate) fn current_thread_relocation(&mut self) -> *mut c_void {
        (&mut *self.thread_pointer as *mut *mut NativeThreadState).cast()
    }

    pub(crate) fn symbols_with_positions_relocation(&mut self) -> *mut bool {
        &mut *self.symbols_with_positions_enabled
    }

    pub(crate) fn pure_relocation(&self) -> *mut c_void {
        usize::MAX as *mut c_void
    }

    pub(crate) fn function_link_table(&mut self) -> *mut c_void {
        self.link_table.as_mut_ptr().cast()
    }

    pub(crate) fn install_trampoline(
        &mut self,
        subroutine_index: usize,
        target: *mut c_void,
    ) -> Result<(), String> {
        const HELPER_COUNT: usize = 15;
        let slot = HELPER_COUNT
            .checked_add(subroutine_index)
            .ok_or_else(|| "native trampoline link-table index overflow".to_string())?;
        let entry = self
            .link_table
            .get_mut(slot)
            .ok_or_else(|| "native trampoline subroutine is outside the runtime ABI".to_string())?;
        *entry = target;
        Ok(())
    }

    pub(crate) fn encode_relocations(
        &mut self,
        values: &[Value],
    ) -> Result<Vec<NativeWord>, LispError> {
        values
            .iter()
            .map(|value| {
                self.heap
                    .encode(value)
                    .map_err(|error| super::lisp::native_ice(&error))
            })
            .collect()
    }

    pub(crate) fn invoke(
        &mut self,
        interpreter: &mut Interpreter,
        environment: &mut Env,
        target: *const c_void,
        convention: NativeCallingConvention,
        arguments: &[Value],
    ) -> Result<Value, LispError> {
        if target.is_null() {
            return Err(super::lisp::native_ice(
                "attempted to call a null native function",
            ));
        }
        if matches!(convention, NativeCallingConvention::Fixed) && arguments.len() > 8 {
            return Err(super::lisp::native_ice(
                "fixed native function exceeds the eight-register ABI",
            ));
        }

        *self.symbols_with_positions_enabled =
            crate::lisp::primitives::symbols_with_pos_enabled(interpreter, environment);

        let mut invocation = NativeInvocation::new(target);
        self.begin_call(invocation.jump_buffer());
        if let Err(error) = self.heap.publish_interpreter_writes() {
            let finish = self.finish_call(interpreter);
            finish?;
            return Err(super::lisp::native_ice(&error));
        }
        // GNU keymaps are Lisp lists.  Emaxx keeps an indexed Rust record for
        // fast lookup internally, but generated Elisp must receive the exact
        // public list object and be free to traverse or mutate it directly.
        let projected_arguments = arguments
            .iter()
            .map(|argument| crate::lisp::primitives::public_keymap_value(interpreter, argument))
            .collect::<Vec<_>>();
        let encoded = projected_arguments
            .iter()
            .map(|argument| self.heap.encode(argument))
            .collect::<Result<Vec<_>, _>>();
        let encoded = match encoded {
            Ok(encoded) => encoded,
            Err(error) => {
                let finish = self.finish_call(interpreter);
                finish?;
                return Err(super::lisp::native_ice(&error));
            }
        };
        match convention {
            NativeCallingConvention::Fixed => {
                invocation.arguments[..encoded.len()].copy_from_slice(&encoded);
            }
            NativeCallingConvention::Many => {
                invocation.arguments[0] = encoded.len();
                invocation.arguments[1] = encoded.as_ptr() as NativeWord;
            }
        }

        let escaped = with_active_call(interpreter, environment, self, || unsafe {
            invoke_platform(&mut invocation)
        });
        let escaped = match escaped {
            Ok(escaped) => escaped,
            Err(error) => {
                let finish = self.finish_call(interpreter);
                finish?;
                return Err(super::lisp::native_ice(&error));
            }
        };
        let result = if escaped {
            None
        } else {
            Some(
                self.heap
                    .decode_result(invocation.result)
                    .map_err(|error| super::lisp::native_ice(&error)),
            )
        };
        let finish = self.finish_call(interpreter);
        if escaped {
            return match finish {
                Err(error) => Err(error),
                Ok(()) => Err(super::lisp::native_ice(
                    "native function escaped without a pending Lisp error",
                )),
            };
        }
        let result = result.expect("ordinary native return has a result")?;
        finish?;
        Ok(result)
    }

    pub(crate) fn finish_call(&mut self, interpreter: &mut Interpreter) -> Result<(), LispError> {
        let sync_result = self.sync_handlers(interpreter);
        let Some(frame) = self.calls.pop() else {
            return Err(super::lisp::native_ice(
                "native call stack underflow while returning from generated code",
            ));
        };
        // GNU native code and C primitives share one Lisp heap.  Emaxx's
        // machine-code mirror must therefore publish direct cons writes when
        // every native frame returns to Rust.  Tracking is frame-local: an
        // outer generated frame retains its own live mirrors independently.
        let heap_result = self.publish_heap_writes(interpreter, false);
        sync_result?;
        heap_result?;
        if self.handlers.len() != frame.handler_depth || self.unwind.len() != frame.unwind_depth {
            return Err(super::lisp::native_ice(
                "generated function returned with an unbalanced dynamic stack",
            ));
        }
        if let Some(error) = frame.pending_error {
            Err(error)
        } else {
            Ok(())
        }
    }

    /// Publish machine-code writes before Rust-owned C primitive semantics
    /// can observe the Lisp heap.
    ///
    /// GNU generated code and GNU C primitives operate on the same cons
    /// cells.  Emaxx preserves GNU's two-word machine layout in a mirror, so
    /// a direct generated `setcar'/`setcdr' must be copied back not just when
    /// a native function returns, but before a generated call enters Rust.
    /// Otherwise a primitive can follow a record or global to a stale cons
    /// that was not one of its explicit arguments.  While generated frames
    /// remain active, keep snapshots registered because those frames can
    /// mutate the same raw pointers again after the primitive returns.
    fn publish_heap_writes(
        &mut self,
        interpreter: &mut Interpreter,
        retain_tracking: bool,
    ) -> Result<(), LispError> {
        let mutated_conses = self
            .heap
            .synchronize_touched_conses(retain_tracking)
            .map_err(|error| super::lisp::native_ice(&error))?;
        let mut keymap_owners = HashSet::new();
        for cons in mutated_conses {
            keymap_owners.extend(interpreter.keymap_public_cons_owner_ids(&cons));
        }
        keymap_owners.into_iter().try_for_each(|owner| {
            crate::lisp::primitives::sync_runtime_keymap_from_public_view(interpreter, owner)
        })
    }

    fn sync_handlers(&mut self, interpreter: &mut Interpreter) -> Result<(), LispError> {
        let current = self.thread.handler();
        while self
            .handlers
            .last()
            .is_some_and(|entry| entry.address() != current)
        {
            let entry = self.handlers.pop().expect("last checked above");
            match entry.registration {
                HandlerRegistration::Catch => interpreter.pop_active_catch_tag(),
                HandlerRegistration::ConditionCase(start) => {
                    interpreter.pop_handler_bindings(start)
                }
            }
        }
        if current.is_null()
            || self
                .handlers
                .last()
                .is_some_and(|entry| entry.address() == current)
        {
            Ok(())
        } else {
            Err(super::lisp::native_ice(
                "native handler list points outside the Rust-owned handler stack",
            ))
        }
    }

    fn remember_error(&mut self, error: LispError) {
        let Some(call) = self.calls.last_mut() else {
            std::process::abort();
        };
        if call.pending_error.is_none() {
            call.pending_error = Some(error);
        }
    }

    fn matching_handler(
        &mut self,
        interpreter: &mut Interpreter,
        environment: &Env,
        error: &LispError,
    ) -> Option<usize> {
        let is_throw = matches!(error, LispError::Throw(_, _));
        let (condition, conditions) = if is_throw || matches!(error, LispError::Terminate(_)) {
            (String::new(), Vec::new())
        } else {
            let condition = error.condition_type();
            let conditions = interpreter.error_condition_names(&condition);
            (condition, conditions)
        };
        let handler_depth = self
            .calls
            .last()
            .map(|call| call.handler_depth)
            .unwrap_or(self.handlers.len());
        self.handlers[handler_depth..]
            .iter()
            .rposition(|handler| match handler.kind {
                0 => match error {
                    LispError::Throw(tag, _) => crate::lisp::primitives::values_eq_in_env(
                        interpreter,
                        &handler.match_value,
                        tag,
                        environment,
                    ),
                    _ => false,
                },
                1 if !is_throw && !matches!(error, LispError::Terminate(_)) => {
                    Interpreter::clause_head_matches(&handler.match_value, &condition, &conditions)
                }
                _ => false,
            })
            .map(|index| handler_depth + index)
    }

    /// Prepare GNU's `unwind_to_catch` transition and return the exact jmp
    /// buffer that the generated `_setjmp` initialized.  A null result means
    /// the outer native-call escape is not installed yet; capability remains
    /// disabled in that state.
    fn prepare_nonlocal_exit(
        &mut self,
        interpreter: &mut Interpreter,
        environment: &mut Env,
        mut error: LispError,
    ) -> *mut c_void {
        let Some((handler_depth, unwind_depth, escape_buffer)) = self
            .calls
            .last()
            .map(|call| (call.handler_depth, call.unwind_depth, call.escape_buffer))
        else {
            std::process::abort();
        };
        loop {
            let Some(target_index) = self.matching_handler(interpreter, environment, &error) else {
                let outer_handler = handler_depth
                    .checked_sub(1)
                    .and_then(|index| self.handlers.get(index))
                    .map(HandlerEntry::address)
                    .unwrap_or(std::ptr::null_mut());
                self.thread.set_handler(outer_handler);
                if let Err(sync_error) = self.sync_handlers(interpreter) {
                    error = sync_error;
                }
                while self.unwind.len() > unwind_depth {
                    if let Err(unwind_error) = self.unwind_one(interpreter, environment) {
                        error = unwind_error;
                    }
                }
                self.remember_error(error);
                return escape_buffer;
            };

            let target = self.handlers[target_index].address();
            let unwind_depth = self.handlers[target_index].unwind_depth;
            self.thread.set_handler(target);
            if let Err(sync_error) = self.sync_handlers(interpreter) {
                self.remember_error(sync_error);
                return escape_buffer;
            }
            let mut superseding_error = None;
            while self.unwind.len() > unwind_depth {
                if let Err(unwind_error) = self.unwind_one(interpreter, environment) {
                    superseding_error = Some(unwind_error);
                    break;
                }
            }
            if let Some(unwind_error) = superseding_error {
                error = unwind_error;
                continue;
            }

            let value = match &error {
                LispError::Throw(_, value) => value.clone(),
                LispError::Terminate(_) => unreachable!("termination cannot match a handler"),
                _ => crate::lisp::eval::error_condition_value(&error),
            };
            let encoded = match self.heap.encode(&value) {
                Ok(encoded) => encoded,
                Err(encode_error) => {
                    self.remember_error(super::lisp::native_ice(&encode_error));
                    return escape_buffer;
                }
            };
            self.handlers[target_index].storage.set_value(encoded);
            return unsafe { target.cast::<u8>().add(HANDLER_JMP_OFFSET).cast() };
        }
    }

    fn unwind_one(
        &mut self,
        interpreter: &mut Interpreter,
        environment: &mut Env,
    ) -> Result<(), LispError> {
        let Some(action) = self.unwind.pop() else {
            return Err(super::lisp::native_ice(
                "native byte-unbind exceeded the dynamic binding stack",
            ));
        };
        match action {
            UnwindAction::Special(restore) => {
                interpreter.restore_special_dynamic(restore, environment)
            }
            UnwindAction::Excursion(saved) => {
                interpreter.restore_excursion_state(saved);
                Ok(())
            }
            UnwindAction::Restriction(saved) => {
                interpreter.restore_restriction_state(saved);
                Ok(())
            }
            UnwindAction::CurrentBuffer(buffer_id) => {
                if interpreter.has_buffer_id(buffer_id) {
                    interpreter.set_current_buffer_id(buffer_id)?;
                }
                Ok(())
            }
            UnwindAction::Cleanup { function, value } => {
                if function {
                    interpreter.call_function_value(value, None, &[], environment)?;
                } else {
                    for form in value.to_vec()? {
                        interpreter.eval(&form, environment)?;
                    }
                }
                Ok(())
            }
        }
    }
}

pub(crate) fn invoke_subr(index: usize, arguments: &[NativeWord]) -> NativeWord {
    with_active(|active| {
        let Some(subroutine) = super::abi::native_subrs().get(index) else {
            remember_helper_error(
                active,
                super::lisp::native_ice("native subroutine index is outside the runtime ABI"),
            );
            return 0;
        };
        if arguments.len() == 2
            && let (Some(left), Some(right)) =
                (decode_fixnum(arguments[0]), decode_fixnum(arguments[1]))
        {
            let result = match subroutine.name {
                "<" => Some(left < right),
                ">" => Some(left > right),
                "<=" => Some(left <= right),
                ">=" => Some(left >= right),
                "=" => Some(left == right),
                _ => None,
            };
            if let Some(result) = result {
                return native_boolean(result);
            }
        }
        // SAFETY: `with_active_call` owns each pointed-to object for the
        // complete native activation.  Primitive dispatch is synchronous and
        // this thread-local never crosses threads.
        if let Err(error) =
            unsafe { &mut *active.runtime }.sync_handlers(unsafe { &mut *active.interpreter })
        {
            remember_helper_error(active, error);
            return 0;
        }
        // alloc.c:Fcons stores its two Lisp words without inspecting either
        // object.  Preserve that O(1) ABI path here: recursively translating
        // a list-valued cdr on every call turns native list construction into
        // quadratic work (and bubble sort into cubic work).
        if subroutine.name == "cons" && arguments.len() == 2 {
            return unsafe { &mut *active.runtime }
                .heap
                .cons(arguments[0], arguments[1]);
        }
        if let Err(error) = unsafe { &mut *active.runtime }
            .publish_heap_writes(unsafe { &mut *active.interpreter }, true)
        {
            remember_helper_error(active, error);
            return 0;
        }
        let decoded = arguments
            .iter()
            .map(|word| unsafe { &mut *active.runtime }.heap.decode(*word))
            .collect::<Result<Vec<_>, _>>();
        let decoded = match decoded {
            Ok(decoded) => decoded,
            Err(error) => {
                remember_helper_error(active, super::lisp::native_ice(&error));
                return 0;
            }
        };
        let mutation_epoch = crate::lisp::types::cons_mutation_epoch();
        let result = crate::lisp::primitives::call(
            unsafe { &mut *active.interpreter },
            subroutine.name,
            &decoded,
            unsafe { &mut *active.environment },
        );
        // GNU's generated code and primitives see the same Lisp object
        // storage.  Emaxx mirrors cons cells at the machine-code ABI so the
        // generated C layout remains exact; a primitive such as `aset' or
        // `setcar' can mutate the Rust-owned object while it is decoded.
        // Push those mutations back into the mirror before generated code
        // consumes the argument again.  Non-cons objects use arena/shared
        // identities directly and need no copy-back.
        if crate::lisp::types::cons_mutation_epoch() != mutation_epoch
            && let Err(error) = unsafe { &mut *active.runtime }
                .heap
                .publish_interpreter_writes()
        {
            remember_helper_error(active, super::lisp::native_ice(&error));
            return 0;
        }
        match result {
            Ok(value) => {
                let value = crate::lisp::primitives::public_keymap_value(
                    unsafe { &*active.interpreter },
                    &value,
                );
                match unsafe { &mut *active.runtime }.heap.encode(&value) {
                    Ok(word) => word,
                    Err(error) => {
                        drop(decoded);
                        remember_helper_error(active, super::lisp::native_ice(&error));
                        0
                    }
                }
            }
            Err(error) => {
                drop(decoded);
                remember_helper_error(active, error);
                0
            }
        }
    })
}

#[derive(Clone, Copy)]
pub(crate) enum FixnumComparison {
    Less,
    Greater,
    LessOrEqual,
    GreaterOrEqual,
    Equal,
}

/// Fast half of the MANY numeric-comparison ABI from data.c.  GNU performs
/// the same immediate-tag test before entering its general number tower; the
/// overwhelmingly common two-fixnum case must not allocate Rust `Value`s or
/// pass through string-named primitive dispatch.
pub(crate) unsafe fn invoke_numeric_comparison(
    index: usize,
    comparison: FixnumComparison,
    argument_count: isize,
    arguments: *const NativeWord,
) -> NativeWord {
    if argument_count == 2 && !arguments.is_null() {
        let left = unsafe { *arguments };
        let right = unsafe { *arguments.add(1) };
        if let (Some(left), Some(right)) = (decode_fixnum(left), decode_fixnum(right)) {
            let result = match comparison {
                FixnumComparison::Less => left < right,
                FixnumComparison::Greater => left > right,
                FixnumComparison::LessOrEqual => left <= right,
                FixnumComparison::GreaterOrEqual => left >= right,
                FixnumComparison::Equal => left == right,
            };
            return native_boolean(result);
        }
    }
    unsafe { invoke_subr_many(index, argument_count, arguments) }
}

/// alloc.c:Fcons is a two-word allocation and cannot inspect its operands.
/// Its generated ABI entry therefore bypasses the generic primitive router.
#[inline(always)]
pub(crate) fn invoke_cons(car: NativeWord, cdr: NativeWord) -> NativeWord {
    with_active_heap(|heap| heap.cons(car, cdr))
}

pub(crate) unsafe fn invoke_subr_many(
    index: usize,
    argument_count: isize,
    arguments: *const NativeWord,
) -> NativeWord {
    let argument_count = if argument_count < 0
        && super::abi::native_subrs()
            .get(index)
            .is_some_and(|subroutine| subroutine.name == "list")
    {
        // alloc.c:Flist intentionally treats every non-positive ptrdiff_t
        // count as the empty list.  comp.c relies on that exact C behavior
        // in the optional-&rest fallback, where `nargs - nonrest` is -1.
        0
    } else if let Ok(argument_count) = usize::try_from(argument_count) {
        argument_count
    } else {
        return invoke_subr_error("negative native subroutine argument count");
    };
    if argument_count != 0 && arguments.is_null() {
        return invoke_subr_error("native subroutine arguments are null");
    }
    // SAFETY: Generated code passes the count and pointer pair used by GNU's
    // MANY ABI.  The zero-length case permits a null pointer.
    let arguments = unsafe { std::slice::from_raw_parts(arguments, argument_count) };
    invoke_subr(index, arguments)
}

fn invoke_subr_error(message: &str) -> NativeWord {
    with_active(|active| {
        remember_helper_error(active, super::lisp::native_ice(message));
        0
    })
}

#[inline]
fn decode_fixnum(word: NativeWord) -> Option<i64> {
    (word & 3 == TAG_FIXNUM_LOW).then_some((word as isize >> FIXNUM_BITS) as i64)
}

#[cfg(target_os = "macos")]
unsafe extern "C" {
    #[link_name = "_longjmp"]
    fn platform_longjmp(buffer: *mut c_void, value: std::ffi::c_int) -> !;
}

#[cfg(not(target_os = "macos"))]
unsafe extern "C" {
    #[link_name = "longjmp"]
    fn platform_longjmp(buffer: *mut c_void, value: std::ffi::c_int) -> !;
}

unsafe fn jump_nonlocal(buffer: *mut c_void) -> ! {
    unsafe { platform_longjmp(buffer, 1) }
}

fn remember_helper_error(active: &mut ActiveCall, error: LispError) {
    let jump_buffer = unsafe { &mut *active.runtime }.prepare_nonlocal_exit(
        unsafe { &mut *active.interpreter },
        unsafe { &mut *active.environment },
        error,
    );
    if !jump_buffer.is_null() {
        unsafe { jump_nonlocal(jump_buffer) };
    }
}

fn decode_word(active: &mut ActiveCall, word: NativeWord) -> Result<Value, LispError> {
    unsafe { &mut *active.runtime }
        .heap
        .decode(word)
        .map_err(|error| super::lisp::native_ice(&error))
}

extern "C" fn runtime_wrong_type_argument(predicate: NativeWord, value: NativeWord) {
    with_active(|active| {
        let result = (|| {
            let predicate = decode_word(active, predicate)?;
            let value = decode_word(active, value)?;
            let predicate = predicate
                .as_symbol()
                .map_err(|_| wrong_type_argument("symbolp", predicate.clone()))?;
            Err::<(), _>(wrong_type_argument(predicate, value))
        })();
        if let Err(error) = result {
            remember_helper_error(active, error);
        }
    });
}

extern "C" fn runtime_pseudovector_typep(value: NativeWord, code: i32) -> bool {
    with_active(|active| match decode_word(active, value) {
        Ok(value) => {
            let interpreter = unsafe { &mut *active.interpreter };
            match code {
                2 => matches!(value, Value::BigInteger(_)),
                6 => symbol_with_pos_parts(interpreter, &value).is_some(),
                _ => false,
            }
        }
        Err(error) => {
            remember_helper_error(active, error);
            false
        }
    })
}

extern "C" fn runtime_pure_write_error(value: NativeWord) {
    with_active(|active| match decode_word(active, value) {
        Ok(value) => remember_helper_error(
            active,
            LispError::SignalValue(Value::list([
                Value::symbol("error"),
                Value::string("Attempt to modify read-only object"),
                value,
            ])),
        ),
        Err(error) => remember_helper_error(active, error),
    });
}

extern "C" fn runtime_push_handler(match_value: NativeWord, kind: i32) -> *mut NativeHandler {
    with_active(|active| {
        let result = (|| {
            let match_value = decode_word(active, match_value)?;
            let interpreter = unsafe { &mut *active.interpreter };
            let runtime = unsafe { &mut *active.runtime };
            runtime.sync_handlers(interpreter)?;
            let registration = match kind {
                0 => {
                    interpreter.push_active_catch_tag(match_value.clone());
                    HandlerRegistration::Catch
                }
                1 => HandlerRegistration::ConditionCase(
                    interpreter.push_condition_case_handler(vec![match_value.clone()]),
                ),
                _ => {
                    return Err(super::lisp::native_ice(
                        "native push_handler received an unknown handler type",
                    ));
                }
            };
            let mut storage = Box::new(NativeHandler::new(runtime.thread.handler()));
            storage.set_value(0);
            let address = (&mut *storage) as *mut NativeHandler;
            runtime.thread.set_handler(address);
            runtime.handlers.push(HandlerEntry {
                storage,
                kind,
                match_value,
                unwind_depth: runtime.unwind.len(),
                registration,
            });
            Ok(address)
        })();
        match result {
            Ok(address) => address,
            Err(error) => {
                remember_helper_error(active, error);
                std::ptr::null_mut()
            }
        }
    })
}

extern "C" fn runtime_record_unwind_protect_excursion() {
    with_active(|active| {
        let interpreter = unsafe { &mut *active.interpreter };
        let saved = interpreter.save_excursion_state();
        unsafe { &mut *active.runtime }
            .unwind
            .push(UnwindAction::Excursion(saved));
    });
}

extern "C" fn runtime_unbind_n(count: NativeWord) -> NativeWord {
    with_active(|active| {
        let result = (|| {
            let decoded = decode_word(active, count)?;
            let count = decoded.as_integer()?;
            let count = usize::try_from(count).map_err(|_| {
                super::lisp::native_ice("native byte-unbind received a negative count")
            })?;
            let interpreter = unsafe { &mut *active.interpreter };
            let environment = unsafe { &mut *active.environment };
            let runtime = unsafe { &mut *active.runtime };
            for _ in 0..count {
                runtime.unwind_one(interpreter, environment)?;
            }
            Ok(0)
        })();
        match result {
            Ok(value) => value,
            Err(error) => {
                remember_helper_error(active, error);
                0
            }
        }
    })
}

extern "C" fn runtime_save_restriction() {
    with_active(|active| {
        let saved = unsafe { &mut *active.interpreter }.save_restriction_state();
        unsafe { &mut *active.runtime }
            .unwind
            .push(UnwindAction::Restriction(saved));
    });
}

extern "C" fn runtime_get_symbol_with_position(value: NativeWord) -> *mut c_void {
    with_active(|active| {
        let result = (|| {
            let value = decode_word(active, value)?;
            let interpreter = unsafe { &mut *active.interpreter };
            let runtime = unsafe { &mut *active.runtime };
            runtime
                .heap
                .symbol_with_position_pointer(interpreter, &value)
                .map_err(|error| super::lisp::native_ice(&error))
        })();
        match result {
            Ok(pointer) => pointer,
            Err(error) => {
                remember_helper_error(active, error);
                std::ptr::null_mut()
            }
        }
    })
}

extern "C" fn runtime_sanitizer_assert(value: NativeWord, kind: NativeWord) -> NativeWord {
    with_active(|active| {
        let result = (|| {
            let value = decode_word(active, value)?;
            let kind = decode_word(active, kind)?;
            let interpreter = unsafe { &mut *active.interpreter };
            let environment = unsafe { &mut *active.environment };
            if !interpreter
                .lookup_var("comp-sanitizer-active", environment)
                .is_some_and(|active| active.is_truthy())
            {
                return Ok(0);
            }
            let valid = super::lisp::call(
                interpreter,
                environment,
                "cl-typep",
                &[value.clone(), kind.clone()],
            )?;
            if valid.is_truthy() {
                return Ok(0);
            }
            let _ = super::lisp::call(
                interpreter,
                environment,
                "message",
                &[
                    Value::string("Comp sanitizer FAIL for %s with type %s"),
                    value.clone(),
                    kind.clone(),
                ],
            )?;
            let _ = super::lisp::call(interpreter, environment, "backtrace", &[])?;
            Err(LispError::SignalValue(Value::list([
                Value::symbol("comp-sanitizer-error"),
                value,
                kind,
            ])))
        })();
        match result {
            Ok(value) => value,
            Err(error) => {
                remember_helper_error(active, error);
                0
            }
        }
    })
}

extern "C" fn runtime_record_unwind_current_buffer() {
    with_active(|active| {
        let buffer_id = unsafe { &mut *active.interpreter }.current_buffer_id();
        unsafe { &mut *active.runtime }
            .unwind
            .push(UnwindAction::CurrentBuffer(buffer_id));
    });
}

extern "C" fn runtime_set_internal(
    symbol: NativeWord,
    value: NativeWord,
    where_: NativeWord,
    bind_kind: i32,
) {
    with_active(|active| {
        let result = (|| {
            let symbol = decode_word(active, symbol)?;
            let value = decode_word(active, value)?;
            let where_ = decode_word(active, where_)?;
            if !where_.is_nil() || bind_kind != 0 {
                return Err(super::lisp::native_ice(
                    "compiled set_internal used a nonstandard context",
                ));
            }
            crate::lisp::primitives::call(
                unsafe { &mut *active.interpreter },
                "set",
                &[symbol, value],
                unsafe { &mut *active.environment },
            )?;
            Ok(())
        })();
        if let Err(error) = result {
            remember_helper_error(active, error);
        }
    });
}

extern "C" fn runtime_unwind_protect(value: NativeWord) {
    with_active(|active| {
        let result = (|| {
            let value = decode_word(active, value)?;
            let function = crate::lisp::primitives::call(
                unsafe { &mut *active.interpreter },
                "functionp",
                std::slice::from_ref(&value),
                unsafe { &mut *active.environment },
            )?
            .is_truthy();
            unsafe { &mut *active.runtime }
                .unwind
                .push(UnwindAction::Cleanup { function, value });
            Ok(())
        })();
        if let Err(error) = result {
            remember_helper_error(active, error);
        }
    });
}

extern "C" fn runtime_specbind(symbol: NativeWord, value: NativeWord) {
    with_active(|active| {
        let result = (|| {
            let symbol = decode_word(active, symbol)?;
            let value = decode_word(active, value)?;
            let name = symbol
                .as_symbol()
                .map_err(|_| wrong_type_argument("symbolp", symbol.clone()))?;
            let restore =
                unsafe { &mut *active.interpreter }
                    .bind_special_dynamic(name, value, unsafe { &mut *active.environment })?;
            unsafe { &mut *active.runtime }
                .unwind
                .push(UnwindAction::Special(restore));
            Ok(())
        })();
        if let Err(error) = result {
            remember_helper_error(active, error);
        }
    });
}

extern "C" fn runtime_maybe_gc() {
    // Emaxx values are reference-counted Rust allocations rather than GNU's
    // tracing heap.  There is no allocation threshold to service here.
}

extern "C" fn runtime_maybe_quit() {
    with_active(|active| {
        let interpreter = unsafe { &mut *active.interpreter };
        let should_quit = interpreter
            .symbol_value_cell("quit-flag")
            .is_ok_and(|flag| flag.is_truthy())
            && !interpreter
                .symbol_value_cell("inhibit-quit")
                .is_ok_and(|inhibited| inhibited.is_truthy());
        if should_quit {
            interpreter.set_symbol_value_cell("quit-flag", Value::Nil);
            remember_helper_error(
                active,
                LispError::SignalValue(Value::list([Value::symbol("quit")])),
            );
        }
    });
}

/// GNU's function relocation array: the 15 non-subr C helpers followed by
/// the C registration order of every primitive.  Every address here names
/// Rust code in this process; none resolves into GNU Emacs.
pub(crate) fn runtime_link_table() -> Vec<*mut c_void> {
    let mut table = Vec::with_capacity(15 + super::abi::native_subrs().len());
    table.extend([
        runtime_wrong_type_argument as *mut c_void,
        runtime_pseudovector_typep as *mut c_void,
        runtime_pure_write_error as *mut c_void,
        runtime_push_handler as *mut c_void,
        runtime_record_unwind_protect_excursion as *mut c_void,
        runtime_unbind_n as *mut c_void,
        runtime_save_restriction as *mut c_void,
        runtime_get_symbol_with_position as *mut c_void,
        runtime_sanitizer_assert as *mut c_void,
        runtime_record_unwind_current_buffer as *mut c_void,
        runtime_set_internal as *mut c_void,
        runtime_unwind_protect as *mut c_void,
        runtime_specbind as *mut c_void,
        runtime_maybe_gc as *mut c_void,
        runtime_maybe_quit as *mut c_void,
    ]);
    table.extend(
        (0..super::abi::native_subrs().len())
            .map(super::generated_native_subrs::native_subr_address),
    );
    table
}

const TAG_MASK: usize = 7;
const TAG_SYMBOL: usize = 0;
const TAG_FIXNUM_LOW: usize = 2;
const TAG_CONS: usize = 3;
const TAG_STRING: usize = 4;
const TAG_VECTORLIKE: usize = 5;
const TAG_FLOAT: usize = 7;
const FIXNUM_BITS: u32 = 2;
const MOST_POSITIVE_FIXNUM: i64 = (1_i64 << 61) - 1;
const MOST_NEGATIVE_FIXNUM: i64 = -(1_i64 << 61);

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
enum NativeIdentity {
    Symbol(String),
    WideInteger(i64),
    BigInteger(usize),
    Float(u64),
    String(usize),
    StringObject(usize),
    Vector(usize),
    Builtin(String),
    Lambda(usize),
    Buffer(usize),
    Marker(u64),
    Overlay(u64),
    CharTable(u64),
    Frame(u64),
    Terminal(u64),
    Record(u64),
    Finalizer(u64),
    ReaderForm(usize),
    Unbound,
}

#[repr(C, align(8))]
#[derive(Clone, Copy)]
struct NativeCons {
    car: NativeWord,
    cdr: NativeWord,
}

struct TouchedCons {
    native: *mut NativeCons,
    value: SharedCons,
    car_word: NativeWord,
    cdr_word: NativeWord,
}

#[derive(Default)]
struct TouchedFrame {
    conses: Vec<TouchedCons>,
    cons_set: IdentitySet,
}

const NATIVE_CONS_CHUNK_CAPACITY: usize = 4096;

/// Stable bump allocation for the two-word cons prefix read by generated
/// code.  GNU allocates Lisp_Cons objects from blocks; doing the same here
/// avoids one system allocation per native `cons`.  Keeping the block at
/// GNU's exact two-word density is important too: generated car/cdr loops
/// traverse this storage directly, while interpreter ownership lives in a
/// separate cold map only for cells that cross the boundary.
struct NativeConsArena {
    chunks: Vec<Box<[MaybeUninit<NativeCons>; NATIVE_CONS_CHUNK_CAPACITY]>>,
    len: usize,
    cursor: *mut NativeCons,
    end: *mut NativeCons,
}

impl Default for NativeConsArena {
    fn default() -> Self {
        Self {
            chunks: Vec::new(),
            len: 0,
            cursor: std::ptr::null_mut(),
            end: std::ptr::null_mut(),
        }
    }
}

impl NativeConsArena {
    fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline(always)]
    fn allocate(&mut self, value: NativeCons) -> *mut NativeCons {
        if self.cursor == self.end {
            return self.allocate_slow(value);
        }
        let pointer = self.cursor;
        unsafe { pointer.write(value) };
        self.cursor = unsafe { self.cursor.add(1) };
        self.len = self.len.wrapping_add(1);
        pointer
    }

    #[cold]
    #[inline(never)]
    fn allocate_slow(&mut self, value: NativeCons) -> *mut NativeCons {
        self.chunks.push(Box::new(
            [const { MaybeUninit::uninit() }; NATIVE_CONS_CHUNK_CAPACITY],
        ));
        self.cursor = self
            .chunks
            .last_mut()
            .expect("a native cons chunk was just allocated")
            .as_mut_ptr()
            .cast();
        self.end = unsafe { self.cursor.add(NATIVE_CONS_CHUNK_CAPACITY) };
        self.allocate(value)
    }
}

#[repr(align(8))]
struct NativeHandle {
    value: Value,
}

struct HandleEntry {
    native: Box<NativeHandle>,
    tag: usize,
}

#[repr(C, align(8))]
struct NativeSymbolWithPosition {
    header: isize,
    symbol: NativeWord,
    position: NativeWord,
}

/// Stable objects retained for native machine code.  GNU's GC provides the
/// same stability in C; this Rust owner also supplies the reverse lookup
/// needed by primitive-call wrappers.
#[derive(Default)]
pub(crate) struct NativeHeap {
    conses: NativeConsArena,
    cons_by_value: IdentityMap<*mut NativeCons>,
    cons_values: IdentityMap<SharedCons>,
    cons_snapshots: IdentityMap<ConsMutationSnapshot>,
    interpreter_dirty: Rc<ConsMutationQueue>,
    handles: Vec<HandleEntry>,
    handle_by_value: HashMap<NativeIdentity, usize>,
    handle_by_address: IdentityMap<usize>,
    symbol_with_position_views: HashMap<u64, Box<NativeSymbolWithPosition>>,
    touched_frames: Vec<TouchedFrame>,
}

impl NativeHeap {
    fn is_empty(&self) -> bool {
        self.conses.is_empty()
            && self.handles.is_empty()
            && self.symbol_with_position_views.is_empty()
            && self.touched_frames.is_empty()
    }

    pub(crate) fn begin_call(&mut self) {
        self.touched_frames.push(TouchedFrame::default());
    }

    pub(crate) fn encode(&mut self, value: &Value) -> Result<NativeWord, String> {
        self.encode_inner(value, &mut IdentitySet::default())
    }

    #[inline(always)]
    fn cons(&mut self, car_word: NativeWord, cdr_word: NativeWord) -> NativeWord {
        let native = self.conses.allocate(NativeCons {
            car: car_word,
            cdr: cdr_word,
        });
        let address = native as usize;
        debug_assert_eq!(address & TAG_MASK, 0);
        // Do not add a freshly allocated cell to the copy-back set.  Its
        // Rust value is deliberately lazy: like GNU's alloc.c, this hot path
        // only bump-allocates the two machine words.  A `ConsCell` is created
        // if and when the word crosses back through `decode` (as a result,
        // argument mutation, or primitive input).  This also avoids retaining
        // and rescanning every superseded swap in native bubble sort.
        address + TAG_CONS
    }

    fn track_cons(
        &mut self,
        native: *mut NativeCons,
        value: &SharedCons,
        car_word: NativeWord,
        cdr_word: NativeWord,
    ) {
        let address = native as usize;
        let Some(frame) = self.touched_frames.last_mut() else {
            return;
        };
        if frame.cons_set.insert(address) {
            frame.conses.push(TouchedCons {
                native,
                value: value.clone(),
                car_word,
                cdr_word,
            });
        }
    }

    fn mark_cons_mirror_current(&mut self, native: *mut NativeCons, value: &SharedCons) {
        let address = native as usize;
        if let Some(snapshot) = self.cons_snapshots.get(&address) {
            snapshot.mark_current();
            return;
        }
        self.cons_snapshots.insert(
            address,
            ConsMutationSnapshot::tracked_cell(value, address, &self.interpreter_dirty),
        );
    }

    /// Translate one tagged word to its interpreter object without walking
    /// through a cons.  Every cons word in generated code already names the
    /// stable mirror entry for the corresponding Rust cons cell.
    fn value_for_word(
        &mut self,
        word: NativeWord,
        decoding_conses: &mut IdentitySet,
    ) -> Result<Value, String> {
        if word == 0 {
            return Ok(Value::Nil);
        }
        if word == native_boolean(true) {
            return Ok(Value::T);
        }
        if word & 3 == TAG_FIXNUM_LOW {
            return Ok(Value::Integer((word as isize >> FIXNUM_BITS) as i64));
        }
        if word & TAG_MASK == TAG_CONS {
            return self.decode_inner(word, decoding_conses, false);
        }
        let tag = word & TAG_MASK;
        let address = word.wrapping_sub(tag);
        let index = self
            .handle_by_address
            .get(&address)
            .copied()
            .ok_or_else(|| format!("unknown native Lisp word 0x{word:x}"))?;
        let entry = &self.handles[index];
        if entry.tag != tag {
            return Err(format!("native Lisp word 0x{word:x} has a mismatched tag"));
        }
        Ok(entry.native.value.clone())
    }

    fn encode_inner(
        &mut self,
        value: &Value,
        encoding_conses: &mut IdentitySet,
    ) -> Result<NativeWord, String> {
        match value {
            Value::Nil => Ok(0),
            Value::T => Ok(native_boolean(true)),
            Value::Symbol(name) if name == "nil" => Ok(0),
            Value::Symbol(name) if name == "t" => Ok(native_boolean(true)),
            Value::Symbol(name) => {
                self.encode_handle(NativeIdentity::Symbol(name.to_string()), value, TAG_SYMBOL)
            }
            Value::Integer(integer)
                if (MOST_NEGATIVE_FIXNUM..=MOST_POSITIVE_FIXNUM).contains(integer) =>
            {
                Ok(integer
                    .wrapping_shl(FIXNUM_BITS)
                    .wrapping_add(TAG_FIXNUM_LOW as i64) as usize)
            }
            Value::Cons(cell) if crate::lisp::primitives::is_vector_value(value) => self
                .encode_handle(
                    NativeIdentity::Vector(ConsCell::identity(cell)),
                    value,
                    TAG_VECTORLIKE,
                ),
            Value::Cons(cell) => self.encode_cons(cell, encoding_conses),
            _ => {
                let (identity, tag) = handle_identity(value)?;
                self.encode_handle(identity, value, tag)
            }
        }
    }

    fn encode_cons(
        &mut self,
        cell: &SharedCons,
        encoding_conses: &mut IdentitySet,
    ) -> Result<NativeWord, String> {
        let identity = ConsCell::identity(cell);
        let (native, existing) = if let Some(native) = self.cons_by_value.get(&identity).copied() {
            (native, true)
        } else {
            let native = self.conses.allocate(NativeCons { car: 0, cdr: 0 });
            let address = native as usize;
            if address & TAG_MASK != 0 {
                return Err("native cons allocation is not tag-aligned".to_string());
            }
            self.cons_by_value.insert(identity, native);
            self.cons_values.insert(address, cell.clone());
            (native, false)
        };
        let address = native as usize;
        if existing
            && self
                .cons_snapshots
                .get(&address)
                .is_some_and(ConsMutationSnapshot::is_current)
        {
            let car_word = unsafe { (*native).car };
            let cdr_word = unsafe { (*native).cdr };
            self.track_cons(native, cell, car_word, cdr_word);
            return Ok(address + TAG_CONS);
        }
        if !encoding_conses.insert(identity) {
            return Ok(address + TAG_CONS);
        }
        let value = self
            .cons_values
            .get(&address)
            .expect("an interpreter cons mirror retains its Rust value")
            .clone();
        let car = value.car.borrow().clone();
        let cdr = value.cdr.borrow().clone();
        let car = self.encode_inner(&car, encoding_conses)?;
        let cdr = self.encode_inner(&cdr, encoding_conses)?;
        unsafe {
            (*native).car = car;
            (*native).cdr = cdr;
        }
        self.mark_cons_mirror_current(native, &value);
        self.track_cons(native, &value, car, cdr);
        encoding_conses.remove(&identity);
        Ok(address + TAG_CONS)
    }

    fn encode_handle(
        &mut self,
        identity: NativeIdentity,
        value: &Value,
        tag: usize,
    ) -> Result<NativeWord, String> {
        let index = if let Some(index) = self.handle_by_value.get(&identity).copied() {
            index
        } else {
            let native = Box::new(NativeHandle {
                value: value.clone(),
            });
            let address = (&*native as *const NativeHandle) as usize;
            if address & TAG_MASK != 0 {
                return Err("native object allocation is not tag-aligned".to_string());
            }
            let index = self.handles.len();
            self.handles.push(HandleEntry { native, tag });
            self.handle_by_value.insert(identity, index);
            self.handle_by_address.insert(address, index);
            index
        };
        let entry = &self.handles[index];
        if entry.tag != tag {
            return Err("native object identity changed Lisp tag".to_string());
        }
        Ok((&*entry.native as *const NativeHandle) as usize + tag)
    }

    pub(crate) fn decode(&mut self, word: NativeWord) -> Result<Value, String> {
        self.decode_inner(word, &mut IdentitySet::default(), false)
    }

    fn decode_result(&mut self, word: NativeWord) -> Result<Value, String> {
        self.decode_inner(word, &mut IdentitySet::default(), true)
    }

    /// Materialize the overwhelmingly common native proper-list shape in one
    /// linear pass.  The general decoder below must install placeholders and
    /// mutation-track both fields so it can preserve arbitrary sharing and
    /// cycles.  A chain whose cars are not conses and whose cdr does not loop
    /// can instead be built back-to-front with each Rust cell initialized
    /// exactly once.
    fn try_decode_linear_cons(
        &mut self,
        word: NativeWord,
        decoding_conses: &mut IdentitySet,
        mark_clean: bool,
    ) -> Result<Option<Value>, String> {
        let mut cursor = word;
        let mut nodes = Vec::new();
        let mut seen = IdentitySet::default();
        let tail = loop {
            if cursor & 3 == TAG_FIXNUM_LOW || cursor & TAG_MASK != TAG_CONS {
                break self.decode_inner(cursor, decoding_conses, mark_clean)?;
            }
            let address = cursor.wrapping_sub(TAG_CONS);
            if let Some(value) = self.cons_values.get(&address) {
                if mark_clean {
                    return Ok(None);
                }
                break Value::Cons(value.clone());
            }
            if decoding_conses.contains(&address) || !seen.insert(address) {
                return Ok(None);
            }
            let native = address as *mut NativeCons;
            let car_word = unsafe { (*native).car };
            if car_word & TAG_MASK == TAG_CONS {
                return Ok(None);
            }
            let cdr_word = unsafe { (*native).cdr };
            nodes.push((address, native, car_word, cdr_word));
            cursor = cdr_word;
        };

        if nodes.is_empty() {
            return Ok(None);
        }
        let mut tail = tail;
        for (address, native, car_word, cdr_word) in nodes.into_iter().rev() {
            let car = self.decode_inner(car_word, decoding_conses, mark_clean)?;
            let Value::Cons(value) = Value::cons(car, tail) else {
                unreachable!("Value::cons always constructs a cons cell");
            };
            self.cons_values.insert(address, value.clone());
            self.cons_by_value
                .insert(ConsCell::identity(&value), native);
            self.mark_cons_mirror_current(native, &value);
            if !mark_clean {
                self.track_cons(native, &value, car_word, cdr_word);
            }
            tail = Value::Cons(value);
        }
        Ok(Some(tail))
    }

    fn decode_inner(
        &mut self,
        word: NativeWord,
        decoding_conses: &mut IdentitySet,
        mark_clean: bool,
    ) -> Result<Value, String> {
        if word == 0 {
            return Ok(Value::Nil);
        }
        if word == native_boolean(true) {
            return Ok(Value::T);
        }
        if word & 3 == TAG_FIXNUM_LOW {
            return Ok(Value::Integer((word as isize >> FIXNUM_BITS) as i64));
        }
        if word & TAG_MASK == TAG_CONS {
            let address = word.wrapping_sub(TAG_CONS);
            if let Some(value) = self.cons_values.get(&address).cloned()
                && self
                    .cons_snapshots
                    .get(&address)
                    .is_some_and(ConsMutationSnapshot::is_current)
            {
                let native = address as *mut NativeCons;
                let car_word = unsafe { (*native).car };
                let cdr_word = unsafe { (*native).cdr };
                if mark_clean {
                    if let Some(frame) = self.touched_frames.last_mut() {
                        frame.cons_set.remove(&address);
                    }
                } else {
                    self.track_cons(native, &value, car_word, cdr_word);
                }
                return Ok(Value::Cons(value));
            }
            if !self.cons_values.contains_key(&address)
                && let Some(value) =
                    self.try_decode_linear_cons(word, decoding_conses, mark_clean)?
            {
                return Ok(value);
            }
            let native = address as *mut NativeCons;
            let car_word = unsafe { (*native).car };
            let cdr_word = unsafe { (*native).cdr };
            // Native-only allocations stay as two words until an interpreter
            // observer needs one.  Install the placeholder before following
            // either field so circular native structures materialize safely.
            let value = if let Some(value) = self.cons_values.get(&address) {
                value.clone()
            } else {
                let Value::Cons(value) = Value::cons(Value::Nil, Value::Nil) else {
                    unreachable!("Value::cons always constructs a cons cell");
                };
                self.cons_values.insert(address, value.clone());
                self.cons_by_value
                    .insert(ConsCell::identity(&value), native);
                value
            };
            if !decoding_conses.insert(address) {
                return Ok(Value::Cons(value));
            }
            let car = self.decode_inner(car_word, decoding_conses, mark_clean)?;
            let cdr = self.decode_inner(cdr_word, decoding_conses, mark_clean)?;
            if !crate::lisp::primitives::values_eql(&value.car.borrow(), &car) {
                *value.car.borrow_mut() = car;
            }
            if !crate::lisp::primitives::values_eql(&value.cdr.borrow(), &cdr) {
                *value.cdr.borrow_mut() = cdr;
            }
            self.mark_cons_mirror_current(native, &value);
            if mark_clean {
                if let Some(frame) = self.touched_frames.last_mut() {
                    frame.cons_set.remove(&address);
                }
            } else {
                self.track_cons(native, &value, car_word, cdr_word);
            }
            decoding_conses.remove(&address);
            return Ok(Value::Cons(value));
        }

        let tag = word & TAG_MASK;
        let address = word.wrapping_sub(tag);
        let index = self
            .handle_by_address
            .get(&address)
            .copied()
            .ok_or_else(|| format!("unknown native Lisp word 0x{word:x}"))?;
        let entry = &self.handles[index];
        if entry.tag != tag {
            return Err(format!("native Lisp word 0x{word:x} has a mismatched tag"));
        }
        Ok(entry.native.value.clone())
    }

    #[cfg(test)]
    pub(crate) fn finish_call(&mut self) -> Result<Vec<Value>, String> {
        self.synchronize_touched_conses(false)
    }

    /// Publish interpreter-side cons mutations before generated code resumes.
    ///
    /// A primitive can mutate a cons reached through a closure, record, or
    /// global rather than through one of its explicit arguments.  GNU's C
    /// runtime needs no bookkeeping because both sides share the same cell;
    /// Emaxx records every mirrored cell that actually changes and refreshes
    /// only those two-word mirrors.
    fn publish_interpreter_writes(&mut self) -> Result<(), String> {
        let mut dirty_addresses = IdentitySet::default();
        for address in self.interpreter_dirty.drain() {
            if !dirty_addresses.insert(address)
                || self
                    .cons_snapshots
                    .get(&address)
                    .is_none_or(ConsMutationSnapshot::is_current)
            {
                continue;
            }
            let value = self
                .cons_values
                .get(&address)
                .cloned()
                .ok_or_else(|| "dirty interpreter cons has no native mirror".to_string())?;
            let mut encoding_conses = IdentitySet::default();
            self.encode_cons(&value, &mut encoding_conses)?;
        }
        for frame in &mut self.touched_frames {
            for touched in &mut frame.conses {
                if !dirty_addresses.contains(&(touched.native as usize)) {
                    continue;
                }
                touched.car_word = unsafe { (*touched.native).car };
                touched.cdr_word = unsafe { (*touched.native).cdr };
            }
        }
        Ok(())
    }

    #[cfg(test)]
    fn synchronize_nested_return(&mut self) -> Result<Vec<Value>, String> {
        self.synchronize_touched_conses(true)
    }

    fn synchronize_touched_conses(&mut self, retain_tracking: bool) -> Result<Vec<Value>, String> {
        let Some(frame_index) = self.touched_frames.len().checked_sub(1) else {
            return Ok(Vec::new());
        };
        let (mut touched, touched_set) = {
            let frame = &mut self.touched_frames[frame_index];
            (
                std::mem::take(&mut frame.conses),
                std::mem::take(&mut frame.cons_set),
            )
        };
        let mut retained = Vec::with_capacity(touched.len());
        let mut mutated_conses = Vec::new();
        let mut decoding_conses = IdentitySet::default();
        let mut result = Ok(());
        for mut touched in touched.drain(..) {
            let address = touched.native as usize;
            if !touched_set.contains(&address) {
                continue;
            }
            if result.is_ok() {
                let car_word = unsafe { (*touched.native).car };
                let cdr_word = unsafe { (*touched.native).cdr };
                let changed = car_word != touched.car_word || cdr_word != touched.cdr_word;
                if car_word != touched.car_word {
                    match self.value_for_word(car_word, &mut decoding_conses) {
                        Ok(car) => {
                            if !crate::lisp::primitives::values_eql(
                                &touched.value.car.borrow(),
                                &car,
                            ) {
                                *touched.value.car.borrow_mut() = car;
                            }
                        }
                        Err(error) => result = Err(error),
                    }
                }
                if result.is_ok() && cdr_word != touched.cdr_word {
                    match self.value_for_word(cdr_word, &mut decoding_conses) {
                        Ok(cdr) => {
                            if !crate::lisp::primitives::values_eql(
                                &touched.value.cdr.borrow(),
                                &cdr,
                            ) {
                                *touched.value.cdr.borrow_mut() = cdr;
                            }
                        }
                        Err(error) => result = Err(error),
                    }
                }
                if result.is_ok() {
                    touched.car_word = car_word;
                    touched.cdr_word = cdr_word;
                    self.mark_cons_mirror_current(touched.native, &touched.value);
                    if changed {
                        mutated_conses.push(Value::Cons(touched.value.clone()));
                    }
                }
            }
            if retain_tracking {
                retained.push(touched);
            }
        }
        if retain_tracking {
            let frame = &mut self.touched_frames[frame_index];
            let mut newly_touched = std::mem::take(&mut frame.conses);
            let mut newly_touched_set = std::mem::take(&mut frame.cons_set);
            for touched in retained {
                let address = touched.native as usize;
                if newly_touched_set.insert(address) {
                    newly_touched.push(touched);
                }
            }
            frame.conses = newly_touched;
            frame.cons_set = newly_touched_set;
        } else {
            self.touched_frames.pop();
        }
        result.map(|()| mutated_conses)
    }

    fn symbol_with_position_pointer(
        &mut self,
        interpreter: &Interpreter,
        value: &Value,
    ) -> Result<*mut c_void, String> {
        let Value::Record(record_id) = value else {
            return Err("native symbol-with-position helper received a non-record".to_string());
        };
        let (symbol, position) = symbol_with_pos_parts(interpreter, value).ok_or_else(|| {
            "native symbol-with-position helper received the wrong record".to_string()
        })?;
        let symbol = self.encode(&symbol)?;
        let position = self.encode(&Value::Integer(position))?;
        // PSEUDOVECTOR_FLAG | (PVEC_SYMBOL_WITH_POS << 24) | two Lisp fields.
        const HEADER: isize = (1_isize << 62) | (6_isize << 24) | 2;
        let view = self
            .symbol_with_position_views
            .entry(*record_id)
            .or_insert_with(|| {
                Box::new(NativeSymbolWithPosition {
                    header: HEADER,
                    symbol: 0,
                    position: 0,
                })
            });
        view.symbol = symbol;
        view.position = position;
        Ok((&mut **view as *mut NativeSymbolWithPosition).cast())
    }
}

fn handle_identity(value: &Value) -> Result<(NativeIdentity, usize), String> {
    Ok(match value {
        Value::Integer(integer) => (NativeIdentity::WideInteger(*integer), TAG_VECTORLIKE),
        Value::BigInteger(integer) => (
            NativeIdentity::BigInteger(integer.identity_ptr()),
            TAG_VECTORLIKE,
        ),
        Value::Float(float) => (NativeIdentity::Float(float.to_bits()), TAG_FLOAT),
        Value::String(string) => (NativeIdentity::String(string.identity_ptr()), TAG_STRING),
        Value::StringObject(string) => (
            NativeIdentity::StringObject(std::rc::Rc::as_ptr(string) as usize),
            TAG_STRING,
        ),
        Value::BuiltinFunc(name) => (NativeIdentity::Builtin(name.to_string()), TAG_VECTORLIKE),
        Value::Lambda(lambda) => (
            NativeIdentity::Lambda(std::rc::Rc::as_ptr(lambda) as usize),
            TAG_VECTORLIKE,
        ),
        Value::Buffer(buffer) => (
            NativeIdentity::Buffer(std::rc::Rc::as_ptr(buffer) as usize),
            TAG_VECTORLIKE,
        ),
        Value::Marker(id) => (NativeIdentity::Marker(*id), TAG_VECTORLIKE),
        Value::Overlay(id) => (NativeIdentity::Overlay(*id), TAG_VECTORLIKE),
        Value::CharTable(id) => (NativeIdentity::CharTable(*id), TAG_VECTORLIKE),
        Value::Frame(id) => (NativeIdentity::Frame(*id), TAG_VECTORLIKE),
        Value::Terminal(id) => (NativeIdentity::Terminal(*id), TAG_VECTORLIKE),
        Value::Record(id) => (NativeIdentity::Record(*id), TAG_VECTORLIKE),
        Value::Finalizer(id) => (NativeIdentity::Finalizer(*id), TAG_VECTORLIKE),
        Value::ReaderForm(form) => (
            NativeIdentity::ReaderForm(std::rc::Rc::as_ptr(form) as usize),
            TAG_VECTORLIKE,
        ),
        Value::Unbound => (NativeIdentity::Unbound, TAG_SYMBOL),
        Value::Nil | Value::T | Value::Symbol(_) | Value::Cons(_) => {
            return Err("native heap received an object with a direct encoding".to_string());
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    extern "C" fn add_one_fixnum(value: NativeWord) -> NativeWord {
        value.wrapping_add(1 << FIXNUM_BITS)
    }

    extern "C" fn raise_wrong_type(predicate: NativeWord, value: NativeWord) -> NativeWord {
        runtime_wrong_type_argument(predicate, value);
        0
    }

    extern "C" fn replace_cons_cdr(pair: NativeWord, cdr: NativeWord) -> NativeWord {
        if pair & TAG_MASK != TAG_CONS {
            return 0;
        }
        let native = pair.wrapping_sub(TAG_CONS) as *mut NativeCons;
        unsafe {
            (*native).cdr = cdr;
        }
        native_boolean(true)
    }

    extern "C" fn mutate_cons_then_compare_records(
        pair: NativeWord,
        car: NativeWord,
        left: NativeWord,
        right: NativeWord,
    ) -> NativeWord {
        if pair & TAG_MASK != TAG_CONS {
            return 0;
        }
        let native = pair.wrapping_sub(TAG_CONS) as *mut NativeCons;
        unsafe {
            (*native).car = car;
        }
        let equal = super::super::abi::native_subrs()
            .iter()
            .position(|subroutine| subroutine.name == "equal")
            .expect("equal belongs to the native ABI");
        invoke_subr(equal, &[left, right])
    }

    extern "C" fn vectorlike_tag_p(value: NativeWord) -> NativeWord {
        native_boolean(value & TAG_MASK == TAG_VECTORLIKE)
    }

    #[test]
    fn runtime_link_table_has_exact_helper_prefix_and_subr_tail() {
        let table = runtime_link_table();
        assert_eq!(table.len(), 15 + super::super::abi::native_subrs().len());
        assert_eq!(table[0], runtime_wrong_type_argument as *mut c_void);
        assert_eq!(table[3], runtime_push_handler as *mut c_void);
        assert_eq!(table[14], runtime_maybe_quit as *mut c_void);
        assert_eq!(
            table[15],
            super::super::generated_native_subrs::native_subr_address(0)
        );
        assert!(table.iter().all(|address| !address.is_null()));
    }

    #[test]
    fn runtime_layout_matches_the_generated_handler_and_thread_types() {
        assert_eq!(
            std::mem::size_of::<NativeCons>(),
            2 * std::mem::size_of::<NativeWord>()
        );
        assert_eq!(std::mem::size_of::<NativeHandler>(), HANDLER_SIZE);
        assert_eq!(std::mem::size_of::<NativeThreadState>(), THREAD_STATE_SIZE);
        assert!(HANDLER_VALUE_OFFSET + std::mem::size_of::<NativeWord>() <= HANDLER_NEXT_OFFSET);
        const { assert!(HANDLER_JMP_OFFSET < HANDLER_SIZE) };

        let mut handler = NativeHandler::new(std::ptr::null_mut());
        handler.set_value(0x1234);
        let stored = unsafe {
            std::ptr::read_unaligned(
                handler
                    .bytes
                    .as_ptr()
                    .add(HANDLER_VALUE_OFFSET)
                    .cast::<NativeWord>(),
            )
        };
        assert_eq!(stored, 0x1234);
    }

    #[test]
    fn native_call_trampoline_returns_values_and_lisp_errors_through_rust() {
        let mut interpreter = Interpreter::new();
        let mut environment = Env::new();
        let mut runtime = NativeRuntime::default();
        assert_eq!(
            runtime
                .invoke(
                    &mut interpreter,
                    &mut environment,
                    add_one_fixnum as *const c_void,
                    NativeCallingConvention::Fixed,
                    &[Value::Integer(41)],
                )
                .expect("ordinary native return"),
            Value::Integer(42)
        );

        let error = runtime
            .invoke(
                &mut interpreter,
                &mut environment,
                raise_wrong_type as *const c_void,
                NativeCallingConvention::Fixed,
                &[Value::symbol("integerp"), Value::T],
            )
            .expect_err("native helper error should return through the call boundary");
        assert_eq!(error.condition_type(), "wrong-type-argument");
        let LispError::SignalValue(data) = error else {
            panic!("wrong-type-argument lost its Lisp condition data");
        };
        assert_eq!(
            data.to_vec().expect("proper condition data"),
            vec![
                Value::symbol("wrong-type-argument"),
                Value::symbol("integerp"),
                Value::T,
            ]
        );
    }

    #[test]
    fn native_keymap_arguments_are_lists_and_direct_mutation_updates_rust_lookup_state() {
        let mut interpreter = Interpreter::new();
        let mut environment = Env::new();
        let mut runtime = NativeRuntime::default();
        let keymap = crate::lisp::primitives::make_runtime_keymap(&mut interpreter, None);
        let definition = Value::symbol("describe-chinese-environment-map");
        let tail = Value::list([Value::cons(Value::symbol("Chinese"), definition.clone())]);

        assert_eq!(
            runtime
                .invoke(
                    &mut interpreter,
                    &mut environment,
                    replace_cons_cdr as *const c_void,
                    NativeCallingConvention::Fixed,
                    &[keymap.clone(), tail],
                )
                .expect("native keymap mutation"),
            Value::T
        );

        let Value::Record(keymap_id) = keymap else {
            panic!("runtime keymap lost its identity-bearing record");
        };
        let record = interpreter
            .find_record(keymap_id)
            .expect("runtime keymap record");
        let bindings =
            crate::lisp::primitives::keymap_bindings(record).expect("synchronized keymap bindings");
        assert_eq!(bindings.len(), 1);
        assert_eq!(bindings[0].value, definition);
    }

    #[test]
    fn native_vector_arguments_use_gnu_vectorlike_tag_not_internal_cons_storage() {
        let mut interpreter = Interpreter::new();
        let mut environment = Env::new();
        let mut runtime = NativeRuntime::default();
        let vector = Value::list([
            Value::symbol("vector-literal"),
            Value::Integer(1),
            Value::Integer(2),
        ]);

        assert_eq!(
            runtime
                .invoke(
                    &mut interpreter,
                    &mut environment,
                    vectorlike_tag_p as *const c_void,
                    NativeCallingConvention::Fixed,
                    &[vector],
                )
                .expect("native vector tag"),
            Value::T
        );
        assert_eq!(
            runtime
                .invoke(
                    &mut interpreter,
                    &mut environment,
                    vectorlike_tag_p as *const c_void,
                    NativeCallingConvention::Fixed,
                    &[Value::list([Value::Integer(1), Value::Integer(2)])],
                )
                .expect("native cons tag"),
            Value::Nil
        );
    }

    #[test]
    fn rust_primitive_observes_direct_native_cons_write_reached_through_a_record() {
        let mut interpreter = Interpreter::new();
        let mut environment = Env::new();
        let mut runtime = NativeRuntime::default();
        let left_list = Value::list([Value::Integer(1), Value::Integer(2)]);
        let mutated_cell = left_list.cdr().expect("second list cell");
        let right_list = Value::list([Value::Integer(1), Value::Integer(3)]);
        let left_record = interpreter.create_record("sample", vec![left_list.clone()]);
        let right_record = interpreter.create_record("sample", vec![right_list]);

        assert_eq!(
            runtime
                .invoke(
                    &mut interpreter,
                    &mut environment,
                    mutate_cons_then_compare_records as *const c_void,
                    NativeCallingConvention::Fixed,
                    &[mutated_cell, Value::Integer(3), left_record, right_record,],
                )
                .expect("primitive call after a direct native write"),
            Value::T
        );
        assert_eq!(
            left_list.to_vec().expect("mutated proper list"),
            vec![Value::Integer(1), Value::Integer(3)]
        );
    }

    #[test]
    fn direct_native_cons_mutation_updates_the_interpreter_value() {
        let value = Value::cons(Value::Integer(1), Value::Integer(2));
        let mut heap = NativeHeap::default();
        heap.begin_call();
        let word = heap.encode(&value).expect("encode cons");
        assert_eq!(word & TAG_MASK, TAG_CONS);
        let native = word.wrapping_sub(TAG_CONS) as *mut NativeCons;
        unsafe {
            (*native).car = ((9_i64 << FIXNUM_BITS) + TAG_FIXNUM_LOW as i64) as NativeWord;
        }
        heap.finish_call().expect("synchronize direct mutation");
        assert_eq!(value.car().expect("car"), Value::Integer(9));
        assert_eq!(value.cdr().expect("cdr"), Value::Integer(2));
    }

    #[test]
    fn intermediate_decode_does_not_hide_later_native_mutation() {
        let value = Value::cons(Value::Integer(1), Value::Integer(2));
        let mut heap = NativeHeap::default();
        heap.begin_call();
        let word = heap.encode(&value).expect("encode cons");
        assert_eq!(heap.decode(word).expect("intermediate decode"), value);
        let native = word.wrapping_sub(TAG_CONS) as *mut NativeCons;
        unsafe {
            (*native).cdr = ((7_i64 << FIXNUM_BITS) + TAG_FIXNUM_LOW as i64) as NativeWord;
        }
        heap.finish_call().expect("synchronize later mutation");
        assert_eq!(value.car().expect("car"), Value::Integer(1));
        assert_eq!(value.cdr().expect("cdr"), Value::Integer(7));
    }

    #[test]
    fn nested_native_return_publishes_writes_and_keeps_tracking() {
        let value = Value::cons(Value::Integer(1), Value::Integer(2));
        let mut heap = NativeHeap::default();
        heap.begin_call();
        let word = heap.encode(&value).expect("encode cons");
        let native = word.wrapping_sub(TAG_CONS) as *mut NativeCons;
        unsafe {
            (*native).car = ((9_i64 << FIXNUM_BITS) + TAG_FIXNUM_LOW as i64) as NativeWord;
        }
        heap.synchronize_nested_return()
            .expect("publish the nested native write");
        assert_eq!(value.car().expect("car"), Value::Integer(9));

        unsafe {
            (*native).cdr = ((7_i64 << FIXNUM_BITS) + TAG_FIXNUM_LOW as i64) as NativeWord;
        }
        heap.finish_call().expect("publish the later outer write");
        assert_eq!(value.car().expect("car"), Value::Integer(9));
        assert_eq!(value.cdr().expect("cdr"), Value::Integer(7));
    }

    #[test]
    fn native_cons_uses_existing_tail_mirror_without_recursive_translation() {
        let tail = Value::list([Value::Integer(2), Value::Integer(3)]);
        let mut heap = NativeHeap::default();
        heap.begin_call();
        let tail_word = heap.encode(&tail).expect("encode tail");
        let head_word = heap.encode(&Value::Integer(1)).expect("encode head fixnum");
        let list_word = heap.cons(head_word, tail_word);
        let list = heap.decode(list_word).expect("decode native cons");
        heap.finish_call().expect("synchronize native cons");
        assert_eq!(
            list,
            Value::list([Value::Integer(1), Value::Integer(2), Value::Integer(3),])
        );
    }

    #[test]
    fn indirect_interpreter_cons_mutation_updates_the_native_mirror() {
        let mut heap = NativeHeap::default();
        heap.begin_call();
        let one = heap
            .encode(&Value::Integer(1))
            .expect("encode initial value");
        let word = heap.cons(one, 0);
        let value = heap.decode(word).expect("materialize native cons");

        value
            .set_car(Value::Integer(9))
            .expect("mutate indirectly retained cons");
        heap.publish_interpreter_writes()
            .expect("publish interpreter mutation");

        let native = word.wrapping_sub(TAG_CONS) as *mut NativeCons;
        assert_eq!(
            unsafe { (*native).car },
            ((9_i64 << FIXNUM_BITS) + TAG_FIXNUM_LOW as i64) as NativeWord
        );
        heap.finish_call().expect("finish synchronized call");
        assert_eq!(value.car().expect("car"), Value::Integer(9));
    }

    #[test]
    fn queued_interpreter_mutation_is_published_on_the_next_native_call() {
        let value = Value::list([Value::Integer(1), Value::Integer(2)]);
        let tail = value.cdr().expect("list tail");
        let mut heap = NativeHeap::default();
        heap.begin_call();
        let word = heap.encode(&value).expect("encode list");
        let tail_word = unsafe { (*(word.wrapping_sub(TAG_CONS) as *mut NativeCons)).cdr };
        heap.finish_call().expect("finish first call");

        tail.set_car(Value::Integer(7)).expect("mutate list tail");
        heap.begin_call();
        heap.publish_interpreter_writes()
            .expect("publish queued mutation");

        let native_tail = tail_word.wrapping_sub(TAG_CONS) as *mut NativeCons;
        assert_eq!(
            unsafe { (*native_tail).car },
            ((7_i64 << FIXNUM_BITS) + TAG_FIXNUM_LOW as i64) as NativeWord
        );
        heap.finish_call().expect("finish second call");
    }

    #[test]
    #[ignore]
    fn native_hot_path_probe() {
        type Many = unsafe extern "C" fn(isize, *const NativeWord) -> NativeWord;
        type FixedTwo = extern "C" fn(NativeWord, NativeWord) -> NativeWord;
        let less: Many = unsafe {
            std::mem::transmute(super::super::generated_native_subrs::native_subr_address(
                1274,
            ))
        };
        let cons: FixedTwo = unsafe {
            std::mem::transmute(super::super::generated_native_subrs::native_subr_address(
                1071,
            ))
        };

        let started = std::time::Instant::now();
        let mut result = 0;
        for index in 0..998_001_i64 {
            let args = [
                ((index & 1023) << FIXNUM_BITS) as NativeWord + TAG_FIXNUM_LOW,
                (((index + 1) & 1023) << FIXNUM_BITS) as NativeWord + TAG_FIXNUM_LOW,
            ];
            result ^= unsafe { less(2, std::hint::black_box(args.as_ptr())) };
        }
        eprintln!(
            "comparison {:?} result={}",
            started.elapsed(),
            std::hint::black_box(result)
        );

        let mut interpreter = Interpreter::new();
        let mut environment = Env::new();
        let mut runtime = NativeRuntime::default();
        let started = std::time::Instant::now();
        let tail = with_active_call(&mut interpreter, &mut environment, &mut runtime, || {
            let mut tail = 0;
            for index in 0..250_000_i64 {
                let car = (index << FIXNUM_BITS) as NativeWord + TAG_FIXNUM_LOW;
                tail = cons(car, tail);
            }
            tail
        });
        eprintln!(
            "cons {:?} result={}",
            started.elapsed(),
            std::hint::black_box(tail)
        );
    }
}
