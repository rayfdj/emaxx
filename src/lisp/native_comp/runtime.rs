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
    ConsCell, ConsMutationQueue, ConsWords, IdentityBuildHasher, NativeConsMutationRegistration,
    SharedCons, SymbolName, Value,
};
use crate::lisp::{
    eval::Interpreter,
    types::{Env, LispError},
};
use std::cell::Cell;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::ffi::c_void;
use std::mem::MaybeUninit;
use std::rc::{Rc, Weak};
use std::sync::{
    Mutex, OnceLock,
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

/// Call the same machine-word ABI that eval.c:funcall_subr calls directly.
/// The surrounding native activation owns non-local-exit handling; this call
/// must not interpose another setjmp frame between Ffuncall and the subr.
unsafe fn call_word_target(
    target: *const c_void,
    convention: NativeCallingConvention,
    arguments: &[NativeWord],
) -> NativeWord {
    match convention {
        NativeCallingConvention::Many => unsafe {
            let function = std::mem::transmute::<
                *const c_void,
                unsafe extern "C" fn(isize, *const NativeWord) -> NativeWord,
            >(target);
            function(arguments.len() as isize, arguments.as_ptr())
        },
        NativeCallingConvention::Fixed => match arguments {
            [] => unsafe {
                std::mem::transmute::<*const c_void, unsafe extern "C" fn() -> NativeWord>(target)()
            },
            [a] => unsafe {
                std::mem::transmute::<*const c_void, unsafe extern "C" fn(NativeWord) -> NativeWord>(
                    target,
                )(*a)
            },
            [a, b] => unsafe {
                std::mem::transmute::<
                    *const c_void,
                    unsafe extern "C" fn(NativeWord, NativeWord) -> NativeWord,
                >(target)(*a, *b)
            },
            [a, b, c] => unsafe {
                std::mem::transmute::<
                    *const c_void,
                    unsafe extern "C" fn(NativeWord, NativeWord, NativeWord) -> NativeWord,
                >(target)(*a, *b, *c)
            },
            [a, b, c, d] => unsafe {
                std::mem::transmute::<
                    *const c_void,
                    unsafe extern "C" fn(
                        NativeWord,
                        NativeWord,
                        NativeWord,
                        NativeWord,
                    ) -> NativeWord,
                >(target)(*a, *b, *c, *d)
            },
            [a, b, c, d, e] => unsafe {
                std::mem::transmute::<
                    *const c_void,
                    unsafe extern "C" fn(
                        NativeWord,
                        NativeWord,
                        NativeWord,
                        NativeWord,
                        NativeWord,
                    ) -> NativeWord,
                >(target)(*a, *b, *c, *d, *e)
            },
            [a, b, c, d, e, f] => unsafe {
                std::mem::transmute::<
                    *const c_void,
                    unsafe extern "C" fn(
                        NativeWord,
                        NativeWord,
                        NativeWord,
                        NativeWord,
                        NativeWord,
                        NativeWord,
                    ) -> NativeWord,
                >(target)(*a, *b, *c, *d, *e, *f)
            },
            [a, b, c, d, e, f, g] => unsafe {
                std::mem::transmute::<
                    *const c_void,
                    unsafe extern "C" fn(
                        NativeWord,
                        NativeWord,
                        NativeWord,
                        NativeWord,
                        NativeWord,
                        NativeWord,
                        NativeWord,
                    ) -> NativeWord,
                >(target)(*a, *b, *c, *d, *e, *f, *g)
            },
            [a, b, c, d, e, f, g, h] => unsafe {
                std::mem::transmute::<
                    *const c_void,
                    unsafe extern "C" fn(
                        NativeWord,
                        NativeWord,
                        NativeWord,
                        NativeWord,
                        NativeWord,
                        NativeWord,
                        NativeWord,
                        NativeWord,
                    ) -> NativeWord,
                >(target)(*a, *b, *c, *d, *e, *f, *g, *h)
            },
            _ => unreachable!("fixed native functions have at most eight arguments"),
        },
    }
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

// alloc.c:flush_stack_call_func forces every callee-saved register onto the
// machine stack before conservative GC scans it.  Rust has no equivalent
// intrinsic, so keep that ABI boundary explicit for generated AArch64 code.
#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
core::arch::global_asm!(
    r#"
    .section __TEXT,__text,regular,pure_instructions
    .p2align 2
    .private_extern _emaxx_native_gc_trampoline
_emaxx_native_gc_trampoline:
    sub sp, sp, #96
    stp x29, x30, [sp]
    stp x19, x20, [sp, #16]
    stp x21, x22, [sp, #32]
    stp x23, x24, [sp, #48]
    stp x25, x26, [sp, #64]
    stp x27, x28, [sp, #80]
    mov x29, sp
    mov x0, sp
    bl _emaxx_native_gc_collect
    ldp x27, x28, [sp, #80]
    ldp x25, x26, [sp, #64]
    ldp x23, x24, [sp, #48]
    ldp x21, x22, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp]
    add sp, sp, #96
    ret
    "#,
);

#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
unsafe extern "C" {
    fn emaxx_native_gc_trampoline();
}

#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
unsafe fn invoke_platform(invocation: *mut NativeInvocation) -> Result<bool, String> {
    Ok(unsafe { native_call_trampoline(invocation) } != 0)
}

// System V x86-64: the first six words travel in registers and the last two
// on the stack.  `rbx` keeps the invocation pointer across `_setjmp`, which
// saves and restores it, so a `longjmp` back here still finds the record.
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
core::arch::global_asm!(
    r#"
    .text
    .p2align 4
    .hidden native_call_trampoline
    .type native_call_trampoline, @function
native_call_trampoline:
    push rbp
    mov rbp, rsp
    push rbx
    push r12
    mov rbx, rdi
    lea rdi, [rbx + {jump_buffer}]
    call _setjmp@PLT
    test eax, eax
    jnz 1f
    mov rax, [rbx + {target}]
    mov rdi, [rbx + {arguments}]
    mov rsi, [rbx + {arguments} + 8]
    mov rdx, [rbx + {arguments} + 16]
    mov rcx, [rbx + {arguments} + 24]
    mov r8, [rbx + {arguments} + 32]
    mov r9, [rbx + {arguments} + 40]
    push qword ptr [rbx + {arguments} + 56]
    push qword ptr [rbx + {arguments} + 48]
    call rax
    add rsp, 16
    mov [rbx + {result}], rax
    xor eax, eax
    jmp 2f
1:
    mov eax, 1
2:
    pop r12
    pop rbx
    pop rbp
    ret
    .size native_call_trampoline, .-native_call_trampoline
    "#,
    target = const std::mem::offset_of!(NativeInvocation, target),
    arguments = const std::mem::offset_of!(NativeInvocation, arguments),
    result = const std::mem::offset_of!(NativeInvocation, result),
    jump_buffer = const std::mem::offset_of!(NativeInvocation, jump_buffer),
);

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
unsafe extern "C" {
    fn native_call_trampoline(invocation: *mut NativeInvocation) -> std::ffi::c_int;
}

// System V counterpart of alloc.c:flush_stack_call_func.  The generated
// caller already preserves live caller-saved registers around a C call; this
// spills every callee-saved register before the collector scans the stack.
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
core::arch::global_asm!(
    r#"
    .text
    .p2align 4
    .hidden emaxx_native_gc_trampoline
    .type emaxx_native_gc_trampoline, @function
emaxx_native_gc_trampoline:
    push rbp
    push rbx
    push r12
    push r13
    push r14
    push r15
    sub rsp, 8
    mov rdi, rsp
    call emaxx_native_gc_collect
    add rsp, 8
    pop r15
    pop r14
    pop r13
    pop r12
    pop rbx
    pop rbp
    ret
    .size emaxx_native_gc_trampoline, .-emaxx_native_gc_trampoline
    "#,
);

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
unsafe extern "C" {
    fn emaxx_native_gc_trampoline();
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
unsafe fn invoke_platform(invocation: *mut NativeInvocation) -> Result<bool, String> {
    Ok(unsafe { native_call_trampoline(invocation) } != 0)
}

struct ActiveCall {
    interpreter: *mut Interpreter,
    environment: *mut Env,
    runtime: *mut NativeRuntime,
}

#[cfg(target_os = "macos")]
fn current_native_stack_bottom() -> *const NativeWord {
    unsafe { libc::pthread_get_stackaddr_np(libc::pthread_self()).cast() }
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
fn current_native_stack_bottom() -> *const NativeWord {
    unsafe {
        let mut attributes = MaybeUninit::<libc::pthread_attr_t>::uninit();
        if libc::pthread_getattr_np(libc::pthread_self(), attributes.as_mut_ptr()) != 0 {
            return std::ptr::null();
        }
        let mut attributes = attributes.assume_init();
        let mut start = std::ptr::null_mut();
        let mut size = 0;
        let status = libc::pthread_attr_getstack(&attributes, &mut start, &mut size);
        libc::pthread_attr_destroy(&mut attributes);
        if status == 0 {
            start.cast::<u8>().add(size).cast()
        } else {
            std::ptr::null()
        }
    }
}

thread_local! {
    static ACTIVE_CALL: Cell<*mut ActiveCall> = const { Cell::new(std::ptr::null_mut()) };
    static CONS_SYNC_DEPTH: Cell<usize> = const { Cell::new(0) };
}

struct ConsSyncGuard;

impl ConsSyncGuard {
    fn enter() -> Self {
        CONS_SYNC_DEPTH.set(CONS_SYNC_DEPTH.get() + 1);
        Self
    }
}

impl Drop for ConsSyncGuard {
    fn drop(&mut self) {
        CONS_SYNC_DEPTH.set(CONS_SYNC_DEPTH.get() - 1);
    }
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
    if outermost {
        runtime.heap.set_stack_bottom(current_native_stack_bottom());
    }
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

/// Refresh one Rust cons field before ordinary runtime code reads it while
/// generated code is active.  GNU needs no hook because both sides already
/// dereference the same `Lisp_Cons`; this is the narrow transition used while
/// Emaxx's typed field cache is being folded into that canonical storage.
pub(crate) fn synchronize_cons_read(address: usize) {
    if CONS_SYNC_DEPTH.get() != 0 {
        return;
    }
    ACTIVE_CALL.with(|active| {
        let active = active.get();
        if active.is_null() {
            return;
        }
        let active = unsafe { &mut *active };
        if let Err(error) = unsafe { &mut *active.runtime }
            .heap
            .synchronize_cons(address)
        {
            remember_helper_error(active, super::lisp::native_ice(&error));
        }
    });
}

thread_local! {
    /// GNU has one process-wide `consing_until_gc'.  Emaxx's Lisp runtime is
    /// single-threaded, so a thread-local monotonic byte total lets the native
    /// collector consume every Lisp allocation, including those made before
    /// generated code becomes active, without an atomic operation per cons.
    static LISP_ALLOCATED_BYTES: Cell<u64> = const { Cell::new(0) };
}

fn lisp_allocated_bytes() -> u64 {
    LISP_ALLOCATED_BYTES.get()
}

/// alloc.c:tally_consing for actual Lisp objects.  Rust bridge wrappers must
/// not call this: GNU has no corresponding Lisp allocation for them.
pub(crate) fn note_lisp_allocation(bytes: usize) {
    let bytes = u64::try_from(bytes).unwrap_or(u64::MAX);
    LISP_ALLOCATED_BYTES.set(lisp_allocated_bytes().saturating_add(bytes));
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

pub(crate) fn decode_active_backtrace_arguments(
    words: &[NativeWord],
) -> Option<Result<Vec<Value>, String>> {
    with_current_runtime(|runtime| {
        words
            .iter()
            .map(|word| runtime.heap.decode(*word))
            .collect()
    })
}

// GNU declares `struct thread_state` GCALIGNED, eight bytes; a wider Rust
// alignment would pad the size past the C `sizeof` on targets where that
// size is not a multiple of sixteen.
#[repr(C, align(8))]
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

    fn value(&self) -> NativeWord {
        unsafe {
            std::ptr::read_unaligned(
                self.bytes
                    .as_ptr()
                    .add(HANDLER_VALUE_OFFSET)
                    .cast::<NativeWord>(),
            )
        }
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
    lisp_eval_depth: usize,
    backtrace_depth: usize,
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
    permanent_root_ranges: Vec<NativeRootRange>,
    ephemeral_root_ranges: Vec<NativeRootRange>,
}

struct NativeCallFrame {
    handler_depth: usize,
    unwind_depth: usize,
    ephemeral_root_depth: usize,
    lisp_eval_depth: usize,
    backtrace_depth: usize,
    pending_error: Option<LispError>,
    escape_buffer: *mut c_void,
}

#[derive(Clone, Copy)]
struct NativeRootRange {
    start: *const NativeWord,
    len: usize,
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
            permanent_root_ranges: Vec::new(),
            ephemeral_root_ranges: Vec::new(),
        }
    }
}

impl NativeRuntime {
    pub(crate) fn is_pristine(&self) -> bool {
        self.handlers.is_empty()
            && self.unwind.is_empty()
            && self.calls.is_empty()
            && self.permanent_root_ranges.is_empty()
            && self.ephemeral_root_ranges.is_empty()
            && self.thread.handler().is_null()
            && self.heap.is_empty()
    }

    pub(crate) fn begin_call(&mut self, escape_buffer: *mut c_void, interpreter: &Interpreter) {
        self.heap.begin_call();
        self.calls.push(NativeCallFrame {
            handler_depth: self.handlers.len(),
            unwind_depth: self.unwind.len(),
            ephemeral_root_depth: self.ephemeral_root_ranges.len(),
            lisp_eval_depth: interpreter.lisp_eval_depth,
            backtrace_depth: interpreter.backtrace_frames_len(),
            pending_error: None,
            escape_buffer,
        });
    }

    fn collect_native_heap(
        &mut self,
        stack_top: *const NativeWord,
        threshold: i64,
        percentage: Option<f64>,
    ) -> Option<Vec<Value>> {
        if !self.heap.collection_due(threshold, percentage) {
            return None;
        }
        Some(self.collect_native_heap_now(stack_top))
    }

    fn collect_native_heap_now(&mut self, stack_top: *const NativeWord) -> Vec<Value> {
        let mut roots = self
            .handlers
            .iter()
            .map(|handler| handler.storage.value())
            .collect::<Vec<_>>();
        roots.extend(
            self.heap
                .symbol_with_position_views
                .values()
                .flat_map(|view| [view.symbol, view.position]),
        );
        for range in self
            .permanent_root_ranges
            .iter()
            .chain(&self.ephemeral_root_ranges)
        {
            if range.len != 0 {
                roots.extend(unsafe { std::slice::from_raw_parts(range.start, range.len) });
            }
        }
        self.heap.collect(stack_top, &roots)
    }

    pub(crate) fn begin_garbage_collection(&mut self) -> Vec<Value> {
        let stack_marker = 0_usize;
        self.collect_native_heap_now(std::ptr::from_ref(&stack_marker))
    }

    pub(crate) fn garbage_collection_finished(
        &mut self,
        live_bytes: usize,
        threshold: i64,
        percentage: Option<f64>,
    ) {
        self.heap
            .collection_finished(live_bytes, threshold, percentage);
    }

    pub(crate) fn register_permanent_root_range(&mut self, start: *const NativeWord, len: usize) {
        if len != 0 {
            self.permanent_root_ranges
                .push(NativeRootRange { start, len });
        }
    }

    pub(crate) fn push_ephemeral_root_range(&mut self, start: *const NativeWord, len: usize) {
        if len != 0 {
            self.ephemeral_root_ranges
                .push(NativeRootRange { start, len });
        }
    }

    pub(crate) fn pop_ephemeral_root_range(&mut self, len: usize) {
        if len != 0 {
            self.ephemeral_root_ranges
                .pop()
                .expect("ephemeral native root range stack is balanced");
        }
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

    /// The value a relocation word written by `encode_relocations` stands
    /// for, as comp.c reads `*saved_cu` back from an already loaded unit.
    pub(crate) fn decode_relocation(&mut self, word: NativeWord) -> Result<Value, LispError> {
        self.heap
            .decode(word)
            .map_err(|error| super::lisp::native_ice(&error))
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

        // data.c exposes this as a forwarded C bool.  Since the variable is
        // intrinsically special, its value cell (including any buffer-local
        // forwarding) is the authority; walking lexical frames here is both
        // unlike GNU and needlessly charges every native call.
        *self.symbols_with_positions_enabled = interpreter
            .symbol_value_cell("symbols-with-pos-enabled")
            .is_ok_and(|value| value.is_truthy());

        let mut invocation = NativeInvocation::new(target);
        self.begin_call(invocation.jump_buffer(), interpreter);
        if let Err(error) = self.heap.publish_interpreter_writes() {
            let finish = self.finish_call(interpreter);
            finish?;
            return Err(super::lisp::native_ice(&error));
        }
        // GNU passes each Lisp_Object to generated code unchanged.
        let mut encoded = smallvec::SmallVec::<[NativeWord; 8]>::new();
        for argument in arguments {
            let word = match self.heap.encode(argument) {
                Ok(word) => word,
                Err(error) => {
                    let finish = self.finish_call(interpreter);
                    finish?;
                    return Err(super::lisp::native_ice(&error));
                }
            };
            encoded.push(word);
        }
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
        self.ephemeral_root_ranges
            .truncate(frame.ephemeral_root_depth);
        // A nested return resumes another generated frame.  Its explicit
        // result was reconciled above, and Rust reads of any indirectly
        // reachable cons synchronize that exact cell on demand.  Only the
        // outermost return must flush every remaining typed cache before the
        // native read barrier is no longer active.
        let heap_result = if self.heap.native_call_depth == 1 {
            self.publish_heap_writes(interpreter, false)
        } else {
            self.heap.finish_nested_call();
            Ok(())
        };
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
        let Some((handler_depth, unwind_depth, lisp_eval_depth, backtrace_depth, escape_buffer)) =
            self.calls.last().map(|call| {
                (
                    call.handler_depth,
                    call.unwind_depth,
                    call.lisp_eval_depth,
                    call.backtrace_depth,
                    call.escape_buffer,
                )
            })
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
                interpreter.lisp_eval_depth = lisp_eval_depth;
                interpreter.truncate_backtrace_frames(backtrace_depth);
                self.remember_error(error);
                return escape_buffer;
            };

            let target = self.handlers[target_index].address();
            let unwind_depth = self.handlers[target_index].unwind_depth;
            let lisp_eval_depth = self.handlers[target_index].lisp_eval_depth;
            let backtrace_depth = self.handlers[target_index].backtrace_depth;
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
            interpreter.lisp_eval_depth = lisp_eval_depth;
            interpreter.truncate_backtrace_frames(backtrace_depth);
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
        // A generated handler pop updates the ABI thread state first.  Bring
        // the Rust-owned handler stack to that exact state before any C-subr
        // behavior, including allocation and otherwise non-signaling word
        // operations.  Delaying this can retain dead handler roots across a
        // collection, which GNU does not do.
        if let Err(error) =
            unsafe { &mut *active.runtime }.sync_handlers(unsafe { &mut *active.interpreter })
        {
            remember_helper_error(active, error);
            return 0;
        }
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
        if subroutine.name == "cons" && arguments.len() == 2 {
            return unsafe { &mut *active.runtime }
                .heap
                .cons(arguments[0], arguments[1]);
        }
        match (subroutine.name, arguments) {
            ("eq", [left, right])
                if left == right
                    || !*unsafe { &*active.runtime }.symbols_with_positions_enabled =>
            {
                return native_boolean(left == right);
            }
            ("null", [value]) => return native_boolean(*value == 0),
            ("consp", [value]) => return native_boolean(native_consp(*value)),
            ("atom", [value]) => return native_boolean(!native_consp(*value)),
            ("bare-symbol-p", [value]) => {
                return native_boolean(*value & TAG_MASK == TAG_SYMBOL);
            }
            ("symbolp", [value])
                if *value & TAG_MASK == TAG_SYMBOL
                    || !*unsafe { &*active.runtime }.symbols_with_positions_enabled =>
            {
                return native_boolean(*value & TAG_MASK == TAG_SYMBOL);
            }
            ("car", [value]) if *value == 0 || native_consp(*value) => {
                return if native_consp(*value) {
                    unsafe { native_car(*value) }
                } else {
                    0
                };
            }
            ("car-safe", [value]) => {
                return if native_consp(*value) {
                    unsafe { native_car(*value) }
                } else {
                    0
                };
            }
            ("cdr", [value]) if *value == 0 || native_consp(*value) => {
                return if native_consp(*value) {
                    unsafe { native_cdr(*value) }
                } else {
                    0
                };
            }
            ("cdr-safe", [value]) => {
                return if native_consp(*value) {
                    unsafe { native_cdr(*value) }
                } else {
                    0
                };
            }
            ("listp", [value]) => return native_boolean(*value == 0 || native_consp(*value)),
            ("nlistp", [value]) => return native_boolean(*value != 0 && !native_consp(*value)),
            ("list", values) => {
                let heap = &mut unsafe { &mut *active.runtime }.heap;
                return values
                    .iter()
                    .rev()
                    .fold(0, |tail, value| heap.cons(*value, tail));
            }
            _ => {}
        }
        // These are direct translations of the corresponding C-owned word
        // operations in data.c and alloc.c.  Keep them at the GNU ABI instead
        // of decoding into Emaxx's richer interpreter representation.
        match (subroutine.name, arguments) {
            ("eq", [left, right]) => match native_eq(active, *left, *right) {
                Ok(equal) => return native_boolean(equal),
                Err(error) => {
                    remember_helper_error(active, error);
                    return 0;
                }
            },
            ("symbolp", [value]) => match native_symbolp(active, *value) {
                Ok(is_symbol) => return native_boolean(is_symbol),
                Err(error) => {
                    remember_helper_error(active, error);
                    return 0;
                }
            },
            ("type-of", [value]) => {
                if let Some(result) = invoke_native_type_of(active, *value) {
                    return result;
                }
            }
            ("symbol-value", [symbol]) => {
                if let Some(result) = invoke_native_symbol_value(active, *symbol) {
                    return result;
                }
            }
            _ => {}
        }
        if subroutine.name == "assq" && arguments.len() == 2 {
            return native_assq(active, arguments[0], arguments[1]);
        }
        if subroutine.name == "memq" && arguments.len() == 2 {
            return native_memq(active, arguments[0], arguments[1]);
        }
        if subroutine.name == "funcall"
            && let Some(result) = invoke_native_funcall(active, arguments)
        {
            return result;
        }
        if subroutine.name == "mapcar"
            && let Some(result) = invoke_native_mapcar(active, arguments)
        {
            return result;
        }
        let mut decoded = smallvec::SmallVec::<[Value; 8]>::new();
        for word in arguments {
            let value = match unsafe { &mut *active.runtime }.heap.decode(*word) {
                Ok(value) => value,
                Err(error) => {
                    remember_helper_error(active, super::lisp::native_ice(&error));
                    return 0;
                }
            };
            decoded.push(value);
        }
        let mutation_epoch = crate::lisp::types::cons_mutation_epoch();
        static SUBROUTINE_FACTS: OnceLock<Box<[crate::lisp::primitives::NameFacts]>> =
            OnceLock::new();
        let facts = SUBROUTINE_FACTS.get_or_init(|| {
            super::abi::native_subrs()
                .iter()
                .map(|subroutine| crate::lisp::primitives::name_facts(subroutine.name))
                .collect()
        })[index];
        let result = crate::lisp::primitives::call_with_facts(
            unsafe { &mut *active.interpreter },
            subroutine.name,
            facts,
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
            Ok(value) => match unsafe { &mut *active.runtime }.heap.encode(&value) {
                Ok(word) => word,
                Err(error) => {
                    drop(decoded);
                    remember_helper_error(active, super::lisp::native_ice(&error));
                    0
                }
            },
            Err(error) => {
                drop(decoded);
                remember_helper_error(active, error);
                0
            }
        }
    })
}

#[derive(Clone, Copy)]
enum DirectFuncallTarget {
    Builtin {
        index: usize,
        minimum: usize,
        maximum: super::abi::NativeMaxArgs,
    },
    Native {
        record_id: u64,
        function: super::loader::DirectNativeFunction,
    },
}

impl DirectFuncallTarget {
    fn invoke(self, arguments: &[NativeWord]) -> Result<NativeWord, LispError> {
        let (target, convention, minimum, maximum, function_value) = match self {
            Self::Builtin {
                index,
                minimum,
                maximum,
            } => {
                let name = super::abi::native_subrs()[index].name;
                if matches!(maximum, super::abi::NativeMaxArgs::Unevalled) {
                    return Err(LispError::SignalValue(Value::list([
                        Value::symbol("invalid-function"),
                        Value::BuiltinFunc(name.into()),
                    ])));
                }
                let (convention, maximum) = match maximum {
                    super::abi::NativeMaxArgs::Fixed(maximum) => (
                        if maximum <= 8 {
                            NativeCallingConvention::Fixed
                        } else {
                            NativeCallingConvention::Many
                        },
                        Some(maximum as usize),
                    ),
                    super::abi::NativeMaxArgs::Many => (NativeCallingConvention::Many, None),
                    super::abi::NativeMaxArgs::Unevalled => unreachable!("handled above"),
                };
                (
                    super::generated_native_subrs::native_subr_address(index).cast_const(),
                    convention,
                    minimum,
                    maximum,
                    Value::BuiltinFunc(name.into()),
                )
            }
            Self::Native {
                record_id,
                function,
            } => (
                function.target,
                function.convention,
                function.min_args,
                function.max_args,
                Value::Record(record_id),
            ),
        };

        if arguments.len() < minimum || maximum.is_some_and(|maximum| arguments.len() > maximum) {
            return Err(LispError::SignalValue(Value::list([
                Value::symbol("wrong-number-of-arguments"),
                function_value,
                Value::Integer(arguments.len() as i64),
            ])));
        }

        if matches!(convention, NativeCallingConvention::Fixed) {
            let maximum = maximum.expect("a fixed subr has a finite maximum arity");
            let mut padded = smallvec::SmallVec::<[NativeWord; 8]>::new();
            padded.extend_from_slice(arguments);
            padded.resize(maximum, 0);
            Ok(unsafe { call_word_target(target, convention, &padded) })
        } else {
            Ok(unsafe { call_word_target(target, convention, arguments) })
        }
    }
}

fn direct_funcall_target(
    interpreter: &Interpreter,
    environment: &Env,
    function: &Value,
) -> Option<DirectFuncallTarget> {
    let resolved = match function {
        Value::Symbol(name) => interpreter.lookup_function(name, environment).ok()?,
        other => other.clone(),
    };
    match resolved {
        Value::BuiltinFunc(name) => {
            static SUBR_INDICES: OnceLock<HashMap<&'static str, usize>> = OnceLock::new();
            let indices = SUBR_INDICES.get_or_init(|| {
                super::abi::native_subrs()
                    .iter()
                    .enumerate()
                    .map(|(index, subroutine)| (subroutine.name, index))
                    .collect()
            });
            let index = *indices.get(name.as_str())?;
            let subroutine = super::abi::native_subrs()[index];
            Some(DirectFuncallTarget::Builtin {
                index,
                minimum: subroutine.min_args as usize,
                maximum: subroutine.max_args,
            })
        }
        Value::Record(record_id) => {
            super::loader::active_direct_function(record_id).map(|function| {
                DirectFuncallTarget::Native {
                    record_id,
                    function,
                }
            })
        }
        _ => None,
    }
}

/// eval.c:Ffuncall -> funcall_general -> funcall_subr for targets whose
/// argument vector can remain in GNU's machine-word ABI.  Unsupported
/// callable classes take the ordinary evaluator path; supported builtins and
/// non-dynamic native subrs are called directly in the current activation,
/// with the same optional-argument rules as their C counterparts.
fn invoke_native_funcall(active: &mut ActiveCall, arguments: &[NativeWord]) -> Option<NativeWord> {
    let (&function_word, call_arguments) = arguments.split_first()?;
    let runtime = unsafe { &mut *active.runtime };
    let interpreter = unsafe { &mut *active.interpreter };
    let environment = unsafe { &mut *active.environment };
    let original_function = runtime.heap.decode(function_word).ok()?;
    let target = direct_funcall_target(interpreter, environment, &original_function)?;

    // do_debug_on_call needs the full debugger/specpdl path.  Keep that cold
    // state on the general evaluator path until that C lifecycle is shared by
    // both entries; the normal raw path below is otherwise the literal
    // Ffuncall order.
    if interpreter
        .lookup_var("debug-on-next-call", environment)
        .is_some_and(|value| value.is_truthy())
    {
        return None;
    }

    if let Err(error) = interpreter.begin_funcall(environment) {
        remember_helper_error(active, error);
        return Some(0);
    }
    interpreter.push_native_backtrace_frame(original_function.clone(), call_arguments);
    interpreter.capture_current_backtrace_context(None, environment, None);

    // Ffuncall calls maybe_gc after record_in_backtrace and before dispatch.
    unsafe { emaxx_native_gc_trampoline() };
    let result = target.invoke(call_arguments);
    let result = match result {
        Ok(word) => Ok(word),
        Err(error @ (LispError::Throw(_, _) | LispError::Terminate(_))) => Err(error),
        Err(error) => match interpreter.dispatch_handler_bindings(error, environment) {
            Ok(value) => runtime
                .heap
                .encode(&value)
                .map_err(|error| super::lisp::native_ice(&error)),
            Err(error) => Err(error),
        },
    };
    if let Err(error) = &result {
        interpreter.capture_batch_error_backtrace(error, environment);
    }
    interpreter.end_funcall();
    interpreter.pop_backtrace_frame();

    Some(match result {
        Ok(word) => word,
        Err(error) => {
            remember_helper_error(active, error);
            0
        }
    })
}

/// fns.c:Fmapcar/mapcar1 for a proper list.  The length is established before
/// the first call, but each cdr is read after its callback so destructive
/// shortening has GNU's observable effect.  Other sequence types remain on
/// the general primitive path.
fn invoke_native_mapcar(active: &mut ActiveCall, arguments: &[NativeWord]) -> Option<NativeWord> {
    let [function, sequence] = arguments else {
        return None;
    };
    let length = match native_proper_list_length(active, *sequence) {
        Ok(Some(length)) => length,
        Ok(None) => return None,
        Err(()) => return Some(0),
    };
    if length == 0 {
        return Some(0);
    }

    static FUNCALL_SUBR_INDEX: OnceLock<usize> = OnceLock::new();
    let funcall = *FUNCALL_SUBR_INDEX.get_or_init(|| {
        super::abi::native_subrs()
            .iter()
            .position(|subroutine| subroutine.name == "funcall")
            .expect("funcall belongs to the native ABI")
    });
    let mut mapped = vec![0; length];
    unsafe { &mut *active.runtime }.push_ephemeral_root_range(mapped.as_ptr(), mapped.len());
    let mut tail = *sequence;
    let mut mapped_count = 0;
    while mapped_count < length && native_consp(tail) {
        let item = unsafe { native_car(tail) };
        mapped[mapped_count] = invoke_subr(funcall, &[*function, item]);
        if native_call_has_pending_error(active) {
            unsafe { &mut *active.runtime }.pop_ephemeral_root_range(mapped.len());
            return Some(0);
        }
        mapped_count += 1;
        tail = unsafe { native_cdr(tail) };
    }
    let result = mapped[..mapped_count].iter().rev().fold(0, |tail, value| {
        unsafe { &mut *active.runtime }.heap.cons(*value, tail)
    });
    unsafe { &mut *active.runtime }.pop_ephemeral_root_range(mapped.len());
    Some(result)
}

fn native_call_has_pending_error(active: &ActiveCall) -> bool {
    unsafe { &*active.runtime }
        .calls
        .last()
        .is_some_and(|call| call.pending_error.is_some())
}

/// fns.c:list_length, including lisp.h:FOR_EACH_TAIL's Brent cycle check and
/// maybe_quit cadence.  Ok(None) leaves detailed improper/circular reporting
/// to the ordinary Fapply/Fmapcar Rust translation; Err means maybe_quit has
/// already installed the non-local exit in the active native call.
fn native_proper_list_length(active: &ActiveCall, list: NativeWord) -> Result<Option<usize>, ()> {
    let mut length = 0_usize;
    let mut tail = list;
    let mut tortoise = tail;
    let mut maximum = 2_isize;
    let mut remaining_high = 0_isize;
    let mut remaining_low = 2_u16;
    while native_consp(tail) {
        let Some(next_length) = length.checked_add(1) else {
            return Ok(None);
        };
        length = next_length;
        tail = unsafe { native_cdr(tail) };
        remaining_low = remaining_low.wrapping_sub(1);
        let mut compare_tortoise = remaining_low != 0;
        if !compare_tortoise {
            runtime_maybe_quit();
            if native_call_has_pending_error(active) {
                return Err(());
            }
            remaining_high = remaining_high.wrapping_sub(1);
            compare_tortoise = remaining_high > 0;
        }
        if !compare_tortoise {
            maximum = maximum.wrapping_shl(1);
            remaining_high = maximum;
            remaining_low = maximum as u16;
            remaining_high >>= u16::BITS;
            tortoise = tail;
        } else if tail == tortoise {
            return Ok(None);
        }
    }
    Ok((tail == 0).then_some(length))
}

/// data.c:SYMBOLP: bare symbols always qualify; positioned symbols qualify
/// only while the process-wide compatibility flag is enabled.
fn native_symbolp(active: &mut ActiveCall, word: NativeWord) -> Result<bool, LispError> {
    if word & TAG_MASK == TAG_SYMBOL {
        return Ok(true);
    }
    if !*unsafe { &*active.runtime }.symbols_with_positions_enabled {
        return Ok(false);
    }
    let value = decode_word(active, word)?;
    Ok(symbol_with_pos_parts(unsafe { &*active.interpreter }, &value).is_some())
}

/// The non-vectorlike portion of data.c:Ftype_of is a pure tag dispatch.
/// Pseudovectors stay on the general path because GNU inspects their precise
/// subtype (including records, closures, and native subrs).
fn invoke_native_type_of(active: &mut ActiveCall, word: NativeWord) -> Option<NativeWord> {
    let type_name = if word & TAG_MASK == TAG_SYMBOL {
        "symbol"
    } else if word & 3 == TAG_FIXNUM_LOW {
        "integer"
    } else {
        match word & TAG_MASK {
            TAG_STRING => "string",
            TAG_CONS => "cons",
            TAG_FLOAT => "float",
            TAG_VECTORLIKE => return None,
            _ => return None,
        }
    };
    Some(
        match unsafe { &mut *active.runtime }
            .heap
            .encode(&Value::symbol(type_name))
        {
            Ok(word) => word,
            Err(error) => {
                remember_helper_error(active, super::lisp::native_ice(&error));
                0
            }
        },
    )
}

/// data.c:Fsymbol_value reads the global symbol value cell, not the current
/// lexical environment.  Restrict this raw-word path to bare symbols; GNU's
/// conditional symbol-with-position handling remains in the general path.
fn invoke_native_symbol_value(active: &mut ActiveCall, word: NativeWord) -> Option<NativeWord> {
    if word & TAG_MASK != TAG_SYMBOL {
        return None;
    }
    let symbol = if word == 0 || word == native_boolean(true) {
        None
    } else {
        match unsafe { &mut *active.runtime }.heap.decode(word).ok()? {
            Value::Symbol(symbol) => Some(symbol),
            _ => return None,
        }
    };
    let name = match &symbol {
        Some(symbol) => symbol.as_str(),
        None if word == 0 => "nil",
        None => "t",
    };
    let value = unsafe { &mut *active.interpreter }.symbol_value_cell(name);
    Some(
        match value.and_then(|value| {
            unsafe { &mut *active.runtime }
                .heap
                .encode(&value)
                .map_err(|error| super::lisp::native_ice(&error))
        }) {
            Ok(word) => word,
            Err(error) => {
                remember_helper_error(active, error);
                0
            }
        },
    )
}

#[inline(always)]
fn native_consp(value: NativeWord) -> bool {
    value & TAG_MASK == TAG_CONS
}

#[inline(always)]
unsafe fn native_car(value: NativeWord) -> NativeWord {
    debug_assert!(native_consp(value));
    unsafe { (*(value.wrapping_sub(TAG_CONS) as *const NativeCons)).car() }
}

#[inline(always)]
unsafe fn native_cdr(value: NativeWord) -> NativeWord {
    debug_assert!(native_consp(value));
    unsafe { (*(value.wrapping_sub(TAG_CONS) as *const NativeCons)).cdr() }
}

fn native_eq(
    active: &mut ActiveCall,
    left: NativeWord,
    right: NativeWord,
) -> Result<bool, LispError> {
    if left == right {
        return Ok(true);
    }
    if !*unsafe { &*active.runtime }.symbols_with_positions_enabled {
        return Ok(false);
    }
    let left = decode_word(active, left)?;
    let right = decode_word(active, right)?;
    Ok(crate::lisp::primitives::values_eq_in_env(
        unsafe { &*active.interpreter },
        &left,
        &right,
        unsafe { &*active.environment },
    ))
}

/// Rust translation of fns.c:Fassq and lisp.h:FOR_EACH_TAIL.
fn native_assq(active: &mut ActiveCall, key: NativeWord, alist: NativeWord) -> NativeWord {
    let mut tail = alist;
    let mut tortoise = tail;
    let mut max = 2_isize;
    let mut n = 0_isize;
    let mut q = 2_u16;

    while native_consp(tail) {
        let entry = unsafe { native_car(tail) };
        if native_consp(entry) {
            let entry_key = unsafe { native_car(entry) };
            match native_eq(active, entry_key, key) {
                Ok(true) => return entry,
                Ok(false) => {}
                Err(error) => {
                    remember_helper_error(active, error);
                    return 0;
                }
            }
        }

        tail = unsafe { native_cdr(tail) };
        q = q.wrapping_sub(1);
        let mut compare_tortoise = q != 0;
        if !compare_tortoise {
            runtime_maybe_quit();
            n = n.wrapping_sub(1);
            compare_tortoise = n > 0;
        }
        if !compare_tortoise {
            max = max.wrapping_shl(1);
            n = max;
            q = max as u16;
            n >>= u16::BITS;
            tortoise = tail;
        } else if tail == tortoise {
            let error = decode_word(active, tail).map(|tail| {
                LispError::SignalValue(Value::list([Value::symbol("circular-list"), tail]))
            });
            remember_helper_error(active, error.unwrap_or_else(|error| error));
            return 0;
        }
    }

    if tail != 0 {
        let error = decode_word(active, alist).map(|alist| wrong_type_argument("listp", alist));
        remember_helper_error(active, error.unwrap_or_else(|error| error));
    }
    0
}

/// Rust translation of fns.c:Fmemq and lisp.h:FOR_EACH_TAIL.
fn native_memq(active: &mut ActiveCall, element: NativeWord, list: NativeWord) -> NativeWord {
    let mut tail = list;
    let mut tortoise = tail;
    let mut max = 2_isize;
    let mut n = 0_isize;
    let mut q = 2_u16;

    while native_consp(tail) {
        let item = unsafe { native_car(tail) };
        match native_eq(active, item, element) {
            Ok(true) => return tail,
            Ok(false) => {}
            Err(error) => {
                remember_helper_error(active, error);
                return 0;
            }
        }

        tail = unsafe { native_cdr(tail) };
        q = q.wrapping_sub(1);
        let mut compare_tortoise = q != 0;
        if !compare_tortoise {
            runtime_maybe_quit();
            n = n.wrapping_sub(1);
            compare_tortoise = n > 0;
        }
        if !compare_tortoise {
            max = max.wrapping_shl(1);
            n = max;
            q = max as u16;
            n >>= u16::BITS;
            tortoise = tail;
        } else if tail == tortoise {
            let error = decode_word(active, tail).map(|tail| {
                LispError::SignalValue(Value::list([Value::symbol("circular-list"), tail]))
            });
            remember_helper_error(active, error.unwrap_or_else(|error| error));
            return 0;
        }
    }

    if tail != 0 {
        let error = decode_word(active, list).map(|list| wrong_type_argument("listp", list));
        remember_helper_error(active, error.unwrap_or_else(|error| error));
    }
    0
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
    let runtime = unsafe { &mut *active.runtime };
    let jump_buffer = runtime.prepare_nonlocal_exit(
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
                lisp_eval_depth: interpreter.lisp_eval_depth,
                backtrace_depth: interpreter.backtrace_frames_len(),
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
            let _ = super::lisp::call_c_primitive(
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

#[unsafe(no_mangle)]
extern "C" fn emaxx_native_gc_collect(stack_top: *const NativeWord) {
    with_active(|active| {
        let interpreter = unsafe { &mut *active.interpreter };
        if interpreter.garbage_collection_is_inhibited() {
            return;
        }
        let threshold = interpreter
            .symbol_value_cell("gc-cons-threshold")
            .ok()
            .and_then(|value| value.as_integer().ok())
            .unwrap_or(NATIVE_GC_DEFAULT_THRESHOLD);
        let percentage = match interpreter.symbol_value_cell("gc-cons-percentage") {
            Ok(Value::Float(value)) => Some(value.get()),
            _ => None,
        };
        let runtime = unsafe { &mut *active.runtime };
        if let Some(native_roots) = runtime.collect_native_heap(stack_top, threshold, percentage) {
            if let Err(error) = crate::lisp::primitives::collect_weak_hash_tables(
                interpreter,
                unsafe { &*active.environment },
                &native_roots,
            ) {
                remember_helper_error(active, error);
                return;
            }
            let live_bytes = interpreter
                .live_object_census()
                .total_bytes_of_live_objects();
            runtime
                .heap
                .collection_finished(live_bytes, threshold, percentage);
        }
    });
}

extern "C" fn runtime_maybe_quit() {
    with_active(|active| {
        if let Err(error) =
            unsafe { &mut *active.interpreter }.maybe_quit(unsafe { &mut *active.environment })
        {
            remember_helper_error(active, error);
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
        emaxx_native_gc_trampoline as *mut c_void,
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
    Symbol(SymbolName),
    WideInteger(i64),
    BigInteger(usize),
    Float(usize),
    String(usize),
    StringObject(usize),
    Vector(usize),
    Builtin(SymbolName),
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

type NativeCons = ConsWords;

const NATIVE_CONS_BLOCK_LEN: usize = 16 * 1024;
const NATIVE_GC_DEFAULT_THRESHOLD: i64 = 800_000;
const NATIVE_GC_MINIMUM_THRESHOLD: i64 = 80_000;
const NATIVE_GC_HIGH_THRESHOLD: i64 = i64::MAX / 2;
const NATIVE_CONS_MARK_WORDS: usize = NATIVE_CONS_BLOCK_LEN.div_ceil(u64::BITS as usize);

struct NativeConsBlock {
    cells: Box<[MaybeUninit<NativeCons>]>,
    marks: Box<[u64]>,
    occupied: Box<[u64]>,
    allocated: usize,
}

enum ArenaMark {
    NotArena,
    AlreadyMarked,
    NewlyMarked([NativeWord; 2]),
}

impl NativeConsBlock {
    fn new() -> Self {
        Self {
            cells: Box::<[NativeCons]>::new_uninit_slice(NATIVE_CONS_BLOCK_LEN),
            marks: vec![0; NATIVE_CONS_MARK_WORDS].into_boxed_slice(),
            occupied: vec![0; NATIVE_CONS_MARK_WORDS].into_boxed_slice(),
            allocated: 0,
        }
    }

    fn start(&self) -> usize {
        self.cells.as_ptr() as usize
    }

    fn bit(bits: &[u64], slot: usize) -> bool {
        bits[slot / u64::BITS as usize] & (1 << (slot % u64::BITS as usize)) != 0
    }

    fn set_bit(bits: &mut [u64], slot: usize) {
        bits[slot / u64::BITS as usize] |= 1 << (slot % u64::BITS as usize);
    }

    fn clear_bit(bits: &mut [u64], slot: usize) {
        bits[slot / u64::BITS as usize] &= !(1 << (slot % u64::BITS as usize));
    }

    fn is_live(&self, slot: usize) -> bool {
        Self::bit(&self.occupied, slot)
    }

    fn mark(&mut self, slot: usize) -> bool {
        if Self::bit(&self.marks, slot) {
            false
        } else {
            Self::set_bit(&mut self.marks, slot);
            true
        }
    }
}

/// Bump storage for conses allocated by generated code.
///
/// GNU's Fcons takes the next cell from an allocator block and writes two
/// Lisp words.  Do the same here.  A native-created cell gets a richer
/// Rust `Value` owner only if execution crosses back into a Rust primitive
/// or returns that cell to the evaluator; transient compiler lists never pay
/// for Rc allocation, hash-table registration, or mirror tracking.
#[derive(Default)]
struct NativeGcState {
    /// alloc.c's `consing_until_gc'.  Lisp allocation subtracts bytes; GNU
    /// only enters `maybe_garbage_collect' after this becomes negative.
    consing_until_gc: i64,
    /// alloc.c's `gc_threshold', retained separately because rebinding either
    /// public threshold variable adjusts the remaining allowance by the
    /// difference between the old and new effective thresholds.
    gc_threshold: i64,
    /// The allocator census captured by the most recent collection, matching
    /// the `gcstat' input to `total_bytes_of_live_objects'.
    live_bytes: usize,
    /// Last point consumed from LISP_ALLOCATED_BYTES.  Keeping the source
    /// monotonic makes allocations outside native activation visible here.
    observed_allocated_bytes: u64,
}

impl NativeGcState {
    fn tally_consing(&mut self, bytes: usize) {
        let bytes = i64::try_from(bytes).unwrap_or(i64::MAX);
        self.consing_until_gc = self.consing_until_gc.saturating_sub(bytes);
    }

    fn synchronize_allocations(&mut self) {
        let current = lisp_allocated_bytes();
        let allocated = current.saturating_sub(self.observed_allocated_bytes);
        self.observed_allocated_bytes = current;
        self.tally_consing(usize::try_from(allocated).unwrap_or(usize::MAX));
    }

    /// alloc.c:consing_threshold, for the ordinary (non-memory-full) path.
    fn consing_threshold(&self, threshold: i64, percentage: Option<f64>, since_gc: i64) -> i64 {
        let mut threshold = threshold.max(NATIVE_GC_MINIMUM_THRESHOLD);
        if let Some(percentage) = percentage {
            let total = percentage * (self.live_bytes as f64 + since_gc as f64);
            if (threshold as f64) < total {
                threshold = if total < NATIVE_GC_HIGH_THRESHOLD as f64 {
                    total as i64
                } else {
                    NATIVE_GC_HIGH_THRESHOLD
                };
            }
        }
        threshold.min(NATIVE_GC_HIGH_THRESHOLD)
    }

    /// alloc.c:bump_consing_until_gc.
    fn bump_consing_until_gc(&mut self, threshold: i64, percentage: Option<f64>) -> i64 {
        // GNU deliberately assumes half of the bytes allocated since the
        // previous collection are still live while reconsidering the
        // percentage threshold.
        let since_gc = self.gc_threshold.saturating_sub(self.consing_until_gc) >> 1;
        let new_gc_threshold = self.consing_threshold(threshold, percentage, since_gc);
        self.consing_until_gc = self
            .consing_until_gc
            .saturating_add(new_gc_threshold.saturating_sub(self.gc_threshold));
        self.gc_threshold = new_gc_threshold;
        self.consing_until_gc
    }

    /// lisp.h:maybe_gc followed by alloc.c:maybe_garbage_collect.
    fn collection_due(&mut self, threshold: i64, percentage: Option<f64>) -> bool {
        self.consing_until_gc < 0 && self.bump_consing_until_gc(threshold, percentage) < 0
    }

    /// The post-sweep reset at the end of alloc.c:garbage_collect.
    fn collection_finished(&mut self, live_bytes: usize, threshold: i64, percentage: Option<f64>) {
        self.observed_allocated_bytes = lisp_allocated_bytes();
        self.live_bytes = live_bytes;
        self.gc_threshold = self.consing_threshold(threshold, percentage, 0);
        self.consing_until_gc = self.gc_threshold;
    }
}

#[derive(Default)]
struct NativeConsArena {
    blocks: Vec<NativeConsBlock>,
    block_starts: BTreeMap<usize, usize>,
    free_list: Vec<*mut NativeCons>,
    live: usize,
    gc: NativeGcState,
}

impl NativeConsArena {
    #[inline(always)]
    fn allocate(&mut self, car: NativeWord, cdr: NativeWord) -> *mut NativeCons {
        let native: *mut NativeCons = if let Some(native) = self.free_list.pop() {
            let (block_index, slot) = self
                .locate(native as usize, false)
                .expect("native cons free-list pointer belongs to its arena");
            NativeConsBlock::set_bit(&mut self.blocks[block_index].occupied, slot);
            self.blocks[block_index].cells[slot].write(NativeCons::new(car, cdr))
        } else {
            if self
                .blocks
                .last()
                .is_none_or(|block| block.allocated == NATIVE_CONS_BLOCK_LEN)
            {
                let block = NativeConsBlock::new();
                let start = block.start();
                let index = self.blocks.len();
                self.blocks.push(block);
                self.block_starts.insert(start, index);
            }
            let block = self.blocks.last_mut().expect("block inserted above");
            let slot = block.allocated;
            block.allocated += 1;
            NativeConsBlock::set_bit(&mut block.occupied, slot);
            block.cells[slot].write(NativeCons::new(car, cdr))
        };
        self.live += 1;
        note_lisp_allocation(std::mem::size_of::<NativeCons>());
        native
    }

    fn locate(&self, pointer: usize, require_live: bool) -> Option<(usize, usize)> {
        let (&start, &block_index) = self.block_starts.range(..=pointer).next_back()?;
        let block = &self.blocks[block_index];
        let offset = pointer.checked_sub(start)?;
        let cell_size = std::mem::size_of::<NativeCons>();
        let slot = offset / cell_size;
        let field_offset = offset % cell_size;
        if slot >= block.allocated
            || !matches!(field_offset, 0 | TAG_CONS)
                && field_offset != std::mem::size_of::<NativeWord>()
            || require_live && !block.is_live(slot)
        {
            return None;
        }
        Some((block_index, slot))
    }

    fn contains(&self, native: *const NativeCons) -> bool {
        self.locate(native as usize, true).is_some()
    }

    fn collection_due(&mut self, threshold: i64, percentage: Option<f64>) -> bool {
        self.gc.synchronize_allocations();
        self.gc.collection_due(threshold, percentage)
    }

    fn mark_word(&mut self, word: NativeWord) -> ArenaMark {
        let Some((block_index, slot)) = self.locate(word, true) else {
            return ArenaMark::NotArena;
        };
        if !self.blocks[block_index].mark(slot) {
            return ArenaMark::AlreadyMarked;
        }
        let native = self.blocks[block_index].cells[slot].as_ptr();
        ArenaMark::NewlyMarked(unsafe { [(*native).car(), (*native).cdr()] })
    }

    fn is_marked(&self, native: *const NativeCons) -> bool {
        self.locate(native as usize, true)
            .is_some_and(|(block_index, slot)| {
                NativeConsBlock::bit(&self.blocks[block_index].marks, slot)
            })
    }

    fn sweep(&mut self) {
        self.live = 0;
        let mut free_before_block = 0;
        let mut keep = vec![true; self.blocks.len()];
        for (index, block) in self.blocks.iter_mut().enumerate().rev() {
            let mut block_live = 0;
            for slot in 0..block.allocated {
                if NativeConsBlock::bit(&block.marks, slot) {
                    NativeConsBlock::clear_bit(&mut block.marks, slot);
                    block_live += 1;
                } else {
                    NativeConsBlock::clear_bit(&mut block.occupied, slot);
                }
            }
            self.live += block_live;
            let block_free = block.allocated - block_live;
            if block.allocated == NATIVE_CONS_BLOCK_LEN
                && block_live == 0
                && free_before_block > NATIVE_CONS_BLOCK_LEN
            {
                keep[index] = false;
            } else {
                free_before_block += block_free;
            }
        }

        let mut index = 0;
        self.blocks.retain(|_| {
            let retain = keep[index];
            index += 1;
            retain
        });
        self.block_starts.clear();
        self.free_list.clear();
        for (block_index, block) in self.blocks.iter().enumerate() {
            self.block_starts.insert(block.start(), block_index);
            for slot in 0..block.allocated {
                if !block.is_live(slot) {
                    self.free_list.push(block.cells[slot].as_ptr().cast_mut());
                }
            }
        }
    }

    fn is_empty(&self) -> bool {
        self.live == 0
    }
}

struct TouchedCons {
    native: *mut NativeCons,
    value: Weak<ConsCell>,
}

#[derive(Default)]
struct TouchedConses {
    conses: Vec<TouchedCons>,
    cons_set: IdentitySet,
}

#[repr(align(8))]
struct NativeHandle {
    value: Value,
}

struct HandleEntry {
    native: Box<NativeHandle>,
    tag: usize,
    identity: NativeIdentity,
}

#[repr(C, align(8))]
struct NativeSymbolWithPosition {
    header: isize,
    symbol: NativeWord,
    position: NativeWord,
}

struct ConsMirror {
    value: SharedCons,
    mutations: NativeConsMutationRegistration,
    gc_marked: bool,
}

/// Stable objects retained for native machine code.  GNU's GC provides the
/// same stability in C; this Rust owner also supplies the reverse lookup
/// needed by primitive-call wrappers.
#[derive(Default)]
pub(crate) struct NativeHeap {
    native_conses: NativeConsArena,
    cons_values: IdentityMap<ConsMirror>,
    /// Mirrors whose reconciliation is in progress, so cyclic structures do
    /// not recurse into themselves.
    reconciling: IdentitySet,
    interpreter_dirty: Rc<ConsMutationQueue>,
    handles: Vec<Option<HandleEntry>>,
    free_handle_slots: Vec<usize>,
    handle_by_value: HashMap<NativeIdentity, usize>,
    handle_by_address: IdentityMap<usize>,
    symbol_with_position_views: HashMap<u64, Box<NativeSymbolWithPosition>>,
    /// Deduplicated union of cons mirrors reachable by the active generated
    /// call stack.  Suspended outer frames and the executing inner frame all
    /// address the same native heap, so one entry per cell is sufficient.
    touched: TouchedConses,
    native_call_depth: usize,
    native_stack_bottom: *const NativeWord,
}

impl Drop for NativeHeap {
    fn drop(&mut self) {
        for (&address, mirror) in &self.cons_values {
            unsafe { mirror.value.detach_native_words(address as *mut NativeCons) };
        }
    }
}

impl NativeHeap {
    fn is_empty(&self) -> bool {
        self.native_conses.is_empty()
            && self.cons_values.is_empty()
            && self.handles.iter().all(Option::is_none)
            && self.symbol_with_position_views.is_empty()
            && self.native_call_depth == 0
            && self.touched.conses.is_empty()
            && self.touched.cons_set.is_empty()
    }

    pub(crate) fn begin_call(&mut self) {
        self.native_call_depth += 1;
    }

    fn set_stack_bottom(&mut self, stack_bottom: *const NativeWord) {
        if self.native_call_depth == 1 {
            self.native_stack_bottom = stack_bottom;
        }
    }

    fn collection_due(&mut self, threshold: i64, percentage: Option<f64>) -> bool {
        self.native_conses.collection_due(threshold, percentage)
    }

    fn collection_finished(
        &mut self,
        rust_live_bytes: usize,
        threshold: i64,
        percentage: Option<f64>,
    ) {
        // A generated cons gets a Rust ConsCell only when it crosses the ABI.
        // Such a cell is already present in the Rust census; only arena cells
        // without that materialized owner must be added here.
        let materialized_arena_conses = self
            .cons_values
            .keys()
            .filter(|address| {
                self.native_conses
                    .contains((**address) as *const NativeCons)
            })
            .count();
        let arena_only_bytes = self
            .native_conses
            .live
            .saturating_sub(materialized_arena_conses)
            .saturating_mul(std::mem::size_of::<NativeCons>());
        self.native_conses.gc.collection_finished(
            rust_live_bytes.saturating_add(arena_only_bytes),
            threshold,
            percentage,
        );
    }

    fn collect(
        &mut self,
        stack_top: *const NativeWord,
        runtime_roots: &[NativeWord],
    ) -> Vec<Value> {
        if self.native_stack_bottom.is_null() {
            return Vec::new();
        }
        let mut pending = Vec::with_capacity(runtime_roots.len());
        pending.extend_from_slice(runtime_roots);
        for entry in self.handles.iter().flatten() {
            if entry.native.value.native_handle_has_external_owner() {
                pending.push((&*entry.native as *const NativeHandle) as usize + entry.tag);
            }
        }

        let start = (stack_top as usize).min(self.native_stack_bottom as usize);
        let end = (stack_top as usize).max(self.native_stack_bottom as usize);
        let alignment = std::mem::align_of::<NativeWord>();
        let mut current = start.div_ceil(alignment) * alignment;
        pending.reserve(end.saturating_sub(current) / alignment);
        while current.saturating_add(std::mem::size_of::<NativeWord>()) <= end {
            pending.push(unsafe { std::ptr::read(current as *const NativeWord) });
            current += alignment;
        }

        let mut marked_handles = vec![false; self.handles.len()];
        while let Some(word) = pending.pop() {
            match self.native_conses.mark_word(word) {
                ArenaMark::AlreadyMarked => continue,
                ArenaMark::NewlyMarked(fields) => {
                    pending.extend(fields);
                    continue;
                }
                ArenaMark::NotArena => {}
            }
            let candidates = [
                Some(word),
                word.checked_sub(TAG_CONS),
                word.checked_sub(std::mem::size_of::<NativeWord>()),
            ];
            if let Some(address) = candidates
                .into_iter()
                .flatten()
                .find(|address| self.cons_values.contains_key(address))
            {
                let mirror = self
                    .cons_values
                    .get_mut(&address)
                    .expect("candidate was found in the cons mirror map");
                if !mirror.gc_marked {
                    mirror.gc_marked = true;
                    let native = address as *mut NativeCons;
                    pending.extend(unsafe { [(*native).car(), (*native).cdr()] });
                }
                continue;
            }
            let tag = word & TAG_MASK;
            let address = word.wrapping_sub(tag);
            if let Some(index) = self.handle_by_address.get(&address) {
                marked_handles[*index] = true;
            }
        }

        // GNU's single mark pass makes objects reached from generated stack
        // words visible to weak-hash processing as well as to ordinary heap
        // sweeping.  Preserve that one-root-graph rule across Emaxx's typed
        // Lisp heap and native ABI arena by publishing every marked bridge
        // value to the subsequent Lisp reachability pass.
        let mut lisp_roots = self
            .handles
            .iter()
            .enumerate()
            .filter_map(|(index, entry)| {
                marked_handles[index]
                    .then(|| entry.as_ref().map(|entry| entry.native.value.clone()))
                    .flatten()
            })
            .collect::<Vec<_>>();
        lisp_roots.extend(self.cons_values.iter().filter_map(|(&address, mirror)| {
            let native = address as *const NativeCons;
            let marked = if self.native_conses.contains(native) {
                self.native_conses.is_marked(native)
            } else {
                mirror.gc_marked
            };
            marked.then(|| Value::Cons(mirror.value.clone()))
        }));

        let mut unreachable = Vec::new();
        for (&address, mirror) in &self.cons_values {
            let native = address as *mut NativeCons;
            let marked = if self.native_conses.contains(native) {
                self.native_conses.is_marked(native)
            } else {
                mirror.gc_marked
            };
            if !marked {
                unreachable.push(address);
            }
        }
        // GNU has one Lisp_Cons representation, so sweeping native storage
        // cannot leave a second, stale view behind.  Rust-owned values can
        // outlive their native reachability; reconcile every departing
        // bridge entry before removing it so the typed cell retains the last
        // writes made by generated code.  Reconciliation can materialize
        // additional descendants, therefore compute the final removal set
        // only after this pass.
        for address in unreachable {
            if let Some(value) = self
                .cons_values
                .get(&address)
                .map(|mirror| mirror.value.clone())
            {
                self.reconcile_mirror(
                    address as *mut NativeCons,
                    &value,
                    &mut IdentitySet::default(),
                )
                .expect("a live native cons mirror contains valid Lisp words");
            }
        }
        let unreachable = self
            .cons_values
            .iter()
            .filter_map(|(&address, mirror)| {
                let native = address as *mut NativeCons;
                if self.native_conses.contains(native) {
                    (!self.native_conses.is_marked(native)).then_some(address)
                } else {
                    (!mirror.gc_marked).then_some(address)
                }
            })
            .collect::<Vec<_>>();
        if !unreachable.is_empty() {
            for address in unreachable {
                if let Some(mirror) = self.cons_values.remove(&address) {
                    unsafe { mirror.value.detach_native_words(address as *mut NativeCons) };
                }
                self.reconciling.remove(&address);
                self.touched.cons_set.remove(&address);
            }
            self.touched
                .conses
                .retain(|entry| self.touched.cons_set.contains(&(entry.native as usize)));
        }
        for mirror in self.cons_values.values_mut() {
            mirror.gc_marked = false;
        }

        let dead_handles = self
            .handles
            .iter()
            .enumerate()
            .filter_map(|(index, entry)| {
                entry
                    .as_ref()
                    .is_some_and(|_| !marked_handles[index])
                    .then_some(index)
            })
            .collect::<Vec<_>>();
        for index in dead_handles {
            let entry = self.handles[index]
                .take()
                .expect("dead handle index was occupied");
            let address = (&*entry.native as *const NativeHandle) as usize;
            self.handle_by_value.remove(&entry.identity);
            self.handle_by_address.remove(&address);
            self.free_handle_slots.push(index);
        }
        self.native_conses.sweep();
        lisp_roots
    }

    fn finish_nested_call(&mut self) {
        debug_assert!(self.native_call_depth > 1);
        self.native_call_depth -= 1;
    }

    pub(crate) fn encode(&mut self, value: &Value) -> Result<NativeWord, String> {
        self.encode_inner(value, &mut IdentitySet::default())
    }

    #[inline(always)]
    fn cons(&mut self, car_word: NativeWord, cdr_word: NativeWord) -> NativeWord {
        // alloc.c:Fcons is a bump allocation followed by two word stores.
        let native = self.native_conses.allocate(car_word, cdr_word);
        let address = native as usize;
        debug_assert_eq!(address & TAG_MASK, 0);
        address + TAG_CONS
    }

    fn track_cons(&mut self, native: *mut NativeCons, value: &SharedCons) {
        let address = native as usize;
        if self.native_call_depth == 0 || self.touched.cons_set.contains(&address) {
            return;
        }
        self.touched.cons_set.insert(address);
        self.touched.conses.push(TouchedCons {
            native,
            value: Rc::downgrade(value),
        });
    }

    fn register_cons_value(&mut self, native: *mut NativeCons, value: &SharedCons) {
        let address = native as usize;
        let words = unsafe { [(*native).car(), (*native).cdr()] };
        unsafe { value.attach_native_words(native, words) };
        self.cons_values.insert(
            address,
            ConsMirror {
                value: value.clone(),
                mutations: NativeConsMutationRegistration::new(address, &self.interpreter_dirty),
                gc_marked: false,
            },
        );
    }

    #[inline(always)]
    fn mirror_is_synchronized(
        native: *const NativeCons,
        value: &SharedCons,
        mutations_current: bool,
    ) -> bool {
        mutations_current
            && value.native_words_agreed() == unsafe { [(*native).car(), (*native).cdr()] }
    }

    fn synchronize_cons(&mut self, address: usize) -> Result<(), String> {
        let Some((value, mutations_current)) = self
            .cons_values
            .get(&address)
            .map(|mirror| (mirror.value.clone(), mirror.mutations.is_current()))
        else {
            return Ok(());
        };
        let native = address as *mut NativeCons;
        if self.reconciling.contains(&address) {
            return Ok(());
        }
        // GNU's XCAR/XCDR are two-word heap reads.  In the usual Emaxx case
        // neither generated code nor a Rust primitive has written the cell
        // since its last boundary crossing, so establish that directly and
        // avoid cloning its owner or entering recursive reconciliation.
        if Self::mirror_is_synchronized(native, &value, mutations_current) {
            return Ok(());
        }
        self.reconcile_mirror(native, &value, &mut IdentitySet::default())?;
        Ok(())
    }

    /// Bring a mirrored cons cell's two representations back into agreement.
    ///
    /// Generated code writes the native words directly and Rust primitives
    /// write the Rust fields; GNU has a single cell and needs neither copy.
    /// A native word that changed since the last agreement is decoded into
    /// the Rust field; otherwise a Rust field that changed since then is
    /// encoded into the native word.  Returns whether a Rust field changed.
    fn reconcile_mirror(
        &mut self,
        native: *mut NativeCons,
        value: &SharedCons,
        decoding_conses: &mut IdentitySet,
    ) -> Result<bool, String> {
        let address = native as usize;
        if decoding_conses.contains(&address) || !self.reconciling.insert(address) {
            return Ok(false);
        }
        let _sync_guard = ConsSyncGuard::enter();
        let result = self.reconcile_mirror_inner(native, value, decoding_conses);
        self.reconciling.remove(&address);
        result
    }

    fn reconcile_mirror_inner(
        &mut self,
        native: *mut NativeCons,
        value: &SharedCons,
        decoding_conses: &mut IdentitySet,
    ) -> Result<bool, String> {
        let address = native as usize;
        let current = unsafe { [(*native).car(), (*native).cdr()] };
        let agreed = value.native_words_agreed();
        let rust_dirty = self
            .cons_values
            .get(&address)
            .is_some_and(|mirror| !mirror.mutations.is_current());
        let mut words = current;
        let mut rust_changed = false;
        for field in 0..2 {
            let slot = if field == 0 { &value.car } else { &value.cdr };
            if current[field] != agreed[field] {
                decoding_conses.insert(address);
                let decoded = self.decode_inner(current[field], decoding_conses, false);
                decoding_conses.remove(&address);
                let decoded = decoded?;
                if !crate::lisp::primitives::values_eql(&slot.borrow(), &decoded) {
                    *slot.borrow_mut() = decoded;
                    rust_changed = true;
                }
            } else if rust_dirty {
                let field_value = slot.borrow().clone();
                let word = self.encode_inner(&field_value, &mut IdentitySet::default())?;
                if word != current[field] {
                    unsafe {
                        if field == 0 {
                            (*native).set_car(word);
                        } else {
                            (*native).set_cdr(word);
                        }
                    }
                    words[field] = word;
                }
            }
        }
        value.set_native_words_agreed(words);
        self.mark_cons_mirror_current(native);
        Ok(rust_changed)
    }

    fn mark_cons_mirror_current(&mut self, native: *mut NativeCons) {
        let address = native as usize;
        self.cons_values
            .get(&address)
            .expect("a reconciled native cons has a registered Rust mirror")
            .mutations
            .mark_current();
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
                self.encode_handle(NativeIdentity::Symbol(name.clone()), value, TAG_SYMBOL)
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
        let existing = cell.attached_native_address().and_then(|address| {
            self.cons_values
                .get(&address)
                .is_some_and(|mirror| Rc::ptr_eq(&mirror.value, cell))
                .then_some(address as *mut NativeCons)
        });
        let (native, existing) = if let Some(native) = existing {
            (native, true)
        } else {
            // GNU's evaluator and generated code use the same `Lisp_Cons`.
            // `ConsCell` carries that ABI prefix at offset zero, so a Rust-
            // allocated cons crosses the boundary without a second object.
            let native = ConsCell::native_words(cell);
            let address = native as usize;
            if address & TAG_MASK != 0 {
                return Err("native cons allocation is not tag-aligned".to_string());
            }
            debug_assert_eq!(address, identity);
            self.register_cons_value(native, cell);
            (native, false)
        };
        let address = native as usize;
        if existing {
            let (value, mutations_current) = self
                .cons_values
                .get(&address)
                .map(|mirror| (mirror.value.clone(), mirror.mutations.is_current()))
                .expect("the matching interpreter cons mirror is live");
            if !Self::mirror_is_synchronized(native, &value, mutations_current) {
                self.reconcile_mirror(native, &value, &mut IdentitySet::default())?;
            }
            self.track_cons(native, &value);
            return Ok(address + TAG_CONS);
        }
        if !encoding_conses.insert(identity) {
            return Ok(address + TAG_CONS);
        }
        let car = cell.car.borrow().clone();
        let cdr = cell.cdr.borrow().clone();
        let car = self.encode_inner(&car, encoding_conses)?;
        let cdr = self.encode_inner(&cdr, encoding_conses)?;
        unsafe {
            (*native).set_car(car);
            (*native).set_cdr(cdr);
        }
        cell.set_native_words_agreed([car, cdr]);
        self.track_cons(native, cell);
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
            let entry = HandleEntry {
                native,
                tag,
                identity: identity.clone(),
            };
            let index = if let Some(index) = self.free_handle_slots.pop() {
                self.handles[index] = Some(entry);
                index
            } else {
                let index = self.handles.len();
                self.handles.push(Some(entry));
                index
            };
            self.handle_by_value.insert(identity, index);
            self.handle_by_address.insert(address, index);
            index
        };
        let entry = self.handles[index]
            .as_ref()
            .expect("native handle maps only contain occupied slots");
        if entry.tag != tag {
            return Err("native object identity changed Lisp tag".to_string());
        }
        Ok((&*entry.native as *const NativeHandle) as usize + tag)
    }

    pub(crate) fn decode(&mut self, word: NativeWord) -> Result<Value, String> {
        self.decode_inner(word, &mut IdentitySet::default(), false)
    }

    /// Decode a native return value.  Only the outermost activation may
    /// stop tracking the cells it returns: while an outer generated frame is
    /// still running, it can go on writing to any cell a nested call handed
    /// back, so those cells stay tracked for the enclosing frame.
    fn decode_result(&mut self, word: NativeWord) -> Result<Value, String> {
        let outermost = self.native_call_depth <= 1;
        self.decode_inner(word, &mut IdentitySet::default(), outermost)
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
            if let Some((value, mutations_current)) = self
                .cons_values
                .get(&address)
                .map(|mirror| (mirror.value.clone(), mirror.mutations.is_current()))
            {
                let native = address as *mut NativeCons;
                if !Self::mirror_is_synchronized(native, &value, mutations_current) {
                    self.reconcile_mirror(native, &value, decoding_conses)?;
                }
                if mark_clean {
                    self.touched.cons_set.remove(&address);
                } else {
                    self.track_cons(native, &value);
                }
                return Ok(Value::Cons(value));
            }
            let native = address as *mut NativeCons;
            if !self.native_conses.contains(native) {
                return Err(format!("unknown native cons address 0x{address:x}"));
            }
            let current = unsafe { [(*native).car(), (*native).cdr()] };
            let value = ConsCell::from_native_words(current[0], current[1]);
            self.register_cons_value(native, &value);
            value.set_native_words_agreed([0, 0]);
            self.reconcile_mirror(native, &value, decoding_conses)?;
            if !mark_clean {
                self.track_cons(native, &value);
            }
            return Ok(Value::Cons(value));
        }

        let tag = word & TAG_MASK;
        let address = word.wrapping_sub(tag);
        let index = self
            .handle_by_address
            .get(&address)
            .copied()
            .ok_or_else(|| format!("unknown native Lisp word 0x{word:x}"))?;
        let entry = self.handles[index]
            .as_ref()
            .ok_or_else(|| format!("native Lisp word 0x{word:x} names a reclaimed handle"))?;
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
        for address in self.interpreter_dirty.dirty_keys() {
            if self
                .cons_values
                .get(&address)
                .is_none_or(|mirror| mirror.mutations.is_current())
            {
                continue;
            }
            let value = self
                .cons_values
                .get(&address)
                .map(|mirror| mirror.value.clone());
            let Some(value) = value else {
                continue;
            };
            self.reconcile_mirror(
                address as *mut NativeCons,
                &value,
                &mut IdentitySet::default(),
            )?;
        }
        Ok(())
    }

    #[cfg(test)]
    fn synchronize_nested_return(&mut self) -> Result<Vec<Value>, String> {
        self.synchronize_touched_conses(true)
    }

    /// Reconcile the cells generated code may have written since Rust last
    /// looked.  Keep the deduplicated union while generated frames remain
    /// active and discard it only when the outermost native call returns.
    fn synchronize_touched_conses(&mut self, retain_tracking: bool) -> Result<Vec<Value>, String> {
        if self.native_call_depth == 0 {
            return Ok(Vec::new());
        }
        let mut mutated_conses = Vec::new();
        let mut decoding_conses = IdentitySet::default();
        let mut result = Ok(());
        // Only entries present when the boundary was entered need checking.
        // Reconciliation can append a newly materialized cell, but it records
        // that cell's current words as the agreement point, so it is already
        // synchronized for this pass.
        let scan_len = self.touched.conses.len();
        for index in 0..scan_len {
            let native = self.touched.conses[index].native;
            let address = native as usize;
            if !self.touched.cons_set.contains(&address) {
                continue;
            }
            let Some(value) = self.touched.conses[index].value.upgrade() else {
                self.touched.cons_set.remove(&address);
                continue;
            };
            let current = unsafe { [(*native).car(), (*native).cdr()] };
            if result.is_ok() && current != value.native_words_agreed() {
                // Drop the vector borrow before reconciliation: decoding a
                // changed word may append another unique tracking entry.
                match self.reconcile_mirror(native, &value, &mut decoding_conses) {
                    Ok(true) => mutated_conses.push(Value::Cons(value)),
                    Ok(false) => {}
                    Err(error) => result = Err(error),
                }
            }
        }

        if !retain_tracking {
            self.native_call_depth -= 1;
            if self.native_call_depth == 0 {
                self.native_stack_bottom = std::ptr::null();
                self.touched.conses.clear();
                self.touched.cons_set.clear();
            }
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
        Value::Float(float) => (NativeIdentity::Float(float.identity_ptr()), TAG_FLOAT),
        Value::String(string) => (NativeIdentity::String(string.identity_ptr()), TAG_STRING),
        Value::StringObject(string) => (
            NativeIdentity::StringObject(std::rc::Rc::as_ptr(string) as usize),
            TAG_STRING,
        ),
        Value::BuiltinFunc(name) => (NativeIdentity::Builtin(name.clone()), TAG_VECTORLIKE),
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

    extern "C" fn call_maybe_quit() -> NativeWord {
        runtime_maybe_quit();
        0
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
            (*native).set_cdr(cdr);
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
            (*native).set_car(car);
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

    extern "C" fn call_assq(key: NativeWord, alist: NativeWord) -> NativeWord {
        let index = super::super::abi::native_subrs()
            .iter()
            .position(|subroutine| subroutine.name == "assq")
            .expect("assq belongs to the native ABI");
        invoke_subr(index, &[key, alist])
    }

    extern "C" fn call_memq(element: NativeWord, list: NativeWord) -> NativeWord {
        let index = super::super::abi::native_subrs()
            .iter()
            .position(|subroutine| subroutine.name == "memq")
            .expect("memq belongs to the native ABI");
        invoke_subr(index, &[element, list])
    }

    extern "C" fn call_list3(
        first: NativeWord,
        second: NativeWord,
        third: NativeWord,
    ) -> NativeWord {
        let index = super::super::abi::native_subrs()
            .iter()
            .position(|subroutine| subroutine.name == "list")
            .expect("list belongs to the native ABI");
        invoke_subr(index, &[first, second, third])
    }

    extern "C" fn call_symbol_value(symbol: NativeWord) -> NativeWord {
        let index = super::super::abi::native_subrs()
            .iter()
            .position(|subroutine| subroutine.name == "symbol-value")
            .expect("symbol-value belongs to the native ABI");
        invoke_subr(index, &[symbol])
    }

    extern "C" fn call_type_of(value: NativeWord) -> NativeWord {
        let index = super::super::abi::native_subrs()
            .iter()
            .position(|subroutine| subroutine.name == "type-of")
            .expect("type-of belongs to the native ABI");
        invoke_subr(index, &[value])
    }

    extern "C" fn cons_then_funcall_car(function: NativeWord, value: NativeWord) -> NativeWord {
        let list = invoke_cons(value, 0);
        let funcall = super::super::abi::native_subrs()
            .iter()
            .position(|subroutine| subroutine.name == "funcall")
            .expect("funcall belongs to the native ABI");
        invoke_subr(funcall, &[function, list])
    }

    extern "C" fn call_funcall_zero(function: NativeWord) -> NativeWord {
        let funcall = super::super::abi::native_subrs()
            .iter()
            .position(|subroutine| subroutine.name == "funcall")
            .expect("funcall belongs to the native ABI");
        invoke_subr(funcall, &[function])
    }

    extern "C" fn call_funcall_one(function: NativeWord, value: NativeWord) -> NativeWord {
        let funcall = super::super::abi::native_subrs()
            .iter()
            .position(|subroutine| subroutine.name == "funcall")
            .expect("funcall belongs to the native ABI");
        invoke_subr(funcall, &[function, value])
    }

    extern "C" fn call_funcall_two(
        function: NativeWord,
        first: NativeWord,
        second: NativeWord,
    ) -> NativeWord {
        let funcall = super::super::abi::native_subrs()
            .iter()
            .position(|subroutine| subroutine.name == "funcall")
            .expect("funcall belongs to the native ABI");
        invoke_subr(funcall, &[function, first, second])
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
    fn native_maybe_quit_follows_eval_c_process_quit_flag() {
        let mut interpreter = Interpreter::new();
        let mut environment = Env::new();
        let mut runtime = NativeRuntime::default();

        interpreter.set_symbol_value_cell("quit-flag", Value::T);
        match runtime.invoke(
            &mut interpreter,
            &mut environment,
            call_maybe_quit as *const c_void,
            NativeCallingConvention::Fixed,
            &[],
        ) {
            Err(LispError::SignalValue(value)) => {
                assert_eq!(value, Value::list([Value::symbol("quit")]))
            }
            other => panic!("ordinary quit must signal (quit), got {other:?}"),
        }
        assert_eq!(
            interpreter
                .symbol_value_cell("quit-flag")
                .expect("quit-flag value cell"),
            Value::Nil
        );

        interpreter.set_symbol_value_cell("quit-flag", Value::T);
        interpreter.set_symbol_value_cell("inhibit-quit", Value::T);
        assert_eq!(
            runtime
                .invoke(
                    &mut interpreter,
                    &mut environment,
                    call_maybe_quit as *const c_void,
                    NativeCallingConvention::Fixed,
                    &[],
                )
                .expect("inhibit-quit suppresses processing"),
            Value::Nil
        );
        assert_eq!(
            interpreter
                .symbol_value_cell("quit-flag")
                .expect("suppressed quit remains pending"),
            Value::T
        );

        let tag = Value::symbol("native-quit-tag");
        interpreter.set_symbol_value_cell("inhibit-quit", Value::Nil);
        interpreter.set_symbol_value_cell("throw-on-input", tag.clone());
        interpreter.set_symbol_value_cell("quit-flag", tag.clone());
        match runtime.invoke(
            &mut interpreter,
            &mut environment,
            call_maybe_quit as *const c_void,
            NativeCallingConvention::Fixed,
            &[],
        ) {
            Err(LispError::Throw(actual_tag, value)) => {
                assert_eq!(actual_tag, tag);
                assert_eq!(value, Value::T);
            }
            other => panic!("throw-on-input must receive t, got {other:?}"),
        }
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

        assert!(matches!(keymap, Value::Cons(_)));
        let keymap_id = crate::lisp::primitives::keymap_record_id(&interpreter, &keymap)
            .expect("private keymap lookup state");
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
    fn native_assq_uses_the_fns_c_cons_walk() {
        let mut interpreter = Interpreter::new();
        let mut environment = Env::new();
        let mut runtime = NativeRuntime::default();
        let match_entry = Value::cons(Value::symbol("match"), Value::Integer(7));
        let alist = Value::list([
            Value::Integer(1),
            Value::cons(Value::symbol("other"), Value::Integer(3)),
            match_entry.clone(),
        ]);

        let result = runtime
            .invoke(
                &mut interpreter,
                &mut environment,
                call_assq as *const c_void,
                NativeCallingConvention::Fixed,
                &[Value::symbol("match"), alist],
            )
            .expect("native assq");
        let (Value::Cons(result), Value::Cons(expected)) = (result, match_entry) else {
            panic!("assq did not return the matching alist cell");
        };
        assert!(Rc::ptr_eq(&result, &expected));
    }

    #[test]
    fn native_memq_uses_the_fns_c_cons_walk() {
        let mut interpreter = Interpreter::new();
        let mut environment = Env::new();
        let mut runtime = NativeRuntime::default();
        let matching_tail = Value::list([Value::symbol("match"), Value::Integer(9)]);
        let list = Value::cons(Value::Integer(1), matching_tail.clone());

        let result = runtime
            .invoke(
                &mut interpreter,
                &mut environment,
                call_memq as *const c_void,
                NativeCallingConvention::Fixed,
                &[Value::symbol("match"), list],
            )
            .expect("native memq");
        let (Value::Cons(result), Value::Cons(expected)) = (result, matching_tail) else {
            panic!("memq did not return the matching list tail");
        };
        assert!(Rc::ptr_eq(&result, &expected));

        let improper = Value::cons(Value::Integer(1), Value::symbol("not-a-list"));
        let error = runtime
            .invoke(
                &mut interpreter,
                &mut environment,
                call_memq as *const c_void,
                NativeCallingConvention::Fixed,
                &[Value::symbol("not-a-list"), improper.clone()],
            )
            .expect_err("GNU memq rejects an improper list without testing its final atom");
        let LispError::SignalValue(data) = error else {
            panic!("memq returned the wrong error: {error:?}");
        };
        assert_eq!(
            data,
            Value::list([
                Value::symbol("wrong-type-argument"),
                Value::symbol("listp"),
                improper,
            ])
        );
    }

    #[test]
    fn native_list_is_the_alloc_c_reverse_cons_loop() {
        let mut interpreter = Interpreter::new();
        let mut environment = Env::new();
        let mut runtime = NativeRuntime::default();

        assert_eq!(
            runtime
                .invoke(
                    &mut interpreter,
                    &mut environment,
                    call_list3 as *const c_void,
                    NativeCallingConvention::Fixed,
                    &[Value::Integer(1), Value::symbol("two"), Value::Integer(3)],
                )
                .expect("native list"),
            Value::list([Value::Integer(1), Value::symbol("two"), Value::Integer(3)])
        );
    }

    #[test]
    fn native_symbol_value_and_type_of_follow_data_c() {
        let mut interpreter = Interpreter::new();
        let mut environment = Env::new();
        let mut runtime = NativeRuntime::default();
        let global = Value::list([Value::Integer(7), Value::symbol("payload")]);
        interpreter.set_global_binding("native-hot-global", global.clone());
        environment.push(crate::lisp::types::EnvFrame::new(vec![(
            "native-hot-global".into(),
            Value::Integer(99),
        )]));

        assert_eq!(
            runtime
                .invoke(
                    &mut interpreter,
                    &mut environment,
                    call_symbol_value as *const c_void,
                    NativeCallingConvention::Fixed,
                    &[Value::symbol("native-hot-global")],
                )
                .expect("native symbol-value"),
            global,
            "symbol-value reads the global value cell, not a lexical binding"
        );

        for (value, expected) in [
            (Value::Nil, Value::symbol("symbol")),
            (Value::symbol("sample"), Value::symbol("symbol")),
            (Value::Integer(7), Value::symbol("integer")),
            (Value::float(1.5), Value::symbol("float")),
            (Value::string("sample"), Value::symbol("string")),
            (
                Value::list([Value::Integer(1), Value::Integer(2)]),
                Value::symbol("cons"),
            ),
            (
                Value::list([
                    Value::symbol("vector-literal"),
                    Value::Integer(1),
                    Value::Integer(2),
                ]),
                Value::symbol("vector"),
            ),
        ] {
            assert_eq!(
                runtime
                    .invoke(
                        &mut interpreter,
                        &mut environment,
                        call_type_of as *const c_void,
                        NativeCallingConvention::Fixed,
                        &[value],
                    )
                    .expect("native type-of"),
                expected
            );
        }
    }

    #[test]
    fn native_funcall_dispatches_builtin_on_the_word_abi() {
        let mut interpreter = Interpreter::new();
        let mut environment = Env::new();
        let mut runtime = NativeRuntime::default();

        assert_eq!(
            runtime
                .invoke(
                    &mut interpreter,
                    &mut environment,
                    cons_then_funcall_car as *const c_void,
                    NativeCallingConvention::Fixed,
                    &[Value::symbol("car"), Value::Integer(37)],
                )
                .expect("funcall of a builtin subr"),
            Value::Integer(37)
        );
        assert!(
            runtime.heap.cons_values.is_empty(),
            "funcall_subr must not materialize a native-only cons as a Rust Value"
        );
        assert_eq!(interpreter.backtrace_frames_len(), 0);
        assert_eq!(interpreter.lisp_eval_depth, 0);
    }

    #[test]
    fn native_funcall_builtin_error_unwinds_its_call_frame() {
        let mut interpreter = Interpreter::new();
        let mut environment = Env::new();
        let mut runtime = NativeRuntime::default();

        let error = runtime
            .invoke(
                &mut interpreter,
                &mut environment,
                call_funcall_one as *const c_void,
                NativeCallingConvention::Fixed,
                &[Value::symbol("car"), Value::Integer(37)],
            )
            .expect_err("car of an integer must signal");
        assert_eq!(
            crate::lisp::eval::error_condition_value(&error),
            Value::list([
                Value::symbol("wrong-type-argument"),
                Value::symbol("listp"),
                Value::Integer(37),
            ]),
            "the direct subr call preserves GNU's condition data"
        );
        assert_eq!(interpreter.backtrace_frames_len(), 0);
        assert_eq!(interpreter.lisp_eval_depth, 0);
        assert!(runtime.calls.is_empty());
    }

    #[test]
    fn native_funcall_wrong_arity_reports_the_resolved_subr() {
        let mut interpreter = Interpreter::new();
        let mut environment = Env::new();
        let mut runtime = NativeRuntime::default();

        let error = runtime
            .invoke(
                &mut interpreter,
                &mut environment,
                call_funcall_zero as *const c_void,
                NativeCallingConvention::Fixed,
                &[Value::symbol("car")],
            )
            .expect_err("car without arguments must signal");
        assert_eq!(
            crate::lisp::eval::error_condition_value(&error),
            Value::list([
                Value::symbol("wrong-number-of-arguments"),
                Value::BuiltinFunc("car".into()),
                Value::Integer(0),
            ]),
            "funcall_subr reports the resolved subr object, not its input symbol"
        );
        assert_eq!(interpreter.backtrace_frames_len(), 0);
        assert_eq!(interpreter.lisp_eval_depth, 0);
        assert!(runtime.calls.is_empty());
    }

    #[test]
    fn native_funcall_builtin_preserves_arithmetic_error() {
        let mut interpreter = Interpreter::new();
        let mut environment = Env::new();
        let mut runtime = NativeRuntime::default();

        let error = runtime
            .invoke(
                &mut interpreter,
                &mut environment,
                call_funcall_two as *const c_void,
                NativeCallingConvention::Fixed,
                &[Value::symbol("/"), Value::Integer(1), Value::Integer(0)],
            )
            .expect_err("division by zero must signal");
        assert_eq!(
            crate::lisp::eval::error_condition_value(&error),
            Value::list([Value::symbol("arith-error")]),
            "the direct subr call preserves GNU's arithmetic condition"
        );
        assert_eq!(interpreter.backtrace_frames_len(), 0);
        assert_eq!(interpreter.lisp_eval_depth, 0);
        assert!(runtime.calls.is_empty());
    }

    #[test]
    fn native_funcall_fixed_optional_nil_matches_the_c_abi() {
        for arguments in [
            vec![Value::symbol("truncate"), Value::float(f64::NAN)],
            vec![
                Value::symbol("truncate"),
                Value::float(f64::NAN),
                Value::Nil,
            ],
        ] {
            let mut interpreter = Interpreter::new();
            let mut environment = Env::new();
            let mut runtime = NativeRuntime::default();
            let target = if arguments.len() == 2 {
                call_funcall_one as *const c_void
            } else {
                call_funcall_two as *const c_void
            };

            let error = runtime
                .invoke(
                    &mut interpreter,
                    &mut environment,
                    target,
                    NativeCallingConvention::Fixed,
                    &arguments,
                )
                .expect_err("truncating NaN must signal overflow-error");
            assert_eq!(
                crate::lisp::eval::error_condition_value(&error),
                Value::list([Value::symbol("overflow-error")])
            );
            assert_eq!(interpreter.backtrace_frames_len(), 0);
            assert_eq!(interpreter.lisp_eval_depth, 0);
            assert!(runtime.calls.is_empty());
        }
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
            (*native).set_car(((9_i64 << FIXNUM_BITS) + TAG_FIXNUM_LOW as i64) as NativeWord);
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
            (*native).set_cdr(((7_i64 << FIXNUM_BITS) + TAG_FIXNUM_LOW as i64) as NativeWord);
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
            (*native).set_car(((9_i64 << FIXNUM_BITS) + TAG_FIXNUM_LOW as i64) as NativeWord);
        }
        heap.synchronize_nested_return()
            .expect("publish the nested native write");
        assert_eq!(value.car().expect("car"), Value::Integer(9));

        unsafe {
            (*native).set_cdr(((7_i64 << FIXNUM_BITS) + TAG_FIXNUM_LOW as i64) as NativeWord);
        }
        heap.finish_call().expect("publish the later outer write");
        assert_eq!(value.car().expect("car"), Value::Integer(9));
        assert_eq!(value.cdr().expect("cdr"), Value::Integer(7));
    }

    #[test]
    fn nested_calls_share_one_cons_tracking_entry() {
        let value = Value::cons(Value::Integer(1), Value::Integer(2));
        let mut heap = NativeHeap::default();
        heap.begin_call();
        let word = heap.encode(&value).expect("encode outer argument");
        assert_eq!(heap.touched.conses.len(), 1);

        heap.begin_call();
        assert_eq!(heap.encode(&value).expect("encode inner argument"), word);
        assert_eq!(heap.touched.conses.len(), 1);

        let native = word.wrapping_sub(TAG_CONS) as *mut NativeCons;
        unsafe {
            (*native).set_car(((9_i64 << FIXNUM_BITS) + TAG_FIXNUM_LOW as i64) as NativeWord);
        }
        heap.synchronize_nested_return()
            .expect("publish inner direct write");
        assert_eq!(value.car().expect("car"), Value::Integer(9));
        assert_eq!(heap.touched.conses.len(), 1);

        heap.finish_call().expect("finish inner call");
        assert_eq!(heap.native_call_depth, 1);
        assert_eq!(heap.touched.conses.len(), 1);
        unsafe {
            (*native).set_cdr(((7_i64 << FIXNUM_BITS) + TAG_FIXNUM_LOW as i64) as NativeWord);
        }
        heap.finish_call().expect("finish outer call");
        assert_eq!(value.cdr().expect("cdr"), Value::Integer(7));
        assert_eq!(heap.native_call_depth, 0);
        assert!(heap.touched.conses.is_empty());
        assert!(heap.touched.cons_set.is_empty());
    }

    #[test]
    fn native_cons_uses_one_two_word_body() {
        let tail = Value::list([Value::Integer(2), Value::Integer(3)]);
        let mut heap = NativeHeap::default();
        heap.begin_call();
        let tail_word = heap.encode(&tail).expect("encode tail");
        let head_word = heap.encode(&Value::Integer(1)).expect("encode head fixnum");
        let list_word = heap.cons(head_word, tail_word);
        let list = heap.decode(list_word).expect("decode native cons");
        let Value::Cons(cell) = &list else {
            panic!("native cons decoded as a non-cons value");
        };
        assert_ne!(ConsCell::identity(cell), 0);
        assert_eq!(
            heap.encode(&list).expect("re-encode materialized cons"),
            list_word
        );
        heap.finish_call().expect("synchronize native cons");
        assert_eq!(
            list,
            Value::list([Value::Integer(1), Value::Integer(2), Value::Integer(3),])
        );
    }

    #[test]
    fn native_created_cons_keeps_direct_writes_across_calls() {
        let mut heap = NativeHeap::default();
        heap.begin_call();
        let ten = heap
            .encode(&Value::Integer(10))
            .expect("encode initial fixnum");
        let word = heap.cons(ten, 0);
        let value = heap.decode(word).expect("materialize native cons");
        let native = word.wrapping_sub(TAG_CONS) as *mut NativeCons;
        unsafe {
            (*native).set_car(((9_i64 << FIXNUM_BITS) + TAG_FIXNUM_LOW as i64) as NativeWord);
        }
        heap.finish_call().expect("publish direct native write");
        assert_eq!(
            value.car().expect("car after first call"),
            Value::Integer(9)
        );

        heap.begin_call();
        assert_eq!(
            heap.encode(&value).expect("re-encode native-created cons"),
            word
        );
        assert_eq!(
            unsafe { (*native).car() },
            ((9_i64 << FIXNUM_BITS) + TAG_FIXNUM_LOW as i64) as NativeWord
        );
        heap.finish_call().expect("finish second call");
    }

    #[test]
    fn native_gc_matches_gnu_threshold_boundary_and_half_live_adjustment() {
        let mut gc = NativeGcState::default();

        // lisp.h:maybe_gc tests for a negative counter, not <= 0.  The first
        // reconsideration also installs alloc.c's default threshold.
        gc.tally_consing(NATIVE_GC_DEFAULT_THRESHOLD as usize);
        assert!(!gc.collection_due(NATIVE_GC_DEFAULT_THRESHOLD, Some(0.1)));
        gc.tally_consing(1);
        assert!(gc.collection_due(NATIVE_GC_DEFAULT_THRESHOLD, Some(0.1)));

        // With 10 MB live after the last collection, GNU allows 10% plus its
        // deliberate half-of-new-allocation live-data estimate.  1,052,631
        // allocated bytes are therefore still permitted; the next byte is
        // the first one that leaves the recalculated allowance negative.
        gc.collection_finished(10_000_000, NATIVE_GC_DEFAULT_THRESHOLD, Some(0.1));
        gc.tally_consing(1_052_631);
        assert!(!gc.collection_due(NATIVE_GC_DEFAULT_THRESHOLD, Some(0.1)));
        gc.tally_consing(1);
        assert!(gc.collection_due(NATIVE_GC_DEFAULT_THRESHOLD, Some(0.1)));
    }

    #[test]
    fn native_gc_matches_gnu_percentage_type_and_saturation_rules() {
        let gc = NativeGcState {
            live_bytes: 10_000_000,
            ..NativeGcState::default()
        };

        assert_eq!(
            gc.consing_threshold(-1, None, 0),
            NATIVE_GC_MINIMUM_THRESHOLD
        );
        assert_eq!(
            gc.consing_threshold(NATIVE_GC_DEFAULT_THRESHOLD, Some(f64::NAN), 0),
            NATIVE_GC_DEFAULT_THRESHOLD
        );
        assert_eq!(
            gc.consing_threshold(NATIVE_GC_DEFAULT_THRESHOLD, Some(f64::INFINITY), 0),
            NATIVE_GC_HIGH_THRESHOLD
        );
        assert_eq!(
            gc.consing_threshold(i64::MAX, None, 0),
            NATIVE_GC_HIGH_THRESHOLD
        );
    }

    #[test]
    fn native_gc_marks_reachable_arena_conses_and_reuses_the_rest() {
        let mut heap = NativeHeap::default();
        heap.begin_call();
        let stack_marker = 0;
        heap.set_stack_bottom(std::ptr::from_ref(&stack_marker));
        for index in 0..5_000 {
            let value = (index << FIXNUM_BITS) + TAG_FIXNUM_LOW;
            std::hint::black_box(heap.cons(value, 0));
        }
        let tail = heap.cons(TAG_FIXNUM_LOW, 0);
        let head = heap.cons((2 << FIXNUM_BITS) + TAG_FIXNUM_LOW, tail);

        heap.collect(std::ptr::from_ref(&stack_marker), &[head]);

        assert_eq!(heap.native_conses.live, 2);
        assert!(
            heap.native_conses
                .contains(head.wrapping_sub(TAG_CONS) as *const NativeCons)
        );
        assert!(
            heap.native_conses
                .contains(tail.wrapping_sub(TAG_CONS) as *const NativeCons)
        );
        let blocks_before_reuse = heap.native_conses.blocks.len();
        std::hint::black_box(heap.cons(TAG_FIXNUM_LOW, 0));
        assert_eq!(heap.native_conses.blocks.len(), blocks_before_reuse);
    }

    #[test]
    fn native_gc_owns_primitive_returned_rust_cons_until_it_is_unreachable() {
        let mut heap = NativeHeap::default();
        heap.begin_call();
        let stack_marker = 0;
        heap.set_stack_bottom(std::ptr::from_ref(&stack_marker));
        let value = Value::cons(Value::Integer(7), Value::Nil);
        let word = heap.encode(&value).expect("encode Rust-created cons");
        drop(value);
        for index in 0..5_000 {
            let value = (index << FIXNUM_BITS) + TAG_FIXNUM_LOW;
            std::hint::black_box(heap.cons(value, 0));
        }

        heap.collect(std::ptr::from_ref(&stack_marker), &[word]);
        assert_eq!(
            heap.decode(word)
                .expect("native root keeps the Rust cons alive")
                .car()
                .expect("cons car"),
            Value::Integer(7)
        );

        for index in 0..5_000 {
            let value = (index << FIXNUM_BITS) + TAG_FIXNUM_LOW;
            std::hint::black_box(heap.cons(value, 0));
        }
        heap.collect(std::ptr::from_ref(&stack_marker), &[]);
        assert!(!heap.cons_values.contains_key(&word.wrapping_sub(TAG_CONS)));
    }

    #[test]
    fn native_gc_detaches_externally_owned_rust_cons_graphs_without_losing_writes() {
        let mut heap = NativeHeap::default();
        heap.begin_call();
        let stack_marker = 0;
        heap.set_stack_bottom(std::ptr::from_ref(&stack_marker));
        let value = Value::list([Value::Integer(7), Value::Integer(8)]);
        let word = heap.encode(&value).expect("encode externally owned list");
        let tail_word = unsafe { (*(word.wrapping_sub(TAG_CONS) as *const NativeCons)).cdr() };
        unsafe {
            (*(tail_word.wrapping_sub(TAG_CONS) as *mut NativeCons))
                .set_car((9 << FIXNUM_BITS) + TAG_FIXNUM_LOW);
        }
        for index in 0..5_000 {
            let value = (index << FIXNUM_BITS) + TAG_FIXNUM_LOW;
            std::hint::black_box(heap.cons(value, 0));
        }

        heap.collect(std::ptr::from_ref(&stack_marker), &[]);

        assert!(!heap.cons_values.contains_key(&word.wrapping_sub(TAG_CONS)));
        assert!(
            !heap
                .cons_values
                .contains_key(&tail_word.wrapping_sub(TAG_CONS))
        );
        assert!(
            value.to_vec().expect("detached list remains valid")
                == [Value::Integer(7), Value::Integer(9)]
        );
    }

    #[test]
    fn native_gc_sweeps_unreachable_reference_counted_handles() {
        let mut heap = NativeHeap::default();
        heap.begin_call();
        let stack_marker = 0;
        heap.set_stack_bottom(std::ptr::from_ref(&stack_marker));
        let value = Value::string("temporary native string");
        let word = heap.encode(&value).expect("encode native string handle");
        drop(value);
        for index in 0..5_000 {
            let value = (index << FIXNUM_BITS) + TAG_FIXNUM_LOW;
            std::hint::black_box(heap.cons(value, 0));
        }

        heap.collect(std::ptr::from_ref(&stack_marker), &[]);

        assert!(heap.handles.iter().all(Option::is_none));
        assert!(heap.decode(word).is_err());
    }

    #[test]
    fn native_gc_publishes_native_only_handles_to_lisp_reachability() {
        let mut heap = NativeHeap::default();
        heap.begin_call();
        let stack_marker = 0;
        heap.set_stack_bottom(std::ptr::from_ref(&stack_marker));
        let word = heap
            .encode(&Value::Record(42))
            .expect("encode record reachable only through native storage");

        let roots = heap.collect(std::ptr::from_ref(&stack_marker), &[word]);

        assert!(roots.contains(&Value::Record(42)));
        assert_eq!(
            heap.decode(word)
                .expect("published native root remains live"),
            Value::Record(42)
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
            unsafe { (*native).car() },
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
        let tail_word = unsafe { (*(word.wrapping_sub(TAG_CONS) as *mut NativeCons)).cdr() };
        heap.finish_call().expect("finish first call");

        tail.set_car(Value::Integer(7)).expect("mutate list tail");
        heap.begin_call();
        heap.publish_interpreter_writes()
            .expect("publish queued mutation");

        let native_tail = tail_word.wrapping_sub(TAG_CONS) as *mut NativeCons;
        assert_eq!(
            unsafe { (*native_tail).car() },
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
