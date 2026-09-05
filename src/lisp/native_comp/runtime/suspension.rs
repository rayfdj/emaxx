//! Quiescent native ownership transfer at a cooperative Lisp-thread switch.
//!
//! Like alloc.c:flush_stack_call_func and thread.c:mark_one_thread, spill the
//! current machine registers and retain each suspended stack's real roots.
//! The spill frame survives until this same logical thread resumes.

use super::*;
use crate::lisp::eval::roots::{LispRootMarker, TraceLispRoots};
use std::any::Any;
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};

#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
core::arch::global_asm!(
    r#"
    .section __TEXT,__text,regular,pure_instructions
    .p2align 2
    .private_extern _emaxx_native_suspend_trampoline
_emaxx_native_suspend_trampoline:
    sub sp, sp, #96
    stp x29, x30, [sp]
    stp x19, x20, [sp, #16]
    stp x21, x22, [sp, #32]
    stp x23, x24, [sp, #48]
    stp x25, x26, [sp, #64]
    stp x27, x28, [sp, #80]
    mov x29, sp
    mov x8, x0
    mov x0, sp
    blr x8
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

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
core::arch::global_asm!(
    r#"
    .text
    .p2align 4
    .hidden emaxx_native_suspend_trampoline
    .type emaxx_native_suspend_trampoline, @function
emaxx_native_suspend_trampoline:
    push rbp
    push rbx
    push r12
    push r13
    push r14
    push r15
    sub rsp, 8
    mov rax, rdi
    mov rdi, rsp
    call rax
    add rsp, 8
    pop r15
    pop r14
    pop r13
    pop r12
    pop rbx
    pop rbp
    ret
    .size emaxx_native_suspend_trampoline, .-emaxx_native_suspend_trampoline
    "#,
);

unsafe extern "C" {
    fn emaxx_native_suspend_trampoline(
        callback: unsafe extern "C" fn(*const NativeWord, *mut c_void),
        argument: *mut c_void,
    );
}

fn with_spilled_registers<R>(body: impl FnOnce(*const NativeWord) -> R) -> R {
    struct Invocation<F, R> {
        body: Option<F>,
        result: Option<Result<R, Box<dyn Any + Send>>>,
    }
    unsafe extern "C" fn invoke<F: FnOnce(*const NativeWord) -> R, R>(
        stack_top: *const NativeWord,
        argument: *mut c_void,
    ) {
        // SAFETY: the trampoline receives this exact, live Invocation and
        // calls back once. No Rust panic may cross its unannotated C frame.
        let invocation = unsafe { &mut *argument.cast::<Invocation<F, R>>() };
        invocation.result = Some(catch_unwind(AssertUnwindSafe(|| {
            invocation.body.take().expect("single spill callback")(stack_top)
        })));
    }
    fn run<F: FnOnce(*const NativeWord) -> R, R>(body: F) -> R {
        let mut invocation = Invocation {
            body: Some(body),
            result: None,
        };
        unsafe {
            emaxx_native_suspend_trampoline(
                invoke::<F, R>,
                std::ptr::from_mut(&mut invocation).cast(),
            );
        }
        match invocation.result.expect("spill callback completed") {
            Ok(result) => result,
            Err(panic) => resume_unwind(panic),
        }
    }
    run(body)
}

struct SuspendedNativeWords {
    ranges: Vec<NativeRootRange>,
    handlers: Vec<NativeWord>,
}

#[derive(Default)]
pub(super) struct SuspendedNativeStacks {
    entries: Rc<RefCell<Vec<Option<SuspendedNativeWords>>>>,
}

impl SuspendedNativeStacks {
    pub(super) fn is_empty(&self) -> bool {
        self.entries.borrow().iter().all(Option::is_none)
    }

    pub(super) fn append_words(&self, words: &mut Vec<NativeWord>) {
        for entry in self.entries.borrow().iter().flatten() {
            words.extend_from_slice(&entry.handlers);
            for range in &entry.ranges {
                // SAFETY: the private SuspendedStackGuard is inside the
                // spill callback, which cannot return before deregistration.
                // Its original native frames/ephemeral arrays remain frozen.
                for index in 0..range.len {
                    words.push(unsafe { range.start.add(index).read() });
                }
            }
        }
    }
}

struct SuspendedStackGuard {
    entries: Rc<RefCell<Vec<Option<SuspendedNativeWords>>>>,
    slot: usize,
}

impl Drop for SuspendedStackGuard {
    fn drop(&mut self) {
        self.entries.borrow_mut()[self.slot] = None;
    }
}

struct SuspendedLispRoots<'a> {
    handlers: &'a [HandlerEntry],
    unwind: &'a [UnwindAction],
    calls: &'a [NativeCallFrame],
    environment: Option<&'a Env>,
}

impl TraceLispRoots for SuspendedLispRoots<'_> {
    fn trace_lisp_roots(&self, marker: &mut LispRootMarker<'_>) {
        for handler in self.handlers {
            marker.value(&handler.match_value);
        }
        for action in self.unwind {
            match action {
                // The canonical specbinding lives in InterpreterState (and
                // then in its owning thread context). This unwind action
                // carries a restore token with a historical copy: tracing
                // that copy would retain values replaced by thread swaps.
                UnwindAction::Special(_) => {}
                UnwindAction::Excursion(saved) => saved.trace_lisp_roots(marker),
                UnwindAction::Restriction(saved) => saved.trace_lisp_roots(marker),
                UnwindAction::Cleanup { value, .. } => marker.value(value),
                UnwindAction::CurrentBuffer(_) => {}
            }
        }
        for call in self.calls {
            if let Some(error) = &call.pending_error {
                error.trace_lisp_roots(marker);
            }
        }
        if let Some(environment) = self.environment {
            marker.environment(environment);
        }
    }
}

struct SuspendedTlsGuard {
    active: *mut ActiveCall,
    heap: Option<*mut NativeHeap>,
    cons_sync_depth: usize,
}

impl Drop for SuspendedTlsGuard {
    fn drop(&mut self) {
        ACTIVE_CALL.set(self.active);
        if let Some(heap) = self.heap {
            ACTIVE_NATIVE_HEAP.store(heap, Ordering::Relaxed);
        }
        CONS_SYNC_DEPTH.set(self.cons_sync_depth);
    }
}

pub(crate) fn with_thread_suspended<R>(
    interpreter: &mut Interpreter,
    body: impl FnOnce(&mut Interpreter) -> R,
) -> Result<R, LispError> {
    with_spilled_registers(|stack_top| {
        super::super::loader::with_suspended_state(interpreter, |interpreter, registered| {
            let active = ACTIVE_CALL.get();
            let (runtime, environment) = if active.is_null() {
                (registered, None)
            } else {
                // SAFETY: these are the current callback's native owner and
                // environment, not a different or already parked thread.
                let active = unsafe { &*active };
                assert_eq!(active.interpreter, std::ptr::from_mut(interpreter));
                (active.runtime, Some(unsafe { &*active.environment }))
            };
            if runtime.is_null() {
                return Ok(body(interpreter));
            }
            let runtime = unsafe { &mut *runtime };
            runtime.sync_handlers(interpreter)?;
            runtime.publish_heap_writes(interpreter, true)?;

            let mut ranges = runtime.ephemeral_root_ranges.clone();
            let bottom = runtime.heap.native_stack_bottom;
            if !bottom.is_null() {
                let start = (stack_top as usize).min(bottom as usize);
                let end = (stack_top as usize).max(bottom as usize);
                let alignment = std::mem::size_of::<NativeWord>();
                let start = start.div_ceil(alignment) * alignment;
                ranges.push(NativeRootRange {
                    start: start as *const NativeWord,
                    len: end.saturating_sub(start) / alignment,
                });
            }
            let roots = SuspendedNativeWords {
                ranges,
                handlers: runtime
                    .handlers
                    .iter()
                    .map(|entry| entry.storage.value())
                    .collect(),
            };
            let entries = Rc::clone(&runtime.suspended_stacks.entries);
            let slot = {
                let mut entries = entries.borrow_mut();
                if let Some(slot) = entries.iter().position(Option::is_none) {
                    entries[slot] = Some(roots);
                    slot
                } else {
                    entries.push(Some(roots));
                    entries.len() - 1
                }
            };
            let _stack_guard = SuspendedStackGuard { entries, slot };
            runtime.heap.native_stack_bottom = std::ptr::null();
            let placeholder = std::mem::replace(
                &mut interpreter.native_compiler.runtime.shared,
                runtime.shared.take(),
            );
            interpreter.native_compiler.runtime.activate_thread();

            let tls = SuspendedTlsGuard {
                active: ACTIVE_CALL.replace(std::ptr::null_mut()),
                // A loader/compiler-only callback does not own the native
                // execution lock. In that case the process-global pointer
                // can belong to an unrelated OS thread: never change it.
                heap: (!active.is_null())
                    .then(|| ACTIVE_NATIVE_HEAP.swap(std::ptr::null_mut(), Ordering::Relaxed)),
                cons_sync_depth: CONS_SYNC_DEPTH.replace(0),
            };
            let roots = SuspendedLispRoots {
                handlers: &runtime.handlers,
                unwind: &runtime.unwind,
                calls: &runtime.calls,
                environment,
            };
            let result = catch_unwind(AssertUnwindSafe(|| {
                interpreter.with_lisp_stack_roots(&roots, body)
            }));
            runtime.shared =
                std::mem::replace(&mut interpreter.native_compiler.runtime.shared, placeholder);
            runtime.activate_thread();
            runtime.heap.native_stack_bottom = bottom;
            *runtime.symbols_with_positions_enabled = interpreter
                .symbol_value_cell("symbols-with-pos-enabled")
                .is_ok_and(|value| value.is_truthy());
            drop(tls);
            let publish = runtime
                .heap
                .publish_interpreter_writes()
                .map_err(|error| super::super::lisp::native_ice(&error));
            match result {
                Ok(result) => publish.map(|()| result),
                Err(panic) => resume_unwind(panic),
            }
        })
    })
}

#[cfg(test)]
pub(crate) fn invoke_suspension_probe(interpreter: &mut Interpreter) -> Result<Value, LispError> {
    extern "C" fn probe() -> NativeWord {
        // Keep an actual native-only cons word live across the call. It is
        // intentionally absent from Rust Value roots and relocation arrays.
        let word = with_active_heap(|heap| heap.cons((17 << FIXNUM_BITS) | TAG_FIXNUM_LOW, 0));
        let interpreter = with_active(|active| active.interpreter);
        let result = crate::lisp::eval::continuations::suspend(unsafe { &mut *interpreter });
        if let Err(error) = result {
            with_active(|active| remember_helper_error(active, error));
            return 0;
        }
        let forwarding_matches = with_active(|active| {
            let expected = unsafe { &*active.interpreter }
                .symbol_value_cell("symbols-with-pos-enabled")
                .is_ok_and(|value| value.is_truthy());
            *unsafe { &*active.runtime }.symbols_with_positions_enabled == expected
        });
        if !forwarding_matches {
            return 0;
        }
        // Avoid dereferencing a reclaimed arena cell if the root assertion
        // fails: report nil and let the Rust-side test produce the failure.
        with_active_heap(|heap| {
            let pointer = word.wrapping_sub(TAG_CONS) as *const NativeCons;
            if heap.native_conses.contains(pointer) {
                word
            } else {
                0
            }
        })
    }
    let mut state = std::mem::take(&mut interpreter.native_compiler);
    let key = Value::list([Value::Integer(73)]);
    let table = interpreter
        .lookup_var("native-suspension-weak-table", &Env::new())
        .expect("test table");
    crate::lisp::primitives::call(
        interpreter,
        "puthash",
        &[key.clone(), Value::T, table],
        &mut Env::new(),
    )?;
    state.runtime.unwind.push(UnwindAction::Cleanup {
        function: false,
        value: key,
    });
    let result = super::super::loader::with_native_state(
        &state.compiler,
        &mut state.registry,
        &mut state.runtime,
        |runtime| {
            runtime.invoke(
                interpreter,
                &mut Env::new(),
                probe as *const c_void,
                NativeCallingConvention::Fixed,
                &[],
            )
        },
    );
    state.runtime.unwind.pop().expect("test unwind root");
    interpreter.native_compiler = state;
    result
}

#[cfg(test)]
pub(crate) fn invoke_suspension_companion(
    interpreter: &mut Interpreter,
) -> Result<Value, LispError> {
    extern "C" fn companion() -> NativeWord {
        (42 << FIXNUM_BITS) | TAG_FIXNUM_LOW
    }
    let mut state = std::mem::take(&mut interpreter.native_compiler);
    let result = state.runtime.invoke(
        interpreter,
        &mut Env::new(),
        companion as *const c_void,
        NativeCallingConvention::Fixed,
        &[],
    );
    interpreter.native_compiler = state;
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spill_callback_preserves_values_and_propagates_panics_outside_the_c_frame() {
        assert_eq!(
            with_spilled_registers(|top| {
                assert!(!top.is_null());
                47
            }),
            47
        );
        let panic = catch_unwind(|| with_spilled_registers(|_| panic!("spill-scope-test")))
            .expect_err("panic returns through Rust, never through assembly");
        assert_eq!(panic.downcast_ref::<&str>(), Some(&"spill-scope-test"));
    }
}
