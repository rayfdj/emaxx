//! Owned, same-OS-thread continuations for Lisp thread execution.
//!
//! The main execution shell owns these stacks outside the movable editor
//! payload. A resume transfers that actual payload to the child shell; a
//! suspension transfers it back without aliasing the parked shell.

use super::{Env, Interpreter, InterpreterState, LispError, Value};
use corosensei::stack::{DefaultStack, Stack};
use corosensei::{Coroutine, CoroutineResult, Yielder};
use std::any::Any;
use std::cell::Cell;
use std::collections::HashMap;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::rc::Rc;

type EditorState = Box<InterpreterState>;
type ThreadYielder = Yielder<EditorState, EditorState>;

// Rust evaluator frames are larger than GNU's C frames. Reserve address space
// consistently with the existing optimized host-test stack; pages are committed
// only as used, and corosensei retains an inaccessible overflow guard page.
const LISP_THREAD_STACK_BYTES: usize = 128 * 1024 * 1024;

pub(super) struct ThreadCompletion {
    pub(super) state: EditorState,
    pub(super) result: Result<Result<Value, LispError>, Box<dyn Any + Send>>,
}

pub(super) struct ThreadContinuation {
    coroutine: Coroutine<EditorState, EditorState, ThreadCompletion>,
    yielder: Rc<Cell<*const ThreadYielder>>,
    stack_base: usize,
}

/// Each resume has its own scope on the driving stack. In particular, a
/// continuation never retains a pointer to a previous resume's caller frame.
struct ResumeContext {
    editor: *const InterpreterState,
    yielder: Rc<Cell<*const ThreadYielder>>,
    stack_base: usize,
}

thread_local! {
    static CURRENT_RESUME: Cell<*const ResumeContext> = const { Cell::new(std::ptr::null()) };
}

struct ResumeGuard(*const ResumeContext);

impl Drop for ResumeGuard {
    fn drop(&mut self) {
        CURRENT_RESUME.set(self.0);
    }
}

impl ThreadContinuation {
    pub(super) fn new(function: Value) -> std::io::Result<Self> {
        Self::with_body(move |interpreter| {
            interpreter.call_function_value(function, None, &[], &mut Env::new())
        })
    }

    fn with_body(
        body: impl FnOnce(&mut Interpreter) -> Result<Value, LispError> + 'static,
    ) -> std::io::Result<Self> {
        let stack = DefaultStack::new(LISP_THREAD_STACK_BYTES)?;
        let stack_base = stack.base().get();
        let yielder = Rc::new(Cell::new(std::ptr::null()));
        let entry_yielder = Rc::clone(&yielder);
        let coroutine = Coroutine::with_stack(stack, move |yielder, state| {
            entry_yielder.set(std::ptr::from_ref(yielder));
            let mut interpreter = Interpreter {
                state: Some(state),
                continuations: ThreadContinuations::default(),
            };
            // Recover the editor payload before propagating a Rust panic to
            // the driving shell, so that it can perform orderly stack teardown.
            let result = catch_unwind(AssertUnwindSafe(|| body(&mut interpreter)));
            ThreadCompletion {
                state: interpreter
                    .state
                    .take()
                    .expect("returning thread owns editor state"),
                result,
            }
        });
        Ok(Self {
            coroutine,
            yielder,
            stack_base,
        })
    }

    pub(super) fn resume(
        &mut self,
        state: EditorState,
    ) -> CoroutineResult<EditorState, ThreadCompletion> {
        let context = ResumeContext {
            editor: std::ptr::from_ref(&*state),
            yielder: Rc::clone(&self.yielder),
            stack_base: self.stack_base,
        };
        let _guard = ResumeGuard(CURRENT_RESUME.replace(std::ptr::from_ref(&context)));
        self.coroutine.resume(state)
    }

    pub(super) fn done(&self) -> bool {
        self.coroutine.done()
    }
}

#[derive(Default)]
pub(super) struct ThreadContinuations {
    pub(super) threads: HashMap<u64, ThreadContinuation>,
}

impl Clone for ThreadContinuations {
    fn clone(&self) -> Self {
        assert!(
            self.threads.is_empty(),
            "cannot clone an editor with suspended threads"
        );
        Self::default()
    }
}

pub(super) fn can_suspend(interpreter: &Interpreter) -> bool {
    CURRENT_RESUME.with(|current| {
        let context = current.get();
        // SAFETY: only resume installs this scope, and its private guard
        // clears/restores the pointer before that caller frame returns.
        !context.is_null() && unsafe { (*context).editor == std::ptr::from_ref(&**interpreter) }
    })
}

pub(crate) fn suspend(interpreter: &mut Interpreter) -> Result<(), LispError> {
    interpreter.materialize_native_backtrace_arguments()?;
    crate::lisp::native_comp::with_thread_suspended(interpreter, suspend_payload)?;
    // During editor teardown, leave native/Rust frames through ordinary
    // non-catchable Lisp termination, never a forced Rust unwind through C.
    interpreter.check_thread_termination()
}

fn suspend_payload(interpreter: &mut Interpreter) {
    let yielder = CURRENT_RESUME.with(|current| {
        let context = current.get();
        assert!(
            !context.is_null(),
            "thread suspension needs a live resume scope"
        );
        // SAFETY: the private resume scope is live throughout this call.
        let context = unsafe { &*context };
        assert_eq!(context.editor, std::ptr::from_ref(&**interpreter));
        context.yielder.get()
    });
    assert!(
        !yielder.is_null(),
        "coroutine entry installs its actual yielder"
    );
    let state = interpreter
        .state
        .take()
        .expect("suspending thread owns editor state");
    // SAFETY: the yielder is borrowed by the coroutine's entry function and
    // remains live until that function returns. It is used only while this
    // same continuation is active; no Send implementation is introduced.
    let state = unsafe { &*yielder }.suspend(state);
    interpreter.state = Some(state);
}

pub(crate) fn current_stack_base() -> Option<*const usize> {
    CURRENT_RESUME.with(|current| {
        let context = current.get();
        // SAFETY: same private resume-scope lifetime as can_suspend. This is
        // the physical stack bound even for a nested independent interpreter.
        (!context.is_null()).then(|| unsafe { (*context).stack_base as *const usize })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dropping_editor_resumes_suspended_native_and_rust_frames_to_termination() {
        struct FrameGuard(Rc<Cell<usize>>);
        impl Drop for FrameGuard {
            fn drop(&mut self) {
                self.0.set(self.0.get() + 1);
            }
        }

        let dropped = Rc::new(Cell::new(0));
        let terminated = Rc::new(Cell::new(0));
        let mut interpreter = Interpreter::new();
        let table = crate::lisp::json::make_hash_table(&mut interpreter, "eq", Vec::new());
        interpreter.set_global_binding("native-suspension-weak-table", table);
        for native in [false, true] {
            let Value::Record(id) = interpreter
                .make_thread(
                    Value::symbol("ignore"),
                    None,
                    crate::lisp::eval::BufferDisposition::Default,
                )
                .expect("create registered thread")
            else {
                panic!("thread record")
            };
            let dropped = Rc::clone(&dropped);
            let terminated = Rc::clone(&terminated);
            let continuation = ThreadContinuation::with_body(move |interpreter| {
                let _guard = FrameGuard(dropped);
                let result = if native {
                    crate::lisp::native_comp::invoke_suspension_probe(interpreter)
                } else {
                    suspend(interpreter).map(|()| Value::Nil)
                };
                if matches!(result, Err(LispError::Terminate(_))) {
                    terminated.set(terminated.get() + 1);
                }
                result
            })
            .expect("actual owned execution stack");
            interpreter
                .new_thread_continuations
                .threads
                .insert(id, continuation);
            interpreter
                .step_thread(id, &mut Env::new())
                .expect("suspend original execution frames");
            assert!(interpreter.thread_live(id));
        }
        assert_eq!(interpreter.continuations.threads.len(), 2);
        assert_eq!(dropped.get(), 0);
        assert_eq!(terminated.get(), 0);
        drop(interpreter);
        assert_eq!(terminated.get(), 2, "both original frames resumed normally");
        assert_eq!(dropped.get(), 2, "both Rust frame guards were released");
        assert!(current_stack_base().is_none());

        // A leaked native activation lock or TLS owner would deadlock or
        // misdirect this fresh interpreter's ordinary native call.
        let mut fresh = Interpreter::new();
        assert_eq!(
            crate::lisp::native_comp::invoke_suspension_companion(&mut fresh)
                .expect("fresh native invocation after editor teardown"),
            Value::Integer(42)
        );
    }

    #[test]
    fn native_continuations_can_return_in_non_lifo_order() {
        let mut interpreter = Interpreter::new();
        let table = crate::lisp::json::make_hash_table(&mut interpreter, "eq", Vec::new());
        let Value::Record(table_id) = table else {
            panic!("weak table record")
        };
        interpreter.find_record_mut(table_id).expect("table").slots[5] = Value::symbol("key");
        interpreter.set_global_binding("native-suspension-weak-table", Value::Record(table_id));
        let mut first =
            ThreadContinuation::with_body(crate::lisp::native_comp::invoke_suspension_probe)
                .expect("first native stack");
        let mut second =
            ThreadContinuation::with_body(crate::lisp::native_comp::invoke_suspension_probe)
                .expect("second native stack");
        for continuation in [&mut first, &mut second] {
            let CoroutineResult::Yield(state) =
                continuation.resume(interpreter.state.take().expect("state"))
            else {
                panic!("both actual native frames must suspend");
            };
            interpreter.state = Some(state);
        }
        for (continuation, roots, enabled) in
            [(&mut first, 2, Value::T), (&mut second, 1, Value::Nil)]
        {
            crate::lisp::primitives::call(
                &mut interpreter,
                "garbage-collect",
                &[],
                &mut Env::new(),
            )
            .expect("collect all suspended machine stacks");
            assert_eq!(
                interpreter
                    .hash_table_runtime_entries(table_id)
                    .expect("table")
                    .len(),
                roots
            );
            interpreter.set_global_binding("symbols-with-pos-enabled", enabled);
            let CoroutineResult::Return(completion) =
                continuation.resume(interpreter.state.take().expect("state"))
            else {
                panic!("each original native frame resumes then returns");
            };
            interpreter.state = Some(completion.state);
            assert_eq!(
                completion.result.expect("no panic").expect("native result"),
                Value::list([Value::Integer(17)])
            );
            assert!(continuation.done());
        }
        crate::lisp::primitives::call(&mut interpreter, "garbage-collect", &[], &mut Env::new())
            .expect("collect after both native stacks return");
        assert!(
            interpreter
                .hash_table_runtime_entries(table_id)
                .expect("table")
                .is_empty()
        );
    }

    #[test]
    fn native_machine_frame_and_unwind_roots_survive_an_actual_stack_switch() {
        let mut interpreter = Interpreter::new();
        let table = crate::lisp::json::make_hash_table(&mut interpreter, "eq", Vec::new());
        let Value::Record(table_id) = table else {
            panic!("weak table record");
        };
        interpreter.find_record_mut(table_id).expect("table").slots[5] = Value::symbol("key");
        interpreter.set_global_binding("native-suspension-weak-table", Value::Record(table_id));
        let mut continuation =
            ThreadContinuation::with_body(crate::lisp::native_comp::invoke_suspension_probe)
                .expect("guarded native stack");
        let CoroutineResult::Yield(state) =
            continuation.resume(interpreter.state.take().expect("state"))
        else {
            panic!("native callback must suspend its actual machine frame");
        };
        interpreter.state = Some(state);
        crate::lisp::primitives::call(&mut interpreter, "garbage-collect", &[], &mut Env::new())
            .expect("real GC while native code is suspended");
        assert_eq!(
            interpreter
                .hash_table_runtime_entries(table_id)
                .expect("table")
                .len(),
            1,
            "the suspended native unwind action owns the weak key"
        );
        // A separate native invocation on the driving stack must use this
        // same heap and must not acquire the OS-thread execution lock again.
        assert_eq!(
            crate::lisp::native_comp::invoke_suspension_companion(&mut interpreter)
                .expect("independent native activation"),
            Value::Integer(42)
        );
        let CoroutineResult::Return(completion) =
            continuation.resume(interpreter.state.take().expect("state"))
        else {
            panic!("native frame must return after resuming");
        };
        interpreter.state = Some(completion.state);
        assert_eq!(
            completion
                .result
                .expect("no Rust panic")
                .expect("native result"),
            Value::list([Value::Integer(17)]),
            "native-only word survived the other stack's collection"
        );
        assert!(continuation.done());
        crate::lisp::primitives::call(&mut interpreter, "garbage-collect", &[], &mut Env::new())
            .expect("collect after native frame exits");
        assert!(
            interpreter
                .hash_table_runtime_entries(table_id)
                .expect("table")
                .is_empty(),
            "completed native unwind roots must be removed"
        );
    }

    #[test]
    fn owned_continuation_resumes_the_actual_nested_rust_frame() {
        fn nested(interpreter: &mut Interpreter) -> Result<Value, LispError> {
            let mut local = vec![Value::list([Value::Integer(11)])];
            let table = interpreter
                .lookup_var("continuation-table", &Env::new())
                .expect("weak table");
            crate::lisp::primitives::call(
                interpreter,
                "puthash",
                &[local[0].clone(), Value::T, table],
                &mut Env::new(),
            )?;
            let address = local.as_ptr();
            assert!(can_suspend(interpreter));
            interpreter.with_lisp_stack_roots(&local, suspend)?;
            assert_eq!(local.as_ptr(), address);
            local.push(
                interpreter
                    .lookup_var("continuation-write", &Env::new())
                    .expect("parent write"),
            );
            interpreter.with_lisp_stack_roots(&local, suspend)?;
            Ok(Value::list(local))
        }
        let mut interpreter = Interpreter::new();
        let table = crate::lisp::json::make_hash_table(&mut interpreter, "eq", Vec::new());
        let Value::Record(table_id) = table else {
            panic!("hash table must be a record")
        };
        interpreter
            .find_record_mut(table_id)
            .expect("weak table")
            .slots[5] = Value::symbol("key");
        interpreter.set_global_binding("continuation-table", Value::Record(table_id));
        let payload = std::ptr::from_ref(&*interpreter);
        let mut continuation =
            ThreadContinuation::with_body(nested).expect("guarded coroutine stack");
        assert!(current_stack_base().is_none());
        for index in 0..2 {
            let state = interpreter.state.take().expect("driver owns the payload");
            let CoroutineResult::Yield(state) = continuation.resume(state) else {
                panic!("nested function must suspend before returning");
            };
            interpreter.state = Some(state);
            assert_eq!(std::ptr::from_ref(&*interpreter), payload);
            assert!(
                current_stack_base().is_none(),
                "resume scope removed on parent stack"
            );
            crate::lisp::primitives::call(
                &mut interpreter,
                "garbage-collect",
                &[],
                &mut Env::new(),
            )
            .expect("collect while the actual child frame is suspended");
            assert_eq!(
                interpreter
                    .hash_table_runtime_entries(table_id)
                    .expect("live weak table")
                    .len(),
                1
            );
            if index == 0 {
                interpreter.set_global_binding("continuation-write", Value::Integer(29));
            }
        }
        let CoroutineResult::Return(completion) =
            continuation.resume(interpreter.state.take().expect("driver owns payload"))
        else {
            panic!("nested function must now return");
        };
        interpreter.state = Some(completion.state);
        assert_eq!(
            completion
                .result
                .expect("no Rust panic")
                .expect("Lisp result"),
            Value::list([Value::list([Value::Integer(11)]), Value::Integer(29)])
        );
        assert!(continuation.done());
        assert_eq!(std::ptr::from_ref(&*interpreter), payload);
        crate::lisp::primitives::call(&mut interpreter, "garbage-collect", &[], &mut Env::new())
            .expect("collect after the child frame and result are released");
        assert!(
            interpreter
                .hash_table_runtime_entries(table_id)
                .expect("live table")
                .is_empty()
        );
    }
}
