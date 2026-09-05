//! GNU 30.2 alloc.c's finalizer allocation, pre-sweep, and callback phases.

use super::{
    Env, GcMarkSet, Interpreter, LispError, LispReachability, Value, error_condition_value,
};

#[derive(Clone)]
pub(super) struct FinalizerState {
    pub(super) function: Value,
    queued: bool,
}

impl Interpreter {
    pub(crate) fn make_finalizer(&mut self, function: Value) -> Value {
        let id = self.next_finalizer_id;
        self.next_finalizer_id += 1;
        self.finalizers.insert(
            id,
            FinalizerState {
                function,
                queued: false,
            },
        );
        Value::Finalizer(id)
    }

    pub(super) fn mark_doomed_finalizers(&self, marked: &mut LispReachability) -> Vec<u64> {
        let doomed = self
            .finalizers
            .iter()
            .filter_map(|(&id, finalizer)| {
                (!finalizer.queued
                    && !finalizer.function.is_nil()
                    && !marked.finalizers.contains(&id))
                .then_some(id)
            })
            .collect::<Vec<_>>();
        // Existing queued callbacks survive nested GC too. Selection above
        // precedes this marking, just as queue_doomed_finalizers precedes
        // mark_finalizer_list; the allocation order is preserved.
        for id in self.doomed_finalizers.iter().chain(&doomed) {
            marked.mark(self, &Value::Finalizer(*id));
        }
        doomed
    }

    pub(crate) fn install_gc_finalizers(&mut self, live: GcMarkSet<u64>, doomed: Vec<u64>) {
        for id in doomed {
            self.finalizers
                .get_mut(&id)
                .expect("marked finalizer is allocated")
                .queued = true;
            self.doomed_finalizers.push_back(id);
        }
        // cleanup_vector unchains unreachable, already-run finalizers.
        self.finalizers.retain(|id, _| live.contains(id));
        let mut scratch = live;
        scratch.clear();
        self.gc_reachability_scratch.borrow_mut().finalizers = scratch;
    }

    pub(crate) fn run_finalizers(&mut self, env: &mut Env) -> Result<(), LispError> {
        while let Some(id) = self.doomed_finalizers.pop_front() {
            let finalizer = self
                .finalizers
                .get_mut(&id)
                .expect("queued finalizer survived sweep");
            finalizer.queued = false;
            // Clear before calling: resurrection and recursive GC cannot run
            // this finalizer twice (alloc.c:run_finalizers).
            let function = std::mem::replace(&mut finalizer.function, Value::Nil);
            if function.is_nil() {
                continue;
            }
            self.number_finalizers_run = self.number_finalizers_run.wrapping_add(1);
            self.with_lisp_stack_roots(&function, |interp| {
                let restore = interp.bind_special_dynamic("inhibit-quit", Value::T, env)?;
                let handlers = interp.push_condition_case_handler(vec![Value::T]);
                let depth = env.len();
                let result = interp.call_function_value(function.clone(), None, &[], env);
                interp.pop_handler_bindings(handlers);
                env.truncate(depth);
                let result = match result {
                    Ok(_) => Ok(()),
                    Err(error) => {
                        if interp.take_condition_case_suspend()
                            || matches!(
                                error,
                                LispError::Throw(_, _)
                                    | LispError::VmReturn(_)
                                    | LispError::Terminate(_)
                            )
                        {
                            Err(error)
                        } else {
                            interp.clear_batch_error_backtrace();
                            // run_finalizer_handler uses add_to_log, NOT
                            // the overridable Lisp `message' function or
                            // echo-area hooks.
                            crate::lisp::primitives::add_to_log(
                                interp,
                                "finalizer failed: %S",
                                &[error_condition_value(&error)],
                                env,
                            )
                        }
                    }
                };
                interp.restore_special_dynamic(restore, env)?;
                result
            })?;
        }
        Ok(())
    }
}
