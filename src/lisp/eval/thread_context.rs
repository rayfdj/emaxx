//! Execution state belonging to one GNU Lisp thread, not the shared editor.
//! Ordinary unwind actions do not run during a switch (eval.c:specpdl_unrewind
//! with vars_only); only the thread's dynamic variable bindings are exchanged.

use super::roots::{LispRootMarker, TraceLispRoots};
use super::*;

#[derive(Clone, Debug, Default)]
pub(super) struct ThreadExecutionContext {
    dlet_active_names: HashMap<String, u32>,
    special_scan_floor: usize,
    lisp_eval_depth: usize,
    current_load_file: Option<String>,
    last_match_data: Option<Vec<Option<(usize, usize)>>>,
    last_match_data_buffer_id: Option<u64>,
    current_activation_id: u64,
    interactive_call_depth: usize,
    lambda_capture_overrides: Vec<bool>,
    pub(super) active_special_restores: Vec<SpecialBindingRestore>,
    pub(super) backtrace_frames: Vec<BacktraceFrame>,
    batch_error_backtrace: Option<BatchErrorBacktrace>,
    active_handlers: Vec<ActiveHandler>,
    active_catch_tags: Vec<Value>,
    handler_dispatch_depth: usize,
    suspend_condition_case_count: usize,
}

impl ThreadExecutionContext {
    fn exchange(&mut self, interpreter: &mut Interpreter) {
        macro_rules! exchange {
            ($($field:ident),* $(,)?) => {
                $(std::mem::swap(&mut self.$field, &mut interpreter.$field);)*
            };
        }
        exchange!(
            dlet_active_names,
            special_scan_floor,
            lisp_eval_depth,
            current_load_file,
            last_match_data,
            last_match_data_buffer_id,
            current_activation_id,
            interactive_call_depth,
            lambda_capture_overrides,
            active_special_restores,
            backtrace_frames,
            batch_error_backtrace,
            active_handlers,
            active_catch_tags,
            handler_dispatch_depth,
            suspend_condition_case_count
        );
    }
}

impl TraceLispRoots for ThreadExecutionContext {
    fn trace_lisp_roots(&self, marker: &mut LispRootMarker<'_>) {
        for restore in &self.active_special_restores {
            restore.trace_lisp_roots(marker);
        }
        for frame in &self.backtrace_frames {
            marker.value(&frame.function);
            frame.args.trace_lisp_roots(marker);
            if let Some(form) = &frame.source_form {
                marker.value(form);
            }
            for (_, value) in &frame.locals {
                marker.value(value);
            }
            if let Some(environment) = &frame.lexical_context {
                marker.environment(environment);
            }
        }
        if let Some(snapshot) = &self.batch_error_backtrace {
            for (_, function, arguments, _) in &snapshot.frames {
                marker.value(function);
                arguments.trace_lisp_roots(marker);
            }
        }
        for handler in &self.active_handlers {
            match handler {
                ActiveHandler::Bind(_, value) => marker.value(value),
                ActiveHandler::Case(values) => values.trace_lisp_roots(marker),
            }
        }
        self.active_catch_tags.trace_lisp_roots(marker);
    }
}

impl Interpreter {
    pub(super) fn materialize_native_backtrace_arguments(&mut self) -> Result<(), LispError> {
        for index in 0..self.backtrace_frames.len() {
            let Some(words) = self.backtrace_frames[index].native_args.as_ref() else {
                continue;
            };
            let arguments = crate::lisp::native_comp::decode_active_backtrace_arguments(words)
                .ok_or_else(|| LispError::Signal("Native backtrace has no active runtime".into()))?
                .map_err(LispError::Signal)?;
            let frame = &mut self.backtrace_frames[index];
            frame.args = arguments;
            frame.native_args = None;
        }
        Ok(())
    }

    pub(super) fn park_thread_context(&mut self) {
        let thread_id = self.active_thread_id;
        let buffer_id = self.current_buffer_id();
        self.swap_special_bindings_for_thread_switch(0, false);
        let mut context = Box::<ThreadExecutionContext>::default();
        context.exchange(self);
        let thread = self
            .find_thread_state_mut(thread_id)
            .expect("current thread");
        assert!(
            thread.context.is_none(),
            "running thread owns the active context"
        );
        thread.buffer_id = buffer_id;
        thread.context = Some(context);
    }

    pub(super) fn activate_thread_context(&mut self, thread_id: u64) -> Result<(), LispError> {
        let thread = self
            .find_thread_state_mut(thread_id)
            .expect("scheduled thread");
        let buffer_id = thread.buffer_id;
        let mut context = thread.context.take().expect("parked execution context");
        self.active_thread_id = thread_id;
        // Emaxx moves the selected Buffer into a direct slot. Select it
        // before rebinding buffer-local variables or the saved undo state.
        if self.has_buffer_id(buffer_id) {
            self.set_current_buffer_id(buffer_id)?;
        }
        context.exchange(self);
        self.swap_special_bindings_for_thread_switch(0, true);
        Ok(())
    }
}

impl Drop for Interpreter {
    fn drop(&mut self) {
        if self.state.is_none() || self.continuations.threads.is_empty() {
            return;
        }
        if self.pending_termination.is_none() {
            self.request_termination(EmacsTermination {
                exit_code: 0,
                restart: false,
            });
        }
        while let Some(thread_id) = self.continuations.threads.keys().next().copied() {
            match self.step_thread(thread_id, &mut Env::new()) {
                Ok(()) | Err(LispError::Terminate(_)) => {}
                Err(error) => panic!("Unable to unwind suspended Lisp thread: {error}"),
            }
        }
    }
}
