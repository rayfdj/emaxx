//! Scoped Lisp roots owned by live Rust execution frames.
//!
//! GNU marks every thread's bytecode stack and specpdl, not just the currently
//! evaluating environment. A pooled, inactive operand Vec is not a live stack.

use super::{
    Env, Interpreter, LabeledRestriction, LispError, LispReachability, SavedExcursion,
    SavedRestriction, SavedRestrictionBounds, SpecialBindingRestore, Value,
    WindowConfigurationSnapshot,
};
use std::cell::RefCell;
use std::rc::Rc;

/// Only the real GC marker is exposed to a root's trace implementation: tracing
/// must inspect values, never execute Lisp or switch execution contexts.
pub(crate) struct LispRootMarker<'a, 'mark, 'heap> {
    interpreter: &'a Interpreter,
    reachable: &'a mut LispReachability<'mark, 'heap>,
}

impl LispRootMarker<'_, '_, '_> {
    pub(crate) fn value(&mut self, value: &Value) {
        self.reachable.mark(self.interpreter, value);
    }

    pub(crate) fn environment(&mut self, environment: &Env) {
        self.reachable.mark_env(self.interpreter, environment);
    }
}

pub(crate) trait TraceLispRoots {
    fn trace_lisp_roots(&self, marker: &mut LispRootMarker<'_, '_, '_>);
}

pub(super) fn mark_source<T: TraceLispRoots>(
    interpreter: &Interpreter,
    reachable: &mut LispReachability,
    source: &T,
) {
    source.trace_lisp_roots(&mut LispRootMarker {
        interpreter,
        reachable,
    });
}

impl TraceLispRoots for Value {
    fn trace_lisp_roots(&self, marker: &mut LispRootMarker<'_, '_, '_>) {
        marker.value(self);
    }
}

impl TraceLispRoots for [Value] {
    fn trace_lisp_roots(&self, marker: &mut LispRootMarker<'_, '_, '_>) {
        for value in self {
            marker.value(value);
        }
    }
}

impl TraceLispRoots for Vec<Value> {
    fn trace_lisp_roots(&self, marker: &mut LispRootMarker<'_, '_, '_>) {
        self.as_slice().trace_lisp_roots(marker);
    }
}

impl TraceLispRoots for Env {
    fn trace_lisp_roots(&self, marker: &mut LispRootMarker<'_, '_, '_>) {
        marker.environment(self);
    }
}

impl TraceLispRoots for LispError {
    fn trace_lisp_roots(&self, marker: &mut LispRootMarker<'_, '_, '_>) {
        match self {
            Self::WrongTypeArgument(_, value) | Self::SignalValue(value) => {
                marker.value(value);
            }
            Self::Throw(tag, value) => {
                marker.value(tag);
                marker.value(value);
            }
            Self::TypeError(..)
            | Self::Void(_)
            | Self::VoidFunction(_)
            | Self::WrongNumberOfArgs(..)
            | Self::Signal(_)
            | Self::ErtTestFailed(_)
            | Self::Terminate(_)
            | Self::TestSkipped(_)
            | Self::EndOfInput
            | Self::ReadError(_) => {}
        }
    }
}

impl<T: TraceLispRoots, E: TraceLispRoots> TraceLispRoots for Result<T, E> {
    fn trace_lisp_roots(&self, marker: &mut LispRootMarker<'_, '_, '_>) {
        match self {
            Ok(value) => value.trace_lisp_roots(marker),
            Err(error) => error.trace_lisp_roots(marker),
        }
    }
}

impl<A: TraceLispRoots, B: TraceLispRoots> TraceLispRoots for (A, B) {
    fn trace_lisp_roots(&self, marker: &mut LispRootMarker<'_, '_, '_>) {
        self.0.trace_lisp_roots(marker);
        self.1.trace_lisp_roots(marker);
    }
}

impl TraceLispRoots for WindowConfigurationSnapshot {
    fn trace_lisp_roots(&self, marker: &mut LispRootMarker<'_, '_, '_>) {
        marker.value(&Value::Frame(self.frame_id));
        marker.value(&Value::Frame(self.selected_frame_id));
        if let Some(buffer) = marker
            .interpreter
            .buffer_identity_value(self.current_buffer_id)
        {
            marker.value(&buffer);
        }
        // The windows by id (C's configuration holds the objects): a
        // window the sweep freed has no entry and is skipped.
        for id in [self.selected_window_id, self.root_window_id] {
            if let Some(window) = marker.interpreter.record_ref(id) {
                marker.value(&Value::Record(window));
            }
        }
        self.selected_window_slots.trace_lisp_roots(marker);
        for (window, slots) in &self.window_records {
            if let Some(window) = marker.interpreter.record_ref(*window) {
                marker.value(&Value::Record(window));
            }
            slots.trace_lisp_roots(marker);
        }
    }
}

impl TraceLispRoots for SpecialBindingRestore {
    fn trace_lisp_roots(&self, marker: &mut LispRootMarker<'_, '_, '_>) {
        if let Some(value) = &self.previous {
            marker.value(value);
        }
        if let Some(state) = &self.previous_undo_state {
            state.visit_lisp_roots(&mut |value| marker.value(value));
        }
    }
}

impl TraceLispRoots for SavedExcursion {
    fn trace_lisp_roots(&self, marker: &mut LispRootMarker<'_, '_, '_>) {
        marker.value(&Value::Marker(self.marker_id));
    }
}

impl TraceLispRoots for LabeledRestriction {
    fn trace_lisp_roots(&self, marker: &mut LispRootMarker<'_, '_, '_>) {
        if let Some(value) = &self.label {
            marker.value(value);
        }
        marker.value(&Value::Marker(self.beg_marker_id));
        marker.value(&Value::Marker(self.end_marker_id));
    }
}

impl TraceLispRoots for SavedRestriction {
    fn trace_lisp_roots(&self, marker: &mut LispRootMarker<'_, '_, '_>) {
        if let SavedRestrictionBounds::Narrow {
            beginning_marker_id,
            end_marker_id,
            ..
        } = self.bounds
        {
            marker.value(&Value::Marker(beginning_marker_id));
            marker.value(&Value::Marker(end_marker_id));
        }
        for restriction in &self.labeled {
            restriction.trace_lisp_roots(marker);
        }
    }
}

impl<T: TraceLispRoots + ?Sized> TraceLispRoots for &T {
    fn trace_lisp_roots(&self, marker: &mut LispRootMarker<'_, '_, '_>) {
        (*self).trace_lisp_roots(marker);
    }
}

struct RootRange {
    address: *const (),
    trace: unsafe fn(*const (), &mut LispRootMarker<'_, '_, '_>),
}

#[derive(Default)]
struct RegisteredRoots {
    ranges: Vec<Option<RootRange>>,
    free: Vec<usize>,
}

#[derive(Default)]
pub(super) struct StackRoots {
    storage: Rc<RefCell<RegisteredRoots>>,
}

impl Clone for StackRoots {
    fn clone(&self) -> Self {
        assert!(
            self.storage.borrow().ranges.iter().all(Option::is_none),
            "cannot clone an interpreter with live execution roots"
        );
        Self::default()
    }
}

impl StackRoots {
    pub(super) fn mark(&self, interpreter: &Interpreter, reachable: &mut LispReachability) {
        let storage = self.storage.borrow();
        let mut marker = LispRootMarker {
            interpreter,
            reachable,
        };
        for range in storage.ranges.iter().flatten() {
            // SAFETY: only with_lisp_stack_roots registers these ranges. Its
            // private guard retains the source borrow throughout the callback,
            // removes the range before returning/unwinding, and is never exposed
            // for a caller to forget. Tracing cannot run Lisp or resume a stack.
            unsafe { (range.trace)(range.address, &mut marker) };
        }
    }
}

/// Kept private: a public borrow guard could be forgotten, leaving a dangling
/// GC range after the caller releases its source. The closure API below keeps
/// the guard on its own Rust frame until the registered borrow ends.
struct RootFrame<'a, T> {
    storage: Rc<RefCell<RegisteredRoots>>,
    slot: usize,
    _source: &'a T,
}

impl<T> Drop for RootFrame<'_, T> {
    fn drop(&mut self) {
        let mut storage = self.storage.borrow_mut();
        storage.ranges[self.slot] = None;
        storage.free.push(self.slot);
    }
}

impl Interpreter {
    pub(crate) fn with_lisp_stack_roots<T: TraceLispRoots, R>(
        &mut self,
        source: &T,
        body: impl FnOnce(&mut Self) -> R,
    ) -> R {
        unsafe fn trace<T: TraceLispRoots>(
            address: *const (),
            marker: &mut LispRootMarker<'_, '_, '_>,
        ) {
            // SAFETY: the private RootFrame owns the immutable &T borrow for
            // every instant this monomorphized trace function is registered.
            unsafe { &*address.cast::<T>() }.trace_lisp_roots(marker);
        }

        let storage = Rc::clone(&self.stack_roots.storage);
        let slot = {
            let mut registered = storage.borrow_mut();
            let range = RootRange {
                address: std::ptr::from_ref(source).cast(),
                trace: trace::<T>,
            };
            if let Some(slot) = registered.free.pop() {
                registered.ranges[slot] = Some(range);
                slot
            } else {
                registered.ranges.push(Some(range));
                registered.ranges.len() - 1
            }
        };
        let _frame = RootFrame {
            storage,
            slot,
            _source: source,
        };
        body(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lisp::types::Kind;
    use crate::lisp::{json, primitives};

    // A key held by a Rust local is reachable while its frame lives (the
    // conservative scan reads it as a C local): each scope below runs in
    // a frame of its own, and the stack under the test is cleared before
    // the collection that expects the key gone.  The helpers are frames
    // of their own too, so no temporary of theirs lands in the test's
    // frame, which the clearing cannot reach.
    #[inline(never)]
    fn insert_weak_key(interpreter: &mut Interpreter, table: &Value, key: &Value) {
        primitives::call(
            interpreter,
            "puthash",
            &[*key, Value::T, *table],
            &mut Env::new(),
        )
        .expect("insert weak key");
    }

    #[inline(never)]
    fn weak_entries(interpreter: &mut Interpreter, table: &Value) -> usize {
        let Kind::Record(id) = table.kind() else {
            panic!("hash table must be a record");
        };
        primitives::call(interpreter, "garbage-collect", &[], &mut Env::new())
            .expect("collect actual roots");
        interpreter
            .hash_table_runtime_entries(id.id)
            .expect("live weak table")
            .len()
    }

    #[inline(never)]
    fn weak_key_table(interpreter: &mut Interpreter, root: &str) -> Value {
        let table = json::make_hash_table(interpreter, "eq", Vec::new());
        let Kind::Record(id) = table.kind() else {
            panic!("hash table must be a record");
        };
        interpreter.find_record_mut(id).expect("weak table").slots[5] = Value::symbol("key");
        interpreter.set_global_binding(root, Value::Record(id));
        table
    }

    #[test]
    fn scoped_stack_roots_follow_the_owned_payload_between_shells() {
        #[inline(never)]
        fn scoped(interpreter: &mut Interpreter, table: &Value, payload: usize) {
            let key = Value::list([Value::Integer(4)]);
            insert_weak_key(interpreter, table, &key);
            interpreter.with_lisp_stack_roots(&key, |parked| {
                let mut active = Interpreter {
                    state: parked.state.take(),
                    continuations: super::super::continuations::ThreadContinuations::default(),
                };
                assert!(parked.state.is_none());
                assert_eq!(
                    std::ptr::from_ref(&*active) as usize,
                    payload,
                    "no editor clone"
                );
                assert_eq!(
                    weak_entries(&mut active, table),
                    1,
                    "the parked Rust scope remains a root"
                );
                let Kind::Record(id) = table.kind() else {
                    panic!("hash table must be a record");
                };
                let entries = active
                    .hash_table_runtime_entries(id.id)
                    .expect("live weak table");
                assert!(primitives::values_eq_in_env(
                    &active,
                    &entries[0].0,
                    &key,
                    &Env::new(),
                ));
                active.buffer.insert("shared editor state");
                active.set_global_binding("state-shell-write", Value::Integer(23));
                parked.state = active.state.take();
                assert!(active.state.is_none());
            });
        }
        let mut interpreter = Interpreter::new();
        let payload = std::ptr::from_ref(&*interpreter) as usize;
        let table = weak_key_table(&mut interpreter, "stack-root-table");
        scoped(&mut interpreter, &table, payload);
        crate::lisp::alloc::clobber_stack();
        assert_eq!(std::ptr::from_ref(&*interpreter) as usize, payload);
        assert_eq!(
            interpreter.buffer.buffer_size(),
            "shared editor state".len()
        );
        assert_eq!(
            interpreter.lookup_var("state-shell-write", &Env::new()),
            Some(Value::Integer(23)),
        );
        assert_eq!(
            weak_entries(&mut interpreter, &table),
            0,
            "the key is gone with the scope's frame"
        );
    }

    #[test]
    fn scoped_backtrace_roots_release_after_panic_and_retained_frames_own_arguments() {
        #[inline(never)]
        fn retained_frame(interpreter: &mut Interpreter, table: &Value) {
            let temporary_arguments = vec![Value::list([Value::Integer(1)])];
            insert_weak_key(interpreter, table, &temporary_arguments[0]);
            interpreter.push_backtrace_frame(Value::symbol("retained"), &temporary_arguments);
        }
        #[inline(never)]
        fn scoped_frame(interpreter: &mut Interpreter, table: &Value) {
            let arguments = [Value::list([Value::Integer(2)])];
            insert_weak_key(interpreter, table, &arguments[0]);
            interpreter.with_backtrace_frame(Value::symbol("scoped"), &arguments, |interpreter| {
                assert_eq!(
                    weak_entries(interpreter, table),
                    2,
                    "both argument vectors are rooted"
                );
                // A callee may leave an owned signaling frame behind.
                interpreter.push_backtrace_frame(Value::symbol("signal"), &[]);
                panic!("unwind the borrowed backtrace scope");
            });
        }
        let mut interpreter = Interpreter::new();
        let table = weak_key_table(&mut interpreter, "backtrace-root-table");
        retained_frame(&mut interpreter, &table);
        crate::lisp::alloc::clobber_stack();
        assert_eq!(
            weak_entries(&mut interpreter, &table),
            1,
            "retained arguments remain valid"
        );
        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            scoped_frame(&mut interpreter, &table);
        }));
        assert!(panic.is_err());
        assert_eq!(interpreter.backtrace_frames_len(), 1);
        crate::lisp::alloc::clobber_stack();
        assert_eq!(
            weak_entries(&mut interpreter, &table),
            1,
            "the inner arguments were released"
        );
        interpreter.pop_backtrace_frame();
        crate::lisp::alloc::clobber_stack();
        assert_eq!(
            weak_entries(&mut interpreter, &table),
            0,
            "all backtrace roots were released"
        );
    }

    #[test]
    fn scoped_stack_roots_survive_collection_and_release_after_unwind() {
        #[inline(never)]
        fn insert(interpreter: &mut Interpreter, table: &Value, index: i64) -> Value {
            let key = Value::list([Value::Integer(index)]);
            insert_weak_key(interpreter, table, &key);
            key
        }
        #[inline(never)]
        fn insert_dead(interpreter: &mut Interpreter, table: &Value) {
            let _ = insert(interpreter, table, 3);
        }
        #[inline(never)]
        fn inner_scope(interpreter: &mut Interpreter, table: &Value) {
            let inner = insert(interpreter, table, 2);
            interpreter.with_lisp_stack_roots(&inner, |interpreter| {
                assert_eq!(
                    weak_entries(interpreter, table),
                    2,
                    "unrelated dead key is not rooted"
                );
                panic!("unwind the inner root scope");
            });
        }
        #[inline(never)]
        fn outer_scope(interpreter: &mut Interpreter, table: &Value) {
            let outer = insert(interpreter, table, 1);
            insert_dead(interpreter, table);
            crate::lisp::alloc::clobber_stack();
            interpreter.with_lisp_stack_roots(&outer, |interpreter| {
                let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    inner_scope(interpreter, table);
                }));
                assert!(panic.is_err());
                crate::lisp::alloc::clobber_stack();
                assert_eq!(
                    weak_entries(interpreter, table),
                    1,
                    "only the outer scope remains live"
                );
            });
        }
        // The keys live in frames of their own (`outer_scope' and below),
        // cleared before the check: the test's own frame must hold no
        // word naming one, as a C local would keep its object.
        #[inline(never)]
        fn released(interpreter: &mut Interpreter, table: &Value) -> usize {
            crate::lisp::alloc::clobber_stack();
            weak_entries(interpreter, table)
        }
        let mut interpreter = Interpreter::new();
        let table = weak_key_table(&mut interpreter, "stack-root-table");
        outer_scope(&mut interpreter, &table);
        crate::lisp::alloc::clobber_stack();
        assert_eq!(
            released(&mut interpreter, &table),
            0,
            "all execution roots released"
        );
        assert!(
            interpreter
                .stack_roots
                .storage
                .borrow()
                .ranges
                .iter()
                .all(Option::is_none)
        );
    }
}
