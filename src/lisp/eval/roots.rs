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
pub(crate) struct LispRootMarker<'a> {
    interpreter: &'a Interpreter,
    reachable: &'a mut LispReachability,
}

impl LispRootMarker<'_> {
    pub(crate) fn value(&mut self, value: &Value) {
        self.reachable.mark(self.interpreter, value);
    }

    pub(crate) fn environment(&mut self, environment: &Env) {
        self.reachable.mark_env(self.interpreter, environment);
    }
}

pub(crate) trait TraceLispRoots {
    fn trace_lisp_roots(&self, marker: &mut LispRootMarker<'_>);
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
    fn trace_lisp_roots(&self, marker: &mut LispRootMarker<'_>) {
        marker.value(self);
    }
}

impl TraceLispRoots for [Value] {
    fn trace_lisp_roots(&self, marker: &mut LispRootMarker<'_>) {
        for value in self {
            marker.value(value);
        }
    }
}

impl TraceLispRoots for Vec<Value> {
    fn trace_lisp_roots(&self, marker: &mut LispRootMarker<'_>) {
        self.as_slice().trace_lisp_roots(marker);
    }
}

impl TraceLispRoots for Env {
    fn trace_lisp_roots(&self, marker: &mut LispRootMarker<'_>) {
        marker.environment(self);
    }
}

impl TraceLispRoots for LispError {
    fn trace_lisp_roots(&self, marker: &mut LispRootMarker<'_>) {
        match self {
            Self::WrongTypeArgument(_, value)
            | Self::SignalValue(value)
            | Self::VmReturn(value) => {
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
    fn trace_lisp_roots(&self, marker: &mut LispRootMarker<'_>) {
        match self {
            Ok(value) => value.trace_lisp_roots(marker),
            Err(error) => error.trace_lisp_roots(marker),
        }
    }
}

impl<A: TraceLispRoots, B: TraceLispRoots> TraceLispRoots for (A, B) {
    fn trace_lisp_roots(&self, marker: &mut LispRootMarker<'_>) {
        self.0.trace_lisp_roots(marker);
        self.1.trace_lisp_roots(marker);
    }
}

impl TraceLispRoots for WindowConfigurationSnapshot {
    fn trace_lisp_roots(&self, marker: &mut LispRootMarker<'_>) {
        marker.value(&Value::Record(self.selected_window_id));
        marker.value(&Value::Record(self.root_window_id));
        self.selected_window_slots.trace_lisp_roots(marker);
        for (window, slots) in &self.window_records {
            marker.value(&Value::Record(*window));
            slots.trace_lisp_roots(marker);
        }
    }
}

impl TraceLispRoots for SpecialBindingRestore {
    fn trace_lisp_roots(&self, marker: &mut LispRootMarker<'_>) {
        if let Some(value) = &self.previous {
            marker.value(value);
        }
        if let Some(state) = &self.previous_undo_state {
            state.visit_lisp_roots(&mut |value| marker.value(value));
        }
    }
}

impl TraceLispRoots for SavedExcursion {
    fn trace_lisp_roots(&self, marker: &mut LispRootMarker<'_>) {
        marker.value(&Value::Marker(self.marker_id));
    }
}

impl TraceLispRoots for LabeledRestriction {
    fn trace_lisp_roots(&self, marker: &mut LispRootMarker<'_>) {
        if let Some(value) = &self.label {
            marker.value(value);
        }
        marker.value(&Value::Marker(self.beg_marker_id));
        marker.value(&Value::Marker(self.end_marker_id));
    }
}

impl TraceLispRoots for SavedRestriction {
    fn trace_lisp_roots(&self, marker: &mut LispRootMarker<'_>) {
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
    fn trace_lisp_roots(&self, marker: &mut LispRootMarker<'_>) {
        (*self).trace_lisp_roots(marker);
    }
}

struct RootRange {
    address: *const (),
    trace: unsafe fn(*const (), &mut LispRootMarker<'_>),
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
        unsafe fn trace<T: TraceLispRoots>(address: *const (), marker: &mut LispRootMarker<'_>) {
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
    use crate::lisp::{json, primitives};

    #[test]
    fn scoped_stack_roots_follow_the_owned_payload_between_shells() {
        let mut interpreter = Interpreter::new();
        let payload = std::ptr::from_ref(&*interpreter);
        let table = json::make_hash_table(&mut interpreter, "eq", Vec::new());
        let Value::Record(id) = table else {
            panic!("hash table must be a record");
        };
        interpreter.find_record_mut(id).expect("weak table").slots[5] = Value::symbol("key");
        interpreter.set_global_binding("stack-root-table", Value::Record(id));
        let key = Value::list([Value::Integer(4)]);
        primitives::call(
            &mut interpreter,
            "puthash",
            &[key.clone(), Value::T, Value::Record(id)],
            &mut Env::new(),
        )
        .expect("insert weak key");

        interpreter.with_lisp_stack_roots(&key, |parked| {
            let mut active = Interpreter {
                state: parked.state.take(),
                continuations: super::super::continuations::ThreadContinuations::default(),
            };
            assert!(parked.state.is_none());
            assert_eq!(std::ptr::from_ref(&*active), payload, "no editor clone");
            primitives::call(&mut active, "garbage-collect", &[], &mut Env::new())
                .expect("collect from the other shell");
            let entries = active
                .hash_table_runtime_entries(id)
                .expect("live weak table");
            assert_eq!(entries.len(), 1, "the parked Rust scope remains a root");
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

        assert_eq!(std::ptr::from_ref(&*interpreter), payload);
        assert_eq!(
            interpreter.buffer.buffer_size(),
            "shared editor state".len()
        );
        assert_eq!(
            interpreter.lookup_var("state-shell-write", &Env::new()),
            Some(Value::Integer(23)),
        );
        primitives::call(&mut interpreter, "garbage-collect", &[], &mut Env::new())
            .expect("collect after the original scope returns");
        assert!(
            interpreter
                .hash_table_runtime_entries(id)
                .expect("weak table remains globally rooted")
                .is_empty()
        );
    }

    #[test]
    fn scoped_stack_roots_survive_collection_and_release_after_unwind() {
        let mut interpreter = Interpreter::new();
        let mut environment = Env::new();
        let table = json::make_hash_table(&mut interpreter, "eq", Vec::new());
        let Value::Record(id) = table else {
            panic!("hash table must be a record");
        };
        interpreter.find_record_mut(id).expect("weak table").slots[5] = Value::symbol("key");
        interpreter.set_global_binding("stack-root-table", Value::Record(id));
        let outer = Value::list([Value::Integer(1)]);
        let inner = Value::list([Value::Integer(2)]);
        let dead = Value::list([Value::Integer(3)]);
        for key in [&outer, &inner, &dead] {
            primitives::call(
                &mut interpreter,
                "puthash",
                &[key.clone(), Value::T, Value::Record(id)],
                &mut environment,
            )
            .expect("insert weak key");
        }
        let collect = |interpreter: &mut Interpreter| {
            primitives::call(interpreter, "garbage-collect", &[], &mut Env::new())
                .expect("collect actual roots");
            interpreter
                .hash_table_runtime_entries(id)
                .expect("weak table remains live")
                .len()
        };
        interpreter.with_lisp_stack_roots(&outer, |interpreter| {
            let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                interpreter.with_lisp_stack_roots(&inner, |interpreter| {
                    assert_eq!(collect(interpreter), 2, "unrelated dead key is not rooted");
                    panic!("unwind the inner root scope");
                });
            }));
            assert!(panic.is_err());
            assert_eq!(collect(interpreter), 1, "only the outer scope remains live");
        });
        assert_eq!(collect(&mut interpreter), 0, "all execution roots released");
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
