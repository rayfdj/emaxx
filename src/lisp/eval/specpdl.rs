//! eval.c's specpdl: the thread's stack of its dynamic bindings and of the
//! unwind records byte code and native code make, one stack as GNU keeps
//! it, undone innermost first by unbind_to.  The interpreter's own unwind
//! forms are its Rust frames.

use super::roots::{LispRootMarker, TraceLispRoots};
use super::*;

// A binding's record leaves the stack by the length alone where its
// unbind is a plain store (`Interpreter::unbind_plain_innermost'): it owns
// nothing to drop.
const _: () = assert!(!std::mem::needs_drop::<SpecialBindingRestore>());

/// eval.c's `union specbinding': a dynamic binding's record or an unwind
/// record.
#[derive(Clone, Debug)]
pub(crate) enum SpecpdlEntry {
    /// SPECPDL_LET and its subkinds SPECPDL_LET_LOCAL and
    /// SPECPDL_LET_DEFAULT.
    Let(SpecialBindingRestore),
    /// SPECPDL_UNWIND and its variants, from byte code and native code.
    Unwind(UnwindRecord),
}

impl SpecpdlEntry {
    /// The binding's record, if this is one.
    #[inline(always)]
    pub(crate) fn as_let(&self) -> Option<&SpecialBindingRestore> {
        match self {
            Self::Let(restore) => Some(restore),
            Self::Unwind(_) => None,
        }
    }

    #[inline(always)]
    pub(crate) fn as_let_mut(&mut self) -> Option<&mut SpecialBindingRestore> {
        match self {
            Self::Let(restore) => Some(restore),
            Self::Unwind(_) => None,
        }
    }

    /// The number of backtrace frames when the entry was made: on GNU's
    /// one stack, the records below it.
    #[inline]
    pub(crate) fn backtrace_depth(&self) -> usize {
        match self {
            Self::Let(restore) => restore.frame_depth,
            Self::Unwind(record) => record.backtrace_depth,
        }
    }
}

/// A SPECPDL_UNWIND entry: what to undo and the depths of the backtrace
/// and of evaluation when it was recorded.  The younger activations are
/// retired before the action runs, as the backtrace records above the
/// entry are popped before it on GNU's one stack.
#[derive(Clone, Debug)]
pub(crate) struct UnwindRecord {
    action: UnwindAction,
    backtrace_depth: usize,
    lisp_eval_depth: usize,
}

/// What an unwind record undoes, as bytecode.c and comp.c's helpers record
/// it.
#[derive(Clone, Debug)]
pub(crate) enum UnwindAction {
    /// record_unwind_protect_excursion.
    Excursion(SavedExcursion),
    /// Fsave_restriction's record of save_restriction_save.
    Restriction(Box<SavedRestriction>),
    /// record_unwind_current_buffer.
    CurrentBuffer(u64),
    /// record_unwind_protect of an unwind-protect's handler: a function
    /// called with no arguments, or a list of forms evaluated.
    Cleanup { function: bool, value: Value },
}

impl UnwindAction {
    /// The Lisp objects the action holds, each passed to MARK.
    fn mark_values(&self, mark: &mut impl FnMut(&Value)) {
        match self {
            Self::Excursion(saved) => mark(&Value::Marker(saved.marker_id)),
            Self::Restriction(saved) => saved.mark_values(mark),
            Self::CurrentBuffer(_) => {}
            Self::Cleanup { value, .. } => mark(value),
        }
    }
}

impl SavedRestriction {
    /// The markers and labels the record holds, each passed to MARK.
    fn mark_values(&self, mark: &mut impl FnMut(&Value)) {
        if let SavedRestrictionBounds::Narrow {
            beginning_marker_id,
            end_marker_id,
            ..
        } = self.bounds
        {
            mark(&Value::Marker(beginning_marker_id));
            mark(&Value::Marker(end_marker_id));
        }
        for restriction in &self.labeled {
            if let Some(value) = &restriction.label {
                mark(value);
            }
            mark(&Value::Marker(restriction.beg_marker_id));
            mark(&Value::Marker(restriction.end_marker_id));
        }
    }
}

impl SpecpdlEntry {
    /// eval.c:mark_specpdl of the entry: the value a binding saved, the
    /// objects an unwind record holds.
    pub(crate) fn mark_values(&self, mark: &mut impl FnMut(&Value)) {
        match self {
            Self::Let(restore) => {
                if let Some(value) = &restore.previous {
                    mark(value);
                }
            }
            Self::Unwind(record) => record.action.mark_values(mark),
        }
    }
}

impl TraceLispRoots for SpecpdlEntry {
    fn trace_lisp_roots(&self, marker: &mut LispRootMarker<'_>) {
        self.mark_values(&mut |value| marker.value(value));
    }
}

impl InterpreterState {
    /// eval.c's SPECPDL_INDEX: the depth of the specpdl, for `unbind_to'.
    #[inline(always)]
    pub(crate) fn specpdl_index(&self) -> usize {
        self.specpdl.len()
    }
}

impl Interpreter {
    /// The binding records on the specpdl, oldest first.
    #[inline]
    pub(super) fn let_records(&self) -> impl DoubleEndedIterator<Item = &SpecialBindingRestore> {
        self.specpdl.iter().filter_map(SpecpdlEntry::as_let)
    }

    #[inline]
    pub(super) fn let_records_mut(
        &mut self,
    ) -> impl DoubleEndedIterator<Item = &mut SpecialBindingRestore> {
        self.specpdl.iter_mut().filter_map(SpecpdlEntry::as_let_mut)
    }

    /// record_unwind_protect and its kin: ACTION on the specpdl, to be
    /// undone at the depths in effect now.
    pub(crate) fn record_unwind(&mut self, action: UnwindAction) {
        let record = UnwindRecord {
            action,
            backtrace_depth: self.backtrace_frames.len(),
            lisp_eval_depth: self.lisp_eval_depth,
        };
        self.specpdl.push(SpecpdlEntry::Unwind(record));
    }

    /// unbind_to's do_one_unbind of the innermost entry when it is a
    /// binding whose unbind is a plain store (`is_plain_restore'), which
    /// notifies no watcher and runs nothing: true when done, false with
    /// nothing done otherwise.
    #[inline(always)]
    pub(crate) fn unbind_plain_innermost(&mut self) -> bool {
        let Some(SpecpdlEntry::Let(restore)) = self.specpdl.last() else {
            return false;
        };
        if !self.is_plain_restore(restore) {
            return false;
        }
        let (name, previous) = (restore.name, restore.previous);
        let depth = self.specpdl.len() - 1;
        // SAFETY: the entry is a binding's record, which owns nothing to
        // drop (asserted above); the length moves back over it as
        // specpdl_ptr does.
        unsafe { self.specpdl.set_len(depth) };
        self.store_plain_value(&name, previous);
        true
    }

    /// `unbind_plain_innermost', or the undo of the innermost entry when
    /// it is a saved current buffer's (record_unwind_current_buffer),
    /// set_buffer_if_live, which runs nothing either: true when done.
    #[inline(always)]
    pub(crate) fn unbind_quiet_innermost(&mut self) -> bool {
        if self.unbind_plain_innermost() {
            return true;
        }
        let Some(SpecpdlEntry::Unwind(UnwindRecord {
            action: UnwindAction::CurrentBuffer(buffer_id),
            ..
        })) = self.specpdl.last()
        else {
            return false;
        };
        let buffer_id = *buffer_id;
        self.specpdl.pop();
        if self.has_buffer_id(buffer_id) {
            let _ = self.set_current_buffer_id(buffer_id);
        }
        true
    }

    /// Whether undoing each of the innermost COUNT entries runs nothing
    /// (`unbind_quiet_innermost').
    pub(crate) fn innermost_entries_quiet(&self, count: usize) -> bool {
        let Some(start) = self.specpdl.len().checked_sub(count) else {
            return false;
        };
        self.specpdl[start..].iter().all(|entry| match entry {
            SpecpdlEntry::Let(restore) => self.is_plain_restore(restore),
            SpecpdlEntry::Unwind(record) => {
                matches!(record.action, UnwindAction::CurrentBuffer(_))
            }
        })
    }

    /// The unwind record of a cleanup of VALUE taken off the stack
    /// wherever it lies, for a test whose native stacks share one specpdl
    /// and return out of order: true if there was one.
    #[cfg(test)]
    pub(crate) fn remove_cleanup_record(&mut self, value: Value) -> bool {
        let Some(index) = self.specpdl.iter().rposition(|entry| {
            matches!(entry, SpecpdlEntry::Unwind(UnwindRecord {
                action: UnwindAction::Cleanup { value: held, .. },
                ..
            }) if held.word() == value.word())
        }) else {
            return false;
        };
        self.specpdl.remove(index);
        true
    }

    /// The innermost entry taken off the stack, for unbind_to's
    /// `undo_specpdl_entry'.
    #[inline]
    pub(crate) fn pop_specpdl_entry(&mut self) -> Option<SpecpdlEntry> {
        self.specpdl.pop()
    }

    /// do_one_unbind of ENTRY, taken off the stack.  An unwind record's
    /// action runs with the backtrace frames recorded since retired and
    /// at the evaluation depth it was made at.
    pub(crate) fn undo_specpdl_entry(
        &mut self,
        entry: SpecpdlEntry,
        env: &mut Env,
    ) -> Result<(), LispError> {
        match entry {
            SpecpdlEntry::Let(restore) => self.apply_special_restore(restore, env),
            SpecpdlEntry::Unwind(record) => {
                self.truncate_backtrace_frames(record.backtrace_depth);
                self.lisp_eval_depth = record.lisp_eval_depth;
                self.run_unwind_action(record.action, env)
            }
        }
    }

    fn run_unwind_action(&mut self, action: UnwindAction, env: &mut Env) -> Result<(), LispError> {
        match action {
            UnwindAction::Excursion(saved) => {
                self.restore_excursion_state(saved);
                Ok(())
            }
            UnwindAction::Restriction(saved) => {
                self.restore_restriction_state(*saved);
                Ok(())
            }
            UnwindAction::CurrentBuffer(buffer_id) => {
                if self.has_buffer_id(buffer_id) {
                    self.set_current_buffer_id(buffer_id)?;
                }
                Ok(())
            }
            // bcall0 of a function handler; prog_ignore, Fprogn's walk
            // while the forms are a cons, of the obsolete list of forms.
            UnwindAction::Cleanup { function, value } => {
                self.with_lisp_stack_roots(&value, |interp| {
                    if function {
                        return interp
                            .call_function_value(value, None, &[], env)
                            .map(|_| ());
                    }
                    let mut forms = value;
                    while let Some(cell) = forms.cons_ref() {
                        interp.eval(&cell.car.get(), env)?;
                        forms = cell.cdr.get();
                    }
                    Ok(())
                })
            }
        }
    }
}
