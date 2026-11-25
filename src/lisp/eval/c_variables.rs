//! globals.h's `struct emacs_globals', the part of it the evaluator reads:
//! the storage of the C variables eval.c, lread.c, fns.c, keyboard.c,
//! undo.c, buffer.c and syntax.c define with DEFVAR_LISP, DEFVAR_INT and
//! DEFVAR_BOOL, each the place its symbol forwards to (lisp.h's
//! Lisp_Objfwd, Lisp_Intfwd and Lisp_Boolfwd, `SymbolCell::forward_to').
//! symbols_with_pos_enabled is the symbol table's, which lisp.h's EQ reads
//! (`types::symbols'), and attempt_orderly_shutdown_on_fatal_signal the
//! fault handler's (primitives/interactive.rs).
//!
//! The variables are an editor's, as its symbols are.  They live in an
//! allocation of their own that the editor's state holds, not in the state
//! itself, so that a symbol's forward reads and writes them through a
//! shared reference while the state is borrowed; a copy of the state has
//! copies of them (`Clone').

use super::super::types::Value;
use super::terminal::Kboard;
use std::cell::{Cell, RefCell};
use std::ptr::NonNull;
use std::rc::Rc;

pub(crate) struct CVariableCells {
    /// eval.c's Vquit_flag.
    pub(crate) quit_flag: Cell<Value>,
    /// eval.c's Vinhibit_quit.
    pub(crate) inhibit_quit: Cell<Value>,
    /// keyboard.c's Vthrow_on_input.
    pub(crate) throw_on_input: Cell<Value>,
    /// fns.c's Voverriding_plist_environment.
    pub(crate) overriding_plist_environment: Cell<Value>,
    /// lread.c's Vload_path.
    pub(crate) load_path: Cell<Value>,
    /// eval.c's DEFVAR_INT max_lisp_eval_depth.
    pub(crate) max_lisp_eval_depth: Cell<i64>,
    /// eval.c's DEFVAR_BOOL debug_on_next_call.
    pub(crate) debug_on_next_call: Cell<bool>,
    /// undo.c's DEFVAR_BOOL undo_inhibit_record_point, which the buffers
    /// hold as well (`Interpreter::set_undo_inhibit_record_point').
    pub(crate) undo_inhibit_record_point: Cell<bool>,
    /// buffer.c's Vcase_fold_search and syntax.c's DEFVAR_BOOL
    /// parse_sexp_lookup_properties, both made buffer-local when set: the
    /// current buffer's values, which set_buffer_internal_2 loads, as the
    /// matcher and the scanners read them.
    pub(crate) case_fold_search: Cell<Value>,
    pub(crate) parse_sexp_lookup_properties: Cell<bool>,
    /// search.c's Vinhibit_changing_match_data, which looking_at_1,
    /// string_match_1 and search_buffer read before they match.
    pub(crate) inhibit_changing_match_data: Cell<Value>,
    /// keyboard.c's current_kboard, which the KBOARD variables' symbols
    /// forward to (Lisp_Kboard_Objfwd), and the kboard it names, which
    /// its terminal shares; `set_current_kboard' sets both.
    current_kboard: Cell<NonNull<Kboard>>,
    current_kboard_owner: RefCell<Rc<Kboard>>,
}

/// An editor's `CVariableCells'.
pub(crate) struct CVariables(Rc<CVariableCells>);

impl CVariables {
    /// The values the C initializers give: syms_of_eval's
    /// max_lisp_eval_depth of 1600, syms_of_buffer's Vcase_fold_search of
    /// Qt, nil and false for the others; KBOARD, the initial terminal's,
    /// current.
    pub(crate) fn new(kboard: Rc<Kboard>) -> Self {
        Self(Rc::new(CVariableCells {
            quit_flag: Cell::new(Value::Nil),
            inhibit_quit: Cell::new(Value::Nil),
            throw_on_input: Cell::new(Value::Nil),
            overriding_plist_environment: Cell::new(Value::Nil),
            load_path: Cell::new(Value::Nil),
            max_lisp_eval_depth: Cell::new(1600),
            debug_on_next_call: Cell::new(false),
            undo_inhibit_record_point: Cell::new(false),
            case_fold_search: Cell::new(Value::T),
            parse_sexp_lookup_properties: Cell::new(false),
            inhibit_changing_match_data: Cell::new(Value::Nil),
            current_kboard: Cell::new(NonNull::from(&*kboard)),
            current_kboard_owner: RefCell::new(kboard),
        }))
    }

    /// current_kboard = KBOARD.
    pub(crate) fn set_current_kboard(&self, kboard: Rc<Kboard>) {
        self.current_kboard.set(NonNull::from(&*kboard));
        *self.current_kboard_owner.borrow_mut() = kboard;
    }

    /// current_kboard.
    pub(crate) fn current_kboard(&self) -> &Kboard {
        // SAFETY: the pointer is the owner's, which lives while it is.
        unsafe { self.current_kboard.get().as_ref() }
    }

    /// Whether KBOARD is current_kboard.
    pub(crate) fn is_current_kboard(&self, kboard: &Rc<Kboard>) -> bool {
        std::ptr::eq(self.current_kboard.get().as_ptr(), Rc::as_ptr(kboard))
    }

    /// The word holding current_kboard, which a KBOARD variable's forward
    /// names.
    pub(crate) fn current_kboard_cell(&self) -> &Cell<NonNull<Kboard>> {
        &self.current_kboard
    }

    /// Whether ADDRESS is inside these variables' storage: a forward made
    /// to one of them.
    pub(crate) fn holds(&self, address: usize) -> bool {
        let start = Rc::as_ptr(&self.0) as usize;
        (start..start + std::mem::size_of::<CVariableCells>()).contains(&address)
    }

    /// The Lisp objects the variables hold, for the mark phase: GNU's
    /// staticpro of each DEFVAR_LISP's C variable (lread.c:defvar_lisp).
    pub(crate) fn objects(&self) -> [Value; 7] {
        [
            self.quit_flag.get(),
            self.inhibit_quit.get(),
            self.throw_on_input.get(),
            self.overriding_plist_environment.get(),
            self.load_path.get(),
            self.case_fold_search.get(),
            self.inhibit_changing_match_data.get(),
        ]
    }
}

impl std::ops::Deref for CVariables {
    type Target = CVariableCells;

    #[inline(always)]
    fn deref(&self) -> &CVariableCells {
        &self.0
    }
}

/// A copy of an editor's state has variables of its own, holding the same
/// values until the copier replaces the objects with its copies.
impl Clone for CVariables {
    fn clone(&self) -> Self {
        Self(Rc::new(CVariableCells {
            quit_flag: self.quit_flag.clone(),
            inhibit_quit: self.inhibit_quit.clone(),
            throw_on_input: self.throw_on_input.clone(),
            overriding_plist_environment: self.overriding_plist_environment.clone(),
            load_path: self.load_path.clone(),
            max_lisp_eval_depth: self.max_lisp_eval_depth.clone(),
            debug_on_next_call: self.debug_on_next_call.clone(),
            undo_inhibit_record_point: self.undo_inhibit_record_point.clone(),
            case_fold_search: self.case_fold_search.clone(),
            parse_sexp_lookup_properties: self.parse_sexp_lookup_properties.clone(),
            inhibit_changing_match_data: self.inhibit_changing_match_data.clone(),
            current_kboard: self.current_kboard.clone(),
            current_kboard_owner: RefCell::new(Rc::clone(&self.current_kboard_owner.borrow())),
        }))
    }
}
