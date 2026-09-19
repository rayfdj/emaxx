use super::*;
use crate::lisp::types::SharedCons;
use crate::lisp::types::SymbolName;

fn byte_code_function_uses_dynamic_binding(record: &RecordState) -> bool {
    matches!(record.slots.get(2), Some(Value::Symbol(symbol)) if symbol == "dynamic-binding")
}

// ── Dev-only flat profiler (EMAXX_PROFILE=<path>) ──
// Per-name call counts, cumulative and self wall time; the report file is
// rewritten every few thousand calls.  Zero cost unless the variable is
// set (one cached Option check per call).

fn profile_path() -> Option<&'static str> {
    static PATH: std::sync::OnceLock<Option<String>> = std::sync::OnceLock::new();
    PATH.get_or_init(|| std::env::var("EMAXX_PROFILE").ok())
        .as_deref()
}

struct ProfileEntry {
    count: u64,
    total: std::time::Duration,
    self_time: std::time::Duration,
}

thread_local! {
    static PROFILE_CHILD_STACK: std::cell::RefCell<Vec<std::time::Duration>> =
        const { std::cell::RefCell::new(Vec::new()) };
    static PROFILE_TABLE: std::cell::RefCell<std::collections::HashMap<String, ProfileEntry>> =
        std::cell::RefCell::new(std::collections::HashMap::new());
    static PROFILE_DUMP_COUNTDOWN: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
}

/// The Lisp identity of a named call when the caller still has it.
///
/// Source evaluation starts from an interned `SymbolName`; preserving that
/// handle through dispatch avoids allocating and re-interning the same name
/// merely to expose it in a backtrace.  Native callers that only have text
/// can retain the existing API without manufacturing a symbol eagerly.
#[derive(Clone, Copy, Debug)]
enum CallName<'a> {
    Symbol(&'a SymbolName),
    Text(&'a str),
}

impl<'a> CallName<'a> {
    fn as_str(self) -> &'a str {
        match self {
            Self::Symbol(name) => name.as_str(),
            Self::Text(name) => name,
        }
    }

    fn symbol_value(self, resolved_name: &SymbolName) -> Value {
        match self {
            Self::Symbol(name) => Value::Symbol(*name),
            Self::Text(name) if name == resolved_name.as_str() => Value::Symbol(*resolved_name),
            Self::Text(name) => Value::Symbol(name.into()),
        }
    }

    fn original_symbol_value(self) -> Value {
        match self {
            Self::Symbol(name) => Value::Symbol(*name),
            Self::Text(name) => Value::Symbol(name.into()),
        }
    }
}

fn profile_enter() {
    PROFILE_CHILD_STACK.with(|stack| stack.borrow_mut().push(std::time::Duration::ZERO));
}

fn profile_leave(name: Option<&str>, elapsed: std::time::Duration, path: &str) {
    let child_time = PROFILE_CHILD_STACK
        .with(|stack| stack.borrow_mut().pop())
        .unwrap_or_default();
    PROFILE_CHILD_STACK.with(|stack| {
        if let Some(parent) = stack.borrow_mut().last_mut() {
            *parent += elapsed;
        }
    });
    let name = name.unwrap_or("<anonymous>");
    PROFILE_TABLE.with(|table| {
        let mut table = table.borrow_mut();
        let entry = table.entry(name.to_string()).or_insert(ProfileEntry {
            count: 0,
            total: std::time::Duration::ZERO,
            self_time: std::time::Duration::ZERO,
        });
        entry.count += 1;
        entry.total += elapsed;
        entry.self_time += elapsed.saturating_sub(child_time);
    });
    let due = PROFILE_DUMP_COUNTDOWN.with(|countdown| {
        let remaining = countdown.get();
        if remaining == 0 {
            countdown.set(50_000);
            true
        } else {
            countdown.set(remaining - 1);
            false
        }
    });
    if due {
        PROFILE_TABLE.with(|table| {
            let table = table.borrow();
            let mut rows: Vec<_> = table.iter().collect();
            rows.sort_by_key(|(_, entry)| std::cmp::Reverse(entry.self_time));
            let mut out = String::new();
            for (name, entry) in rows.iter().take(80) {
                out.push_str(&format!(
                    "{:9} calls  self {:9.3}ms  total {:9.3}ms  {}\n",
                    entry.count,
                    entry.self_time.as_secs_f64() * 1000.0,
                    entry.total.as_secs_f64() * 1000.0,
                    name
                ));
            }
            let _ = std::fs::write(path, out);
        });
    }
}

/// Define every form intercepted before ordinary macro/function dispatch.
///
/// This registry is deliberately limited to the generated GNU C special-form
/// surface.  GNU Elisp macros enter through their real function cells and have
/// no alternate evaluator arm.
macro_rules! define_native_forms {
    ($($variant:ident => $($name:literal)|+;)+) => {
        #[derive(Clone, Copy)]
        pub(crate) enum NativeForm {
            $($variant,)+
        }

        impl NativeForm {
            fn for_name(name: &str) -> Option<Self> {
                match name {
                    $($($name)|+ => Some(Self::$variant),)+
                    _ => None,
                }
            }
        }
    };
}

impl NativeForm {
    /// The subr's `min_args' (eval.c's DEFUNs): eval_sub signals
    /// wrong-number-of-arguments below it before the form runs.
    fn min_args(self) -> usize {
        match self {
            Self::Quote
            | Self::Prog1
            | Self::Let
            | Self::LetStar
            | Self::Defvar
            | Self::Function
            | Self::While
            | Self::UnwindProtect
            | Self::Catch => 1,
            Self::If | Self::Defconst | Self::ConditionCase => 2,
            Self::And
            | Self::Or
            | Self::Cond
            | Self::Progn
            | Self::Setq
            | Self::Interactive
            | Self::SaveCurrentBuffer
            | Self::SaveExcursion
            | Self::SaveRestriction => 0,
        }
    }
}

define_native_forms! {
    Quote => "quote";
    If => "if";
    And => "and";
    Or => "or";
    Cond => "cond";
    Progn => "progn";
    Prog1 => "prog1";
    Let => "let";
    LetStar => "let*";
    Setq => "setq";
    Defvar => "defvar";
    Defconst => "defconst";
    Function => "function";
    Interactive => "interactive";
    While => "while";
    UnwindProtect => "unwind-protect";
    ConditionCase => "condition-case";
    Catch => "catch";
    SaveCurrentBuffer => "save-current-buffer";
    SaveExcursion => "save-excursion";
    SaveRestriction => "save-restriction";
}

/// The cell after CELL, when its cdr is one (`CONSP (XCDR (tail))').
#[inline]
pub(super) fn next_cons(cell: &crate::lisp::types::ConsCell) -> Option<SharedCons> {
    match &*cell.cdr.borrow() {
        Value::Cons(next) => Some(*next),
        _ => None,
    }
}

/// A walk over a list's elements by its cells (eval_sub's `while (CONSP
/// (args_left))'): each cell is held by its shared pointer while its
/// element is copied out, so the walk itself takes no value copies.
pub(super) struct ListForms(Option<SharedCons>);

/// The elements of LIST, from its first cell.
#[inline]
pub(super) fn list_forms(list: &Value) -> ListForms {
    ListForms(match list {
        Value::Cons(cell) => Some(*cell),
        _ => None,
    })
}

impl ListForms {
    #[inline]
    pub(super) fn from_cell(cell: Option<SharedCons>) -> Self {
        Self(cell)
    }
}

impl Iterator for ListForms {
    type Item = Value;

    #[inline]
    fn next(&mut self) -> Option<Value> {
        let cell = self.0.take()?;
        let form = *cell.car.borrow();
        self.0 = next_cons(&cell);
        Some(form)
    }
}

/// XCAR and XCDR of a list cell: the element and the rest, or None at
/// the end of the list (a non-cons tail ends the walk, as `CONSP (tail)'
/// loops do).
#[inline]
pub(super) fn list_next(tail: &Value) -> Option<(Value, Value)> {
    match tail {
        Value::Cons(cell) => Some((*cell.car.borrow(), *cell.cdr.borrow())),
        _ => None,
    }
}

/// Fcar of a list: the first element, nil for nil.
#[inline]
pub(super) fn list_car(list: &Value) -> Value {
    match list {
        Value::Cons(cell) => *cell.car.borrow(),
        _ => Value::Nil,
    }
}

/// Fcdr of a list: the rest, nil for nil.
#[inline]
pub(super) fn list_cdr(list: &Value) -> Value {
    match list {
        Value::Cons(cell) => *cell.cdr.borrow(),
        _ => Value::Nil,
    }
}

/// Fnth: the Nth element, nil past the end.
pub(super) fn list_nth(list: &Value, n: usize) -> Value {
    let mut tail = *list;
    for _ in 0..n {
        tail = list_cdr(&tail);
    }
    list_car(&tail)
}

/// Whether LIST has at least N cells: eval_sub's `numargs < min_args'
/// test, walking no further than N cells (by their shared pointers, no
/// element copied).
#[inline]
pub(super) fn list_has_at_least(list: &Value, n: usize) -> bool {
    if n == 0 {
        return true;
    }
    let Value::Cons(first) = list else {
        return false;
    };
    let mut cur = *first;
    for _ in 1..n {
        match next_cons(&cur) {
            Some(next) => cur = next,
            None => return false,
        }
    }
    true
}

/// The number of conses in LIST (list_length without the circularity
/// check; the walks here read what they count).
pub(super) fn list_cons_count(list: &Value) -> usize {
    let mut count = 0;
    let mut tail = *list;
    while let Some((_, next)) = list_next(&tail) {
        count += 1;
        tail = next;
    }
    count
}

/// The elements of a proper list as a vector (apply1's spread of an
/// argument list); a dotted tail signals listp.
pub(super) fn list_to_vector(list: &Value) -> Result<smallvec::SmallVec<[Value; 8]>, LispError> {
    let mut items = smallvec::SmallVec::new();
    let mut tail = *list;
    loop {
        match &tail {
            Value::Nil => return Ok(items),
            Value::Cons(cell) => {
                items.push(*cell.car.borrow());
                let next = *cell.cdr.borrow();
                tail = next;
            }
            other => return Err(LispError::WrongTypeArgument("listp".into(), *other)),
        }
    }
}

pub(crate) fn is_special_form_name(name: &str) -> bool {
    crate::lisp::primitives::generated_gnu_c_primitive_special_form(name)
}

/// The evaluator arm a symbol in function position selects, remembered
/// per symbol: the manifest's ownership check is a binary search over
/// the primitive names, which a fresh form (each macro expansion of
/// interpreted code) paid on every evaluation.  Symbol ids are never
/// reused, so a verdict stays the symbol's.
fn native_form_for_symbol(name: &SymbolName) -> Option<NativeForm> {
    thread_local! {
        static BY_SYMBOL: std::cell::RefCell<Vec<Option<Option<NativeForm>>>> =
            const { std::cell::RefCell::new(Vec::new()) };
    }
    let id = name.id();
    let compute = || {
        // A Lisp symbol may select a Rust evaluator arm only when the
        // generated GNU C manifest owns that native surface.  In
        // particular, an Emaxx-private prefix is not an ownership
        // boundary and cannot turn an Elisp macro into a host fallback.
        crate::lisp::primitives::generated_gnu_c_primitive_available(name)
            .is_some_and(|available| available)
            .then(|| NativeForm::for_name(name))
            .flatten()
    };
    if id & crate::lisp::types::UNINTERNED_SYMBOL_ID_BIT != 0 {
        return compute();
    }
    let index = id as usize;
    if let Some(known) = BY_SYMBOL.with_borrow(|table| table.get(index).copied().flatten()) {
        return known;
    }
    let native_form = compute();
    BY_SYMBOL.with_borrow_mut(|table| {
        if table.len() <= index {
            table.resize(index + 1, None);
        }
        table[index] = Some(native_form);
    });
    native_form
}

impl Interpreter {
    /// GNU treats a symbol-with-position in function position as its bare
    /// symbol while `symbols-with-pos-enabled' is non-nil.  The byte compiler
    /// enables that mode while macroexpanding source forms, so this is part of
    /// ordinary evaluator dispatch rather than merely a predicate detail.
    fn callable_symbol_name(&self, value: &Value, env: &Env) -> Option<SymbolName> {
        if let Value::Symbol(name) = value {
            return Some(*name);
        }
        if crate::lisp::primitives::symbols_with_pos_enabled(self, env)
            && let Some((symbol, _)) = crate::lisp::primitives::symbol_with_pos_parts(self, value)
            && let Ok(name) = symbol.as_symbol()
        {
            return Some(name.into());
        }
        None
    }

    pub fn eval(&mut self, expr: &Value, env: &mut Env) -> Result<Value, LispError> {
        let outermost = self.lisp_eval_depth == 0;
        if outermost {
            self.clear_batch_error_backtrace();
        }
        if self.pending_termination.is_some() {
            return Err(LispError::Terminate(
                self.pending_termination()
                    .cloned()
                    .expect("checked pending termination"),
            ));
        }
        // eval_sub: a symbol or a self-evaluating object returns before the
        // quit, collection and depth tests; the body below is inlined here,
        // one function as eval_sub is (its result crossed two frames before).
        if !matches!(expr, Value::Cons(_)) {
            let result = self.eval_inner(expr, env);
            if outermost && result.is_ok() {
                self.clear_batch_error_backtrace();
            }
            return result;
        }
        // eval.c:eval_sub checks for a pending quit after the symbol/scalar
        // fast paths and before GC, depth accounting, or form dispatch.
        self.maybe_quit(env)?;
        // eval.c:2502 calls maybe_gc after maybe_quit and before the depth
        // increment.  An active native call owns the conservative stack
        // scan; the ordinary interpreter collects when its consing counter
        // says alloc.c would.
        crate::lisp::native_comp::maybe_gc(self, env);
        self.lisp_eval_depth += 1;
        // eval.c:2504-2509.  GNU increments separately in `eval_sub' and
        // `Ffuncall'; this is the eval_sub half.  Public
        // `call_function_value' below owns the Ffuncall half, while direct
        // source dispatch stays on `call_function_value_named'.
        //   if (++lisp_eval_depth > max_lisp_eval_depth) {
        //     if (max_lisp_eval_depth < 100) max_lisp_eval_depth = 100;
        //     if (lisp_eval_depth > max_lisp_eval_depth)
        //       xsignal1 (Qexcessive_lisp_nesting, make_fixnum (lisp_eval_depth));
        //   }
        // Three things were wrong here (audit finding 105).  The limit came
        // from the GLOBAL cell, so `(let ((max-lisp-eval-depth 100)) ...)'
        // was invisible and a runaway recursion under a deliberately small
        // binding ran to completion.  It was multiplied by 384 and floored at
        // 307200, so the variable could not lower the limit at all.  And it
        // raised a plain `error' where GNU raises `excessive-lisp-nesting'
        // carrying the depth -- a condition this tree already defines
        // (eval.rs:732) and never signalled.
        if self.lisp_eval_depth_exceeded() {
            let reached = self.lisp_eval_depth;
            self.lisp_eval_depth -= 1;
            return Err(LispError::SignalValue(Value::list([
                Value::symbol("excessive-lisp-nesting"),
                Value::Integer(reached as i64),
            ])));
        }
        let result = self.eval_inner(expr, env);
        self.lisp_eval_depth -= 1;
        if outermost && result.is_ok() {
            self.clear_batch_error_backtrace();
        }
        result
    }

    /// eval.c's literal post-increment depth check.  DEFVAR_INT makes the
    /// limit an intmax_t field, so the evaluator reads it directly; only a
    /// depth that already exceeds a sub-100 value raises that live cell to
    /// 100 before deciding whether to signal.
    #[inline(always)]
    fn lisp_eval_depth_exceeded(&mut self) -> bool {
        let depth = i64::try_from(self.lisp_eval_depth).unwrap_or(i64::MAX);
        if depth <= self.max_lisp_eval_depth_value() {
            return false;
        }
        self.lisp_eval_depth_exceeded_slow(depth)
    }

    #[cold]
    fn lisp_eval_depth_exceeded_slow(&mut self, depth: i64) -> bool {
        if self.max_lisp_eval_depth_value() < 100 {
            if self
                .detached_forwarded_variables
                .contains_key("max-lisp-eval-depth")
            {
                self.max_lisp_eval_depth = 100;
            } else {
                self.set_symbol_value_cell("max-lisp-eval-depth", Value::Integer(100));
            }
        }
        depth > self.max_lisp_eval_depth_value()
    }

    #[inline(always)]
    fn eval_inner(&mut self, expr: &Value, env: &mut Env) -> Result<Value, LispError> {
        match expr {
            Value::Nil
            | Value::T
            | Value::Integer(_)
            | Value::BigInteger(_)
            | Value::Float(_)
            | Value::StringObject(_) => Ok(*expr),

            // GNU has already constructed every nested reader object by the
            // time eval_sub sees a vector.  Emaxx's parser is deliberately
            // interpreter-free, so finish that existing reader contract at
            // the evaluation boundary before returning this self-evaluating
            // object.
            Value::Vector(_) => self.materialize_read_object_literals(*expr, env),

            // Evaluating a string literal yields a string object with its
            // own identity, so `eq' distinguishes evaluations of distinct
            // literals while `(memq (car l) l)' still finds the element the
            // evaluation put there (GNU strings are always heap objects).
            Value::String(_) => Ok(Self::stored_value(*expr)),

            Value::Record(_)
                if crate::lisp::primitives::symbols_with_pos_enabled(self, env)
                    && crate::lisp::primitives::symbol_with_pos_parts(self, expr).is_some() =>
            {
                let Some((symbol, _)) = crate::lisp::primitives::symbol_with_pos_parts(self, expr)
                else {
                    return Ok(*expr);
                };
                let name = symbol
                    .as_symbol()
                    .map_err(|_| LispError::WrongTypeArgument("symbolp".into(), *expr))?;
                if name == "t" {
                    return Ok(Value::T);
                }
                if name == "nil" {
                    return Ok(Value::Nil);
                }
                match self.lookup(name, env) {
                    Ok(value) => Ok(value),
                    Err(LispError::Void(_)) => Err(LispError::SignalValue(Value::list([
                        Value::Symbol("void-variable".into()),
                        *expr,
                    ]))),
                    Err(error) => Err(error),
                }
            }

            Value::BuiltinFunc(_)
            | Value::Lambda(_)
            | Value::Buffer(_)
            | Value::Marker(_)
            | Value::Overlay(_)
            | Value::CharTable(_)
            | Value::Frame(_)
            | Value::Terminal(_)
            | Value::Record(_)
            | Value::Finalizer(_)
            | Value::Unbound => Ok(*expr),

            Value::ReaderForm(_) => self.materialize_read_object_literals(*expr, env),

            Value::Symbol(name) => self.lookup_symbol(name, env),

            Value::Cons(cell) => {
                // eval_sub: XCAR (form) read for its symbol (copied only
                // when it is something else), XCDR (form) held by its
                // cell, CHECK_LIST (original_args).
                let (head_symbol, head_value) = match &*cell.car.borrow() {
                    Value::Symbol(name) => (Some(*name), None),
                    other => (None, Some(*other)),
                };
                let args_cell: Option<SharedCons> = match &*cell.cdr.borrow() {
                    Value::Cons(args) => Some(*args),
                    Value::Nil => None,
                    other => {
                        return Err(LispError::WrongTypeArgument("listp".into(), *other));
                    }
                };
                let callable_name = match head_value.as_ref() {
                    None => head_symbol,
                    Some(value) => self.callable_symbol_name(value, env),
                };
                if let Some(ref name) = callable_name {
                    // The subr whose max_args is UNEVALLED, by the symbol.
                    // A function alias of a special form (GNU's `(defalias
                    // 'inline 'progn)') keeps the target's calling
                    // convention: a cell holding a symbol (or a subr) is
                    // followed to the target.
                    let effective_native_form = self
                        .globals
                        .native_form_or(name, || native_form_for_symbol(name))
                        .or_else(|| {
                            if !matches!(
                                self.globals.function(name),
                                Some(Value::Symbol(_) | Value::BuiltinFunc(_))
                            ) {
                                return None;
                            }
                            let Value::BuiltinFunc(target) =
                                self.lookup_function_symbol(name, env).ok()?
                            else {
                                return None;
                            };
                            // The target's arm by its symbol (every special
                            // form has one; the manifest was searched by
                            // name per call through an alias before).
                            self.globals
                                .native_form_or(&target, || native_form_for_symbol(&target))
                        });
                    if let Some(native_form) = effective_native_form {
                        let args_value = match &args_cell {
                            Some(args) => Value::Cons(*args),
                            None => Value::Nil,
                        };
                        let args = &args_value;
                        // eval_sub's `numargs < XSUBR (fun)->min_args' for
                        // the UNEVALLED subr: the list walked only as far as
                        // min_args (the count itself only when it signals).
                        let min_args = native_form.min_args();
                        if !list_has_at_least(args, min_args) {
                            return Err(LispError::WrongNumberOfArgs(
                                name.as_str().to_string(),
                                list_cons_count(args),
                            ));
                        }
                        match native_form {
                            NativeForm::Quote => return self.sf_quote(args, env),
                            NativeForm::If => return self.sf_if(args, env),
                            NativeForm::And => return self.sf_and(args, env),
                            NativeForm::Or => return self.sf_or(args, env),
                            NativeForm::Cond => {
                                self.push_unevaluated_backtrace_frame(expr);
                                let result = self.sf_cond(args, env);
                                let result = self.settle_frame_result(result, env);
                                self.pop_backtrace_frame();
                                return result;
                            }
                            NativeForm::Progn => return self.progn_list(args, env),
                            NativeForm::Prog1 => return self.sf_prog1(args, env),
                            NativeForm::Let => {
                                self.push_unevaluated_backtrace_frame(expr);
                                let result = self.sf_let(args, env);
                                let result = self.settle_frame_result(result, env);
                                self.pop_backtrace_frame();
                                return result;
                            }
                            NativeForm::LetStar => {
                                self.push_unevaluated_backtrace_frame(expr);
                                let result = self.sf_letstar(args, env);
                                let result = self.settle_frame_result(result, env);
                                self.pop_backtrace_frame();
                                return result;
                            }
                            NativeForm::Setq => {
                                self.push_unevaluated_backtrace_frame(expr);
                                let result = self.sf_setq(args, env);
                                let result = self.settle_frame_result(result, env);
                                self.pop_backtrace_frame();
                                return result;
                            }
                            NativeForm::Defvar => return self.sf_defvar(args, env),
                            NativeForm::Defconst => return self.sf_defconst(args, env),
                            NativeForm::Function => return self.sf_function(args, env),
                            NativeForm::Interactive => return Ok(Value::Nil),
                            NativeForm::While => {
                                self.push_unevaluated_backtrace_frame(expr);
                                let result = self.sf_while(args, env);
                                let result = self.settle_frame_result(result, env);
                                self.pop_backtrace_frame();
                                return result;
                            }
                            NativeForm::UnwindProtect => {
                                return self.sf_unwind_protect(args, env);
                            }
                            NativeForm::ConditionCase => {
                                return self.sf_condition_case(args, env);
                            }
                            NativeForm::Catch => return self.sf_catch(args, env),
                            NativeForm::SaveCurrentBuffer => {
                                return self.sf_save_current_buffer(args, env);
                            }
                            NativeForm::SaveExcursion => {
                                return self.sf_save_excursion(args, env);
                            }
                            NativeForm::SaveRestriction => {
                                return self.sf_save_restriction(args, env);
                            }
                        }
                    }

                    // eval_sub's macro arm: the function cell holds
                    // `(macro . EXPANDER)', an alias to one, or an autoload
                    // of one -- read by id; a void cell (a builtin's, or
                    // an undefined name's) is no macro.  The expander runs
                    // on every evaluation: it can inspect state, perform
                    // side effects, or create fresh uninterned symbols.
                    if matches!(
                        self.globals.function(name),
                        Some(Value::Cons(_) | Value::Symbol(_))
                    ) {
                        // apply1's spread of the unevaluated forms.
                        let args_value = match &args_cell {
                            Some(args) => Value::Cons(*args),
                            None => Value::Nil,
                        };
                        let args = list_to_vector(&args_value)?;
                        if let Some(expanded) = self.try_macroexpand(name, &args, env)? {
                            return self.eval(&expanded, env);
                        }
                    }
                }

                // Regular function call
                self.eval_call(expr, head_value.as_ref(), callable_name, args_cell, env)
            }
        }
    }

    pub(super) fn eval_call(
        &mut self,
        source_form: &Value,
        head: Option<&Value>,
        callable_name: Option<SymbolName>,
        args_list: Option<SharedCons>,
        env: &mut Env,
    ) -> Result<Value, LispError> {
        // GNU resolves the function cell before evaluating any argument.
        // Keep that observable ordering while retaining a direct native
        // verdict instead of materializing `BuiltinFunc' and throwing away
        // the name-facts cache on every ordinary source call.
        //
        // eval_sub records the call, with its unevaluated argument forms,
        // before it resolves the function cell and while the arguments
        // evaluate: a void function or an error inside an argument reaches
        // `handler-bind' handlers and backtraces with this frame innermost.
        let depth = self.backtrace_frames.len();
        let unevald_frame = callable_name.is_some();
        if unevald_frame {
            self.push_unevaluated_backtrace_frame(source_form);
        }
        let prepared = if let Some(name) = callable_name.as_ref() {
            match self.resolve_symbol_call_with_frame_state(name, env, false) {
                Ok(prepared) => prepared,
                Err(error) => {
                    let result = self.settle_frame_result(Err(error), env);
                    self.pop_backtrace_frame();
                    return result;
                }
            }
        } else {
            let head = head.expect("a callee that is no symbol is held as a value");
            FunctionResolution::Resolved(self.eval(head, env)?)
        };
        // eval_sub's argvals[8]: the evaluated arguments on the stack for
        // up to eight, the forms read off the list cell by cell; past
        // eight, SAFE_ALLOCA_LISP's array, which the collector scans (a
        // heap vector the stack scan cannot see lost a fresh argument to
        // the collection a later argument's evaluation ran).  The count
        // is `list_length (args_left)' taken first, for every callee, as
        // eval_sub takes it (a fixed-arity subr's count was skipped
        // before, and nine arguments to it wrote past the array).
        let argnum = ListForms::from_cell(args_list).count();
        // eval_sub's SUBRP arm signals `wrong-number-of-arguments' on the
        // count before any argument is evaluated.
        if let FunctionResolution::DirectBuiltin(facts) = &prepared
            && !facts.special_form
            && facts
                .max_args
                .is_some_and(|maximum| usize::from(maximum) < argnum)
        {
            let name = callable_name.expect("a direct subr verdict names its symbol");
            let error = LispError::WrongNumberOfArgs(name.as_str().to_owned(), argnum);
            let result = self.settle_frame_result(Err(error), env);
            self.pop_backtrace_frame();
            return result;
        }
        if argnum > 8 {
            return self.eval_call_rooted(
                depth,
                unevald_frame,
                callable_name,
                prepared,
                args_list,
                env,
            );
        }
        // The array's storage is zeroed before use (one small memset): the
        // conservative scan reads every word of a live frame, and a stale
        // pointer left there by an earlier, deeper call would keep its
        // object (C's argvals is uninitialized; its frames are smaller).
        // Only the ARGNUM values written are ever read or dropped.
        let mut argvals = std::mem::MaybeUninit::<[Value; 8]>::zeroed();
        let base = argvals.as_mut_ptr().cast::<Value>();
        let mut argnum = 0usize;
        // SAFETY: BASE addresses eight slots on this frame; ARGNUM counts
        // the slots written, which are the only ones read or dropped.
        let drop_written = |count: usize| unsafe {
            for index in 0..count {
                base.add(index).drop_in_place();
            }
        };
        for form in ListForms::from_cell(args_list) {
            match self.eval(&form, env) {
                // SAFETY: as above; ARGNUM < 8 by the count taken.
                Ok(value) => unsafe {
                    base.add(argnum).write(value);
                    argnum += 1;
                },
                Err(error) => {
                    drop_written(argnum);
                    return self.eval_call_argument_error(depth, unevald_frame, error, env);
                }
            }
        }
        // SAFETY: the ARGNUM slots written, read in place for the call.
        let args = unsafe { std::slice::from_raw_parts(base, argnum) };
        let result = self.finish_eval_call(depth, callable_name, prepared, args, env);
        drop_written(argnum);
        result
    }

    /// `eval_call' past eight arguments: SAFE_ALLOCA_LISP's array.
    #[inline(never)]
    fn eval_call_rooted(
        &mut self,
        depth: usize,
        unevald_frame: bool,
        callable_name: Option<SymbolName>,
        prepared: FunctionResolution,
        args_list: Option<SharedCons>,
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let mut args =
            crate::lisp::alloc::RootedVec::with_capacity(ListForms::from_cell(args_list).count());
        for form in ListForms::from_cell(args_list) {
            match self.eval(&form, env) {
                Ok(value) => args.push(value),
                Err(error) => {
                    return self.eval_call_argument_error(depth, unevald_frame, error, env);
                }
            }
        }
        self.finish_eval_call(depth, callable_name, prepared, &args, env)
    }

    /// An argument's evaluation signaled: the frame recorded before the
    /// arguments settles the error and is popped.
    #[cold]
    fn eval_call_argument_error(
        &mut self,
        depth: usize,
        unevald_frame: bool,
        error: LispError,
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if unevald_frame {
            let result = self.settle_frame_result(Err(error), env);
            self.truncate_backtrace_frames(depth);
            return result;
        }
        Err(error)
    }

    #[inline]
    fn finish_eval_call(
        &mut self,
        depth: usize,
        callable_name: Option<SymbolName>,
        prepared: FunctionResolution,
        args: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        match (callable_name.as_ref(), prepared) {
            (Some(name), FunctionResolution::DirectBuiltin(facts)) => {
                // eval_sub's SUBRP arm: the frame recorded before the
                // arguments were evaluated names the subr and holds them
                // now (set_backtrace_args), and the subr runs under that
                // one frame -- a second frame was pushed and popped around
                // every primitive call before.
                self.set_backtrace_args(Value::Symbol(*name), args);
                self.capture_current_backtrace_context(Some(name.as_str()), env, None);
                let result = primitives::call_with_facts(self, name, facts, args, env)
                    .map_err(|error| Self::builtin_call_error(name, args.len(), false, error));
                let result = self.settle_frame_result(result, env);
                self.truncate_backtrace_frames(depth);
                result
            }
            (Some(name), FunctionResolution::Resolved(func)) => {
                self.pop_backtrace_frame();
                self.call_function_value_named(func, Some(CallName::Symbol(name)), args, env, false)
            }
            (None, FunctionResolution::Resolved(func)) => {
                self.call_function_value_named(func, None, args, env, false)
            }
            (None, FunctionResolution::DirectBuiltin(_)) => {
                unreachable!("only a symbol callee can have a direct native verdict")
            }
        }
    }

    pub fn call_function_value(
        &mut self,
        func: Value,
        original_name: Option<&str>,
        args: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        self.begin_funcall(env)?;
        let result = self.call_function_value_named(
            func,
            original_name.map(CallName::Text),
            args,
            env,
            true,
        );
        self.end_funcall();
        result
    }

    /// `call_function_value' with the call's name as a symbol in hand: the
    /// frame records that symbol, where a text name was interned again
    /// for every call (each macro expansion of interpreted code).
    pub(crate) fn call_function_value_as(
        &mut self,
        func: Value,
        name: &SymbolName,
        args: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        self.begin_funcall(env)?;
        let result =
            self.call_function_value_named(func, Some(CallName::Symbol(name)), args, env, true);
        self.end_funcall();
        result
    }

    /// exec_byte_code's Bcall: Ffuncall's depth and quit checks, then the
    /// callee's own entry without the general dispatch between them -- a
    /// byte-code object, or a symbol whose function cell holds one, goes to
    /// funcall_lambda's byte-code branch; a symbol naming a subr to its
    /// dispatch.  Anything else is the general call.
    pub(crate) fn funcall_from_bytecode(
        &mut self,
        func: &Value,
        args: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if profile_path().is_some() {
            return self.call_function_value(*func, None, args, env);
        }
        match func {
            Value::Record(id) if self.has_cached_bytecode_program(*id) => {
                let id = *id;
                self.begin_funcall(env)?;
                let result = if let Some(termination) = self.pending_termination().cloned() {
                    Err(LispError::Terminate(termination))
                } else {
                    self.execute_bytecode_record_named(id, None, args, env)
                };
                self.end_funcall();
                result
            }
            Value::Symbol(name) => {
                self.begin_funcall(env)?;
                let result = if let Some(termination) = self.pending_termination().cloned() {
                    Err(LispError::Terminate(termination))
                } else {
                    match self.resolve_symbol_call(name, env) {
                        Ok(FunctionResolution::DirectBuiltin(facts)) => self
                            .dispatch_named_builtin(
                                name,
                                facts,
                                Some(CallName::Symbol(name)),
                                args,
                                env,
                                true,
                            ),
                        Ok(FunctionResolution::Resolved(Value::Record(id)))
                            if self.has_cached_bytecode_program(id) =>
                        {
                            self.execute_bytecode_record_named(
                                id,
                                Some(CallName::Symbol(name)),
                                args,
                                env,
                            )
                        }
                        // A lambda, an autoload, a local function binding or
                        // a void function: the general path, which resolves
                        // again from the cache.
                        _ => self.call_function_value_inner(*func, None, args, env, true),
                    }
                };
                self.end_funcall();
                result
            }
            _ => self.call_function_value(*func, None, args, env),
        }
    }

    /// eval.c:call_debugger.  The C path specbinds the debugger control
    /// variables around apply1(Vdebugger, arg); keep those bindings in the
    /// same dynamic scope for native Ffuncall exits.
    pub(crate) fn call_debugger(&mut self, arg: Value, env: &mut Env) -> Result<Value, LispError> {
        self.clear_debug_on_next_call();
        let mut restores = Vec::with_capacity(4);
        for (name, value) in [
            ("debugger-may-continue", Value::T),
            ("inhibit-redisplay", Value::Nil),
            ("inhibit-debugger", Value::T),
            ("inhibit-changing-match-data", Value::Nil),
        ] {
            match self.bind_special_dynamic(name, value, env) {
                Ok(restore) => restores.push(restore),
                Err(error) => {
                    for restore in restores.into_iter().rev() {
                        let _ = self.restore_special_dynamic(restore, env);
                    }
                    return Err(error);
                }
            }
        }

        let debugger = self.lookup_var("debugger", env).unwrap_or(Value::Nil);
        let result = self.call_function_value(debugger, None, &[arg], env);
        for restore in restores.into_iter().rev() {
            self.restore_special_dynamic(restore, env)?;
        }
        result
    }

    /// eval.c:Ffuncall's entry sequence.  Generated code uses the same
    /// boundary before it dispatches an already encoded Lisp_Object vector.
    #[inline(always)]
    pub(crate) fn begin_funcall(&mut self, env: &mut Env) -> Result<(), LispError> {
        self.maybe_quit(env)?;
        self.lisp_eval_depth += 1;
        if self.lisp_eval_depth_exceeded() {
            let reached = self.lisp_eval_depth;
            self.lisp_eval_depth -= 1;
            return Err(LispError::SignalValue(Value::list([
                Value::symbol("excessive-lisp-nesting"),
                Value::Integer(reached as i64),
            ])));
        }
        Ok(())
    }

    #[inline(always)]
    pub(crate) fn end_funcall(&mut self) {
        self.lisp_eval_depth = self
            .lisp_eval_depth
            .checked_sub(1)
            .expect("Ffuncall depth is balanced");
    }

    /// eval.c:maybe_quit/probably_quit/process_quit_flag for the Lisp-visible
    /// quit state.  Platform pending-signal delivery remains owned by the
    /// process/terminal layer; once it sets quit-flag, this is the exact C
    /// dispatch among kill-emacs, throw-on-input, and ordinary quit.
    #[inline(always)]
    pub(crate) fn maybe_quit(&mut self, env: &mut Env) -> Result<(), LispError> {
        // lisp.h:maybe_quit first reads Vquit_flag directly and returns on
        // the overwhelmingly common nil case.  These are eval.c's forwarded
        // cells, not symbol-name lookups.
        if self.quit_flag_is_nil() {
            return Ok(());
        }
        self.process_quit_flag(env)
    }

    /// eval.c:process_quit_flag, reached only with a non-nil quit-flag.
    #[cold]
    fn process_quit_flag(&mut self, env: &mut Env) -> Result<(), LispError> {
        let flag = self.quit_flag_value();
        if self.inhibit_quit_is_truthy() {
            return Ok(());
        }

        // process_quit_flag writes Vquit_flag, not a possibly detached plain
        // Lisp binding with the same name.
        if self.detached_forwarded_variables.contains_key("quit-flag") {
            self.quit_flag = Value::Nil;
        } else {
            self.set_symbol_value_cell("quit-flag", Value::Nil);
        }
        if matches!(&flag, Value::Symbol(name) if name == "kill-emacs") {
            return primitives::call(self, "kill-emacs", &[Value::Nil, Value::Nil], env).map(drop);
        }

        let throw_on_input = self.throw_on_input_value();
        if primitives::values_eq_in_env(self, &throw_on_input, &flag, env) {
            Err(LispError::Throw(throw_on_input, Value::T))
        } else {
            Err(LispError::SignalValue(Value::list([Value::symbol("quit")])))
        }
    }

    #[inline(always)]
    fn call_function_value_named(
        &mut self,
        func: Value,
        original_name: Option<CallName<'_>>,
        args: &[Value],
        env: &mut Env,
        funcall: bool,
    ) -> Result<Value, LispError> {
        if self.pending_termination().is_some() {
            return Err(LispError::Terminate(
                self.pending_termination()
                    .cloned()
                    .expect("checked pending termination"),
            ));
        }
        // Dev-only flat profiler: EMAXX_PROFILE=<path> accumulates per-name
        // call counts and self-time, periodically rewriting <path>.
        if let Some(path) = profile_path() {
            return self.call_function_value_profiled(
                func,
                original_name,
                args,
                env,
                funcall,
                path,
            );
        }
        self.call_function_value_inner(func, original_name, args, env, funcall)
    }

    #[cold]
    fn call_function_value_profiled(
        &mut self,
        func: Value,
        original_name: Option<CallName<'_>>,
        args: &[Value],
        env: &mut Env,
        funcall: bool,
        path: &'static str,
    ) -> Result<Value, LispError> {
        let started = std::time::Instant::now();
        profile_enter();
        let result = self.call_function_value_inner(func, original_name, args, env, funcall);
        profile_leave(original_name.map(CallName::as_str), started.elapsed(), path);
        result
    }

    /// Resolve a symbol function cell once, before argument evaluation.
    ///
    /// This is the single authority used by both ordinary source calls and
    /// `funcall' of a symbol.  Global verdicts are generation-stamped; local
    /// function-binding frames always take the uncached lookup path.
    fn resolve_symbol_call(
        &mut self,
        name: &SymbolName,
        env: &Env,
    ) -> Result<FunctionResolution, LispError> {
        self.resolve_symbol_call_with_frame_state(name, env, false)
    }

    /// Resolve an ordinary source call through its callsite-local verdict.
    /// Symbolic `funcall' retains the global name cache above; both paths use
    /// the same generation and uncached resolution authority below.
    fn resolve_symbol_call_with_frame_state(
        &mut self,
        name: &SymbolName,
        env: &Env,
        local_context: bool,
    ) -> Result<FunctionResolution, LispError> {
        // eval_sub reads the function cell off the symbol (`XSYMBOL
        // (fun)->u.s.function'): the facts and the cell by id, with no
        // hash of the name and no memo in front of a field read.  Under a
        // frame that holds a callable (cl-flet), only a frame binding
        // this very name changes the answer.
        let facts = self
            .globals
            .facts_or(name, || crate::lisp::primitives::name_facts_symbol(name));
        let global = !local_context;
        // `selected-window' keeps its own arm; its id read once.
        static SELECTED_WINDOW_ID: std::sync::OnceLock<u32> = std::sync::OnceLock::new();
        let selected_window =
            *SELECTED_WINDOW_ID.get_or_init(|| SymbolName::intern_str("selected-window").id());
        let resolution = if name.id() != selected_window
            && (facts.prefer_override
                || (facts.builtin && !facts.special_form && self.globals.function(name).is_none()))
            && global
        {
            FunctionResolution::DirectBuiltin(facts)
        } else {
            FunctionResolution::Resolved(self.lookup_function_symbol(name, env)?)
        };
        Ok(resolution)
    }

    /// The BuiltinFunc arm of `call_function_value_inner' for a callee
    /// still in name form: backtrace frame, native dispatch with the
    /// already-fetched facts, handler mapping.
    fn dispatch_named_builtin(
        &mut self,
        name: &SymbolName,
        facts: crate::lisp::primitives::NameFacts,
        original_name: Option<CallName<'_>>,
        args: &[Value],
        env: &mut Env,
        funcall: bool,
    ) -> Result<Value, LispError> {
        let backtrace_function = original_name
            .map(|original| original.symbol_value(name))
            .unwrap_or_else(|| Value::Symbol(*name));
        self.with_backtrace_frame(backtrace_function, args, |interp| {
            interp.capture_current_backtrace_context(
                Some(original_name.map_or(name.as_str(), CallName::as_str)),
                env,
                None,
            );
            // eval.c:Ffuncall: maybe_quit, the depth check, record_in_backtrace,
            // then maybe_gc -- the arguments are on the specpdl by then.
            crate::lisp::native_comp::maybe_gc(interp, env);
            let result = primitives::call_with_facts(interp, name, facts, args, env)
                .map_err(|error| Self::builtin_call_error(name, args.len(), funcall, error));
            interp.settle_frame_result(result, env)
        })
    }

    // eval.c:funcall_subr reports the resolved subr object. eval_sub
    // reports the source callee instead. Translate before signaling to
    // handler-bind, while the callee's backtrace frame is still live.
    fn builtin_call_error(name: &str, nargs: usize, funcall: bool, error: LispError) -> LispError {
        match error {
            LispError::WrongNumberOfArgs(ref failed_name, count)
                if funcall
                    && failed_name == name
                    && count == nargs
                    && primitives::GNU_C_PRIMITIVES
                        .binary_search_by_key(&name, |contract| contract.name)
                        .ok()
                        .and_then(|index| primitives::GNU_C_PRIMITIVES[index].arity)
                        .is_some_and(|(minimum, maximum)| {
                            nargs < minimum as usize || (maximum >= 0 && nargs > maximum as usize)
                        }) =>
            {
                LispError::SignalValue(Value::list([
                    Value::symbol("wrong-number-of-arguments"),
                    Value::BuiltinFunc(name.into()),
                    Value::Integer(count as i64),
                ]))
            }
            error => error,
        }
    }

    /// What every backtrace frame does with an error on its way out, while
    /// the frame is still live.  GNU's signal_or_quit runs `handler-bind'
    /// handlers from `signal' itself, with the signaling frames intact;
    /// Emaxx runs them at the innermost frame boundary that sees the error
    /// (the dispatch remembers the object, so the outer boundaries pass it
    /// on), then records the batch backtrace snapshot.  Special forms,
    /// interpreted lambdas, byte-code, native code and primitives all pass
    /// through here, so an error the evaluator itself signals -- a void
    /// function or variable, a wrong arity -- reaches the handlers with the
    /// same innermost frame GNU shows.
    ///
    /// A normal return settles nothing (eval_sub's `unbind_to' and the
    /// frame pop are all that happen in C); the error path is out of line.
    #[inline(always)]
    pub(crate) fn settle_frame_result(
        &mut self,
        result: Result<Value, LispError>,
        env: &mut Env,
    ) -> Result<Value, LispError> {
        match result {
            Ok(value) => Ok(value),
            Err(error) => self.settle_frame_error(error, env),
        }
    }

    #[cold]
    #[inline(never)]
    pub(crate) fn settle_frame_error(
        &mut self,
        error: LispError,
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let result = match error {
            error @ (LispError::Throw(_, _) | LispError::Terminate(_)) => Err(error),
            error => self.dispatch_handler_bindings(error, env),
        };
        if let Err(error) = &result {
            self.capture_batch_error_backtrace(error, env);
        }
        result
    }

    /// Execute one GNU byte-code closure with the activation-frame contract
    /// that eval.c exposes to backtrace-frame/backtrace-eval.
    #[inline]
    fn execute_bytecode_record_named(
        &mut self,
        record_id: u64,
        original_name: Option<CallName<'_>>,
        args: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let backtrace_function = original_name
            .map(CallName::original_symbol_value)
            .unwrap_or(Value::Record(record_id));
        self.with_backtrace_frame(backtrace_function, args, |interp| {
            interp.capture_current_backtrace_context(
                original_name.map(CallName::as_str),
                env,
                None,
            );
            // Ffuncall's maybe_gc, after record_in_backtrace.
            crate::lisp::native_comp::maybe_gc(interp, env);
            let result = interp.execute_bytecode_funcall_body(record_id, args, env);
            interp.settle_frame_result(result, env)
        })
    }

    /// eval.c:funcall_lambda's direct `exec_byte_code' branch.  The caller
    /// owns Ffuncall's depth and backtrace entry; this supplies only the
    /// byte-code activation boundary shared by source and native callers.
    #[inline(always)]
    pub(crate) fn execute_bytecode_funcall_body(
        &mut self,
        record_id: u64,
        args: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        // Byte code reads variables with Fsymbol_value, never through the
        // interpreter environment (bytecode.c's Bvarref); a nil frame keeps
        // the caller's lexical bindings out of the VM's reads.
        let depth = env.len();
        env.push(EnvFrame::dynamic());
        let result = crate::lisp::bytecode::vm::execute_record(self, record_id, args, env);
        env.truncate(depth);
        result
    }

    /// Only execute_record fills this cache, so a hit is a genuine
    /// byte-code function whose slots have not been mutated since.
    /// Bcall's fast path: FUNC as a lexbound byte-code function whose
    /// program is cached (a bare symbol read through its function cell,
    /// as `XBARE_SYMBOL (call_fun)->u.s.function'), or None for anything
    /// Ffuncall must handle -- an alias, an autoload, a builtin that
    /// overrides its Lisp definition, a dynamic arglist, the profiler.
    pub(crate) fn bytecode_callee(
        &self,
        func: &Value,
    ) -> Option<(std::rc::Rc<crate::lisp::bytecode::vm::CachedProgram>, u64)> {
        let id = match func {
            Value::Record(id) => *id,
            Value::Symbol(name) => {
                let facts = self
                    .globals
                    .facts_or(name, || crate::lisp::primitives::name_facts_symbol(name));
                if facts.prefer_override {
                    return None;
                }
                match self.globals.function(name) {
                    Some(Value::Record(id)) => *id,
                    _ => return None,
                }
            }
            _ => return None,
        };
        if profile_path().is_some() {
            return None;
        }
        let program = self
            .bytecode_program_cache
            .get((id as usize).checked_sub(1)?)?
            .as_ref()?;
        matches!(
            program.argspec,
            crate::lisp::bytecode::ArgSpec::Packed { .. }
        )
        .then(|| (std::rc::Rc::clone(program), id))
    }

    fn has_cached_bytecode_program(&self, record_id: u64) -> bool {
        (record_id as usize)
            .checked_sub(1)
            .and_then(|index| self.bytecode_program_cache.get(index))
            .is_some_and(|slot| slot.is_some())
    }

    pub(crate) fn is_genuine_bytecode_function(&self, record_id: u64) -> bool {
        // data.c:Fbyte_code_function_p: classification neither executes nor
        // validates the bytecode and does not inspect payload contents.
        self.find_record(record_id).is_some_and(|record| {
            record.kind == RecordKind::Closure && record.slots.get(1).is_some_and(Value::is_string)
        })
    }

    fn call_function_value_inner(
        &mut self,
        func: Value,
        original_name: Option<CallName<'_>>,
        args: &[Value],
        env: &mut Env,
        funcall: bool,
    ) -> Result<Value, LispError> {
        // eval.c/bytecode.c use XBARE_SYMBOL for a positioned callee while
        // the byte compiler's symbol-position mode is active.  This covers
        // explicit `funcall'/`apply' as well as ordinary source dispatch.
        let func = if func.is_symbol() {
            func
        } else {
            self.callable_symbol_name(&func, env)
                .map(Value::Symbol)
                .unwrap_or(func)
        };
        // A record with a cached program is a genuine byte-code function
        // (only execute_record populates the cache), so skip the
        // lambda/autoload probes and the record-type guards below.
        if let Value::Record(id) = &func
            && self.has_cached_bytecode_program(*id)
        {
            return self.execute_bytecode_record_named(*id, original_name, args, env);
        }
        let mut owned_name: Option<SymbolName> = None;
        let func = match func {
            Value::Symbol(name) => {
                let resolution = match self.resolve_symbol_call(&name, env) {
                    Ok(resolution) => resolution,
                    Err(error) => {
                        // eval.c's Ffuncall records the backtrace frame
                        // BEFORE resolving the function cell, so a
                        // void-function report carries the attempted call
                        // itself (`foo(ARGS)') as its innermost frame.
                        let function = original_name
                            .map(|original| original.symbol_value(&name))
                            .unwrap_or_else(|| Value::Symbol(name));
                        return self.with_backtrace_frame(function, args, |interp| {
                            interp.settle_frame_result(Err(error), env)
                        });
                    }
                };
                match resolution {
                    FunctionResolution::DirectBuiltin(facts) => {
                        let call_name = original_name.or(Some(CallName::Symbol(&name)));
                        return self
                            .dispatch_named_builtin(&name, facts, call_name, args, env, funcall);
                    }
                    // funcall_general's COMPILEDP arm: the function cell
                    // holds a byte-code object already decoded once.
                    FunctionResolution::Resolved(Value::Record(id))
                        if self.has_cached_bytecode_program(id) =>
                    {
                        let call_name = original_name.or(Some(CallName::Symbol(&name)));
                        return self.execute_bytecode_record_named(id, call_name, args, env);
                    }
                    FunctionResolution::Resolved(value) => {
                        if original_name.is_none() {
                            owned_name = Some(name);
                        }
                        value
                    }
                }
            }
            other => other,
        };
        let original_name = original_name.or_else(|| owned_name.as_ref().map(CallName::Symbol));
        let func = match func {
            Value::Cons(_) => {
                let func = if is_lambda_form(self, &func, env) {
                    let mut lambda = func.to_vec()?;
                    lambda[0] = Value::symbol("lambda");
                    let source = Value::list(lambda.clone());
                    self.sf_lambda_from_source(&source, &lambda, env)?
                } else {
                    func
                };
                if let Some((file, _, _)) = crate::lisp::primitives::autoload_parts(&func) {
                    let Some(name) = original_name.map(CallName::as_str) else {
                        return Err(LispError::SignalValue(Value::list([
                            Value::Symbol("invalid-function".into()),
                            func,
                        ])));
                    };
                    // The arguments are held here, in no Lisp object, while
                    // the file loads and runs: a collection inside the
                    // load must reach them (GNU's conservative stack scan
                    // does; `set-auto-mode' called a search with a marker
                    // bound through an autoload, and the marker was
                    // unchained by the load's collection).
                    let loaded = self.with_lisp_stack_roots(&args, |interp| {
                        interp.load_autoload_target(&file, env)
                    });
                    match loaded {
                        Ok(_) => self.lookup_function(name, env)?,
                        // Only a genuinely file-less environment (unit tests
                        // with no resolvable Lisp tree) may fall back to a
                        // native arm.  An error raised while EVALUATING a
                        // found file must propagate like GNU's
                        // Fautoload_do_load; anything else would make a
                        // broken .el invisible for every native-armed name.
                        Err(error) => {
                            if error.condition_type() == "file-missing"
                                && crate::lisp::primitives::is_builtin(name)
                            {
                                Value::BuiltinFunc(name.to_string().into())
                            } else {
                                return Err(error);
                            }
                        }
                    }
                } else {
                    func
                }
            }
            other => other,
        };

        match func {
            Value::BuiltinFunc(ref name) if name == "selected-window" && args.is_empty() => {
                Ok(self.selected_window_value())
            }
            Value::BuiltinFunc(ref name) => {
                let backtrace_function = original_name
                    .map(|original| original.symbol_value(name))
                    .unwrap_or_else(|| Value::Symbol(*name));
                self.with_backtrace_frame(backtrace_function, args, |interp| {
                    interp.capture_current_backtrace_context(
                        original_name.map(CallName::as_str),
                        env,
                        None,
                    );
                    crate::lisp::native_comp::maybe_gc(interp, env);
                    let result = primitives::call(interp, name, args, env).map_err(|error| {
                        Self::builtin_call_error(name, args.len(), funcall, error)
                    });
                    interp.settle_frame_result(result, env)
                })
            }
            Value::Record(id)
                if self
                    .find_record(id)
                    .is_some_and(|record| record.kind == RecordKind::NativeCompiledFunction) =>
            {
                let backtrace_function = original_name
                    .map(CallName::original_symbol_value)
                    .unwrap_or(Value::Record(id));
                self.with_backtrace_frame(backtrace_function, args, |interp| {
                    interp.capture_current_backtrace_context(
                        original_name.map(CallName::as_str),
                        env,
                        None,
                    );
                    crate::lisp::native_comp::maybe_gc(interp, env);
                    let result = crate::lisp::native_comp::call_function(interp, env, id, args);
                    interp.settle_frame_result(result, env)
                })
            }
            Value::Record(id)
                if self
                    .find_record(id)
                    .is_some_and(|record| record.kind == RecordKind::ModuleFunction) =>
            {
                let backtrace_function = original_name
                    .map(CallName::original_symbol_value)
                    .unwrap_or(Value::Record(id));
                self.with_backtrace_frame(backtrace_function, args, |interp| {
                    interp.capture_current_backtrace_context(
                        original_name.map(CallName::as_str),
                        env,
                        None,
                    );
                    let result = interp.with_lisp_stack_roots(&Value::Record(id), |interp| {
                        // Ffuncall collects after recording the arguments for
                        // module functions too. Keep the resolved function
                        // live if a finalizer rebinds its original symbol.
                        crate::lisp::native_comp::maybe_gc(interp, env);
                        crate::lisp::modules::call(interp, env, id, args)
                    });
                    interp.settle_frame_result(result, env)
                })
            }
            Value::Record(id)
                if self
                    .find_record(id)
                    .is_some_and(|record| record.kind == RecordKind::Closure) =>
            {
                let (inner, uses_dynamic_binding) = {
                    let Some(record) = self.find_record(id) else {
                        unreachable!("checked record presence");
                    };
                    // A byte-code closure has a string code slot. Leave
                    // instruction validation to the VM, not this type check.
                    if record.slots.get(1).is_some_and(Value::is_string) {
                        return self.execute_bytecode_record_named(id, original_name, args, env);
                    }
                    let Some(inner) = record.slots.first().cloned() else {
                        return Err(LispError::SignalValue(Value::list([
                            Value::Symbol("invalid-function".into()),
                            Value::Record(id),
                        ])));
                    };
                    (inner, byte_code_function_uses_dynamic_binding(record))
                };
                // Unwrapping the record is still the same Ffuncall entry.
                if uses_dynamic_binding {
                    self.push_lambda_capture_override(false);
                    let result =
                        self.call_function_value_named(inner, original_name, args, env, funcall);
                    self.pop_lambda_capture_override();
                    result
                } else {
                    self.call_function_value_named(inner, original_name, args, env, funcall)
                }
            }
            Value::Lambda(ref lambda) => {
                let params = &lambda.params;
                let body = &lambda.body;
                let wrong_arity = || {
                    LispError::SignalValue(Value::list([
                        Value::Symbol("wrong-number-of-arguments".into()),
                        func,
                        Value::Integer(args.len() as i64),
                    ]))
                };
                // funcall_lambda signals the arity error after Ffuncall has
                // recorded the frame, so the offending call is the
                // innermost frame the handlers and backtraces see.
                let signal_arity = |this: &mut Self, env: &mut Env| -> LispError {
                    let function = original_name
                        .map(CallName::original_symbol_value)
                        .unwrap_or_else(|| func);
                    this.with_backtrace_frame(function, args, |interp| {
                        interp
                            .settle_frame_result(Err(wrong_arity()), env)
                            .err()
                            .unwrap_or_else(wrong_arity)
                    })
                };
                if params.len() != args.len() {
                    let min_params = params
                        .iter()
                        .position(|p| p == "&optional" || p == "&rest")
                        .unwrap_or(params.len());
                    if args.len() < min_params {
                        if std::env::var_os("EMAXX_DBG_ARITY").is_some() {
                            eprintln!(
                                "EMAXX-DBG arity: params={params:?} args={args:?} name={original_name:?} body_head={:?}",
                                body.first()
                            );
                        }
                        return Err(signal_arity(self, env));
                    }
                    // GNU also signals on EXCESS arguments (no &rest and more
                    // args than fixed + optional parameters).
                    if !params.iter().any(|p| p == "&rest") {
                        let max_params = params.iter().filter(|p| *p != "&optional").count();
                        if args.len() > max_params {
                            if std::env::var_os("EMAXX_DBG_ARITY").is_some() {
                                eprintln!(
                                    "EMAXX-DBG arity-excess: params={params:?} args={args:?} name={original_name:?}",
                                );
                            }
                            return Err(signal_arity(self, env));
                        }
                    }
                }

                // GNU binds an interpreted function's arguments LEXICALLY
                // even when a variable of the same name is special: in
                // lexical-binding code, "function arguments are always
                // statically scoped" (bug#47552).
                let mut frame = Vec::new();
                let mut arg_idx = 0;
                let mut optional = false;
                let mut rest = false;

                for param in params.iter() {
                    if param == "&optional" {
                        optional = true;
                        continue;
                    }
                    if param == "&rest" {
                        rest = true;
                        continue;
                    }
                    if rest {
                        let rest_args: Vec<Value> = args.get(arg_idx..).unwrap_or(&[]).to_vec();
                        frame.push((*param, Self::stored_value(Value::list(rest_args))));
                        break;
                    }
                    let consumed_arg = arg_idx < args.len();
                    let val = if consumed_arg {
                        args[arg_idx]
                    } else if optional {
                        Value::Nil
                    } else {
                        return Err(signal_arity(self, env));
                    };
                    frame.push((*param, Self::stored_value(val)));
                    if consumed_arg {
                        arg_idx += 1;
                    }
                }
                let backtrace_function = original_name
                    .map(CallName::original_symbol_value)
                    .unwrap_or_else(|| func);
                self.with_backtrace_frame(backtrace_function, args, |interp| {
                    // funcall_lambda: a closure's arguments are consed onto
                    // its environment (lexically, whatever the names'
                    // flags) and the result installed as one frame; a
                    // dynamic lambda's are specbound under a nil frame, so
                    // its body sees no caller's lexical binding.  The
                    // context stack keeps the dialect for the `let's and
                    // lambdas of the body.
                    let lexical_closure = lambda.environment().is_some();
                    let call_capture_override = (interp.lambda_capture_override()
                        != Some(lexical_closure))
                    .then_some(lexical_closure);
                    if let Some(capture) = call_capture_override {
                        interp.push_lambda_eval_context(capture);
                    }
                    let depth = env.len();
                    let count = interp.specpdl_index();
                    let setup = match lambda.environment() {
                        Some(closure_env) => {
                            let mut lexenv = *closure_env;
                            for (name, value) in &frame {
                                lexenv = Self::cons_binding(*name, *value, lexenv);
                            }
                            env.push(EnvFrame::from_alist(lexenv));
                            Ok(())
                        }
                        None => {
                            env.push(EnvFrame::dynamic());
                            frame.iter().try_for_each(|(name, value)| {
                                interp.specbind_symbol(name, *value, env)
                            })
                        }
                    };
                    interp
                        .backtrace_frames
                        .last_mut()
                        .expect("just pushed frame")
                        .detail_mut()
                        .locals = frame;
                    interp.capture_current_backtrace_context(
                        original_name.map(CallName::as_str),
                        env,
                        None,
                    );
                    crate::lisp::native_comp::maybe_gc(interp, env);
                    let result = match setup {
                        Ok(()) => interp.sf_progn(function_executable_body(body), env),
                        Err(error) => Err(error),
                    };
                    env.truncate(depth);
                    let unbind = interp.unbind_to(count, env);
                    let result = match result {
                        Ok(value) => unbind.map(|()| value),
                        Err(error) => Err(error),
                    };
                    if call_capture_override.is_some() {
                        interp.pop_lambda_capture_override();
                    }
                    interp.settle_frame_result(result, env)
                })
            }
            Value::Nil => Err(LispError::SignalValue(Value::list([
                Value::Symbol("void-function".into()),
                Value::Nil,
            ]))),
            other => Err(LispError::SignalValue(Value::list([
                Value::Symbol("invalid-function".into()),
                other,
            ]))),
        }
    }

    // ── Special forms ──
}

#[cfg(test)]
mod eval_value_buffer_tests {
    use super::*;

    #[test]
    fn load_path_forwarding_preserves_the_original_list() {
        let mut interpreter = Interpreter::new();
        interpreter.set_load_path(vec![PathBuf::from("/source/lisp")]);
        let first = interpreter
            .symbol_value_cell("load-path")
            .expect("load-path is bound");
        let second = interpreter
            .symbol_value_cell("load-path")
            .expect("load-path is bound");
        assert_eq!(
            primitives::call(&mut interpreter, "eq", &[first, second], &mut Env::new())
                .expect("ordinary primitive succeeds"),
            Value::T,
            "lread.c Vload_path and data.c Lisp_Fwd_Obj return the stored list"
        );
    }

    #[test]
    fn load_path_forwarding_exposes_spliced_directories() {
        let mut interpreter = Interpreter::new();
        interpreter.set_load_path(vec![PathBuf::from("/source/lisp")]);
        let original = interpreter
            .symbol_value_cell("load-path")
            .expect("load-path is bound");
        let tail = Value::list([Value::string("/source/lisp/emacs-lisp")]);
        primitives::call(
            &mut interpreter,
            "setcdr",
            &[original, tail],
            &mut Env::new(),
        )
        .expect("ordinary primitive succeeds");
        let current = interpreter
            .symbol_value_cell("load-path")
            .expect("load-path is bound");
        assert_eq!(
            primitives::call(
                &mut interpreter,
                "eq",
                &[current.cdr().expect("load-path is a cons"), tail],
                &mut Env::new(),
            )
            .expect("ordinary primitive succeeds"),
            Value::T,
            "GNU startup.el splices subdirectories into the live Vload_path list"
        );
        assert_eq!(
            interpreter.configured_load_path(),
            [
                PathBuf::from("/source/lisp"),
                PathBuf::from("/source/lisp/emacs-lisp")
            ]
        );
    }

    #[test]
    fn load_path_forwarding_tracks_binding_restore_and_buffer_selection() {
        let mut interpreter = Interpreter::new();
        let mut env = Env::new();
        interpreter.set_load_path(vec![PathBuf::from("/source/lisp")]);
        let original = interpreter
            .symbol_value_cell("load-path")
            .expect("load-path is bound");
        let temporary = Value::list([Value::string("/temporary")]);
        let restore = interpreter
            .bind_special_dynamic("load-path", temporary, &mut env)
            .expect("bind forwarded load-path");
        assert!(primitives::values_eq_in_env(
            &interpreter,
            &interpreter.load_path,
            &temporary,
            &env
        ));
        interpreter
            .restore_special_dynamic(restore, &mut env)
            .expect("restore forwarded load-path");
        assert!(primitives::values_eq_in_env(
            &interpreter,
            &interpreter.load_path,
            &original,
            &env
        ));

        let first_buffer = interpreter.current_buffer_id();
        let (second_buffer, _) = interpreter.create_buffer(" *load-path forwarding*");
        interpreter.set_buffer_local_value(first_buffer, "load-path", temporary);
        assert!(primitives::values_eq_in_env(
            &interpreter,
            &interpreter.load_path,
            &temporary,
            &env
        ));
        interpreter
            .set_current_buffer_id(second_buffer)
            .expect("select default-path buffer");
        assert!(primitives::values_eq_in_env(
            &interpreter,
            &interpreter.load_path,
            &original,
            &env
        ));
        interpreter
            .set_current_buffer_id(first_buffer)
            .expect("select local-path buffer");
        assert!(primitives::values_eq_in_env(
            &interpreter,
            &interpreter.load_path,
            &temporary,
            &env
        ));
        interpreter.remove_buffer_local_value(first_buffer, "load-path");
        assert!(primitives::values_eq_in_env(
            &interpreter,
            &interpreter.load_path,
            &original,
            &env
        ));
    }

    #[test]
    fn load_path_forwarding_keeps_detached_c_roots_without_stale_snapshots() {
        // The old root is a Rust local of the frame that holds it (the
        // conservative scan reads it as a C local): that frame is the
        // helper's, dead and cleared before the census that expects the
        // root released.
        #[inline(never)]
        fn before_replacement(interpreter: &mut Interpreter) -> (Value, Value) {
            let mut env = Env::new();
            let original = interpreter
                .symbol_value_cell("load-path")
                .expect("load-path is bound");
            let table = primitives::call(
                interpreter,
                "make-hash-table",
                &[
                    Value::symbol(":test"),
                    Value::symbol("eq"),
                    Value::symbol(":weakness"),
                    Value::symbol("key"),
                ],
                &mut env,
            )
            .expect("ordinary primitive succeeds");
            interpreter.set_global_binding("weak-table-root", table);
            // The negative control is consed in a frame of its own and the
            // stack under it cleared, or the conservative scan would read
            // it (a C local's object).
            #[inline(never)]
            fn insert_keys(interpreter: &mut Interpreter, table: &Value, original: &Value) {
                for key in [*original, Value::cons(Value::Integer(7), Value::Nil)] {
                    primitives::call(
                        interpreter,
                        "puthash",
                        &[key, Value::T, *table],
                        &mut Env::new(),
                    )
                    .expect("ordinary primitive succeeds");
                }
            }
            insert_keys(interpreter, &table, &original);
            crate::lisp::alloc::clobber_stack();
            primitives::call(
                interpreter,
                "makunbound",
                &[Value::symbol("load-path")],
                &mut env,
            )
            .expect("ordinary primitive succeeds");
            assert!(interpreter.symbol_value_cell("load-path").is_err());
            let plain = Value::list([Value::string("/plain")]);
            interpreter.set_symbol_value_cell("load-path", plain);
            assert!(primitives::values_eq_in_env(
                interpreter,
                &interpreter.load_path,
                &original,
                &env
            ));
            let reachability = interpreter.weak_hash_reachability(&env, &[]);
            let Value::Record(id) = table else {
                panic!("hash table")
            };
            let (_, entries, retained) = reachability
                .tables
                .iter()
                .find(|(key, _, _)| *key == id)
                .expect("weak table participates in root traversal");
            assert_eq!(entries.len(), 2, "rooted key and unrooted negative control");
            assert_eq!(retained.len(), 2);
            for ((key, _), retained) in entries.iter().zip(retained) {
                assert_eq!(
                    *retained,
                    primitives::values_eq_in_env(interpreter, key, &original, &env)
                );
            }
            (table, plain)
        }
        let mut interpreter = Interpreter::new();
        let env = Env::new();
        interpreter.set_load_path(vec![PathBuf::from("/source/lisp")]);
        let (table, plain) = before_replacement(&mut interpreter);
        crate::lisp::alloc::clobber_stack();
        interpreter.set_load_path(vec![PathBuf::from("/replacement")]);
        assert!(primitives::values_eq_in_env(
            &interpreter,
            &interpreter
                .symbol_value_cell("load-path")
                .expect("load-path is bound"),
            &plain,
            &env
        ));
        let reachability = interpreter.weak_hash_reachability(&env, &[]);
        let Value::Record(id) = table else {
            panic!("hash table")
        };
        let (_, _, retained) = reachability
            .tables
            .iter()
            .find(|(key, _, _)| *key == id)
            .expect("weak table participates in root traversal");
        assert_eq!(retained.len(), 2);
        assert!(
            retained.iter().all(|retained| !retained),
            "C replacement releases the old root"
        );
    }

    #[test]
    fn load_path_forwarding_image_copy_preserves_sharing_without_sharing_the_template() {
        let mut interpreter = Interpreter::new();
        interpreter.set_load_path(vec![PathBuf::from("/source/lisp")]);
        let original = interpreter
            .symbol_value_cell("load-path")
            .expect("load-path is bound");
        interpreter.set_global_binding("saved-load-path", original);
        let copied = interpreter.deep_clone_image();
        let copied_path = copied
            .symbol_value_cell("load-path")
            .expect("load-path is bound");
        assert!(primitives::values_eq_in_env(
            &copied,
            &copied_path,
            &copied.load_path,
            &Env::new()
        ));
        assert!(primitives::values_eq_in_env(
            &copied,
            &copied_path,
            &copied
                .symbol_value_cell("saved-load-path")
                .expect("saved list is bound"),
            &Env::new()
        ));
        assert!(!primitives::values_eq_in_env(
            &copied,
            &copied_path,
            &original,
            &Env::new()
        ));
        copied_path
            .set_cdr(Value::list([Value::string("/extra")]))
            .expect("mutate copied list");
        assert!(original.cdr().expect("load-path is a cons").is_nil());
        // This checks the existing in-process fixture copier only, not pdumper.
    }

    #[test]
    fn load_path_forwarding_loader_uses_the_c_slot_after_makunbound() {
        let mut interpreter = Interpreter::new();
        let mut env = Env::new();
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../emacs/lisp/emacs-lisp")
            .canonicalize()
            .expect("GNU fixture directory");
        let source = root.join("seq.el");
        assert!(source.is_file(), "unchanged GNU fixture must exist");
        interpreter.set_load_path(vec![root]);
        interpreter.set_variable(
            "load-suffixes",
            Value::list([Value::string(".el")]),
            &mut env,
        );
        for detached in [false, true] {
            if detached {
                primitives::call(
                    &mut interpreter,
                    "makunbound",
                    &[Value::symbol("load-path")],
                    &mut env,
                )
                .expect("ordinary primitive succeeds");
                interpreter.set_symbol_value_cell(
                    "load-path",
                    Value::list([Value::string("/not-the-c-path")]),
                );
            }
            assert_eq!(
                primitives::resolve_load_target_in_env(&mut interpreter, "seq", &env)
                    .expect("resolve GNU source"),
                Some(source.clone()),
                "lread.c:Fload passes Vload_path to openp, detached={detached}"
            );
        }
    }

    #[test]
    fn lexical_binding_symbols_are_gc_roots_before_closure_projection() {
        let interpreter = Interpreter::new();
        let lisp_name = Value::string("binding");
        let name = SymbolName::make_uninterned(lisp_name, "binding", 1);
        let key = Value::Symbol(name);
        let env = crate::lisp::types::Env::from_vec(vec![EnvFrame::bindings(
            [(name, Value::Integer(7))],
            &Value::Nil,
        )]);
        let mut marked = LispReachability::default();
        marked.mark_env(&interpreter, &env);
        assert!(
            marked.contains(&key),
            "alloc.c marks the symbol car of a lexical binding"
        );
        assert!(marked.contains(&lisp_name), "alloc.c marks SYMBOL_NAME");

        let function = Value::lambda(vec![name].into(), vec![Value::Nil].into(), Value::Nil);
        let mut marked = LispReachability::default();
        marked.mark(&interpreter, &function);
        assert!(
            marked.contains(&key),
            "closure parameters retain their symbol objects"
        );
        assert!(!LispReachability::default().contains(&key));
    }

    #[test]
    fn eval_depth_limit_follows_eval_c_post_increment_floor() {
        let mut interpreter = Interpreter::new();
        interpreter.set_symbol_value_cell("max-lisp-eval-depth", Value::Integer(50));

        interpreter.lisp_eval_depth = 50;
        assert!(!interpreter.lisp_eval_depth_exceeded());
        assert_eq!(
            interpreter
                .symbol_value_cell("max-lisp-eval-depth")
                .expect("forwarded depth cell"),
            Value::Integer(50)
        );

        interpreter.lisp_eval_depth = 51;
        assert!(!interpreter.lisp_eval_depth_exceeded());
        assert_eq!(
            interpreter
                .symbol_value_cell("max-lisp-eval-depth")
                .expect("raised forwarded depth cell"),
            Value::Integer(100)
        );

        interpreter.lisp_eval_depth = 101;
        assert!(interpreter.lisp_eval_depth_exceeded());
    }

    #[test]
    fn funcall_depth_limit_follows_eval_c_post_increment_floor() {
        let mut interpreter = Interpreter::new();
        let mut environment = Env::new();
        interpreter.set_symbol_value_cell("max-lisp-eval-depth", Value::Integer(50));

        interpreter.lisp_eval_depth = 50;
        interpreter
            .begin_funcall(&mut environment)
            .expect("eval.c raises a sub-100 limit before rejecting the call");
        assert_eq!(interpreter.lisp_eval_depth, 51);
        assert_eq!(interpreter.max_lisp_eval_depth_value(), 100);
        interpreter.end_funcall();
        assert_eq!(interpreter.lisp_eval_depth, 50);

        interpreter.lisp_eval_depth = 100;
        let error = interpreter
            .begin_funcall(&mut environment)
            .expect_err("eval.c rejects the first call beyond the raised limit");
        assert_eq!(error.condition_type(), "excessive-lisp-nesting");
        assert_eq!(interpreter.lisp_eval_depth, 100);
    }

    #[test]
    fn eval_sub_processes_quit_before_form_dispatch() {
        let mut interpreter = Interpreter::new();
        let mut environment = Env::new();
        interpreter.set_symbol_value_cell("quit-flag", Value::T);
        let form = Value::list([Value::symbol("quote"), Value::symbol("unreached")]);

        match interpreter.eval(&form, &mut environment) {
            Err(LispError::SignalValue(value)) => {
                assert_eq!(value, Value::list([Value::symbol("quit")]))
            }
            other => panic!("eval_sub must process the pending quit first, got {other:?}"),
        }
        assert_eq!(interpreter.lisp_eval_depth, 0);
        assert_eq!(
            interpreter
                .symbol_value_cell("quit-flag")
                .expect("forwarded quit cell"),
            Value::Nil
        );
    }

    #[test]
    fn forwarded_eval_cells_follow_current_buffer() {
        let mut interpreter = Interpreter::new();
        let original_buffer = interpreter.current_buffer_id();
        let (other_buffer, _) = interpreter.create_buffer(" *forwarded-cell-test*");

        interpreter.set_buffer_local_value(
            original_buffer,
            "debug-on-next-call",
            Value::symbol("non-nil"),
        );
        assert!(interpreter.debug_on_next_call());
        assert_eq!(
            interpreter
                .symbol_value_cell("debug-on-next-call")
                .expect("localized bool cell"),
            Value::T
        );

        interpreter
            .set_current_buffer_id(other_buffer)
            .expect("switch to buffer using the default cell");
        assert!(!interpreter.debug_on_next_call());

        interpreter
            .set_current_buffer_id(original_buffer)
            .expect("switch back to buffer using the local cell");
        assert!(interpreter.debug_on_next_call());
        interpreter.remove_buffer_local_value(original_buffer, "debug-on-next-call");
        assert!(!interpreter.debug_on_next_call());
    }

    #[test]
    fn position_flag_relocation_tracks_alias_bindings_and_buffer_selection() {
        let mut interpreter = Interpreter::new();
        let mut env = Env::new();
        let flag = Value::symbol("symbols-with-pos-enabled");
        let relocation = interpreter.symbols_with_positions_relocation();
        // comp.c gives native code this address. The interpreter owns the
        // boxed cell for the whole test; all reads are on this same thread.
        let relocated_value = || unsafe { relocation.read() };
        assert!(
            !relocated_value(),
            "data.c initializes the raw C flag false"
        );
        interpreter.set_symbol_value_cell("symbols-with-pos-enabled", Value::Integer(17));
        assert!(relocated_value());
        assert_eq!(
            interpreter
                .symbol_value_cell("symbols-with-pos-enabled")
                .expect("a forwarded bool reads back its normalized value"),
            Value::T
        );
        primitives::call(
            &mut interpreter,
            "set-default",
            &[flag, Value::Nil],
            &mut env,
        )
        .expect("store directly through the forwarded default");
        assert!(!relocated_value());
        let alias = Value::symbol("position-flag-alias");
        primitives::call(&mut interpreter, "defvaralias", &[alias, flag], &mut env)
            .expect("alias an ordinary symbol to the forwarded variable");
        let restore = interpreter
            .bind_special_dynamic("position-flag-alias", Value::Integer(19), &mut env)
            .expect("specbind resolves the alias before binding the C cell");
        assert!(relocated_value());
        interpreter
            .restore_special_dynamic(restore, &mut env)
            .expect("unbind restores the original forwarded value");
        assert!(!relocated_value());

        let original = interpreter.current_buffer_id();
        let (other, _) = interpreter.create_buffer(" *position-flag-local*");
        primitives::call(
            &mut interpreter,
            "make-local-variable",
            std::slice::from_ref(&flag),
            &mut env,
        )
        .expect("localize the forwarded cell");
        primitives::call(&mut interpreter, "set", &[alias, Value::T], &mut env)
            .expect("set the selected local value through its alias");
        assert!(relocated_value());
        interpreter
            .set_current_buffer_id(other)
            .expect("select the buffer using the default cell");
        assert!(!relocated_value(), "the other buffer selects the default");
        interpreter.set_buffer_local_value(original, "symbols-with-pos-enabled", Value::Nil);
        assert!(
            !relocated_value(),
            "an inactive local cell is not the C cell"
        );
        interpreter
            .set_current_buffer_id(original)
            .expect("select the buffer with a local cell");
        assert!(!relocated_value());
        primitives::call(&mut interpreter, "set-default", &[flag, Value::T], &mut env)
            .expect("store the non-selected default without changing the local");
        assert!(!relocated_value());
        interpreter
            .set_current_buffer_id(other)
            .expect("select the changed default");
        assert!(relocated_value());
        interpreter
            .set_current_buffer_id(original)
            .expect("select the unchanged local value");
        assert!(!relocated_value());
        primitives::call(&mut interpreter, "kill-local-variable", &[flag], &mut env)
            .expect("removing the local binding reloads the default into C");
        assert!(relocated_value());
        assert_eq!(relocation, interpreter.symbols_with_positions_relocation());
    }

    #[test]
    fn position_flag_relocation_survives_moves_and_clones_independently() {
        let mut interpreter = Interpreter::new();
        interpreter.set_symbol_value_cell("symbols-with-pos-enabled", Value::T);
        let original = interpreter.symbols_with_positions_relocation();
        let moved = Box::new(interpreter);
        assert_eq!(original, moved.symbols_with_positions_relocation());
        let mut cloned = moved.as_ref().clone();
        let copied = cloned.symbols_with_positions_relocation();
        assert_ne!(original, copied);
        // Both interpreters remain alive; cloning raw state without loaded
        // libraries must not make their C slots share one allocation.
        assert!(unsafe { original.read() && copied.read() });
        cloned.set_symbol_value_cell("symbols-with-pos-enabled", Value::Nil);
        assert!(unsafe { original.read() });
        assert!(!unsafe { copied.read() });
    }

    #[test]
    fn makunbound_disconnects_all_direct_eval_fields() {
        for (name, initial, c_value) in [
            ("quit-flag", Value::Integer(17), Value::Integer(17)),
            ("inhibit-quit", Value::Integer(17), Value::Integer(17)),
            ("throw-on-input", Value::Integer(17), Value::Integer(17)),
            (
                "overriding-plist-environment",
                Value::Integer(17),
                Value::Integer(17),
            ),
            (
                "max-lisp-eval-depth",
                Value::Integer(50),
                Value::Integer(50),
            ),
            ("debug-on-next-call", Value::Integer(17), Value::T),
            ("symbols-with-pos-enabled", Value::Integer(17), Value::T),
        ] {
            let mut interpreter = Interpreter::new();
            let mut env = Env::new();
            let symbol = Value::symbol(name);
            interpreter.set_symbol_value_cell(name, initial);
            for _ in 0..2 {
                primitives::call(
                    &mut interpreter,
                    "makunbound",
                    std::slice::from_ref(&symbol),
                    &mut env,
                )
                .expect("data.c detaches the symbol without changing its C slot");
                assert!(interpreter.symbol_value_cell(name).is_err(), "{name}");
                assert_eq!(interpreter.forwarded_c_value(name, &env), Some(c_value));
                let plain = Value::string("uncoerced plain value");
                primitives::call(&mut interpreter, "set", &[symbol, plain], &mut env)
                    .expect("a detached plain symbol has no forwarded type restriction");
                interpreter.refresh_forwarded_eval_cells();
                assert_eq!(
                    interpreter.symbol_value_cell(name).expect("plain binding"),
                    plain
                );
                assert_eq!(interpreter.forwarded_c_value(name, &env), Some(c_value));
                assert_eq!(
                    interpreter.detached_forwarded_variables.get(name),
                    Some(&Value::Nil)
                );
            }
        }
    }

    #[test]
    fn c_eval_writes_do_not_overwrite_detached_lisp_bindings() {
        let mut interpreter = Interpreter::new();
        let mut env = Env::new();
        for (name, value) in [
            ("quit-flag", Value::T),
            ("max-lisp-eval-depth", Value::Integer(50)),
        ] {
            interpreter.set_symbol_value_cell(name, value);
            primitives::call(
                &mut interpreter,
                "makunbound",
                &[Value::symbol(name)],
                &mut env,
            )
            .expect("detach forwarded symbol");
            primitives::call(
                &mut interpreter,
                "set",
                &[Value::symbol(name), Value::string("plain")],
                &mut env,
            )
            .expect("store plain value");
        }
        match interpreter.maybe_quit(&mut env) {
            Err(LispError::SignalValue(value)) => {
                assert_eq!(value, Value::list([Value::symbol("quit")]));
            }
            other => panic!("process_quit_flag must consume the C flag: {other:?}"),
        }
        interpreter.maybe_quit(&mut env).expect("C flag is now nil");
        assert_eq!(
            interpreter.forwarded_c_value("quit-flag", &env),
            Some(Value::Nil)
        );
        interpreter.lisp_eval_depth = 51;
        assert!(!interpreter.lisp_eval_depth_exceeded());
        assert_eq!(interpreter.max_lisp_eval_depth_value(), 100);
        for name in ["quit-flag", "max-lisp-eval-depth"] {
            assert_eq!(
                interpreter.symbol_value_cell(name).expect("plain value"),
                Value::string("plain")
            );
        }
    }

    #[test]
    fn c_slot_and_incoming_main_objects_are_gc_roots() {
        let mut interpreter = Interpreter::new();
        let mut env = Env::new();
        let table = primitives::call(
            &mut interpreter,
            "make-hash-table",
            &[
                Value::symbol(":test"),
                Value::symbol("eq"),
                Value::symbol(":weakness"),
                Value::symbol("key"),
            ],
            &mut env,
        )
        .expect("weak-key table");
        let Value::Record(table_id) = table else {
            panic!("hash table record")
        };
        interpreter.set_global_binding("weak-table-root", Value::Record(table_id));
        let keys: Vec<Value> = (0..9)
            .map(|n| Value::cons(Value::Integer(n), Value::Nil))
            .collect();
        for (index, key) in keys.iter().enumerate() {
            primitives::call(
                &mut interpreter,
                "puthash",
                &[*key, Value::Integer(index as i64), Value::Record(table_id)],
                &mut env,
            )
            .expect("weak entry");
        }
        for (name, key) in [
            "quit-flag",
            "inhibit-quit",
            "throw-on-input",
            "overriding-plist-environment",
        ]
        .into_iter()
        .zip(&keys)
        {
            interpreter.set_symbol_value_cell(name, *key);
            primitives::call(
                &mut interpreter,
                "makunbound",
                &[Value::symbol(name)],
                &mut env,
            )
            .expect("C slot remains independently rooted");
        }
        interpreter
            .detached_forwarded_variables
            .insert("text-quoting-style".into(), keys[4]);
        interpreter.pending_thread_events.push(keys[5]);
        interpreter.coding_systems[0].charset_list = keys[6];
        interpreter.coding_systems[0].type_args = vec![keys[7]];
        let marked = interpreter.weak_hash_reachability(&env, &[]);
        let (_, entries, keep) = marked
            .tables
            .iter()
            .find(|(id, _, _)| *id == table_id)
            .expect("marked weak table");
        assert_eq!(entries.len(), 9);
        for ((_, index), retained) in entries.iter().zip(keep) {
            assert_eq!(
                *retained,
                index != &Value::Integer(8),
                "unrooted negative control must be rejected"
            );
        }

        // Once C clears the quit slot, no detachment snapshot may keep its
        // former object alive. The other seven independently rooted keys stay.
        interpreter.quit_flag = Value::Nil;
        let marked = interpreter.weak_hash_reachability(&env, &[]);
        let (_, entries, keep) = marked
            .tables
            .iter()
            .find(|(id, _, _)| *id == table_id)
            .expect("marked weak table");
        for ((_, index), retained) in entries.iter().zip(keep) {
            assert_eq!(
                *retained,
                index != &Value::Integer(0) && index != &Value::Integer(8)
            );
        }
    }

    #[test]
    fn image_copy_preserves_detached_c_slots_and_new_main_children() {
        let mut interpreter = Interpreter::new();
        let mut env = Env::new();
        let c_value = Value::cons(Value::Integer(1), Value::Nil);
        let plain = Value::cons(Value::Integer(2), Value::Nil);
        interpreter.set_symbol_value_cell("quit-flag", c_value);
        primitives::call(
            &mut interpreter,
            "makunbound",
            &[Value::symbol("quit-flag")],
            &mut env,
        )
        .expect("detach C field");
        interpreter.set_symbol_value_cell("quit-flag", plain);
        interpreter.set_symbol_value_cell("inhibit-quit", c_value);
        interpreter
            .detached_forwarded_variables
            .insert("text-quoting-style".into(), c_value);
        interpreter.pending_thread_events.push(c_value);
        interpreter.coding_systems[0].charset_list = c_value;
        interpreter.coding_systems[0].type_args = vec![c_value];
        let mut copied = interpreter.deep_clone_image();
        let copied_c = copied.quit_flag_value();
        assert!(!primitives::values_eq_in_env(
            &copied, &copied_c, &c_value, &env
        ));
        assert_eq!(copied_c, c_value);
        for child in [
            copied.inhibit_quit,
            copied
                .symbol_value_cell("inhibit-quit")
                .expect("still forwarded"),
            copied.detached_forwarded_variables["text-quoting-style"],
            copied.pending_thread_events[0],
            copied.coding_systems[0].charset_list,
            copied.coding_systems[0].type_args[0],
        ] {
            assert!(
                primitives::values_eq_in_env(&copied, &copied_c, &child, &env),
                "copied graph must preserve aliasing"
            );
        }
        let copied_plain = copied
            .symbol_value_cell("quit-flag")
            .expect("independent Lisp binding");
        assert_eq!(copied_plain, plain);
        assert!(!primitives::values_eq_in_env(
            &copied,
            &copied_plain,
            &plain,
            &env
        ));
        assert!(!primitives::values_eq_in_env(
            &copied,
            &copied_plain,
            &copied_c,
            &env
        ));
        primitives::call(
            &mut copied,
            "setcar",
            &[copied_c, Value::Integer(9)],
            &mut env,
        )
        .expect("mutate copied C graph");
        assert_eq!(c_value, Value::cons(Value::Integer(1), Value::Nil));
        assert_eq!(copied_plain, plain);
    }
}
