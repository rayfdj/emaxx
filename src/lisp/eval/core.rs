use super::*;
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

const EVAL_VALUE_BUFFER_POOL_LIMIT: usize = 128;
const EVAL_VALUE_BUFFER_CAPACITY_LIMIT: usize = 256;
const SOURCE_FORM_ITEMS_CACHE_LIMIT: usize = 1 << 18;

thread_local! {
    static EVAL_VALUE_BUFFER_POOL: std::cell::RefCell<Vec<Vec<Value>>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

/// One temporary evaluated-argument vector.
///
/// Evaluation has many early-return paths for special forms and errors. RAII
/// guarantees that every path clears the vector before recycling its storage,
/// without making the evaluator's control flow responsible for pool hygiene.
struct EvalValueBuffer(Vec<Value>);

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
            Self::Symbol(name) => Value::Symbol(name.clone()),
            Self::Text(name) if name == resolved_name.as_str() => {
                Value::Symbol(resolved_name.clone())
            }
            Self::Text(name) => Value::Symbol(name.into()),
        }
    }

    fn original_symbol_value(self) -> Value {
        match self {
            Self::Symbol(name) => Value::Symbol(name.clone()),
            Self::Text(name) => Value::Symbol(name.into()),
        }
    }
}

impl EvalValueBuffer {
    fn take() -> Self {
        Self(EVAL_VALUE_BUFFER_POOL.with_borrow_mut(|pool| pool.pop().unwrap_or_default()))
    }
}

impl std::ops::Deref for EvalValueBuffer {
    type Target = Vec<Value>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for EvalValueBuffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Drop for EvalValueBuffer {
    fn drop(&mut self) {
        let mut buffer = std::mem::take(&mut self.0);
        buffer.clear();
        if buffer.capacity() <= EVAL_VALUE_BUFFER_CAPACITY_LIMIT {
            EVAL_VALUE_BUFFER_POOL.with_borrow_mut(|pool| {
                if pool.len() < EVAL_VALUE_BUFFER_POOL_LIMIT {
                    pool.push(buffer);
                }
            });
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
        pub(super) enum NativeForm {
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

pub(crate) fn is_special_form_name(name: &str) -> bool {
    crate::lisp::primitives::generated_gnu_c_primitive_special_form(name)
}

#[derive(Clone, Copy, Default)]
pub(super) enum SourceLiteralKind {
    #[default]
    None,
    Vector,
}

impl Interpreter {
    /// GNU treats a symbol-with-position in function position as its bare
    /// symbol while `symbols-with-pos-enabled' is non-nil.  The byte compiler
    /// enables that mode while macroexpanding source forms, so this is part of
    /// ordinary evaluator dispatch rather than merely a predicate detail.
    fn callable_symbol_name(&self, value: &Value, env: &Env) -> Option<SymbolName> {
        if let Value::Symbol(name) = value {
            return Some(name.clone());
        }
        if crate::lisp::primitives::symbols_with_pos_enabled(self, env)
            && let Some((Value::Symbol(name), _)) =
                crate::lisp::primitives::symbol_with_pos_parts(self, value)
        {
            return Some(name);
        }
        None
    }

    fn source_form_analysis(&mut self, source: &Value) -> Result<SourceFormAnalysis, LispError> {
        let Some((source_anchor, _)) = source.cons_cells() else {
            return Err(LispError::WrongTypeArgument("listp".into(), source.clone()));
        };
        let source_id = source_anchor.cell_id();
        if let Some(cached) = self
            .source_form_items_cache
            .get(&source_id)
            .and_then(ConsMutationStamped::current)
            && cached
                .source
                .upgrade()
                .is_some_and(|cached| cached.ptr_eq(&source_anchor))
        {
            return Ok(cached.analysis.clone());
        }

        let items = Rc::new(source.to_vec()?);
        // Flattening and head classification depend on this list's spine,
        // not on every mutable cons in the process.  `if' additionally
        // caches a bounded recursive property of its test form, so include
        // that subtree in the same validity snapshot.
        let mutations = crate::lisp::types::ConsMutationSnapshot::list_spine(source);
        let mut native_form = None;
        let mut literal_kind = SourceLiteralKind::None;
        if let Some(Value::Symbol(name)) = items.first() {
            // A Lisp symbol may select a Rust evaluator arm only when the
            // generated GNU C manifest owns that native surface.  In
            // particular, an Emaxx-private prefix is not an ownership
            // boundary and cannot turn an Elisp macro into a host fallback.
            native_form = crate::lisp::primitives::generated_gnu_c_primitive_available(name)
                .is_some_and(|available| available)
                .then(|| NativeForm::for_name(name))
                .flatten();
            literal_kind = match name.as_str() {
                "vector-literal" => SourceLiteralKind::Vector,
                _ => SourceLiteralKind::None,
            };
        }
        let analysis = SourceFormAnalysis {
            items,
            native_form,
            literal_kind,
            macro_calls: Rc::new(RefCell::new(SourceMacroCallCache::default())),
            function_call: Rc::new(RefCell::new(None)),
        };
        if self.source_form_items_cache.len() >= SOURCE_FORM_ITEMS_CACHE_LIMIT {
            self.source_form_items_cache.clear();
        }
        self.source_form_items_cache.insert(
            source_id,
            ConsMutationStamped::new(
                mutations,
                SourceFormCacheEntry {
                    source: source_anchor.downgrade(),
                    analysis: analysis.clone(),
                },
            ),
        );
        Ok(analysis)
    }

    pub fn eval(&mut self, expr: &Value, env: &mut Env) -> Result<Value, LispError> {
        let outermost = self.lisp_eval_depth == 0;
        if outermost {
            self.clear_batch_error_backtrace();
        }
        if let Some(termination) = self.pending_termination().cloned() {
            return Err(LispError::Terminate(termination));
        }
        if !matches!(expr, Value::Cons(_)) {
            let result = self.eval_inner(expr, env);
            if outermost && result.is_ok() {
                self.clear_batch_error_backtrace();
            }
            return result;
        }
        self.lisp_eval_depth += 1;
        // eval.c:2504-2509.  NOTE: GNU increments at TWO sites -- `eval_sub'
        // here and `Ffuncall' (eval.c:3078) -- while Emaxx increments only
        // here, so a `funcall'/`apply' chain counts 2 units per level where
        // GNU counts 3.  Emaxx therefore trips LATER on those paths, never
        // earlier, so no honest program fails that GNU accepts; the limit
        // simply means something slightly different there.  Tracked
        // separately rather than claimed as done.
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
        let limit = self.lisp_eval_depth_limit(env);
        if self.lisp_eval_depth > limit {
            let reached = self.lisp_eval_depth;
            self.lisp_eval_depth -= 1;
            return Err(LispError::SignalValue(Value::list([
                Value::symbol("excessive-lisp-nesting"),
                Value::Integer(reached as i64),
            ])));
        }
        // GNU grows `max-lisp-eval-depth' while C stack remains and signals
        // before the stack dies (eval.c near_C_stack_top).  The counter above
        // cannot see the actual stack, and a deep non-tail recursion can
        // exhaust even the 8 GiB batch thread before it trips (the pinned
        // semantic-utest-ia.el did exactly that, as a SIGABRT with no
        // report).  Mirror GNU's contract directly: when the running thread's
        // stack headroom falls below the margin, signal
        // `excessive-lisp-nesting' instead of crashing -- and signal it as
        // that CONDITION, which this arm previously did not do.  It raised a
        // plain `error' while the comment above claimed otherwise, so a
        // `condition-case' keyed on `recursion-error' missed it.
        if self.lisp_eval_depth.is_multiple_of(64) && !Self::stack_headroom_remains() {
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

    /// True while the current thread still has comfortable stack left.
    /// macOS reports the thread's stack extent exactly; the margin covers
    /// the deepest single native frame chain between two depth checks plus
    /// unwinding.  On other platforms the probe is inert (the counter
    /// guard above still applies).
    #[cfg(target_os = "macos")]
    fn stack_headroom_remains() -> bool {
        let approximate_sp = {
            let probe = 0u8;
            std::ptr::addr_of!(probe) as usize
        };
        unsafe {
            let thread = libc::pthread_self();
            let top = libc::pthread_get_stackaddr_np(thread) as usize;
            let size = libc::pthread_get_stacksize_np(thread);
            // The stack grows down from `top'; headroom is what remains
            // above the guard page.
            let bottom = top.saturating_sub(size);
            const MARGIN: usize = 48 * 1024 * 1024;
            approximate_sp > bottom.saturating_add(MARGIN)
        }
    }

    #[cfg(not(target_os = "macos"))]
    fn stack_headroom_remains() -> bool {
        true
    }

    /// The effective `max-lisp-eval-depth', read through the DYNAMIC binding
    /// so a `let' is honoured, with eval.c:2506's floor: a limit below 100 is
    /// raised to 100 rather than rejected.
    fn lisp_eval_depth_limit(&self, env: &Env) -> usize {
        // Clamp BEFORE converting: `usize::try_from(-5)' fails, so folding
        // the conversion into the default turned a negative limit into 1600 --
        // LARGER than requested, where eval.c:2506 floors it at 100.
        let requested = self
            .lookup_var("max-lisp-eval-depth", env)
            .and_then(|value| value.as_integer().ok())
            .unwrap_or(1600);
        usize::try_from(requested.max(100)).unwrap_or(100)
    }

    fn eval_inner(&mut self, expr: &Value, env: &mut Env) -> Result<Value, LispError> {
        match expr {
            Value::Nil
            | Value::T
            | Value::Integer(_)
            | Value::BigInteger(_)
            | Value::Float(_)
            | Value::StringObject(_) => Ok(expr.clone()),

            // Evaluating a string literal yields a string object with its
            // own identity, so `eq' distinguishes evaluations of distinct
            // literals while `(memq (car l) l)' still finds the element the
            // evaluation put there (GNU strings are always heap objects).
            Value::String(_) => Ok(Self::stored_value(expr.clone())),

            Value::Record(_)
                if crate::lisp::primitives::symbols_with_pos_enabled(self, env)
                    && crate::lisp::primitives::symbol_with_pos_parts(self, expr).is_some() =>
            {
                let Some((Value::Symbol(name), _)) =
                    crate::lisp::primitives::symbol_with_pos_parts(self, expr)
                else {
                    unreachable!("guard accepted a positioned symbol without a symbol slot");
                };
                match self.lookup(&name, env) {
                    Ok(value) => Ok(value),
                    Err(LispError::Void(_)) => Err(LispError::SignalValue(Value::list([
                        Value::Symbol("void-variable".into()),
                        expr.clone(),
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
            | Value::Unbound => Ok(expr.clone()),

            Value::ReaderForm(_) => self.materialize_read_object_literals(expr.clone()),

            Value::Symbol(name) => self.lookup(name, env),

            Value::Cons(_) => {
                let SourceFormAnalysis {
                    items,
                    native_form,
                    literal_kind,
                    macro_calls,
                    function_call,
                } = self.source_form_analysis(expr)?;
                if items.is_empty() {
                    return Ok(Value::Nil);
                }

                match literal_kind {
                    SourceLiteralKind::Vector => {
                        return self.materialize_read_object_literals(expr.clone());
                    }
                    SourceLiteralKind::None => {}
                }

                let callable_name = self.callable_symbol_name(&items[0], env);

                // Check for special forms first.  Source analysis can cache
                // this classification only for a bare symbol; positioned
                // symbols depend on the current dynamic mode and are handled
                // here on every evaluation.
                if let Some(ref name) = callable_name {
                    let direct_native_form = native_form.or_else(|| {
                        (!matches!(items[0], Value::Symbol(_))
                            && crate::lisp::primitives::generated_gnu_c_primitive_available(name)
                                .is_some_and(|available| available))
                        .then(|| NativeForm::for_name(name))
                        .flatten()
                    });
                    // Function aliases to special forms retain the target's
                    // unevaluated-argument calling convention.  GNU's
                    // `(defalias 'inline 'progn)' is the common case: treating
                    // INLINE as an ordinary function evaluates its forms and
                    // then attempts an invalid funcall of PROGN.
                    let effective_native_form = direct_native_form.or_else(|| {
                        let Value::BuiltinFunc(target) = self.lookup_function(name, env).ok()?
                        else {
                            return None;
                        };
                        if !is_special_form_name(&target) {
                            return None;
                        }
                        NativeForm::for_name(&target)
                    });
                    if let Some(native_form) = effective_native_form {
                        match native_form {
                            NativeForm::Quote => return self.sf_quote(&items),
                            NativeForm::If => return self.sf_if(&items, env),
                            NativeForm::And => return self.sf_and(&items, env),
                            NativeForm::Or => return self.sf_or(&items, env),
                            NativeForm::Cond => {
                                self.push_unevaluated_backtrace_frame(expr);
                                let result = self.sf_cond(&items, env);
                                self.pop_backtrace_frame();
                                return result;
                            }
                            NativeForm::Progn => return self.sf_progn(&items[1..], env),
                            NativeForm::Prog1 => return self.sf_prog1(&items, env),
                            NativeForm::Let => {
                                self.push_unevaluated_backtrace_frame(expr);
                                let result = self.sf_let(&items, env);
                                self.pop_backtrace_frame();
                                return result;
                            }
                            NativeForm::LetStar => {
                                self.push_unevaluated_backtrace_frame(expr);
                                let result = self.sf_letstar(&items, env);
                                self.pop_backtrace_frame();
                                return result;
                            }
                            NativeForm::Setq => {
                                self.push_unevaluated_backtrace_frame(expr);
                                let result = self.sf_setq(&items, env);
                                self.pop_backtrace_frame();
                                return result;
                            }
                            NativeForm::Defvar => return self.sf_defvar(&items, env),
                            NativeForm::Defconst => return self.sf_defconst(&items, env),
                            NativeForm::Function => {
                                if items.len() >= 2 {
                                    if let Value::Symbol(name) = &items[1] {
                                        return Ok(Value::Symbol(name.clone()));
                                    }
                                    if let Ok(name) = function_name_from_binding_form(&items[1]) {
                                        return Ok(Value::Symbol(name.into()));
                                    }
                                    if matches!(
                                        items[1].car(),
                                        Ok(Value::Symbol(ref head)) if head == "lambda"
                                    ) {
                                        let lambda_items = items[1].to_vec()?;
                                        return self.sf_lambda_from_source(
                                            &items[1],
                                            &lambda_items,
                                            env,
                                        );
                                    }
                                    return Ok(items[1].clone());
                                }
                                return Ok(Value::Nil);
                            }
                            NativeForm::Interactive => return Ok(Value::Nil),
                            NativeForm::While => {
                                self.push_unevaluated_backtrace_frame(expr);
                                let result = self.sf_while(&items, env);
                                self.pop_backtrace_frame();
                                return result;
                            }
                            NativeForm::UnwindProtect => {
                                return self.sf_unwind_protect(&items, env);
                            }
                            NativeForm::ConditionCase => {
                                return self.sf_condition_case(&items, env);
                            }
                            NativeForm::Catch => return self.sf_catch(&items, env),
                            NativeForm::SaveCurrentBuffer => {
                                return self.sf_save_current_buffer(&items, env);
                            }
                            NativeForm::SaveExcursion => {
                                return self.sf_save_excursion(&items, env);
                            }
                            NativeForm::SaveRestriction => {
                                return self.sf_save_restriction(&items, env);
                            }
                        }
                    }
                }

                // GNU's interpreted evaluator invokes a macro expander on
                // every evaluation.  Do not cache the resulting form: an
                // expander can inspect state, perform side effects, or create
                // fresh uninterned symbols.  Only the generation-stamped
                // negative "not a macro" verdict is reusable here.
                if let Some(name) = callable_name.as_ref()
                    && !self.source_call_known_not_macro(&macro_calls)
                {
                    if let Some(expanded) = self.try_macroexpand(name, &items[1..], env)? {
                        return self.eval(&expanded, env);
                    }
                    if self.macro_nonexpansion_is_callsite_cacheable(name) {
                        self.cache_source_not_macro(&macro_calls);
                    }
                }

                // Regular function call
                self.eval_call(expr, &items, &function_call, env)
            }
        }
    }

    pub(super) fn eval_call(
        &mut self,
        source_form: &Value,
        items: &[Value],
        source_resolution: &Rc<RefCell<Option<SourceFunctionCallCacheEntry>>>,
        env: &mut Env,
    ) -> Result<Value, LispError> {
        // GNU resolves the function cell before evaluating any argument.
        // Keep that observable ordering while retaining a direct native
        // verdict instead of materializing `BuiltinFunc' and throwing away
        // the name-facts cache on every ordinary source call.
        let callable_name = self.callable_symbol_name(&items[0], env);
        let prepared = if let Some(name) = callable_name.as_ref() {
            self.resolve_source_symbol_call(name, env, source_resolution)?
        } else {
            FunctionResolution::Resolved(self.eval(&items[0], env)?)
        };
        // While the arguments evaluate, the call is visible in backtraces as
        // an in-progress frame with its unevaluated argument forms, the way
        // GNU records the eval of a list form.
        let unevald_frame = callable_name.is_some();
        if unevald_frame {
            self.push_unevaluated_backtrace_frame(source_form);
        }
        let mut args = EvalValueBuffer::take();
        let mut arg_error = None;
        for item in &items[1..] {
            match self.eval(item, env) {
                Ok(value) => args.push(value),
                Err(error) => {
                    arg_error = Some(error);
                    break;
                }
            }
        }
        if unevald_frame {
            self.pop_backtrace_frame();
        }
        if let Some(error) = arg_error {
            return Err(error);
        }
        match (callable_name.as_ref(), prepared) {
            (Some(name), FunctionResolution::DirectBuiltin(facts)) => {
                self.dispatch_named_builtin(name, facts, Some(CallName::Symbol(name)), &args, env)
            }
            (Some(name), FunctionResolution::Resolved(func)) => {
                self.call_function_value_named(func, Some(CallName::Symbol(name)), &args, env)
            }
            (None, FunctionResolution::Resolved(func)) => {
                self.call_function_value_named(func, None, &args, env)
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
        self.call_function_value_named(func, original_name.map(CallName::Text), args, env)
    }

    fn call_function_value_named(
        &mut self,
        func: Value,
        original_name: Option<CallName<'_>>,
        args: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if let Some(termination) = self.pending_termination().cloned() {
            return Err(LispError::Terminate(termination));
        }
        // Dev-only flat profiler: EMAXX_PROFILE=<path> accumulates per-name
        // call counts and self-time, periodically rewriting <path>.
        if let Some(path) = profile_path() {
            let started = std::time::Instant::now();
            profile_enter();
            let result = self.call_function_value_inner(func, original_name, args, env);
            profile_leave(original_name.map(CallName::as_str), started.elapsed(), path);
            return result;
        }
        self.call_function_value_inner(func, original_name, args, env)
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
        let local_context = Self::env_may_affect_function_resolution(env);
        self.resolve_symbol_call_with_frame_state(name, env, local_context)
    }

    /// Resolve an ordinary source call through its callsite-local verdict.
    /// Symbolic `funcall' retains the global name cache above; both paths use
    /// the same generation and uncached resolution authority below.
    fn resolve_source_symbol_call(
        &mut self,
        name: &SymbolName,
        env: &Env,
        source_resolution: &Rc<RefCell<Option<SourceFunctionCallCacheEntry>>>,
    ) -> Result<FunctionResolution, LispError> {
        let local_context = Self::env_may_affect_function_resolution(env);
        if !local_context
            && let Some(cached) = source_resolution.borrow().as_ref()
            && cached.definition_generation == self.definition_generation
        {
            return Ok(cached.resolution.clone());
        }

        let resolution = self.resolve_symbol_call_with_frame_state(name, env, local_context)?;
        if !local_context {
            *source_resolution.borrow_mut() = Some(SourceFunctionCallCacheEntry {
                definition_generation: self.definition_generation,
                resolution: resolution.clone(),
            });
        }
        Ok(resolution)
    }

    fn resolve_symbol_call_with_frame_state(
        &mut self,
        name: &SymbolName,
        env: &Env,
        local_context: bool,
    ) -> Result<FunctionResolution, LispError> {
        if !local_context
            && let Some((generation, resolution)) =
                self.function_resolution_cache.get(name.as_str())
            && *generation == self.definition_generation
        {
            return Ok(resolution.clone());
        }

        let facts = crate::lisp::primitives::name_facts(name);
        let resolution = if name != "selected-window"
            && (facts.prefer_override
                || (facts.builtin && !facts.special_form && !self.function_index_has(name)))
            && !local_context
        {
            FunctionResolution::DirectBuiltin(facts)
        } else {
            FunctionResolution::Resolved(self.lookup_function(name, env)?)
        };
        if !local_context {
            self.function_resolution_cache.insert(
                name.to_string(),
                (self.definition_generation, resolution.clone()),
            );
        }
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
    ) -> Result<Value, LispError> {
        let backtrace_function = original_name
            .map(|original| original.symbol_value(name))
            .unwrap_or_else(|| Value::Symbol(name.clone()));
        self.push_backtrace_frame(backtrace_function, args);
        self.capture_current_backtrace_context(
            Some(original_name.map_or(name.as_str(), CallName::as_str)),
            env,
            None,
        );
        let result = match primitives::call_with_facts(self, name, facts, args, env) {
            Ok(value) => Ok(value),
            Err(error @ (LispError::Throw(_, _) | LispError::Terminate(_))) => Err(error),
            Err(error) => self.dispatch_handler_bindings(error, env),
        };
        if let Err(error) = &result {
            self.capture_batch_error_backtrace(error, env);
        }
        self.pop_backtrace_frame();
        result
    }

    /// Execute one GNU byte-code closure with the activation-frame contract
    /// that eval.c exposes to backtrace-frame/backtrace-eval.
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
        self.push_backtrace_frame(backtrace_function, args);
        self.capture_current_backtrace_context(original_name.map(CallName::as_str), env, None);
        // A genuine byte-code function starts a new evaluator scope just as
        // an interpreted lexical closure does.  Its Bvarbind opcodes update
        // special value cells, so native calls made by the VM must not see a
        // same-named lexical frame belonging to its caller first.  Advice
        // wrappers happened to introduce this boundary, which is why merely
        // advising `prin1' used to make package byte compilation work.
        let previous_floor = self.special_scan_floor;
        self.special_scan_floor = env.len();
        let result = crate::lisp::bytecode::vm::execute_record(self, record_id, args, env);
        self.special_scan_floor = previous_floor;
        if let Err(error) = &result {
            self.capture_batch_error_backtrace(error, env);
        }
        self.pop_backtrace_frame();
        result
    }

    fn call_function_value_inner(
        &mut self,
        func: Value,
        original_name: Option<CallName<'_>>,
        args: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        // eval.c/bytecode.c use XBARE_SYMBOL for a positioned callee while
        // the byte compiler's symbol-position mode is active.  This covers
        // explicit `funcall'/`apply' as well as ordinary source dispatch.
        let func = self
            .callable_symbol_name(&func, env)
            .map(Value::Symbol)
            .unwrap_or(func);
        // A record with a cached program is a genuine byte-code function
        // (only execute_record populates the cache), so skip the
        // lambda/autoload probes and the record-type guards below.
        if let Value::Record(id) = &func
            && (*id as usize)
                .checked_sub(1)
                .and_then(|index| self.bytecode_program_cache.get(index))
                .is_some_and(|slot| slot.is_some())
        {
            return self.execute_bytecode_record_named(*id, original_name, args, env);
        }
        let mut owned_name: Option<SymbolName> = None;
        let func = match func {
            Value::Symbol(name) => {
                let resolution = self.resolve_symbol_call(&name, env)?;
                if original_name.is_none() {
                    owned_name = Some(name.clone());
                }
                match resolution {
                    FunctionResolution::DirectBuiltin(facts) => {
                        let call_name = original_name.or(Some(CallName::Symbol(&name)));
                        return self.dispatch_named_builtin(&name, facts, call_name, args, env);
                    }
                    FunctionResolution::Resolved(value) => value,
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
                    match self.load_target_with_env(&file, env) {
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
            Value::BuiltinFunc(ref name) if name == "selected-window" => {
                if !args.is_empty() {
                    return Err(LispError::WrongNumberOfArgs(name.to_string(), args.len()));
                }
                Ok(self.selected_window_value())
            }
            Value::BuiltinFunc(ref name) => {
                let backtrace_function = original_name
                    .map(|original| original.symbol_value(name))
                    .unwrap_or_else(|| Value::Symbol(name.clone()));
                self.push_backtrace_frame(backtrace_function, args);
                self.capture_current_backtrace_context(
                    original_name.map(CallName::as_str),
                    env,
                    None,
                );
                let result = match primitives::call(self, name, args, env) {
                    Ok(value) => Ok(value),
                    Err(error @ (LispError::Throw(_, _) | LispError::Terminate(_))) => Err(error),
                    Err(error) => self.dispatch_handler_bindings(error, env),
                };
                if let Err(error) = &result {
                    self.capture_batch_error_backtrace(error, env);
                }
                self.pop_backtrace_frame();
                result
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
                    // Genuine GNU bytecode (argspec/code/constants/depth
                    // slots) executes on the VM; Emaxx byte-compile facade
                    // objects carry an executable lambda in slot 0 instead.
                    // A cached program implies the slots already passed the
                    // genuineness check, so skip re-walking them.
                    if (id as usize)
                        .checked_sub(1)
                        .and_then(|index| self.bytecode_program_cache.get(index))
                        .is_some_and(|slot| slot.is_some())
                        || crate::lisp::bytecode::slots_are_genuine_bytecode(&record.slots)
                    {
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
                if uses_dynamic_binding {
                    self.push_lambda_capture_override(false);
                    let result = self.call_function_value_named(inner, original_name, args, env);
                    self.pop_lambda_capture_override();
                    result
                } else {
                    self.call_function_value_named(inner, original_name, args, env)
                }
            }
            Value::Lambda(ref lambda) => {
                let params = &lambda.params;
                let body = &lambda.body;
                let closure_env = &lambda.env;
                self.register_captured_lexical_frames(closure_env);
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
                        return Err(LispError::WrongNumberOfArgs(
                            "lambda".to_string(),
                            args.len(),
                        ));
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
                            return Err(LispError::WrongNumberOfArgs(
                                "lambda".to_string(),
                                args.len(),
                            ));
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
                        frame.push((param.clone(), Self::stored_value(Value::list(rest_args))));
                        break;
                    }
                    let consumed_arg = arg_idx < args.len();
                    let val = if consumed_arg {
                        args[arg_idx].clone()
                    } else if optional {
                        Value::Nil
                    } else {
                        return Err(LispError::WrongNumberOfArgs(
                            "lambda".to_string(),
                            args.len(),
                        ));
                    };
                    frame.push((param.clone(), Self::stored_value(val)));
                    if consumed_arg {
                        arg_idx += 1;
                    }
                }
                let backtrace_function = original_name
                    .map(CallName::original_symbol_value)
                    .unwrap_or_else(|| func.clone());
                self.push_backtrace_frame_with_locals(
                    backtrace_function,
                    args.to_vec(),
                    frame.clone(),
                    true,
                );
                let frame = EnvFrame::with_identity(frame, Self::fresh_frame_identity());
                self.capture_current_backtrace_context(
                    original_name.map(CallName::as_str),
                    env,
                    Some(&frame),
                );
                // An empty lexical capture is still a scope boundary.  Keep
                // that fact as closure metadata instead of an artificial
                // environment binding, since instrumentation and capture
                // analysis must see only real Lisp bindings.
                let closure_eval_context = self.closure_eval_context(closure_env);
                let lexical_closure = closure_eval_context == Some(true);
                // A closure without a lexical environment is a GNU dynamic
                // lambda.  Calling it from lexical code must switch the
                // hidden interpreter environment to nil; otherwise it can
                // read the caller's lexical frames and create lexical nested
                // lambdas that GNU would keep dynamic.
                let call_context = closure_eval_context.unwrap_or(false);
                let call_capture_override =
                    (self.lambda_capture_override() != Some(call_context)).then_some(call_context);
                if let Some(capture) = call_capture_override {
                    self.push_lambda_eval_context(capture, false);
                }
                let previous_activation = self.enter_activation();
                let result = if closure_env.borrow().is_empty() && !lexical_closure {
                    // GNU funcall_lambda binds a dynamic lambda's arguments
                    // with specbind and installs a nil interpreter
                    // environment for its body.  No caller lexical frame is
                    // visible, while ordinary dynamic lets remain visible
                    // through their value-cell bindings.
                    let mut call_env = Vec::new();
                    let mut restores = Vec::with_capacity(frame.len());
                    let setup = frame.iter().try_for_each(|(name, value)| {
                        self.bind_special_variable(name, value.clone(), &mut call_env)
                            .map(|restore| restores.push(restore))
                    });
                    let previous_floor = self.special_scan_floor;
                    self.special_scan_floor = 0;
                    let result = match setup {
                        Ok(()) => self.sf_progn(function_executable_body(body), &mut call_env),
                        Err(error) => Err(error),
                    };
                    self.special_scan_floor = previous_floor;
                    let mut restore_error = None;
                    for restore in restores.into_iter().rev() {
                        if let Err(error) = self.restore_special_binding(restore, &mut call_env)
                            && restore_error.is_none()
                        {
                            restore_error = Some(error);
                        }
                    }
                    match result {
                        Ok(value) => restore_error.map_or(Ok(value), Err),
                        Err(error) => Err(error),
                    }
                } else if body_has_marker(body, ":closure-transparent-env") {
                    // Advice wrappers are plumbing: run them on the caller's
                    // environment chain with the wrapper's captured frames
                    // appended, so lexical mutations made below the wrapper
                    // still reach the calling scope.
                    let caller_len = env.len();
                    // A captured frame whose IDENTITY is live in the caller
                    // env is the same binding frame: the caller's version is
                    // current (the capture is a snapshot), so skip the stale
                    // copy and let the live frame be seen and mutated.  Run
                    // directly on the caller's chain (no full-chain clone).
                    let captured_frames = closure_env.borrow().clone();
                    let mut frame_sources: Vec<usize> = Vec::with_capacity(captured_frames.len());
                    for captured_frame in &captured_frames {
                        let live_position = Self::frame_identity(captured_frame).and_then(|id| {
                            env[..caller_len]
                                .iter()
                                .position(|frame| Self::frame_identity(frame) == Some(id))
                        });
                        match live_position {
                            Some(position) => frame_sources.push(position),
                            None => {
                                env.push(captured_frame.clone());
                                frame_sources.push(env.len() - 1);
                            }
                        }
                    }
                    let captured_len = env.len();
                    env.push(frame.clone());
                    let previous_floor = self.special_scan_floor;
                    self.special_scan_floor = caller_len;
                    let result = self.sf_progn(function_executable_body(body), env);
                    self.special_scan_floor = previous_floor;
                    env.truncate(captured_len);
                    let refreshed: Vec<_> = frame_sources
                        .iter()
                        .map(|&position| env[position].clone())
                        .collect();
                    env.truncate(caller_len);
                    {
                        let mut stored = closure_env.borrow_mut();
                        stored.clear();
                        stored.extend(refreshed);
                    }
                    result
                } else if body_has_marker(body, ":closure-isolated-current-env") {
                    let mut call_env = closure_env.borrow().clone();
                    let captured_len = call_env.len();
                    call_env.push(vec![("__closure-isolated-current-env".into(), Value::T)].into());
                    call_env.push(frame.clone());
                    let previous_floor = self.special_scan_floor;
                    self.special_scan_floor = 0;
                    let result = self.sf_progn(function_executable_body(body), &mut call_env);
                    self.special_scan_floor = previous_floor;
                    call_env.truncate(captured_len);
                    result
                } else {
                    let previous_floor = self.special_scan_floor;
                    self.special_scan_floor = 0;
                    let result =
                        self.eval_with_closure_env(closure_env, env, |interp, call_env| {
                            let depth = call_env.len();
                            call_env.push(frame.clone());
                            let result = interp.sf_progn(function_executable_body(body), call_env);
                            call_env.truncate(depth);
                            result
                        });
                    self.special_scan_floor = previous_floor;
                    result
                };
                self.leave_activation(previous_activation);
                if call_capture_override.is_some() {
                    self.pop_lambda_capture_override();
                }
                if let Err(error) = &result {
                    self.capture_batch_error_backtrace(error, env);
                }
                self.pop_backtrace_frame();
                result
            }
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
    fn scratch_buffers_are_cleared_and_oversized_storage_is_not_retained() {
        EVAL_VALUE_BUFFER_POOL.with_borrow_mut(Vec::clear);

        {
            let mut buffer = EvalValueBuffer::take();
            buffer.extend([Value::string("temporary"), Value::symbol("value")]);
        }
        EVAL_VALUE_BUFFER_POOL.with_borrow(|pool| {
            assert_eq!(pool.len(), 1);
            assert!(pool[0].is_empty());
        });

        {
            let mut buffer = EvalValueBuffer::take();
            assert!(buffer.is_empty());
            buffer.reserve(EVAL_VALUE_BUFFER_CAPACITY_LIMIT + 1);
        }
        EVAL_VALUE_BUFFER_POOL.with_borrow(|pool| assert!(pool.is_empty()));
    }
}
