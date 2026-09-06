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
            && let Some((symbol, _)) = crate::lisp::primitives::symbol_with_pos_parts(self, value)
            && let Ok(name) = symbol.as_symbol()
        {
            return Some(name.into());
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
        if let Some(Value::Symbol(name)) = items.first() {
            // A Lisp symbol may select a Rust evaluator arm only when the
            // generated GNU C manifest owns that native surface.  In
            // particular, an Emaxx-private prefix is not an ownership
            // boundary and cannot turn an Elisp macro into a host fallback.
            native_form = crate::lisp::primitives::generated_gnu_c_primitive_available(name)
                .is_some_and(|available| available)
                .then(|| NativeForm::for_name(name))
                .flatten();
        }
        let analysis = SourceFormAnalysis {
            items,
            native_form,
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
        // eval.c:eval_sub checks for a pending quit after the symbol/scalar
        // fast paths and before GC, depth accounting, or form dispatch.
        self.maybe_quit(env)?;
        // eval.c:2502 calls maybe_gc after maybe_quit and before the depth
        // increment. The active native boundary owns the conservative stack
        // scan; outside native execution this is the corresponding fast no-op.
        crate::lisp::native_comp::maybe_gc_active();
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
    fn lisp_eval_depth_exceeded(&mut self) -> bool {
        let depth = i64::try_from(self.lisp_eval_depth).unwrap_or(i64::MAX);
        if depth <= self.max_lisp_eval_depth_value() {
            return false;
        }
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

    fn eval_inner(&mut self, expr: &Value, env: &mut Env) -> Result<Value, LispError> {
        match expr {
            Value::Nil
            | Value::T
            | Value::Integer(_)
            | Value::BigInteger(_)
            | Value::Float(_)
            | Value::StringObject(_) => Ok(expr.clone()),

            // GNU has already constructed every nested reader object by the
            // time eval_sub sees a vector.  Emaxx's parser is deliberately
            // interpreter-free, so finish that existing reader contract at
            // the evaluation boundary before returning this self-evaluating
            // object.
            Value::Vector(_) => self.materialize_read_object_literals(expr.clone(), env),

            // Evaluating a string literal yields a string object with its
            // own identity, so `eq' distinguishes evaluations of distinct
            // literals while `(memq (car l) l)' still finds the element the
            // evaluation put there (GNU strings are always heap objects).
            Value::String(_) => Ok(Self::stored_value(expr.clone())),

            Value::Record(_)
                if crate::lisp::primitives::symbols_with_pos_enabled(self, env)
                    && crate::lisp::primitives::symbol_with_pos_parts(self, expr).is_some() =>
            {
                let Some((symbol, _)) = crate::lisp::primitives::symbol_with_pos_parts(self, expr)
                else {
                    return Ok(expr.clone());
                };
                let name = symbol
                    .as_symbol()
                    .map_err(|_| LispError::WrongTypeArgument("symbolp".into(), expr.clone()))?;
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

            Value::ReaderForm(_) => self.materialize_read_object_literals(expr.clone(), env),

            Value::Symbol(name) => self.lookup_symbol(name, env),

            Value::Cons(_) => {
                let SourceFormAnalysis {
                    items,
                    native_form,
                    macro_calls,
                    function_call,
                } = self.source_form_analysis(expr)?;
                if items.is_empty() {
                    return Ok(Value::Nil);
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
                            NativeForm::Quote => return self.sf_quote(&items, env),
                            NativeForm::If => return self.sf_if(&items, env),
                            NativeForm::And => return self.sf_and(&items, env),
                            NativeForm::Or => return self.sf_or(&items, env),
                            NativeForm::Cond => {
                                self.push_unevaluated_backtrace_frame(expr);
                                let result = self.sf_cond(&items, env);
                                let result = self.settle_frame_result(result, env);
                                self.pop_backtrace_frame();
                                return result;
                            }
                            NativeForm::Progn => return self.sf_progn(&items[1..], env),
                            NativeForm::Prog1 => return self.sf_prog1(&items, env),
                            NativeForm::Let => {
                                self.push_unevaluated_backtrace_frame(expr);
                                let result = self.sf_let(&items, env);
                                let result = self.settle_frame_result(result, env);
                                self.pop_backtrace_frame();
                                return result;
                            }
                            NativeForm::LetStar => {
                                self.push_unevaluated_backtrace_frame(expr);
                                let result = self.sf_letstar(&items, env);
                                let result = self.settle_frame_result(result, env);
                                self.pop_backtrace_frame();
                                return result;
                            }
                            NativeForm::Setq => {
                                self.push_unevaluated_backtrace_frame(expr);
                                let result = self.sf_setq(&items, env);
                                let result = self.settle_frame_result(result, env);
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
                                let result = self.settle_frame_result(result, env);
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
        // eval_sub records the call, with its unevaluated argument forms,
        // before it resolves the function cell and while the arguments
        // evaluate: a void function or an error inside an argument reaches
        // `handler-bind' handlers and backtraces with this frame innermost.
        let unevald_frame = callable_name.is_some();
        if unevald_frame {
            self.push_unevaluated_backtrace_frame(source_form);
        }
        let prepared = if let Some(name) = callable_name.as_ref() {
            match self.resolve_source_symbol_call(name, env, source_resolution) {
                Ok(prepared) => prepared,
                Err(error) => {
                    let result = self.settle_frame_result(Err(error), env);
                    self.pop_backtrace_frame();
                    return result;
                }
            }
        } else {
            FunctionResolution::Resolved(self.eval(&items[0], env)?)
        };
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
            if let Some(error) = arg_error {
                let result = self.settle_frame_result(Err(error), env);
                self.pop_backtrace_frame();
                return result;
            }
            self.pop_backtrace_frame();
        } else if let Some(error) = arg_error {
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
        self.begin_funcall(env)?;
        let result =
            self.call_function_value_named(func, original_name.map(CallName::Text), args, env);
        self.end_funcall();
        result
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
    pub(crate) fn maybe_quit(&mut self, env: &mut Env) -> Result<(), LispError> {
        // lisp.h:maybe_quit first reads Vquit_flag directly and returns on
        // the overwhelmingly common nil case.  These are eval.c's forwarded
        // cells, not symbol-name lookups.
        if self.quit_flag_is_nil() {
            return Ok(());
        }
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
        let result = primitives::call_with_facts(self, name, facts, args, env);
        let result = self.settle_frame_result(result, env);
        self.pop_backtrace_frame();
        result
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
    fn settle_frame_result(
        &mut self,
        result: Result<Value, LispError>,
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let result = match result {
            Err(error @ (LispError::Throw(_, _) | LispError::Terminate(_))) => Err(error),
            Err(error) => self.dispatch_handler_bindings(error, env),
            ok => ok,
        };
        if let Err(error) = &result {
            self.capture_batch_error_backtrace(error, env);
        }
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
        let result = self.execute_bytecode_funcall_body(record_id, args, env);
        let result = self.settle_frame_result(result, env);
        self.pop_backtrace_frame();
        result
    }

    /// eval.c:funcall_lambda's direct `exec_byte_code' branch.  The caller
    /// owns Ffuncall's depth and backtrace entry; this supplies only the
    /// byte-code activation boundary shared by source and native callers.
    pub(crate) fn execute_bytecode_funcall_body(
        &mut self,
        record_id: u64,
        args: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
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
        result
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
                let resolution = match self.resolve_symbol_call(&name, env) {
                    Ok(resolution) => resolution,
                    Err(error) => {
                        // eval.c's Ffuncall records the backtrace frame
                        // BEFORE resolving the function cell, so a
                        // void-function report carries the attempted call
                        // itself (`foo(ARGS)') as its innermost frame.
                        let function = original_name
                            .map(|original| original.symbol_value(&name))
                            .unwrap_or_else(|| Value::Symbol(name.clone()));
                        self.push_backtrace_frame(function, args);
                        let result = self.settle_frame_result(Err(error), env);
                        self.pop_backtrace_frame();
                        return result;
                    }
                };
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
                    match self.load_autoload_target(&file, env) {
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
                let result = primitives::call(self, name, args, env);
                let result = self.settle_frame_result(result, env);
                self.pop_backtrace_frame();
                result
            }
            Value::Record(id)
                if self
                    .find_record(id)
                    .is_some_and(|record| record.kind == RecordKind::NativeCompiledFunction) =>
            {
                let backtrace_function = original_name
                    .map(CallName::original_symbol_value)
                    .unwrap_or(Value::Record(id));
                self.push_backtrace_frame(backtrace_function, args);
                self.capture_current_backtrace_context(
                    original_name.map(CallName::as_str),
                    env,
                    None,
                );
                let result = crate::lisp::native_comp::call_function(self, env, id, args);
                let result = self.settle_frame_result(result, env);
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
                let wrong_arity = || {
                    LispError::SignalValue(Value::list([
                        Value::Symbol("wrong-number-of-arguments".into()),
                        func.clone(),
                        Value::Integer(args.len() as i64),
                    ]))
                };
                // funcall_lambda signals the arity error after Ffuncall has
                // recorded the frame, so the offending call is the
                // innermost frame the handlers and backtraces see.
                let signal_arity = |this: &mut Self, env: &mut Env| -> LispError {
                    let function = original_name
                        .map(CallName::original_symbol_value)
                        .unwrap_or_else(|| func.clone());
                    this.push_backtrace_frame(function, args);
                    let result = this.settle_frame_result(Err(wrong_arity()), env);
                    this.pop_backtrace_frame();
                    result.err().unwrap_or_else(wrong_arity)
                };
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
                        frame.push((param.clone(), Self::stored_value(Value::list(rest_args))));
                        break;
                    }
                    let consumed_arg = arg_idx < args.len();
                    let val = if consumed_arg {
                        args[arg_idx].clone()
                    } else if optional {
                        Value::Nil
                    } else {
                        return Err(signal_arity(self, env));
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
                    self.push_lambda_eval_context(capture);
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
                let result = self.settle_frame_result(result, env);
                self.pop_backtrace_frame();
                result
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
            &[original, tail.clone()],
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
            .bind_special_dynamic("load-path", temporary.clone(), &mut env)
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
        interpreter.set_buffer_local_value(first_buffer, "load-path", temporary.clone());
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
        let mut interpreter = Interpreter::new();
        let mut env = Env::new();
        interpreter.set_load_path(vec![PathBuf::from("/source/lisp")]);
        let original = interpreter
            .symbol_value_cell("load-path")
            .expect("load-path is bound");
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
        .expect("ordinary primitive succeeds");
        interpreter.set_global_binding("weak-table-root", table.clone());
        for key in [original.clone(), Value::cons(Value::Integer(7), Value::Nil)] {
            primitives::call(
                &mut interpreter,
                "puthash",
                &[key, Value::T, table.clone()],
                &mut env,
            )
            .expect("ordinary primitive succeeds");
        }
        primitives::call(
            &mut interpreter,
            "makunbound",
            &[Value::symbol("load-path")],
            &mut env,
        )
        .expect("ordinary primitive succeeds");
        assert!(interpreter.symbol_value_cell("load-path").is_err());
        let plain = Value::list([Value::string("/plain")]);
        interpreter.set_symbol_value_cell("load-path", plain.clone());
        assert!(primitives::values_eq_in_env(
            &interpreter,
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
                primitives::values_eq_in_env(&interpreter, key, &original, &env)
            );
        }

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
        interpreter.set_global_binding("saved-load-path", original.clone());
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
        let name = SymbolName::make_uninterned(lisp_name.clone(), "binding", 1);
        let key = Value::Symbol(name.clone());
        let env = vec![EnvFrame::with_identity(
            vec![(name.clone(), Value::Integer(7))],
            Interpreter::fresh_frame_identity(),
        )];
        let mut marked = LispReachability::default();
        marked.mark_env(&interpreter, &env);
        assert!(
            marked.contains(&key),
            "alloc.c marks the symbol car of a lexical binding"
        );
        assert!(marked.contains(&lisp_name), "alloc.c marks SYMBOL_NAME");

        let function = Value::lambda(
            vec![name].into(),
            vec![Value::Nil].into(),
            shared_env(Vec::new()),
        );
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
            &[flag.clone(), Value::Nil],
            &mut env,
        )
        .expect("store directly through the forwarded default");
        assert!(!relocated_value());
        let alias = Value::symbol("position-flag-alias");
        primitives::call(
            &mut interpreter,
            "defvaralias",
            &[alias.clone(), flag.clone()],
            &mut env,
        )
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
        primitives::call(
            &mut interpreter,
            "set-default",
            &[flag.clone(), Value::T],
            &mut env,
        )
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
                assert_eq!(
                    interpreter.forwarded_c_value(name, &env),
                    Some(c_value.clone())
                );
                let plain = Value::string("uncoerced plain value");
                primitives::call(
                    &mut interpreter,
                    "set",
                    &[symbol.clone(), plain.clone()],
                    &mut env,
                )
                .expect("a detached plain symbol has no forwarded type restriction");
                interpreter.refresh_forwarded_eval_cells();
                assert_eq!(
                    interpreter.symbol_value_cell(name).expect("plain binding"),
                    plain
                );
                assert_eq!(
                    interpreter.forwarded_c_value(name, &env),
                    Some(c_value.clone())
                );
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
                &[
                    key.clone(),
                    Value::Integer(index as i64),
                    Value::Record(table_id),
                ],
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
            interpreter.set_symbol_value_cell(name, key.clone());
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
            .insert("text-quoting-style".into(), keys[4].clone());
        interpreter.pending_thread_events.push(keys[5].clone());
        interpreter.coding_systems[0].charset_list = keys[6].clone();
        interpreter.coding_systems[0].type_args = vec![keys[7].clone()];
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
        interpreter.set_symbol_value_cell("quit-flag", c_value.clone());
        primitives::call(
            &mut interpreter,
            "makunbound",
            &[Value::symbol("quit-flag")],
            &mut env,
        )
        .expect("detach C field");
        interpreter.set_symbol_value_cell("quit-flag", plain.clone());
        interpreter.set_symbol_value_cell("inhibit-quit", c_value.clone());
        interpreter
            .detached_forwarded_variables
            .insert("text-quoting-style".into(), c_value.clone());
        interpreter.pending_thread_events.push(c_value.clone());
        interpreter.coding_systems[0].charset_list = c_value.clone();
        interpreter.coding_systems[0].type_args = vec![c_value.clone()];
        let mut copied = interpreter.deep_clone_image();
        let copied_c = copied.quit_flag_value();
        assert!(!primitives::values_eq_in_env(
            &copied, &copied_c, &c_value, &env
        ));
        assert_eq!(copied_c, c_value);
        for child in [
            copied.inhibit_quit.clone(),
            copied
                .symbol_value_cell("inhibit-quit")
                .expect("still forwarded"),
            copied.detached_forwarded_variables["text-quoting-style"].clone(),
            copied.pending_thread_events[0].clone(),
            copied.coding_systems[0].charset_list.clone(),
            copied.coding_systems[0].type_args[0].clone(),
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
