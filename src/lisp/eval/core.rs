use super::*;
use crate::lisp::types::{SharedEnv, SymbolName};

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

macro_rules! native_form_is_special {
    (Special) => {
        true
    };
    (Internal) => {
        false
    };
}

/// Define every form intercepted before ordinary macro/function dispatch.
///
/// Keeping the Lisp names and their special-form classification here makes
/// registration atomic.  `eval_inner` dispatches on the generated enum, so a
/// newly registered form also produces a non-exhaustive-match error until its
/// evaluator arm is implemented.
macro_rules! define_native_forms {
    ($($kind:ident $variant:ident => $($name:literal)|+;)+) => {
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

            fn is_special(self) -> bool {
                match self {
                    $(Self::$variant => native_form_is_special!($kind),)+
                }
            }
        }
    };
}

define_native_forms! {
    Special Quote => "quote";
    Special If => "if" | "static-if";
    Special IfLet => "if-let";
    Special IfLetStar => "if-let*";
    Special When => "when";
    Special StaticWhen => "static-when";
    Special WhenLet => "when-let";
    Special WhenLetStar => "when-let*";
    Special Unless => "unless";
    Special StaticUnless => "static-unless";
    Special BoundAndTrue => "bound-and-true-p";
    Special Cond => "cond";
    Special Pcase => "pcase";
    Special PcaseDefmacro => "pcase-defmacro";
    Special PcaseExhaustive => "pcase-exhaustive";
    Special AndLetStar => "and-let*";
    Special And => "and";
    Special Or => "or";
    Special Not => "not";
    Special Progn => "progn";
    Special DelayModeHooks => "delay-mode-hooks";
    Internal AtomicChangeGroup => "atomic-change-group";
    Internal ClReturn => "cl-return";
    Internal ClReturnFrom => "cl-return-from";
    Special Prog1 => "prog1";
    Internal Prog2 => "prog2";
    Special Let => "let";
    Special Dlet => "dlet";
    Internal Letrec => "letrec";
    Internal ForcedLexicalLetStar => "--emaxx-lexical-let*";
    Special LetStar => "let*";
    Special ClProgv => "cl-progv";
    Special PcaseLet => "pcase-let";
    Special PcaseLetStar => "pcase-let*";
    Special LetAlist => "let-alist";
    Special Setq => "setq";
    Special SetqDefault => "setq-default";
    Special SetqLocal => "setq-local";
    Special Setopt => "setopt";
    Special Setf => "setf";
    Special Incf => "incf" | "cl-incf";
    Special Decf => "decf" | "cl-decf";
    Internal ClCallf => "cl-callf";
    Special Defvar => "defvar" | "defconst" | "defcustom";
    Special DefvarLocal => "defvar-local";
    Special Defgroup => "defgroup";
    Special Defface => "defface";
    Special DefvarKeymap => "defvar-keymap";
    Special DefineShortDocumentationGroup => "define-short-documentation-group";
    Internal Insert => "insert";
    Internal InsertAndInherit => "insert-and-inherit";
    Internal InsertChar => "insert-char";
    Internal InsertBeforeMarkers => "insert-before-markers";
    Internal InsertBeforeMarkersAndInherit => "insert-before-markers-and-inherit";
    Special DefineMode => "define-minor-mode" | "define-globalized-minor-mode" | "define-derived-mode";
    Internal EmaxxDefineDerivedMode => "emaxx--define-derived-mode";
    Special Defclass => "defclass";
    Special Defun => "defun" | "defsubst";
    Internal DefineAdvice => "define-advice";
    Special ClDefun => "cl-defun";
    Special ClDefmacro => "cl-defmacro";
    Special ClGenericDefineGeneralizer => "cl-generic-define-generalizer";
    Special ClDefgeneric => "cl-defgeneric";
    Special ClDefmethod => "cl-defmethod";
    Special ClGenericDefineContextRewriter => "cl-generic-define-context-rewriter";
    Special OclosureDefine => "oclosure-define";
    Special OclosureLambda => "oclosure-lambda";
    Special DefineInline => "define-inline";
    Special Defmacro => "defmacro";
    Special WithMemoization => "with-memoization";
    Special EasyMenuDefine => "easy-menu-define";
    Special ClDefstruct => "cl-defstruct";
    Internal EmaxxClDefstruct => "emaxx--cl-defstruct";
    Special Backquote => "backquote";
    Internal BackquoteReaderAlias => "`";
    Internal Comma => "comma" | ",";
    Special Lambda => "lambda";
    Special Interactive => "interactive";
    Special Function => "function" | "function-quote";
    Special While => "while";
    Special Dolist => "dolist";
    Internal DolistWithProgressReporter => "dolist-with-progress-reporter";
    Special PcaseDolist => "pcase-dolist";
    Special Dotimes => "dotimes";
    Special ClLoop => "cl-loop";
    Special UnwindProtect => "unwind-protect";
    Special IgnoreError => "ignore-error";
    Special IgnoreErrors => "ignore-errors";
    Special ConditionCase => "condition-case";
    Special ConditionCaseUnlessDebug => "condition-case-unless-debug";
    Special HandlerBind => "handler-bind";
    Special ClAssert => "cl-assert";
    Special WithTempBuffer => "with-temp-buffer";
    Special ErtWithTestBuffer => "ert-with-test-buffer";
    Special ErtWithTempDirectory => "ert-with-temp-directory";
    Special ErtWithMessageCapture => "ert-with-message-capture";
    Special WithEnvironmentVariables => "with-environment-variables";
    Special WithOutputToString => "with-output-to-string";
    Special WithMutex => "with-mutex";
    Special WithTempFile => "with-temp-file";
    Special ErtWithTempFile => "ert-with-temp-file";
    Special WithCurrentBuffer => "with-current-buffer";
    Internal WithCurrentBufferWindow => "with-current-buffer-window";
    Special WithRestriction => "with-restriction";
    Special WithoutRestriction => "without-restriction";
    Special AddFunction => "add-function";
    Special WithSelectedWindow => "with-selected-window";
    Internal WithSyntaxTable => "with-syntax-table";
    Special SaveMatchData => "save-match-data";
    Special SaveExcursion => "save-excursion";
    Special SaveWindowExcursion => "save-window-excursion";
    Special SaveCurrentBuffer => "save-current-buffer";
    Special SaveRestriction => "save-restriction";
    Special WithSuppressedWarnings => "with-suppressed-warnings";
    Special WithDemotedErrors => "with-demoted-errors";
    Special WithCodingPriority => "with-coding-priority";
    Special WithSilentModifications => "with-silent-modifications";
    Special CombineChangeCalls => "combine-change-calls";
    Special ClDestructuringBind => "cl-destructuring-bind";
    Special ClLetf => "cl-letf";
    Special ClFlet => "cl-flet";
    Special ClLabels => "cl-labels";
    Special ClMacrolet => "cl-macrolet";
    Internal ClSymbolMacrolet => "cl-symbol-macrolet";
    Special Push => "push";
    Special ClPushnew => "cl-pushnew";
    Special Pop => "pop";
    Special Catch => "catch";
    Special AddToList => "add-to-list";
    Special ErtDeftest => "ert-deftest";
    Special Should => "should";
    Special ShouldNot => "should-not";
    Special ShouldError => "should-error";
    Special SkipUnless => "skip-unless" | "ert--skip-unless";
    Special SkipWhen => "skip-when" | "ert--skip-when";
    Special Rx => "rx";
    Special RxDefine => "rx-define";
    Internal RxLet => "rx-let";
    Internal RxLetEval => "rx-let-eval";
    Special WithEvalAfterLoad => "with-eval-after-load";
    Special Declare => "declare" | "declare-function" | "cl-declaim" | "declaim";
    Special DefEdebugSpec => "def-edebug-spec";
    Special DefEdebugElemSpec => "def-edebug-elem-spec";
    Special ClDeftype => "cl-deftype";
    Special EvalAndCompile => "eval-and-compile";
    Special EvalWhenCompile => "eval-when-compile";
    Special WhileNoInput => "while-no-input";
    Special ErtInfo => "ert-info";
}

pub(crate) fn is_special_form_name(name: &str) -> bool {
    NativeForm::for_name(name).is_some_and(NativeForm::is_special)
}

/// Arity metadata for native forms that are not covered by the generated GNU
/// C primitive manifest.  Match on the typed registry variant so the Lisp name
/// is never repeated in a second table.
pub(crate) fn native_form_fallback_arity(name: &str) -> Option<(i64, i64)> {
    match NativeForm::for_name(name)? {
        NativeForm::Dlet => Some((1, -2)),
        _ => None,
    }
}

#[derive(Clone, Copy, Default)]
pub(super) enum SourceLiteralKind {
    #[default]
    None,
    Vector,
    BoolVector,
    CharTable,
    Closure,
    Record,
}

impl Interpreter {
    // This evaluator recurses once per subform rather than once per
    // funcall/eval level like GNU Emacs, so the same Lisp program nests
    // several times deeper here.  Scale the user-visible limit so honest
    // deep recursion still fits while runaway recursion keeps signaling
    // the GNU error instead of exhausting the Rust stack.
    // GNU 30 additionally grows `max-lisp-eval-depth' dynamically while C
    // stack remains, so honest recursion tens of thousands of calls deep
    // succeeds interpreted (cl-macs--labels recurses 42000 deep); the batch
    // thread's large stack backs the same headroom here.
    const LISP_EVAL_DEPTH_SCALE: usize = 384;

    fn source_form_analysis(&mut self, source: &Value) -> Result<SourceFormAnalysis, LispError> {
        let Some((source_anchor, _)) = source.cons_cells() else {
            return Err(LispError::TypeError("list".into(), source.type_name()));
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
        let mut mutations = crate::lisp::types::ConsMutationSnapshot::list_spine(source);
        let mut native_form = None;
        let mut literal_kind = SourceLiteralKind::None;
        let mut if_test_mentions_setcdr = false;
        if let Some(Value::Symbol(name)) = items.first() {
            native_form = NativeForm::for_name(name);
            literal_kind = match name.as_str() {
                "vector-literal" => SourceLiteralKind::Vector,
                "bool-vector-literal" => SourceLiteralKind::BoolVector,
                CHAR_TABLE_LITERAL_SYMBOL => SourceLiteralKind::CharTable,
                CLOSURE_LITERAL_SYMBOL if items[1..].iter().all(is_record_literal_slot_form) => {
                    SourceLiteralKind::Closure
                }
                RECORD_LITERAL_SYMBOL if items[1..].iter().all(is_record_literal_slot_form) => {
                    SourceLiteralKind::Record
                }
                _ => SourceLiteralKind::None,
            };
            if matches!(native_form, Some(NativeForm::If)) {
                let mut scan_budget = 512;
                if_test_mentions_setcdr = items
                    .get(1)
                    .is_some_and(|test| form_mentions_setcdr(test, &mut scan_budget));
                if let Some(test) = items.get(1) {
                    mutations.include_tree(test);
                }
            }
        }
        let analysis = SourceFormAnalysis {
            items,
            native_form,
            literal_kind,
            if_test_mentions_setcdr,
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
        if self.lisp_eval_depth > 800 * Self::LISP_EVAL_DEPTH_SCALE
            && self.lisp_eval_depth > self.max_lisp_eval_depth()
        {
            self.lisp_eval_depth -= 1;
            return Err(LispError::Signal(
                "Lisp nesting exceeds `max-lisp-eval-depth'".into(),
            ));
        }
        let result = self.eval_inner(expr, env);
        self.lisp_eval_depth -= 1;
        if outermost && result.is_ok() {
            self.clear_batch_error_backtrace();
        }
        result
    }

    fn max_lisp_eval_depth(&self) -> usize {
        self.global_value("max-lisp-eval-depth")
            .and_then(|value| value.as_integer().ok())
            .and_then(|value| usize::try_from(value).ok())
            .unwrap_or(1600)
            .saturating_mul(Self::LISP_EVAL_DEPTH_SCALE)
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

            Value::Symbol(name) => self.lookup(name, env),

            Value::Cons(_) => {
                let SourceFormAnalysis {
                    items,
                    native_form,
                    literal_kind,
                    if_test_mentions_setcdr,
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
                    SourceLiteralKind::BoolVector => {
                        return Ok(self.create_pseudovector(
                            RecordKind::BoolVector,
                            "bool-vector",
                            items[1..].to_vec(),
                        ));
                    }
                    SourceLiteralKind::CharTable => {
                        return crate::lisp::primitives::materialize_read_char_table_literals(
                            self, expr,
                        );
                    }
                    SourceLiteralKind::Closure => {
                        return self.eval_closure_literal_form(&items[1..], env);
                    }
                    SourceLiteralKind::Record => {
                        return self.eval_record_literal_form(&items[1..], env);
                    }
                    SourceLiteralKind::None => {}
                }

                // Check for special forms first
                if let Value::Symbol(ref name) = items[0] {
                    if matches!(
                        name.as_str(),
                        "pcase"
                            | "pcase-exhaustive"
                            | "pcase-defmacro"
                            | "pcase-let"
                            | "pcase-let*"
                            | "pcase-dolist"
                            | "pcase-setq"
                    ) {
                        // GNU pcase.el takes over the family when loadable;
                        // the native arms below are the no-file fallback.
                        self.ensure_gnu_pcase_loaded();
                    }
                    if let Some(native_form) = native_form {
                        match native_form {
                            NativeForm::Quote => return self.sf_quote(&items),
                            NativeForm::If => {
                                return self.sf_if(&items, env, if_test_mentions_setcdr);
                            }
                            NativeForm::IfLet => {
                                if !self.has_macro_binding("if-let") {
                                    return self.sf_if_let(&items, env);
                                }
                            }
                            NativeForm::IfLetStar => {
                                if !self.has_macro_binding("if-let*") {
                                    return self.sf_if_let_star(&items, env);
                                }
                            }
                            NativeForm::When => {
                                if !self.has_macro_binding("when") {
                                    return self.sf_when(&items, env);
                                }
                            }
                            NativeForm::StaticWhen => return self.sf_when(&items, env),
                            NativeForm::WhenLet => {
                                if !self.has_macro_binding("when-let") {
                                    return self.sf_when_let(&items, env);
                                }
                            }
                            NativeForm::WhenLetStar => {
                                if !self.has_macro_binding("when-let*") {
                                    return self.sf_when_let_star(&items, env);
                                }
                            }
                            NativeForm::Unless => {
                                if !self.has_macro_binding("unless") {
                                    return self.sf_unless(&items, env);
                                }
                            }
                            NativeForm::StaticUnless => return self.sf_unless(&items, env),
                            NativeForm::BoundAndTrue => {
                                return self.sf_bound_and_true_p(&items, env);
                            }
                            NativeForm::Cond => {
                                // Keep the in-progress form visible in
                                // backtraces like GNU's eval frames.
                                self.push_unevaluated_backtrace_frame(expr);
                                let result = self.sf_cond(&items, env);
                                self.pop_backtrace_frame();
                                return result;
                            }
                            NativeForm::Pcase => {
                                if !self.has_macro_binding("pcase") {
                                    return self.sf_pcase(&items, env);
                                }
                            }
                            NativeForm::PcaseDefmacro => {
                                if !self.has_macro_binding("pcase-defmacro") {
                                    return self.sf_pcase_defmacro(&items, env);
                                }
                            }
                            NativeForm::PcaseExhaustive => {
                                if !self.has_macro_binding("pcase-exhaustive") {
                                    return self.sf_pcase_exhaustive(&items, env);
                                }
                            }
                            NativeForm::AndLetStar => {
                                if !self.has_macro_binding("and-let*") {
                                    return self.sf_and_let_star(&items, env);
                                }
                            }
                            NativeForm::And => return self.sf_and(&items, env),
                            NativeForm::Or => return self.sf_or(&items, env),
                            NativeForm::Not => return self.sf_not(&items, env),
                            NativeForm::Progn => return self.sf_progn(&items[1..], env),
                            NativeForm::DelayModeHooks => {
                                // A real dynamic binding: the mode functions run
                                // as callees and must see it like GNU's specbind.
                                let restore =
                                    self.bind_special_variable("delay-mode-hooks", Value::T, env)?;
                                let result = self.sf_progn(&items[1..], env);
                                self.restore_special_binding(restore, env)?;
                                return result;
                            }
                            NativeForm::AtomicChangeGroup => {
                                return self.sf_atomic_change_group(&items[1..], env);
                            }
                            NativeForm::ClReturn => return self.sf_cl_return(&items, env),
                            NativeForm::ClReturnFrom => return self.sf_cl_return_from(&items, env),
                            NativeForm::Prog1 => return self.sf_prog1(&items, env),
                            NativeForm::Prog2 => return self.sf_prog2(&items, env),
                            NativeForm::Let => {
                                // Keep the in-progress form visible in
                                // backtraces like GNU's eval frames.
                                self.push_unevaluated_backtrace_frame(expr);
                                let result = self.sf_let(&items, env);
                                self.pop_backtrace_frame();
                                return result;
                            }
                            NativeForm::Dlet => {
                                // GNU dlet `defvar's each binder before a `let',
                                // so every binding is DYNAMIC (diary sexps read
                                // `date'/`entry' from inside `eval').
                                self.push_unevaluated_backtrace_frame(expr);
                                let result = self.sf_dlet(&items, env);
                                self.pop_backtrace_frame();
                                return result;
                            }
                            NativeForm::Letrec => return self.sf_letrec(&items, env),
                            NativeForm::ForcedLexicalLetStar => {
                                return self.sf_letstar_forced_lexical(&items, env);
                            }
                            NativeForm::LetStar => {
                                // Keep the in-progress form visible in
                                // backtraces like GNU's eval frames.
                                self.push_unevaluated_backtrace_frame(expr);
                                let result = self.sf_letstar(&items, env);
                                self.pop_backtrace_frame();
                                return result;
                            }
                            NativeForm::ClProgv => return self.sf_cl_progv(&items, env),
                            NativeForm::PcaseLet => {
                                if !self.has_macro_binding("pcase-let") {
                                    return self.sf_pcase_let(&items, env, false);
                                }
                            }
                            NativeForm::PcaseLetStar => {
                                if !self.has_macro_binding("pcase-let*") {
                                    return self.sf_pcase_let(&items, env, true);
                                }
                            }
                            // GNU's let-alist.el macro handles nested
                            // `.sublist.foo' fields and `..outer' escapes;
                            // prefer it once loaded and keep the native form
                            // as the no-file fallback.
                            NativeForm::LetAlist => {
                                if !{
                                    self.ensure_autoloaded_macro_loaded("let-alist");
                                    self.has_macro_binding("let-alist")
                                } {
                                    return self.sf_let_alist(&items, env);
                                }
                            }
                            NativeForm::Setq => {
                                self.push_unevaluated_backtrace_frame(expr);
                                let result = self.sf_setq(&items, env);
                                self.pop_backtrace_frame();
                                return result;
                            }
                            NativeForm::SetqDefault => return self.sf_setq_default(&items, env),
                            NativeForm::SetqLocal => return self.sf_setq_local(&items, env),
                            NativeForm::Setopt => return self.sf_setopt(&items, env),
                            // gv.el owns generalized-variable expansion once its
                            // public `setf' macro is loaded.  The native arm is a
                            // bootstrap/file-less fallback only.
                            NativeForm::Setf => {
                                if !self.has_macro_binding("setf") {
                                    return self.sf_setf(&items, env);
                                }
                            }
                            NativeForm::Incf => return self.sf_incf(&items, env, 1),
                            NativeForm::Decf => return self.sf_incf(&items, env, -1),
                            NativeForm::ClCallf => return self.sf_cl_callf(&items, env),
                            NativeForm::Defvar => {
                                // GNU custom.el owns `defcustom' keyword policy.
                                // Keep the native implementation for bootstrap and
                                // file-less interpreters, but stop shadowing the real
                                // macro once the preloaded Elisp owner is available.
                                if !matches!(&items[0], Value::Symbol(name)
                                    if name == "defcustom" && self.has_macro_binding(name))
                                {
                                    return self.sf_defvar(&items, env);
                                }
                            }
                            NativeForm::DefvarLocal => return self.sf_defvar_local(&items, env),
                            NativeForm::Defgroup => return self.sf_defgroup(&items),
                            NativeForm::Defface => return self.sf_defface(&items),
                            NativeForm::DefvarKeymap => {
                                if !self.has_macro_binding("defvar-keymap") {
                                    return self.sf_defvar_keymap(&items, env);
                                }
                            }
                            NativeForm::DefineShortDocumentationGroup => {
                                if !self.has_macro_binding("define-short-documentation-group") {
                                    return self.sf_defgroup(&items);
                                }
                            }
                            NativeForm::Insert => {
                                return self.sf_insert_function(&items, env, false, false);
                            }
                            NativeForm::InsertAndInherit => {
                                return self.sf_insert_function(&items, env, true, false);
                            }
                            NativeForm::InsertChar => {
                                return self.sf_insert_char_function(&items, env);
                            }
                            NativeForm::InsertBeforeMarkers => {
                                return self.sf_insert_function(&items, env, false, true);
                            }
                            NativeForm::InsertBeforeMarkersAndInherit => {
                                return self.sf_insert_function(&items, env, true, true);
                            }
                            NativeForm::DefineMode => {
                                // derived.el owns major-mode inheritance once its
                                // autoload can be resolved.  Keep the native executor
                                // only as the file-less/bootstrap fallback; otherwise
                                // it would shadow GNU's activation-time keymap,
                                // syntax-table, and abbrev-table parent wiring.
                                if name != "define-derived-mode"
                                    || !{
                                        self.ensure_autoloaded_macro_loaded(name);
                                        self.has_macro_binding(name)
                                    }
                                {
                                    return self.sf_define_mode(&items, env);
                                }
                            }
                            NativeForm::EmaxxDefineDerivedMode => {
                                return self.sf_define_mode(&items, env);
                            }
                            NativeForm::Defclass => return self.sf_defclass(&items),
                            NativeForm::Defun => return self.sf_defun(&items, env),
                            NativeForm::DefineAdvice => return self.sf_define_advice(&items, env),
                            NativeForm::ClDefun => return self.sf_cl_defun(&items, env),
                            NativeForm::ClDefmacro => return self.sf_cl_defmacro(&items, env),
                            NativeForm::ClGenericDefineGeneralizer => {
                                return self.sf_cl_generic_define_generalizer(&items);
                            }
                            NativeForm::ClDefgeneric => return self.sf_cl_defgeneric(&items, env),
                            NativeForm::ClDefmethod => return self.sf_cl_defmethod(&items, env),
                            NativeForm::ClGenericDefineContextRewriter => {
                                // (cl-generic-define-context-rewriter NAME ARGS &rest
                                // BODY): store the expander macro-style so
                                // cl-defmethod &context entries can expand
                                // (erc-obsolete-var VAR SPEC) into ((EXPR) SPEC).
                                if let (Some(Value::Symbol(name)), Some(args)) =
                                    (items.get(1), items.get(2))
                                    && let Ok(param_values) = args.to_vec()
                                    && param_values
                                        .iter()
                                        .map(|p| p.as_symbol().map(str::to_string))
                                        .collect::<Result<Vec<_>, _>>()
                                        .is_ok()
                                {
                                    let rewriter_name =
                                        format!("cl-generic--context-rewriter--{name}");
                                    let lambda_form = Value::list(
                                        std::iter::once(Value::Symbol("lambda".into()))
                                            .chain(std::iter::once(args.clone()))
                                            .chain(items[3..].iter().cloned()),
                                    );
                                    let expander = self.eval(&lambda_form, env)?;
                                    self.push_macro_binding(MacroBinding {
                                        name: rewriter_name,
                                        expander,
                                    });
                                }
                                return Ok(Value::Nil);
                            }
                            NativeForm::OclosureDefine => {
                                return self.sf_oclosure_define(&items, env);
                            }
                            NativeForm::OclosureLambda => {
                                return self.sf_oclosure_lambda(&items, env);
                            }
                            NativeForm::DefineInline => return self.sf_define_inline(&items, env),
                            NativeForm::Defmacro => return self.sf_defmacro(&items, env),
                            NativeForm::WithMemoization => {
                                return self.sf_with_memoization(&items, env);
                            }
                            NativeForm::EasyMenuDefine => {
                                return self.sf_easy_menu_define(&items, env);
                            }
                            NativeForm::ClDefstruct | NativeForm::EmaxxClDefstruct => {
                                return self.sf_cl_defstruct(&items);
                            }
                            NativeForm::Backquote | NativeForm::BackquoteReaderAlias => {
                                return self.eval_backquote(&items[1], env);
                            }
                            NativeForm::Comma => {
                                if let Some(value) = items.get(1) {
                                    return self.eval(value, env);
                                }
                                return Ok(Value::Nil);
                            }
                            NativeForm::Lambda => {
                                return self.sf_lambda_from_source(expr, &items, env);
                            }
                            NativeForm::Interactive => return Ok(Value::Nil),
                            NativeForm::Function => {
                                // #'foo or (function foo)
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
                                        return self.eval(&items[1], env);
                                    }
                                    // Unlike quote, `function' gives lambdas
                                    // lexical closure semantics.  Other list
                                    // objects are still returned literally:
                                    // GNU evaluates #'(1 2) to (1 2).
                                    return Ok(items[1].clone());
                                }
                                return Ok(Value::Nil);
                            }
                            NativeForm::While => {
                                // Keep the in-progress form visible in
                                // backtraces like GNU's eval frames.
                                self.push_unevaluated_backtrace_frame(expr);
                                let result = self.sf_while(&items, env);
                                self.pop_backtrace_frame();
                                return result;
                            }
                            NativeForm::Dolist => {
                                if !self.has_macro_binding("dolist") {
                                    // Keep the in-progress form visible in
                                    // backtraces like GNU's eval frames.
                                    self.push_unevaluated_backtrace_frame(expr);
                                    let result = self.sf_dolist(&items, env);
                                    self.pop_backtrace_frame();
                                    return result;
                                }
                            }
                            NativeForm::DolistWithProgressReporter => {
                                if !self.has_macro_binding("dolist-with-progress-reporter") {
                                    return self.sf_dolist_with_progress_reporter(&items, env);
                                }
                            }
                            NativeForm::PcaseDolist => {
                                if !self.has_macro_binding("pcase-dolist") {
                                    return self.sf_pcase_dolist(&items, env);
                                }
                            }
                            NativeForm::Dotimes => {
                                if !self.has_macro_binding("dotimes") {
                                    return self.sf_dotimes(&items, env);
                                }
                            }
                            // The preloaded GNU `cl-loop' macro takes precedence;
                            // the native special form remains as a bootstrap
                            // fallback before simple_compat.el is loaded.
                            NativeForm::ClLoop => {
                                if !self.has_lisp_macro("cl-loop") {
                                    return self.sf_cl_loop(&items, env);
                                }
                            }
                            NativeForm::UnwindProtect => {
                                return self.sf_unwind_protect(&items, env);
                            }
                            NativeForm::IgnoreError => {
                                if !self.has_macro_binding("ignore-error") {
                                    return self.sf_ignore_error(&items, env);
                                }
                            }
                            NativeForm::IgnoreErrors => {
                                if !self.has_macro_binding("ignore-errors") {
                                    return self.sf_ignore_errors(&items, env);
                                }
                            }
                            NativeForm::ConditionCase => {
                                return self.sf_condition_case(&items, env);
                            }
                            NativeForm::ConditionCaseUnlessDebug => {
                                if !self.has_macro_binding("condition-case-unless-debug") {
                                    return self.sf_condition_case(&items, env);
                                }
                            }
                            NativeForm::HandlerBind => return self.sf_handler_bind(&items, env),
                            NativeForm::ClAssert => return self.sf_cl_assert(&items, env),
                            // GNU preloads `with-temp-buffer' as a subr.el macro.
                            // Keep the native implementation only as a bootstrap
                            // fallback before simple_compat.el is loaded.
                            NativeForm::WithTempBuffer => {
                                if !self.has_macro_binding("with-temp-buffer") {
                                    return self.sf_with_temp_buffer(&items, env);
                                }
                            }
                            NativeForm::ErtWithTestBuffer => {
                                return self.sf_ert_with_test_buffer(&items, env);
                            }
                            NativeForm::ErtWithTempDirectory => {
                                return self.sf_ert_with_temp_directory(&items, env);
                            }
                            NativeForm::ErtWithMessageCapture => {
                                return self.sf_ert_with_message_capture(&items, env);
                            }
                            NativeForm::WithEnvironmentVariables => {
                                return self.sf_with_environment_variables(&items, env);
                            }
                            NativeForm::WithOutputToString => {
                                return self.sf_with_output_to_string(&items, env);
                            }
                            NativeForm::WithMutex => return self.sf_with_mutex(&items, env),
                            NativeForm::WithTempFile => return self.sf_with_temp_file(&items, env),
                            NativeForm::ErtWithTempFile => {
                                return self.sf_ert_with_temp_file(&items, env);
                            }
                            NativeForm::WithCurrentBuffer => {
                                return self.sf_with_current_buffer(&items, env);
                            }
                            // GNU window.el owns this macro's setup/body/display
                            // lifecycle.  The native arm is only a file-less
                            // bootstrap fallback; once the macro is loaded it
                            // must not be pre-empted or its ACTION is never run.
                            NativeForm::WithCurrentBufferWindow => {
                                if !self.has_macro_binding("with-current-buffer-window") {
                                    return self.sf_with_current_buffer_window(&items, env);
                                }
                            }
                            NativeForm::WithRestriction => {
                                if !self.has_macro_binding("with-restriction") {
                                    return self.sf_with_restriction(&items, env);
                                }
                            }
                            NativeForm::WithoutRestriction => {
                                if !self.has_macro_binding("without-restriction") {
                                    return self.sf_without_restriction(&items, env);
                                }
                            }
                            // GNU nadvice.el's macro handles this (autoloading
                            // it if needed); the native arm is the file-less
                            // fallback.
                            NativeForm::AddFunction => {
                                if let Ok(Some(expanded)) =
                                    self.try_macroexpand("add-function", &items[1..], env)
                                {
                                    return self.eval(&expanded, env);
                                }
                                return self.sf_add_function(&items, env);
                            }
                            NativeForm::WithSelectedWindow => {
                                return self.sf_with_selected_window(&items, env);
                            }
                            NativeForm::WithSyntaxTable => {
                                return self.sf_with_syntax_table(&items, env);
                            }
                            NativeForm::SaveMatchData => {
                                return self.sf_save_match_data(&items, env);
                            }
                            NativeForm::SaveExcursion => {
                                return self.sf_save_excursion(&items, env);
                            }
                            NativeForm::SaveWindowExcursion => {
                                return self.sf_save_window_excursion(&items, env);
                            }
                            NativeForm::SaveCurrentBuffer => {
                                return self.sf_save_current_buffer(&items, env);
                            }
                            NativeForm::SaveRestriction => {
                                return self.sf_save_restriction(&items, env);
                            }
                            NativeForm::WithSuppressedWarnings => {
                                return self.sf_with_suppressed_warnings(&items, env);
                            }
                            // Prefer GNU's loaded macro.  Its
                            // `condition-case-unless-debug' expansion cooperates
                            // with ERT's debugger bindings; the native arm is
                            // only a bootstrap fallback before subr is present.
                            NativeForm::WithDemotedErrors => {
                                if !self.has_macro_binding("with-demoted-errors") {
                                    return self.sf_with_demoted_errors(&items, env);
                                }
                            }
                            NativeForm::WithCodingPriority => {
                                return self.sf_with_coding_priority(&items, env);
                            }
                            NativeForm::WithSilentModifications => {
                                return self.sf_with_silent_modifications(&items, env);
                            }
                            NativeForm::CombineChangeCalls => {
                                return self.sf_combine_change_calls(&items, env);
                            }
                            NativeForm::ClDestructuringBind => {
                                return self.sf_cl_destructuring_bind(&items, env);
                            }
                            NativeForm::ClLetf => return self.sf_cl_letf(&items, env),
                            NativeForm::ClFlet => {
                                if !self.has_lisp_macro("cl-flet") {
                                    return self.sf_cl_flet(&items, env);
                                }
                            }
                            NativeForm::ClLabels => {
                                if !self.has_lisp_macro("cl-labels") {
                                    return self.sf_cl_labels(&items, env);
                                }
                            }
                            NativeForm::ClMacrolet => return self.sf_cl_macrolet(&items, env),
                            NativeForm::ClSymbolMacrolet => {
                                return self.sf_cl_symbol_macrolet(&items, env);
                            }
                            NativeForm::Push => {
                                // (push NEWELT PLACE)
                                if items.len() < 3 {
                                    return Err(LispError::WrongNumberOfArgs(
                                        "push".into(),
                                        items.len() - 1,
                                    ));
                                }
                                let val = self.eval(&items[1], env)?;
                                let place = self.resolve_setf_place(&items[2], env)?;
                                let cur =
                                    self.eval_resolved_setf_place_current_value(&place, env)?;
                                let new_val = Value::cons(val, cur);
                                self.set_resolved_setf_place_value(&place, new_val.clone(), env)?;
                                return Ok(new_val);
                            }
                            NativeForm::ClPushnew => return self.sf_cl_pushnew(&items, env),
                            NativeForm::Pop => {
                                // (pop PLACE)
                                if items.len() < 2 {
                                    return Err(LispError::WrongNumberOfArgs(
                                        "pop".into(),
                                        items.len() - 1,
                                    ));
                                }
                                let place = self.resolve_setf_place(&items[1], env)?;
                                let cur =
                                    self.eval_resolved_setf_place_current_value(&place, env)?;
                                let result = cur.car()?;
                                let rest = cur.cdr()?;
                                self.set_resolved_setf_place_value(&place, rest, env)?;
                                return Ok(result);
                            }
                            NativeForm::Catch => return self.sf_catch(&items, env),
                            NativeForm::AddToList => return self.sf_add_to_list(&items, env),
                            // Keep the native definition path only for bootstrap
                            // interpreters.  GNU ert.el macroexpands test bodies
                            // when they are defined; deferring that work until a
                            // test runs makes macro availability and caches depend
                            // on the order in which tests execute.
                            NativeForm::ErtDeftest => {
                                if !self.has_macro_binding("ert-deftest") {
                                    return self.sf_ert_deftest(&items, env);
                                }
                            }
                            // These are native bootstrap fallbacks.  Once ert.el
                            // has installed its real macros, their expansion owns
                            // the observable condition payload and should-form
                            // observer protocol used by nested `ert-run-test'.
                            NativeForm::Should => {
                                if !self.has_macro_binding("should") {
                                    return self.sf_should(&items, env);
                                }
                            }
                            NativeForm::ShouldNot => {
                                if !self.has_macro_binding("should-not") {
                                    return self.sf_should_not(&items, env);
                                }
                            }
                            NativeForm::ShouldError => {
                                if !self.has_macro_binding("should-error") {
                                    return self.sf_should_error(&items, env);
                                }
                            }
                            NativeForm::SkipUnless => {
                                if !self.has_macro_binding(name) {
                                    return self.sf_skip_unless(&items, env);
                                }
                            }
                            NativeForm::SkipWhen => {
                                if !self.has_macro_binding(name) {
                                    return self.sf_skip_when(&items, env);
                                }
                            }
                            NativeForm::Rx => {
                                if !{
                                    self.ensure_gnu_rx_loaded();
                                    self.has_macro_binding("rx")
                                } {
                                    return self.sf_rx(&items, env);
                                }
                            }
                            NativeForm::RxDefine => {
                                if !{
                                    self.ensure_gnu_rx_loaded();
                                    self.has_macro_binding("rx")
                                } {
                                    return self.sf_rx_define(&items);
                                }
                            }
                            NativeForm::RxLet => {
                                if !{
                                    self.ensure_gnu_rx_loaded();
                                    self.has_macro_binding("rx")
                                } {
                                    return self.sf_rx_let(&items, env);
                                }
                            }
                            // rx-let-eval is defined only by GNU rx.el; loading it
                            // makes the macro available so the normal macro dispatch
                            // (below) expands it.  There is no native fallback.
                            NativeForm::RxLetEval => {
                                if !{
                                    self.ensure_gnu_rx_loaded();
                                    self.has_macro_binding("rx-let-eval")
                                } {
                                    return Err(LispError::SignalValue(Value::list([
                                        Value::Symbol("void-function".into()),
                                        Value::Symbol("rx-let-eval".into()),
                                    ])));
                                }
                            }
                            NativeForm::WithEvalAfterLoad => {
                                return self.sf_with_eval_after_load(&items, env);
                            }
                            NativeForm::Declare => {
                                return Ok(Value::Nil);
                            }
                            NativeForm::DefEdebugSpec => {
                                // (def-edebug-spec SYMBOL SPEC), both unevaluated.
                                if let (Some(symbol), Some(spec)) = (items.get(1), items.get(2))
                                    && let Ok(symbol_name) = symbol.as_symbol()
                                {
                                    let symbol_name = symbol_name.to_string();
                                    self.put_symbol_property(
                                        &symbol_name,
                                        "edebug-form-spec",
                                        spec.clone(),
                                    );
                                }
                                return Ok(Value::Nil);
                            }
                            NativeForm::DefEdebugElemSpec => {
                                return self.sf_def_edebug_elem_spec(&items, env);
                            }
                            // cl-macs.el owns the public type-expander metadata
                            // once its `cl-deftype' macro is loaded.  Keep the
                            // native implementation for bootstrap/file-less use.
                            NativeForm::ClDeftype => {
                                if !self.has_macro_binding("cl-deftype") {
                                    return self.sf_cl_deftype(&items, env);
                                }
                            }
                            NativeForm::EvalAndCompile => return self.sf_progn(&items[1..], env),
                            NativeForm::EvalWhenCompile => {
                                return self.with_current_load_history_suppressed(|interp| {
                                    interp.sf_progn(&items[1..], env)
                                });
                            }
                            NativeForm::WhileNoInput => return self.sf_progn(&items[1..], env),
                            NativeForm::ErtInfo => {
                                if !self.has_macro_binding("ert-info") {
                                    // (ert-info (MESSAGE-FORM &key ((:prefix P) "Info: "))
                                    //   BODY...): push (PREFIX . MESSAGE) onto the
                                    // `ert--infos' the failure reporter displays.
                                    let spec = items
                                        .get(1)
                                        .and_then(|value| value.to_vec().ok())
                                        .unwrap_or_default();
                                    let message_form = spec.first().cloned().unwrap_or(Value::Nil);
                                    let mut prefix_form = Value::String("Info: ".into());
                                    let mut index = 1usize;
                                    while index + 1 < spec.len() {
                                        if matches!(&spec[index], Value::Symbol(key) if key == ":prefix")
                                        {
                                            prefix_form = spec[index + 1].clone();
                                        }
                                        index += 2;
                                    }
                                    let message = self.eval(&message_form, env)?;
                                    let prefix = self.eval(&prefix_form, env)?;
                                    let existing =
                                        self.lookup_var("ert--infos", env).unwrap_or(Value::Nil);
                                    let infos = Value::cons(Value::cons(prefix, message), existing);
                                    // `ert--infos' is a defvar; GNU's expansion is a
                                    // dynamic let so the failure handler sees it.
                                    let restore =
                                        self.bind_special_variable("ert--infos", infos, env)?;
                                    let result = self.sf_progn(&items[2..], env);
                                    self.restore_special_binding(restore, env)?;
                                    return result;
                                }
                            }
                        }
                    }
                }

                // Check for macro expansion.  A callsite's expansion is
                // cached against the definition generation: compiled GNU
                // code expands each macro call exactly once, and per-eval
                // re-expansion (pcase/rx/when-let machinery) dominated
                // interpreted hot loops.
                if let Value::Symbol(name) = &items[0]
                    && !self.source_call_known_not_macro(&macro_calls)
                {
                    let lexical = self.lambda_capture_override().unwrap_or_else(|| {
                        self.lookup_var("lexical-binding", env)
                            .is_some_and(|value| value.is_truthy())
                    });
                    if let Some(expanded) =
                        self.cached_source_macro_expansion(&macro_calls, lexical)
                    {
                        return self.eval(&expanded, env);
                    }
                    if let Some(expanded) = self.try_macroexpand(name, &items[1..], env)? {
                        self.cache_source_macro_expansion(
                            &macro_calls,
                            expr,
                            lexical,
                            expanded.clone(),
                        );
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
        let prepared = if let Value::Symbol(name) = &items[0] {
            self.resolve_source_symbol_call(name, env, source_resolution)?
        } else {
            FunctionResolution::Resolved(self.eval(&items[0], env)?)
        };
        // While the arguments evaluate, the call is visible in backtraces as
        // an in-progress frame with its unevaluated argument forms, the way
        // GNU records the eval of a list form.
        let unevald_frame = matches!(&items[0], Value::Symbol(_));
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
        match (&items[0], prepared) {
            (Value::Symbol(name), FunctionResolution::DirectBuiltin(facts)) => {
                self.dispatch_named_builtin(name, facts, Some(CallName::Symbol(name)), &args, env)
            }
            (Value::Symbol(name), FunctionResolution::Resolved(func)) => {
                self.call_function_value_named(func, Some(CallName::Symbol(name)), &args, env)
            }
            (_, FunctionResolution::Resolved(func)) => {
                self.call_function_value_named(func, None, &args, env)
            }
            (_, FunctionResolution::DirectBuiltin(_)) => {
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
                || (facts.builtin
                    && !facts.special_form
                    && !facts.autoloadable
                    && !self.function_index_has(name)))
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

    fn call_function_value_inner(
        &mut self,
        func: Value,
        original_name: Option<CallName<'_>>,
        args: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        // A record with a cached program is a genuine byte-code function
        // (only execute_record populates the cache), so skip the
        // lambda/autoload probes and the record-type guards below.
        if let Value::Record(id) = &func
            && (*id as usize)
                .checked_sub(1)
                .and_then(|index| self.bytecode_program_cache.get(index))
                .is_some_and(|slot| slot.is_some())
        {
            return crate::lisp::bytecode::vm::execute_record(self, *id, args, env);
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
                let func = if is_lambda_form(&func) {
                    self.eval(&func, env)?
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
                        // A file-less environment (unit tests) falls back to
                        // the native arm when one exists.
                        Err(error) => {
                            if crate::lisp::primitives::is_builtin(name) {
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
                        return crate::lisp::bytecode::vm::execute_record(self, id, args, env);
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
                if is_semantic_lambda_params(params) {
                    return self.call_semantic_lambda(body, closure_env, args);
                }
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
                frame.push(Self::fresh_frame_identity());
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
                let call_capture_override = closure_eval_context
                    .filter(|context| self.lambda_capture_override() != Some(*context));
                if let Some(capture) = call_capture_override {
                    self.push_lambda_eval_context(capture, false);
                }
                let previous_activation = self.enter_activation();
                let result = if closure_env.borrow().is_empty() && !lexical_closure {
                    // Run directly on the caller's chain: cloning the whole
                    // chain per call made deep call stacks quadratic (the
                    // erc two-network scenario spends most of its time
                    // there).  Truncate (not pop) at the call boundary: a
                    // non-local exit can leave binding frames above the
                    // argument frame, and those must not leak into the
                    // caller's environment.
                    let caller_len = env.len();
                    env.push(frame.into());
                    let previous_floor = self.special_scan_floor;
                    self.special_scan_floor = caller_len;
                    let result = self.sf_progn(function_executable_body(body), env);
                    self.special_scan_floor = previous_floor;
                    env.truncate(caller_len);
                    result
                } else if body_has_marker(body, ":closure-transparent-env")
                    || body_has_marker(body, ":closure-oclosure")
                {
                    // Advice wrappers are plumbing: run them on the caller's
                    // environment chain with the wrapper's captured frames
                    // appended, so lexical mutations made below the wrapper
                    // still reach the calling scope.  Oclosures (nadvice's
                    // advice objects) are the same plumbing; their
                    // identity-stamped slot frames keep two objects'
                    // look-alike slot frames from unifying.
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
                    env.push(frame.into());
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
                    if std::env::var_os("EMAXX_DEBUG_OCLOSURE").is_some() {
                        for frame in call_env.iter().rev() {
                            if frame
                                .iter()
                                .any(|(k, _)| k == crate::lisp::eval::OCLOSURE_TYPE_MARKER)
                            {
                                let how = frame
                                    .iter()
                                    .find(|(k, _)| k == "how")
                                    .map(|(_, v)| format!("{v}"));
                                let cdr = frame
                                    .iter()
                                    .find(|(k, _)| k == "cdr")
                                    .map(|(_, v)| format!("{:.30}", format!("{v}")));
                                eprintln!(
                                    "[oclosure] invoke how={how:?} cdr={cdr:?} frames={}",
                                    call_env.len()
                                );
                                break;
                            }
                        }
                    }
                    let captured_len = call_env.len();
                    call_env.push(vec![("__closure-isolated-current-env".into(), Value::T)].into());
                    call_env.push(frame.into());
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
                            call_env.push(frame.into());
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

    fn call_semantic_lambda(
        &mut self,
        body: &[Value],
        closure_env: &SharedEnv,
        args: &[Value],
    ) -> Result<Value, LispError> {
        if args.len() != 3 {
            return Err(LispError::WrongNumberOfArgs(
                "lambda".to_string(),
                args.len(),
            ));
        }
        if let Some(value) = eval_generated_semantic_lambda_body(body, args)? {
            return Ok(value);
        }
        let mut call_env = closure_env.borrow().clone();
        call_env.push(
            vec![
                ("vals".into(), Self::stored_value(args[0].clone())),
                ("start".into(), Self::stored_value(args[1].clone())),
                ("end".into(), Self::stored_value(args[2].clone())),
            ]
            .into(),
        );
        self.sf_progn(function_executable_body(body), &mut call_env)
    }

    // ── Special forms ──
}

fn is_semantic_lambda_params(params: &[String]) -> bool {
    params == ["vals", "start", "end"]
}

fn eval_generated_semantic_lambda_body(
    body: &[Value],
    args: &[Value],
) -> Result<Option<Value>, LispError> {
    let executable = function_executable_body(body);
    if executable.len() != 2 || !is_ignore_vals_form(&executable[0]) {
        return Ok(None);
    }
    match eval_semantic_action_expr(&executable[1], args) {
        Ok(value) => Ok(Some(value)),
        Err(_) => Ok(None),
    }
}

fn is_ignore_vals_form(value: &Value) -> bool {
    value.to_vec().is_ok_and(|items| {
        matches!(
            items.as_slice(),
            [Value::Symbol(head), Value::Symbol(arg)]
                if head == "ignore" && arg == "vals"
        )
    })
}

fn eval_semantic_action_expr(expr: &Value, args: &[Value]) -> Result<Value, LispError> {
    match expr {
        Value::Symbol(symbol) if symbol == "vals" => Ok(args[0].clone()),
        Value::Symbol(symbol) if symbol == "start" => Ok(args[1].clone()),
        Value::Symbol(symbol) if symbol == "end" => Ok(args[2].clone()),
        Value::Cons(_) => {
            let items = expr.to_vec()?;
            let Some(Value::Symbol(head)) = items.first() else {
                return Err(LispError::Signal("unsupported semantic action".into()));
            };
            match head.as_str() {
                "quote" if items.len() == 2 => Ok(items[1].clone()),
                "nth" if items.len() == 3 => {
                    let index = match eval_semantic_action_expr(&items[1], args)? {
                        Value::Integer(index) if index >= 0 => index as usize,
                        _ => return Err(LispError::Signal("invalid nth index".into())),
                    };
                    let values = eval_semantic_action_expr(&items[2], args)?;
                    Ok(values.to_vec()?.get(index).cloned().unwrap_or(Value::Nil))
                }
                "car" if items.len() == 2 => Ok(eval_semantic_action_expr(&items[1], args)?
                    .car()
                    .unwrap_or(Value::Nil)),
                "cdr" if items.len() == 2 => Ok(eval_semantic_action_expr(&items[1], args)?
                    .cdr()
                    .unwrap_or(Value::Nil)),
                "list" => {
                    let values = items[1..]
                        .iter()
                        .map(|item| eval_semantic_action_expr(item, args))
                        .collect::<Result<Vec<_>, _>>()?;
                    Ok(Value::list(values))
                }
                "append" => {
                    let mut values = Vec::new();
                    for item in &items[1..] {
                        let value = eval_semantic_action_expr(item, args)?;
                        if value.is_nil() {
                            continue;
                        }
                        match value.to_vec() {
                            Ok(list) => values.extend(list),
                            Err(_) => values.push(value),
                        }
                    }
                    Ok(Value::list(values))
                }
                "cons" if items.len() == 3 => Ok(Value::cons(
                    eval_semantic_action_expr(&items[1], args)?,
                    eval_semantic_action_expr(&items[2], args)?,
                )),
                "1+" if items.len() == 2 => match eval_semantic_action_expr(&items[1], args)? {
                    Value::Integer(value) => Ok(Value::Integer(value + 1)),
                    _ => Err(LispError::Signal("invalid 1+ argument".into())),
                },
                "concat" => {
                    let mut text = String::new();
                    for item in &items[1..] {
                        text.push_str(&semantic_action_string(&eval_semantic_action_expr(
                            item, args,
                        )?)?);
                    }
                    Ok(Value::String(text.into()))
                }
                "if" if items.len() >= 3 => {
                    if eval_semantic_action_expr(&items[1], args)?.is_truthy() {
                        eval_semantic_action_expr(&items[2], args)
                    } else if let Some(else_expr) = items.get(3) {
                        eval_semantic_action_expr(else_expr, args)
                    } else {
                        Ok(Value::Nil)
                    }
                }
                "member" if items.len() == 3 => {
                    let needle = eval_semantic_action_expr(&items[1], args)?;
                    let haystack = eval_semantic_action_expr(&items[2], args)?;
                    let found = haystack
                        .to_vec()
                        .ok()
                        .is_some_and(|items| items.iter().any(|item| item == &needle));
                    Ok(if found { Value::T } else { Value::Nil })
                }
                "delete" if items.len() == 3 => {
                    let needle = eval_semantic_action_expr(&items[1], args)?;
                    let list = eval_semantic_action_expr(&items[2], args)?;
                    let items = list
                        .to_vec()?
                        .into_iter()
                        .filter(|item| item != &needle)
                        .collect::<Vec<_>>();
                    Ok(Value::list(items))
                }
                "semantic-tag" => semantic_action_tag(&items[1..], args),
                "semantic-tag-new-variable" => {
                    semantic_action_typed_tag("variable", &items[1..], args)
                }
                "semantic-tag-new-function" => {
                    semantic_action_typed_tag("function", &items[1..], args)
                }
                "semantic-tag-new-type" => semantic_action_type_tag(&items[1..], args),
                "semantic-tag-new-include" => semantic_action_include_tag(&items[1..], args),
                _ => Err(LispError::Signal("unsupported semantic action".into())),
            }
        }
        _ => Ok(expr.clone()),
    }
}

fn semantic_action_string(value: &Value) -> Result<String, LispError> {
    match value {
        Value::String(text) => Ok(text.to_string()),
        Value::Symbol(text) => Ok(text.to_string()),
        Value::Integer(value) => Ok(value.to_string()),
        _ => Err(LispError::TypeError("string".into(), value.type_name())),
    }
}

fn semantic_action_tag(exprs: &[Value], args: &[Value]) -> Result<Value, LispError> {
    if exprs.len() < 2 {
        return Err(LispError::WrongNumberOfArgs(
            "semantic-tag".into(),
            exprs.len(),
        ));
    }
    let name = eval_semantic_action_expr(&exprs[0], args)?;
    let class = eval_semantic_action_expr(&exprs[1], args)?;
    let attrs = semantic_action_plist(&exprs[2..], args)?;
    Ok(Value::list([name, class, attrs, Value::Nil, Value::Nil]))
}

fn semantic_action_typed_tag(
    class: &str,
    exprs: &[Value],
    args: &[Value],
) -> Result<Value, LispError> {
    if exprs.len() < 3 {
        return Err(LispError::WrongNumberOfArgs(
            format!("semantic-tag-new-{class}"),
            exprs.len(),
        ));
    }
    let name = eval_semantic_action_expr(&exprs[0], args)?;
    let type_value = eval_semantic_action_expr(&exprs[1], args)?;
    let third_key = if class == "function" {
        ":arguments"
    } else {
        ":default-value"
    };
    let third_value = eval_semantic_action_expr(&exprs[2], args)?;
    let mut attrs = vec![
        Value::Symbol(":type".into()),
        type_value,
        Value::Symbol(third_key.into()),
        third_value,
    ];
    attrs.extend(semantic_action_plist_items(&exprs[3..], args)?);
    Ok(Value::list([
        name,
        Value::Symbol(class.into()),
        semantic_action_filter_plist(attrs),
        Value::Nil,
        Value::Nil,
    ]))
}

fn semantic_action_type_tag(exprs: &[Value], args: &[Value]) -> Result<Value, LispError> {
    if exprs.len() < 4 {
        return Err(LispError::WrongNumberOfArgs(
            "semantic-tag-new-type".into(),
            exprs.len(),
        ));
    }
    let name = eval_semantic_action_expr(&exprs[0], args)?;
    let type_value = eval_semantic_action_expr(&exprs[1], args)?;
    let members = eval_semantic_action_expr(&exprs[2], args)?;
    let parents = eval_semantic_action_expr(&exprs[3], args)?;
    let superclasses = parents.car().unwrap_or(Value::Nil);
    let interfaces = parents.cdr().unwrap_or(Value::Nil);
    let mut attrs = vec![
        Value::Symbol(":type".into()),
        type_value,
        Value::Symbol(":members".into()),
        members,
        Value::Symbol(":superclasses".into()),
        superclasses,
        Value::Symbol(":interfaces".into()),
        interfaces,
    ];
    attrs.extend(semantic_action_plist_items(&exprs[4..], args)?);
    Ok(Value::list([
        name,
        Value::Symbol("type".into()),
        semantic_action_filter_plist(attrs),
        Value::Nil,
        Value::Nil,
    ]))
}

fn semantic_action_include_tag(exprs: &[Value], args: &[Value]) -> Result<Value, LispError> {
    if exprs.len() < 2 {
        return Err(LispError::WrongNumberOfArgs(
            "semantic-tag-new-include".into(),
            exprs.len(),
        ));
    }
    let name = eval_semantic_action_expr(&exprs[0], args)?;
    let system_flag = eval_semantic_action_expr(&exprs[1], args)?;
    let mut attrs = vec![Value::Symbol(":system-flag".into()), system_flag];
    attrs.extend(semantic_action_plist_items(&exprs[2..], args)?);
    Ok(Value::list([
        name,
        Value::Symbol("include".into()),
        semantic_action_filter_plist(attrs),
        Value::Nil,
        Value::Nil,
    ]))
}

fn semantic_action_plist(exprs: &[Value], args: &[Value]) -> Result<Value, LispError> {
    Ok(semantic_action_filter_plist(semantic_action_plist_items(
        exprs, args,
    )?))
}

fn semantic_action_plist_items(exprs: &[Value], args: &[Value]) -> Result<Vec<Value>, LispError> {
    exprs
        .iter()
        .map(|expr| eval_semantic_action_expr(expr, args))
        .collect()
}

fn semantic_action_filter_plist(items: Vec<Value>) -> Value {
    let mut filtered = Vec::new();
    let mut iter = items.into_iter();
    while let Some(key) = iter.next() {
        let Some(value) = iter.next() else {
            break;
        };
        let skip = value.is_nil()
            || matches!(&value, Value::String(text) if text.is_empty())
            || matches!(value, Value::Integer(0));
        if !skip {
            filtered.insert(0, value);
            filtered.insert(0, key);
        }
    }
    Value::list(filtered)
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
