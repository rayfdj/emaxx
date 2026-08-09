use super::*;

/// Emaxx-owned bootstrap macro expanders, keyed by one typed registry.
///
/// Some of these names also have native evaluation forms. Keeping their
/// expansion identity here lets the ordinary evaluator decide whether a
/// cached non-macro call can bypass macro setup without duplicating a string
/// whitelist beside the expansion implementation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum BuiltinMacroForm {
    ClCase,
    ClWithGensyms,
    Push,
    Pop,
    Incf,
    Decf,
    Setf,
    Prog2,
    ClSymbolMacrolet,
    ClMacrolet,
    ErtSimulateKeys,
    CLangConst,
    CLangDefconstEvalImmediately,
    Letrec,
    ClDefstruct,
    DefineDerivedMode,
    NamedLet,
    WithWrapperHook,
    SubrWithWrapperHookNoWarnings,
    WithSelectedFrame,
}

impl BuiltinMacroForm {
    fn for_name(name: &str) -> Option<Self> {
        Some(match name {
            "cl-case" => Self::ClCase,
            "cl-with-gensyms" => Self::ClWithGensyms,
            "push" => Self::Push,
            "pop" => Self::Pop,
            "cl-incf" | "incf" => Self::Incf,
            "cl-decf" | "decf" => Self::Decf,
            "setf" => Self::Setf,
            "prog2" => Self::Prog2,
            "cl-symbol-macrolet" => Self::ClSymbolMacrolet,
            "cl-macrolet" => Self::ClMacrolet,
            "ert-simulate-keys" => Self::ErtSimulateKeys,
            "c-lang-const" => Self::CLangConst,
            "c-lang-defconst-eval-immediately" => Self::CLangDefconstEvalImmediately,
            "letrec" => Self::Letrec,
            "cl-defstruct" => Self::ClDefstruct,
            "define-derived-mode" => Self::DefineDerivedMode,
            "named-let" => Self::NamedLet,
            "with-wrapper-hook" => Self::WithWrapperHook,
            "subr--with-wrapper-hook-no-warnings" => Self::SubrWithWrapperHookNoWarnings,
            "with-selected-frame" => Self::WithSelectedFrame,
            _ => return None,
        })
    }
}

/// Native behavior that must still run even after global lookup has proved a
/// name is not an ordinary Lisp macro. This is the sole name registry used by
/// both the fast rejection path and the actual dispatch below.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum NativeMacroProbe {
    Backquote,
    Pcase,
    OclosureDefine,
    OclosureLambda,
    NeverExpand,
    Builtin(BuiltinMacroForm),
}

impl NativeMacroProbe {
    fn for_name(name: &str) -> Option<Self> {
        if is_backquote_head(name) {
            return Some(Self::Backquote);
        }
        Some(match name {
            "pcase" | "pcase-exhaustive" | "pcase-let" | "pcase-let*" | "pcase-dolist" => {
                Self::Pcase
            }
            "oclosure-define" => Self::OclosureDefine,
            "oclosure-lambda" => Self::OclosureLambda,
            "cl-defgeneric" | "cl-defmethod" => Self::NeverExpand,
            _ => Self::Builtin(BuiltinMacroForm::for_name(name)?),
        })
    }
}

fn backquote_splice_elements(interp: &Interpreter, value: Value) -> Result<Vec<Value>, LispError> {
    // Runtime keymaps have identity-bearing host storage but expose GNU's
    // public `(keymap ...)' list shape.  Backquote splicing is a list
    // consumer just like `append' and must use that projection rather than
    // trying to iterate the raw record.
    let mut items = crate::lisp::primitives::list_sequence_items(interp, &value)?;
    if matches!(
        items.first(),
        Some(Value::Symbol(symbol)) if symbol == "vector-literal"
    ) {
        items.remove(0);
    }
    Ok(items)
}

impl Interpreter {
    /// Materialize `#[...]' and ordinary `#s(...)' reader forms throughout a
    /// freshly-read object.  GNU creates record objects in the reader, even
    /// below `quote'.  Emaxx keeps parsing independent of an Interpreter, so
    /// perform that object-allocation step at the read/evaluation boundary.
    /// Mutating surrounding cons cells in place preserves reader sharing.
    pub(crate) fn materialize_read_record_literals(
        &mut self,
        value: &Value,
    ) -> Result<Value, LispError> {
        self.materialize_read_record_literals_inner(
            value,
            &mut std::collections::HashSet::new(),
            &mut std::collections::HashMap::new(),
        )
    }

    fn materialize_read_record_literals_inner(
        &mut self,
        value: &Value,
        seen: &mut std::collections::HashSet<usize>,
        records: &mut std::collections::HashMap<usize, Value>,
    ) -> Result<Value, LispError> {
        let Some((car_cell, cdr_cell)) = (value).cons_cells() else {
            return Ok(value.clone());
        };
        let identity = car_cell.cell_id();
        if is_record_literal_reader_form(value) {
            if let Some(record) = records.get(&identity) {
                return Ok(record.clone());
            }
            if !seen.insert(identity) {
                return Err(LispError::ReadError("circular record literal".into()));
            }
            let items = value.to_vec()?;
            let mut slots = Vec::with_capacity(items.len().saturating_sub(1));
            for slot in &items[1..] {
                slots.push(self.materialize_read_record_literals_inner(slot, seen, records)?);
            }
            let record = self.eval_record_literal_form(&slots, &mut Env::new())?;
            records.insert(identity, record.clone());
            return Ok(record);
        }
        if !seen.insert(identity) {
            return Ok(value.clone());
        }
        let car = car_cell.borrow().clone();
        *car_cell.borrow_mut() =
            self.materialize_read_record_literals_inner(&car, seen, records)?;
        let cdr = cdr_cell.borrow().clone();
        *cdr_cell.borrow_mut() =
            self.materialize_read_record_literals_inner(&cdr, seen, records)?;
        Ok(value.clone())
    }

    pub(super) fn eval_backquote(
        &mut self,
        expr: &Value,
        env: &mut Env,
    ) -> Result<Value, LispError> {
        self.eval_backquote_with_depth(expr, env, 0)
    }

    pub(super) fn eval_record_literal_form(
        &mut self,
        slots: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let mut values = Vec::with_capacity(slots.len());
        for slot in slots {
            values.push(self.eval(slot, env)?);
        }
        if let Some(first) = values.first()
            && let Ok(type_name) = first.as_symbol()
        {
            if type_name == "interpreted-function" {
                return self.make_interpreted_closure_value(&values[1..]);
            }
            return Ok(self.create_record(type_name, values[1..].to_vec()));
        }
        Ok(self.create_record("literal-record", values))
    }

    /// Construct GNU's interpreted `#[ARGS BODY ENV ...]' closure object.
    ///
    /// GNU serializes both compiled and interpreted closures with `#[...]'.
    /// Emaxx stores interpreted closures directly as `Value::Lambda', so the
    /// reader materialization boundary translates the pseudovector slots into
    /// the native representation while retaining lexical bindings and local
    /// special declarations from ENV.
    pub(crate) fn make_interpreted_closure_value(
        &mut self,
        slots: &[Value],
    ) -> Result<Value, LispError> {
        if !(3..=6).contains(&slots.len()) {
            return Err(LispError::Signal("Invalid interpreted closure".into()));
        }
        let params = self.parse_params(&slots[0])?;
        let body = slots[1].to_vec()?;
        if body.is_empty() {
            return Err(LispError::Signal("Invalid interpreted closure body".into()));
        }

        let lexical = !slots[2].is_nil();
        let mut frame = Vec::new();
        for entry in slots[2].to_vec()? {
            match entry {
                Value::Cons(cons_cell) => {
                    let car = &cons_cell.car;
                    let cdr = &cons_cell.cdr;
                    let name = car.borrow().as_symbol()?.to_string();
                    let tail = cdr.borrow().clone();
                    // Accept both GNU's usual (NAME . VALUE) lexical binding
                    // and the equivalent one-element-list spelling already
                    // accepted by `make-interpreted-closure'.
                    let value = match tail {
                        Value::Cons(cell) if cell.cdr.borrow().is_nil() => {
                            cell.car.borrow().clone()
                        }
                        other => other,
                    };
                    frame.push((name, Self::stored_value(value)));
                }
                Value::Symbol(name) => {
                    frame.push(self.captured_local_special_marker(&name));
                }
                // `t' is GNU's empty-lexical-environment sentinel.  Other
                // non-binding entries are ignored by GNU's assq lookup too.
                Value::T | Value::Nil => {}
                _ => {}
            }
        }
        let closure_env = shared_env(if frame.is_empty() {
            Vec::new()
        } else {
            vec![frame.into()]
        });
        if lexical {
            self.mark_lexical_closure_env(&closure_env);
        } else {
            self.mark_closure_eval_context(&closure_env, false);
        }

        let mut lambda_body = Vec::with_capacity(body.len() + 2);
        if let Some(doc) = slots.get(4).filter(|value| !value.is_nil()) {
            lambda_body.push(doc.clone());
        }
        if let Some(spec) = slots.get(5).filter(|value| !value.is_nil()) {
            if spec.to_vec().ok().is_some_and(|items| {
                matches!(items.first(), Some(Value::Symbol(symbol)) if symbol == "interactive")
            }) {
                lambda_body.push(spec.clone());
            } else {
                lambda_body.push(Value::list([
                    Value::Symbol("interactive".into()),
                    spec.clone(),
                ]));
            }
        }
        lambda_body.extend(body);
        Ok(Value::lambda(
            params.into(),
            lambda_body.into(),
            closure_env,
        ))
    }

    pub(super) fn eval_backquote_with_depth(
        &mut self,
        expr: &Value,
        env: &mut Env,
        depth: usize,
    ) -> Result<Value, LispError> {
        if let Some((_kind, value)) = backquote_unquote_form(expr) {
            if depth == 0 {
                return self.eval(&value, env);
            }
            // Preserve the original head symbol (`\,'/`\,@' vs the
            // canonical names): pcase patterns rebuilt through nested
            // templates must keep the reader's raw symbols.
            let head = expr.to_vec()?[0].clone();
            return Ok(Value::list([
                head,
                self.eval_backquote_with_depth(&value, env, depth - 1)?,
            ]));
        }

        if let Some(body) = nested_backquote_body(expr) {
            let head = expr.to_vec()?[0].clone();
            return Ok(Value::list([
                head,
                self.eval_backquote_with_depth(&body, env, depth + 1)?,
            ]));
        }

        match expr {
            Value::Cons(_) => {
                let mut result: Vec<Value> = Vec::new();
                let mut current = expr.clone();
                loop {
                    if backquote_unquote_form(&current).is_some() {
                        let tail = self.eval_backquote_with_depth(&current, env, depth)?;
                        return Ok(cons_list_with_tail(result, tail));
                    }
                    if !result.is_empty() && is_backquote_atomic_cons_tail(&current) {
                        let tail = self.eval_backquote_with_depth(&current, env, depth)?;
                        return Ok(cons_list_with_tail(result, tail));
                    }
                    match current {
                        Value::Cons(cons_cell) => {
                            let car = &cons_cell.car;
                            let cdr = &cons_cell.cdr;
                            let car_value = car.borrow().clone();
                            let cdr_value = cdr.borrow().clone();

                            if depth == 0
                                && let Some(("comma-at", value)) =
                                    backquote_unquote_form(&car_value)
                            {
                                let evaled = self.eval(&value, env)?;
                                result.extend(backquote_splice_elements(self, evaled)?);
                                current = cdr_value;
                                continue;
                            }

                            result.push(self.eval_backquote_with_depth(&car_value, env, depth)?);
                            current = cdr_value;
                        }
                        Value::Nil => break,
                        other => {
                            let tail = self.eval_backquote_with_depth(&other, env, depth)?;
                            return Ok(cons_list_with_tail(result, tail));
                        }
                    }
                }
                let result = Value::list(result);
                if depth == 0
                    && (is_record_literal_reader_form(expr)
                        || is_char_table_literal_reader_form(expr))
                {
                    return self.eval(&result, env);
                }
                Ok(result)
            }
            _ => Ok(expr.clone()),
        }
    }

    // ── Macros ──

    pub(super) fn sf_defmacro(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 3 {
            return Err(LispError::WrongNumberOfArgs("defmacro".into(), items.len()));
        }
        let name = items[1].as_symbol()?.to_string();
        let replaces_autoload = self
            .logical_function_binding(&name, &Env::new())
            .is_some_and(|binding| crate::lisp::primitives::autoload_parts(&binding).is_some());
        let params_list = items[2].to_vec()?;
        let mut params = Vec::new();
        for p in &params_list {
            params.push(p.as_symbol()?.to_string());
        }
        // Body starts at index 3, skip docstrings
        let body_start = if items.len() > 4 {
            if matches!(&items[3], Value::String(_) | Value::StringObject(_)) {
                4
            } else {
                3
            }
        } else {
            3
        };
        // Process and skip (declare ...) forms.
        let body_start = if body_start < items.len() {
            if let Value::Cons(_) = &items[body_start] {
                if let Ok(decl) = items[body_start].to_vec() {
                    if let Some(Value::Symbol(s)) = decl.first() {
                        if s == "declare" {
                            self.record_defmacro_declarations(&name, &decl[1..]);
                            body_start + 1
                        } else {
                            body_start
                        }
                    } else {
                        body_start
                    }
                } else {
                    body_start
                }
            } else {
                body_start
            }
        } else {
            body_start
        };
        let body: Vec<Value> = items[body_start..].to_vec();
        let lambda_form = Value::list(
            std::iter::once(Value::Symbol("lambda".into()))
                .chain(std::iter::once(Value::list(
                    params
                        .iter()
                        .cloned()
                        .map(|value| Value::Symbol(value.into())),
                )))
                .chain(body.iter().cloned()),
        );
        let expander = self.eval(&lambda_form, env)?;
        self.push_macro_binding(MacroBinding {
            name: name.clone(),
            expander: expander.clone(),
        });
        self.record_definition_in_load_history("defun", &name);
        // Pending advice on a macro: GNU defalias hands the fresh
        // (macro . EXPANDER) cell to `defalias-fset-function', and nadvice
        // fsets the advised cell back (the cell wins over the macro table).
        if self
            .get_symbol_property(&name, "defalias-fset-function")
            .is_some_and(|value| value.is_truthy())
        {
            let cell = Value::cons(Value::Symbol("macro".into()), expander);
            if let Some(old_definition) = self.logical_function_binding(&name, &Env::new()) {
                self.record_function_redefinition(&name, old_definition);
            }
            self.defalias_fset_function_handles(&name, &cell, env);
        } else if replaces_autoload {
            // Loading an autoloaded macro replaces its ordinary function
            // cell.  Other source macros stay in the compact macro table so
            // dumped byte-code facades keep their introspection metadata.
            let cell = Value::cons(Value::Symbol("macro".into()), expander);
            if let Some(old_definition) = self.logical_function_binding(&name, &Env::new()) {
                self.record_function_redefinition(&name, old_definition);
            }
            self.set_function_binding(&name, Some(cell));
        }
        Ok(Value::Symbol(name.into()))
    }

    fn record_defmacro_declarations(&mut self, name: &str, declarations: &[Value]) {
        for declaration in declarations {
            let Ok(parts) = declaration.to_vec() else {
                continue;
            };
            match (parts.first(), parts.get(1)) {
                (Some(Value::Symbol(head)), Some(spec)) if head == "debug" => {
                    self.put_symbol_property(name, "edebug-form-spec", spec.clone());
                }
                // GNU macro-declarations-alist: (obsolete NEW WHEN) runs
                // `make-obsolete', which stores (NEW nil WHEN).
                (Some(Value::Symbol(head)), Some(new)) if head == "obsolete" => {
                    let when = parts.get(2).cloned().unwrap_or(Value::Nil);
                    self.put_symbol_property(
                        name,
                        "byte-obsolete-info",
                        Value::list([new.clone(), Value::Nil, when]),
                    );
                }
                _ => {}
            }
        }
    }

    pub(super) fn sf_easy_menu_define(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() != 5 {
            return Err(LispError::WrongNumberOfArgs(
                "easy-menu-define".into(),
                items.len().saturating_sub(1),
            ));
        }
        let Some(symbol_name) = (match &items[1] {
            Value::Nil => None,
            Value::Symbol(name) => Some(name.to_string()),
            other => {
                return Err(LispError::TypeError("symbol".into(), other.type_name()));
            }
        }) else {
            return Ok(Value::Nil);
        };

        if self.lookup_var(&symbol_name, env).is_none() {
            self.set_variable(
                &symbol_name,
                crate::lisp::primitives::keymap_placeholder(Some(&symbol_name)),
                env,
            );
        }
        if self.lookup_function(&symbol_name, env).is_err() {
            self.set_function_binding(&symbol_name, Some(Value::BuiltinFunc("ignore".into())));
        }
        Ok(Value::Symbol(symbol_name.into()))
    }

    /// Implement GNU's ordinary `defalias' primitive after its arguments
    /// have been evaluated by the normal function-call path.
    pub(crate) fn defalias_value(
        &mut self,
        args: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if !(2..=3).contains(&args.len()) {
            return Err(LispError::WrongNumberOfArgs("defalias".into(), args.len()));
        }
        let name = args[0].as_symbol()?.to_string();
        let function = args[1].clone();
        let docstring = args.get(2).cloned().unwrap_or(Value::Nil);
        self.validate_function_binding(&name, &function)?;
        let old_definition = self.logical_function_binding(&name, &Env::new());
        self.record_definition_in_load_history("defun", &name);
        if let Some(old_definition) = old_definition {
            self.record_function_redefinition(&name, old_definition);
        }
        if crate::lisp::primitives::prefer_builtin_override(&name) {
            // Calls still resolve through the preferred native primitive,
            // but retain GNU's logical function cell for symbol-function,
            // alias chasing, compiler macros, and generalized variables.
            self.set_function_binding(&name, Some(function));
        } else if self.defalias_fset_function_handles(&name, &function, env) {
            self.advice_note_new_definition(&name);
        } else {
            // Like fset: only a (macro . EXPANDER) cell or a symbol alias
            // keeps macro-ness; any other definition erases the macro.
            let keeps_macro = matches!(&function, Value::Symbol(_))
                || function
                    .cons_values()
                    .is_some_and(|(car, _)| matches!(&car, Value::Symbol(s) if s == "macro"));
            if !keeps_macro {
                self.shadow_macro_binding(&name);
            }
            // A nil function definition voids the cell.  In particular,
            // loadhist uses `(defalias NAME nil)' while unloading; leaving
            // a literal nil binding here would hide any dumped autoload and
            // turn the next call into `(invalid-function nil)'.
            // GNU unloads a generic's `(defun . NAME)' entry before its
            // `cl-defmethod' entries.  Native method wrappers are the only
            // representation Emaxx has to peel, so keep that live chain until
            // loadhist's generic-owned cl-defmethod handler removes the
            // recorded methods.  A generic with no methods still follows the
            // ordinary defalias-to-nil path.
            let defer_native_generic_unload = function.is_nil()
                && self
                    .lookup_var("loadhist-unload-filename", env)
                    .is_some_and(|value| value.is_truthy())
                && self
                    .get_symbol_property(&name, "emaxx-cl-defmethod-specializers")
                    .and_then(|value| value.to_vec().ok())
                    .is_some_and(|specializers| !specializers.is_empty());
            if !function.is_nil()
                || (!self.defer_unloaded_defsubst(&name, env) && !defer_native_generic_unload)
            {
                self.set_function_binding(
                    &name,
                    if function.is_nil() {
                        None
                    } else {
                        Some(function)
                    },
                );
            }
            self.advice_note_new_definition(&name);
        }
        if !docstring.is_nil() {
            self.put_symbol_property(&name, "function-documentation", docstring);
        }
        Ok(Value::Symbol(name.into()))
    }

    pub(super) fn try_macroexpand(
        &mut self,
        name: &str,
        args: &[Value],
        env: &mut Env,
    ) -> Result<Option<Value>, LispError> {
        self.try_macroexpand_with_environment(name, args, None, env)
    }

    pub(super) fn macro_nonexpansion_is_callsite_cacheable(&self, name: &str) -> bool {
        NativeMacroProbe::for_name(name).is_none()
    }

    /// Invoke a macro-environment expander (from cl-flet/cl-labels/
    /// cl-macrolet and friends) in a fresh environment.  Expanders are
    /// closures over their own captured bindings; running them inside the
    /// caller's frames would let same-named caller locals (e.g. the `var'
    /// bound by cl-labels' pcase-let*) shadow the captured ones.
    fn call_macro_environment_expander(
        &mut self,
        expander: Value,
        name: &str,
        args: &[Value],
    ) -> Result<Value, LispError> {
        let mut expander_env = Env::new();
        self.call_function_value(expander, Some(name), args, &mut expander_env)
    }

    pub(super) fn try_macroexpand_with_environment(
        &mut self,
        name: &str,
        args: &[Value],
        macro_environment: Option<&Value>,
        env: &mut Env,
    ) -> Result<Option<Value>, LispError> {
        let native_probe = NativeMacroProbe::for_name(name);
        // GNU resolves the function cell first and binds `lexical-binding'
        // only after that resolution proves the form is a macro call. A
        // generation-stamped global non-macro verdict gives us the same
        // answer without buffer-local binding, watcher notification, and
        // unwind setup on every ordinary interpreted call. An explicit
        // macro environment and native bootstrap probes must still run.
        if macro_environment.is_none() && native_probe.is_none() && self.known_not_macro(name) {
            return Ok(None);
        }
        self.try_macroexpand_with_environment_inner(
            name,
            args,
            macro_environment,
            native_probe,
            env,
        )
    }

    /// Run only the macro expander itself with GNU's temporary
    /// `lexical-binding' value.
    ///
    /// GNU resolves and autoloads a function cell before establishing this
    /// special binding. Restricting the scope likewise keeps ordinary
    /// non-macro probes free of buffer-local writes and watcher events.
    fn with_macro_lexical_binding<T>(
        &mut self,
        env: &mut Env,
        operation: impl FnOnce(&mut Self, &mut Env) -> Result<T, LispError>,
    ) -> Result<T, LispError> {
        let Some(lexical) = self.lambda_capture_override() else {
            return operation(self, env);
        };
        let restore = self.bind_special_variable(
            "lexical-binding",
            if lexical { Value::T } else { Value::Nil },
            env,
        )?;
        let result = operation(self, env);
        let restore_result = self.restore_special_binding(restore, env);
        match (result, restore_result) {
            (Err(error), _) => Err(error),
            (Ok(_), Err(error)) => Err(error),
            (Ok(value), Ok(())) => Ok(value),
        }
    }

    fn try_macroexpand_with_environment_inner(
        &mut self,
        name: &str,
        args: &[Value],
        macro_environment: Option<&Value>,
        native_probe: Option<NativeMacroProbe>,
        env: &mut Env,
    ) -> Result<Option<Value>, LispError> {
        if let Some(expander) = macro_environment_expander(macro_environment, name) {
            return self
                .with_macro_lexical_binding(env, |interp, _env| {
                    interp.call_macro_environment_expander(expander, name, args)
                })
                .map(Some);
        }

        // Backquote evaluation is native (the reader encodes unquotes as
        // `comma'/`comma-at' markers that GNU backquote.el's `\`' macro does
        // not recognize, so expanding through it would drop the unquotes).
        // Treat backquote as a special form here; `eval' and
        // `macroexpand-all' both handle it natively.
        match native_probe {
            Some(NativeMacroProbe::Backquote) => {
                // GNU's `\`' macro expands templates into list/append
                // constructor code (generator.el's CPS transformer requires
                // that shape). Nested backquotes stay opaque.
                if let Some(template) = args.first() {
                    return self.with_macro_lexical_binding(env, |_interp, _env| {
                        Ok(Some(backquote_template_code(template)))
                    });
                }
                return Ok(None);
            }
            // The pcase family is evaluated natively UNLESS GNU pcase.el has
            // been loaded (its macros then own the family; the reader encodes
            // patterns with the same `\`'/`\,' symbols pcase.el registers).
            Some(NativeMacroProbe::Pcase) => {
                self.ensure_gnu_pcase_loaded();
                if !self.has_macro_binding(name) {
                    return Ok(None);
                }
            }
            // GNU oclosure.el signals duplicate-slot errors at macroexpansion
            // time; the forms themselves stay native special forms.
            Some(NativeMacroProbe::OclosureDefine) => {
                return self.with_macro_lexical_binding(env, |interp, _env| {
                    interp.validate_oclosure_define_slots(args)?;
                    Ok(None)
                });
            }
            Some(NativeMacroProbe::OclosureLambda) => {
                return self.with_macro_lexical_binding(env, |_interp, _env| {
                    validate_oclosure_lambda_slots(args)?;
                    Ok(None)
                });
            }
            // These definition forms are Rust-backed bootstrap facades. If a
            // client explicitly loads cl-generic.el for its higher-level
            // helpers, its macro definitions must not lower later forms into
            // a second, incompatible dispatch engine.
            Some(NativeMacroProbe::NeverExpand) => return Ok(None),
            Some(NativeMacroProbe::Builtin(_)) | None => {}
        }

        // A cached (and still current) not-a-macro verdict skips the whole
        // probe.  cl-flet frame shadowing can only make a name LESS of a
        // macro, so a global "not a macro" verdict stays correct under any
        // frames; verdicts influenced by frames are never cached.
        if self.known_not_macro(name) {
            if let Some(NativeMacroProbe::Builtin(form)) = native_probe
                && let Some(expanded) = self.try_builtin_macroexpand_in_context(form, args, env)?
            {
                return Ok(Some(expanded));
            }
            return Ok(None);
        }

        let mut attempted_autoload = false;
        let expander = loop {
            if let Some(NativeMacroProbe::Builtin(form)) = native_probe
                && let Some(expanded) = self.try_builtin_macroexpand_in_context(form, args, env)?
            {
                return Ok(Some(expanded));
            }

            // GNU keeps macros in the function cell as (macro . EXPANDER);
            // nadvice fsets advised macros (and advised macro ALIASES) that
            // way, so the cell wins over the native macro table.
            if let Some(expander) = self.function_cell_macro_expander(name, env) {
                let expanded = self.with_macro_lexical_binding(env, |interp, env| {
                    interp.call_function_value(expander, Some(name), args, env)
                })?;
                return Ok(Some(expanded));
            }

            if let Some(binding) = self.resolve_macro_binding(name) {
                break binding;
            }

            if attempted_autoload {
                self.note_not_macro(name);
                return Ok(None);
            }
            // Only global state can hold an autoload stub (env frames
            // never resolve to autoload conses), so probe the macro
            // position without scanning ordinary frames.
            let Some((mut function, from_frame)) = self.macro_position_function(name, env) else {
                self.note_not_macro(name);
                return Ok(None);
            };
            // A native fallback arm can shadow a preloaded macro autoload
            // (add-function before nadvice.el loads): honor the autoload.
            if crate::lisp::primitives::autoload_parts(&function).is_none()
                && let Some(stub) = preload::builtin_autoload_function(name)
                && crate::lisp::primitives::autoload_parts(&stub).is_some()
            {
                function = stub;
            }
            let Some((file, _, _kind)) = crate::lisp::primitives::autoload_parts(&function) else {
                if !from_frame {
                    self.note_not_macro(name);
                }
                return Ok(None);
            };
            let loads_macro =
                crate::lisp::primitives::autoload_is_macro(self, Some(name), &function);
            if !loads_macro {
                self.note_not_macro(name);
                return Ok(None);
            }
            // A file-less environment (unit tests) falls back to whatever
            // native arm handles the name.
            if self.load_target_with_env(&file, env).is_err() {
                return Ok(None);
            }
            attempted_autoload = true;
        };

        // Advised macros expand through the advice chain: the expander
        // (resolved now, so redefinitions are seen) is the innermost
        // function and receives the unevaluated argument forms.
        let expander = if self
            .advice_registry
            .get(name)
            .is_some_and(|state| !state.entries.is_empty())
        {
            self.compose_advice_chain(name, expander)
        } else {
            expander
        };
        let expanded = self.with_macro_lexical_binding(env, |interp, env| {
            interp.call_function_value(expander, Some(name), args, env)
        })?;
        Ok(Some(expanded))
    }

    pub(crate) fn macroexpand_1_form_with_environment(
        &mut self,
        form: &Value,
        macro_environment: Option<&Value>,
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let Ok(items) = form.to_vec() else {
            return Ok(form.clone());
        };
        let Some(Value::Symbol(name)) = items.first() else {
            return Ok(form.clone());
        };
        Ok(self
            .try_macroexpand_with_environment(name, &items[1..], macro_environment, env)?
            .unwrap_or_else(|| form.clone()))
    }

    /// Walk a backquote template, macro-expanding only the expressions under
    /// `comma'/`comma-at' markers at the current backquote depth.  Nested
    /// backquotes raise the depth; their unquotes stay untouched until the
    /// matching level is reached, mirroring GNU backquote nesting.
    fn macroexpand_all_backquote_template(
        &mut self,
        value: &Value,
        macro_environment: Option<&Value>,
        env: &mut Env,
        depth: usize,
    ) -> Result<Value, LispError> {
        if let Some((_, inner)) = backquote_unquote_form(value) {
            let marker = value.car()?;
            let inner = if depth == 0 {
                self.macroexpand_all_form_with_environment(&inner, macro_environment, env)?
            } else {
                self.macroexpand_all_backquote_template(&inner, macro_environment, env, depth - 1)?
            };
            return Ok(Value::list([marker, inner]));
        }
        if let Some(body) = nested_backquote_body(value) {
            let head = value.car()?;
            let body =
                self.macroexpand_all_backquote_template(&body, macro_environment, env, depth + 1)?;
            return Ok(Value::list([head, body]));
        }
        if value.cons_values().is_some() {
            // Walk the list spine iteratively: templates can be arbitrarily
            // long and per-cons recursion would exhaust the stack.
            let mut fronts = Vec::new();
            let mut tail = value.clone();
            loop {
                if backquote_unquote_form(&tail).is_some() || nested_backquote_body(&tail).is_some()
                {
                    tail = self.macroexpand_all_backquote_template(
                        &tail,
                        macro_environment,
                        env,
                        depth,
                    )?;
                    break;
                }
                match tail.cons_values() {
                    Some((car, cdr)) => {
                        fronts.push(self.macroexpand_all_backquote_template(
                            &car,
                            macro_environment,
                            env,
                            depth,
                        )?);
                        tail = cdr;
                    }
                    None => break,
                }
            }
            let mut rebuilt = tail;
            for front in fronts.into_iter().rev() {
                rebuilt = Value::cons(front, rebuilt);
            }
            return Ok(rebuilt);
        }
        Ok(value.clone())
    }

    // GNU macroexp-macroexpand warns when macroexpand-all expands a macro
    // carrying `byte-obsolete-info' (macroexp-warn-and-return sends the
    // warning through `message', so cl-letf interception reaches it).
    fn warn_when_expanding_obsolete_macro(
        &mut self,
        name: &str,
        env: &mut Env,
    ) -> Result<(), LispError> {
        if self
            .get_symbol_property(name, "byte-obsolete-info")
            .is_none_or(|info| !info.is_truthy())
            || !self.has_lisp_function("macroexp-warn-and-return")
            || !self.has_lisp_function("macroexp--obsolete-warning")
        {
            return Ok(());
        }
        let quoted_name = Value::list([
            Value::Symbol("quote".into()),
            Value::Symbol(name.to_string().into()),
        ]);
        let warn = Value::list([
            Value::Symbol("macroexp-warn-and-return".into()),
            Value::list([
                Value::Symbol("macroexp--obsolete-warning".into()),
                quoted_name.clone(),
                Value::list([
                    Value::Symbol("get".into()),
                    quoted_name.clone(),
                    Value::list([
                        Value::Symbol("quote".into()),
                        Value::Symbol("byte-obsolete-info".into()),
                    ]),
                ]),
                Value::String("macro".into()),
            ]),
            Value::Nil,
            Value::list([
                Value::Symbol("list".into()),
                Value::list([
                    Value::Symbol("quote".into()),
                    Value::Symbol("obsolete".into()),
                ]),
                quoted_name.clone(),
            ]),
            Value::Nil,
            quoted_name,
        ]);
        self.eval(&warn, env)?;
        Ok(())
    }

    /// Run one GNU `macroexpand-all' scope with a private dynamic
    /// `macroexp--dynvars' list.  Declarations encountered while walking
    /// sequential forms remain visible to later siblings, but never leak out
    /// of the form whose expansion established the scope.
    pub(crate) fn macroexpand_all_scoped_with_environment(
        &mut self,
        form: &Value,
        macro_environment: Option<&Value>,
        env: &mut Env,
    ) -> Result<Value, LispError> {
        self.with_macroexp_dynvars_scope(env, |interp, env| {
            interp.macroexpand_all_form_with_environment(form, macro_environment, env)
        })
    }

    fn with_macroexp_dynvars_scope(
        &mut self,
        env: &mut Env,
        expand: impl FnOnce(&mut Self, &mut Env) -> Result<Value, LispError>,
    ) -> Result<Value, LispError> {
        let current = self
            .lookup_var("macroexp--dynvars", env)
            .unwrap_or(Value::Nil);
        let restore = self.bind_special_variable("macroexp--dynvars", current, env)?;
        let result = expand(self, env);
        let restore_result = self.restore_special_binding(restore, env);
        match (result, restore_result) {
            (Err(error), _) => Err(error),
            (Ok(_), Err(error)) => Err(error),
            (Ok(value), Ok(())) => Ok(value),
        }
    }

    pub(crate) fn macroexpand_all_form_with_environment(
        &mut self,
        form: &Value,
        macro_environment: Option<&Value>,
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let Ok(items) = form.to_vec() else {
            return Ok(form.clone());
        };
        let Some(head) = items.first() else {
            return Ok(Value::Nil);
        };
        if let Value::Symbol(name) = head {
            // The macro environment takes precedence even over `function' and
            // `quote': cl-flet/cl-labels bind `function' to cl--labels-convert
            // so `#'local-fn' references get rewritten (GNU macroexpand-1
            // consults ENVIRONMENT before anything else).
            if matches!(name.as_str(), "quote" | "function")
                && let Some(expander) = macro_environment_expander(macro_environment, name)
            {
                let expanded = self.call_macro_environment_expander(expander, name, &items[1..])?;
                if &expanded != form {
                    return self.macroexpand_all_form_with_environment(
                        &expanded,
                        macro_environment,
                        env,
                    );
                }
                // Unchanged (e.g. cl--labels-convert on a non-local
                // function): fall through so `#'(lambda ...)' bodies still
                // get descended into below.
            }
            match name.as_str() {
                "quote" => return Ok(form.clone()),
                "function" => {
                    // GNU macroexp--expand-all descends into `#'(lambda ...)'
                    // bodies; other function forms stay opaque.
                    if let Some(func) = items.get(1)
                        && let Ok(func_items) = func.to_vec()
                        && matches!(
                            func_items.first(),
                            Some(Value::Symbol(symbol)) if symbol == "lambda"
                        )
                    {
                        return self.with_macroexp_dynvars_scope(env, |interp, env| {
                            let mut expanded_lambda = Vec::with_capacity(func_items.len());
                            expanded_lambda.push(func_items[0].clone());
                            if let Some(params) = func_items.get(1) {
                                expanded_lambda.push(params.clone());
                            }
                            for item in func_items.iter().skip(2) {
                                expanded_lambda.push(
                                    interp.macroexpand_all_form_with_environment(
                                        item,
                                        macro_environment,
                                        env,
                                    )?,
                                );
                            }
                            Ok(Value::list([
                                items[0].clone(),
                                Value::list(expanded_lambda),
                            ]))
                        });
                    }
                    return Ok(form.clone());
                }
                "defvar" | "defconst"
                    if items
                        .get(1)
                        .is_some_and(|value| matches!(value, Value::Symbol(_))) =>
                {
                    let Value::Symbol(declared) = &items[1] else {
                        unreachable!("guarded above");
                    };
                    let current = self
                        .lookup_var("macroexp--dynvars", env)
                        .unwrap_or(Value::Nil);
                    self.set_variable(
                        "macroexp--dynvars",
                        Value::cons(Value::Symbol(declared.clone()), current),
                        env,
                    );
                    let mut expanded = items[..2].to_vec();
                    for item in &items[2..] {
                        expanded.push(self.macroexpand_all_form_with_environment(
                            item,
                            macro_environment,
                            env,
                        )?);
                    }
                    return Ok(Value::list(expanded));
                }
                "eval-when-compile" => {
                    let value = if items.len() <= 1 {
                        Value::Nil
                    } else if items.len() == 2 {
                        self.eval(&items[1], env)?
                    } else {
                        let progn = Value::list(
                            std::iter::once(Value::Symbol("progn".into()))
                                .chain(items[1..].iter().cloned()),
                        );
                        self.eval(&progn, env)?
                    };
                    return Ok(quoted_literal(&value));
                }
                "eval-and-compile" => {
                    // GNU macroexpand-all evaluates eval-and-compile bodies
                    // at expansion time (compile-time side effects, e.g.
                    // rx-define's `(put ... 'rx-definition ...)') AND keeps
                    // the forms so they also run at load/runtime.
                    for item in &items[1..] {
                        self.eval(item, env)?;
                    }
                    let mut expanded = vec![items[0].clone()];
                    for item in &items[1..] {
                        expanded.push(self.macroexpand_all_form_with_environment(
                            item,
                            macro_environment,
                            env,
                        )?);
                    }
                    return Ok(Value::list(expanded));
                }
                "let" | "let*" | "letrec" => {
                    return self.with_macroexp_dynvars_scope(env, |interp, env| {
                        interp.macroexpand_all_let_form_with_environment(
                            &items,
                            macro_environment,
                            env,
                        )
                    });
                }
                // Backquote templates stay in place; only the unquoted
                // expressions inside them are macro-expanded (this is how the
                // env expanders from cl-flet/cl-labels reach `,(local-fn ...)'
                // calls while the template text survives verbatim).
                // The pcase family is evaluated natively and its patterns
                // use backquote SYNTAX; expand only the subject and clause
                // bodies so patterns survive while cl-labels-style env
                // expanders still reach the code inside.
                "pcase" | "pcase-exhaustive"
                    if items.len() >= 2 && !self.has_macro_binding("pcase") =>
                {
                    let mut rebuilt = vec![items[0].clone()];
                    rebuilt.push(self.macroexpand_all_form_with_environment(
                        &items[1],
                        macro_environment,
                        env,
                    )?);
                    for clause in &items[2..] {
                        let Ok(parts) = clause.to_vec() else {
                            rebuilt.push(clause.clone());
                            continue;
                        };
                        if parts.is_empty() {
                            rebuilt.push(clause.clone());
                            continue;
                        }
                        let mut new_clause = vec![parts[0].clone()];
                        for body in &parts[1..] {
                            new_clause.push(self.macroexpand_all_form_with_environment(
                                body,
                                macro_environment,
                                env,
                            )?);
                        }
                        rebuilt.push(Value::list(new_clause));
                    }
                    return Ok(Value::list(rebuilt));
                }
                // GNU's definition macros consume names and lambda lists as
                // syntax, then macro-expand only executable body positions.
                // Emaxx keeps these bootstrap facades native, so its generic
                // walker must preserve the same operand roles.  Otherwise a
                // setf method name such as `(setf accessor)' is mistaken for
                // a one-argument invocation once gv.el has been autoloaded.
                "defun" | "defmacro" | "defsubst" | "cl-defun" | "cl-defmacro"
                    if items.len() >= 3 =>
                {
                    return self.with_macroexp_dynvars_scope(env, |interp, env| {
                        interp.macroexpand_all_definition_body(&items, 3, macro_environment, env)
                    });
                }
                "cl-defmethod" if items.len() >= 3 => {
                    let lambda_list_index =
                        items.iter().enumerate().skip(2).find_map(|(index, value)| {
                            matches!(value, Value::Cons(_) | Value::Nil).then_some(index)
                        });
                    if let Some(lambda_list_index) = lambda_list_index {
                        return self.macroexpand_all_definition_body(
                            &items,
                            lambda_list_index + 1,
                            macro_environment,
                            env,
                        );
                    }
                    return Ok(form.clone());
                }
                "pcase-let" | "pcase-let*" | "pcase-dolist"
                    if items.len() >= 2 && !self.has_macro_binding("pcase-let") =>
                {
                    let mut rebuilt = vec![items[0].clone(), items[1].clone()];
                    for body in &items[2..] {
                        rebuilt.push(self.macroexpand_all_form_with_environment(
                            body,
                            macro_environment,
                            env,
                        )?);
                    }
                    return Ok(Value::list(rebuilt));
                }
                other if is_backquote_head(other) && items.len() == 2 => {
                    let template = self.macroexpand_all_backquote_template(
                        &items[1],
                        macro_environment,
                        env,
                        0,
                    )?;
                    // GNU's `\`' is a macro: macroexpand-all yields the
                    // constructor code.
                    return Ok(backquote_template_code(&template));
                }
                _ => {}
            }
            if let Some(expander) = macro_environment_expander(macro_environment, name) {
                let expanded = self.call_macro_environment_expander(expander, name, &items[1..])?;
                return self.macroexpand_all_form_with_environment(
                    &expanded,
                    macro_environment,
                    env,
                );
            } else if let Some(expanded) =
                self.try_macroexpand_with_environment(name, &items[1..], None, env)?
            {
                self.warn_when_expanding_obsolete_macro(name, env)?;
                return self.macroexpand_all_form_with_environment(
                    &expanded,
                    macro_environment,
                    env,
                );
            }
        }

        if matches!(head, Value::Symbol(name) if name == "lambda") {
            return self.with_macroexp_dynvars_scope(env, |interp, env| {
                let mut expanded = Vec::with_capacity(items.len());
                expanded.push(items[0].clone());
                if let Some(params) = items.get(1) {
                    expanded.push(params.clone());
                }
                for item in &items[2..] {
                    expanded.push(interp.macroexpand_all_form_with_environment(
                        item,
                        macro_environment,
                        env,
                    )?);
                }
                Ok(Value::list(expanded))
            });
        }

        let mut expanded = Vec::with_capacity(items.len());
        if matches!(head, Value::Symbol(_)) {
            expanded.push(items[0].clone());
            for item in &items[1..] {
                expanded.push(self.macroexpand_all_form_with_environment(
                    item,
                    macro_environment,
                    env,
                )?);
            }
        } else {
            for item in &items {
                expanded.push(self.macroexpand_all_form_with_environment(
                    item,
                    macro_environment,
                    env,
                )?);
            }
        }
        Ok(Value::list(expanded))
    }

    fn macroexpand_all_definition_body(
        &mut self,
        items: &[Value],
        body_start: usize,
        macro_environment: Option<&Value>,
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let mut rebuilt = items[..body_start].to_vec();
        for form in &items[body_start..] {
            let head = form
                .to_vec()
                .ok()
                .and_then(|parts| parts.first().cloned())
                .and_then(|head| head.as_symbol().ok().map(str::to_string));
            let verbatim = matches!(form, Value::String(_) | Value::StringObject(_))
                || head
                    .as_deref()
                    .is_some_and(|head| head == "declare" || head == "interactive");
            if verbatim {
                rebuilt.push(form.clone());
            } else {
                rebuilt.push(self.macroexpand_all_form_with_environment(
                    form,
                    macro_environment,
                    env,
                )?);
            }
        }
        Ok(Value::list(rebuilt))
    }

    fn macroexpand_all_let_form_with_environment(
        &mut self,
        items: &[Value],
        macro_environment: Option<&Value>,
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let Some(bindings_value) = items.get(1) else {
            return Ok(Value::list(items.iter().cloned()));
        };
        let bindings = bindings_value.to_vec()?;
        let mut expanded_bindings = Vec::with_capacity(bindings.len());
        for binding in bindings {
            match binding {
                Value::Symbol(_) => expanded_bindings.push(binding),
                Value::Cons(_) => {
                    let parts = binding.to_vec()?;
                    if parts.is_empty() {
                        expanded_bindings.push(Value::Nil);
                        continue;
                    }
                    let mut expanded = Vec::with_capacity(parts.len());
                    expanded.push(parts[0].clone());
                    for initializer in &parts[1..] {
                        expanded.push(self.macroexpand_all_form_with_environment(
                            initializer,
                            macro_environment,
                            env,
                        )?);
                    }
                    expanded_bindings.push(Value::list(expanded));
                }
                _ => expanded_bindings.push(binding),
            }
        }

        let mut expanded = Vec::with_capacity(items.len());
        expanded.push(items[0].clone());
        expanded.push(Value::list(expanded_bindings));
        for body in &items[2..] {
            expanded.push(self.macroexpand_all_form_with_environment(
                body,
                macro_environment,
                env,
            )?);
        }
        Ok(Value::list(expanded))
    }

    fn builtin_macroexpand_enabled(&self, form: BuiltinMacroForm) -> bool {
        match form {
            BuiltinMacroForm::ClCase => !self.has_lisp_macro("cl-case"),
            BuiltinMacroForm::Letrec => !self.has_lisp_macro("letrec"),
            _ => true,
        }
    }

    pub(super) fn try_builtin_macroexpand_in_context(
        &mut self,
        form: BuiltinMacroForm,
        args: &[Value],
        env: &mut Env,
    ) -> Result<Option<Value>, LispError> {
        if !self.builtin_macroexpand_enabled(form) {
            return Ok(None);
        }
        self.with_macro_lexical_binding(env, |interp, env| {
            interp.try_builtin_macroexpand(form, args, env)
        })
    }

    fn try_builtin_macroexpand(
        &mut self,
        form: BuiltinMacroForm,
        args: &[Value],
        env: &mut Env,
    ) -> Result<Option<Value>, LispError> {
        match form {
            BuiltinMacroForm::ClCase => self.expand_cl_case(args, env).map(Some),
            BuiltinMacroForm::ClWithGensyms => self.expand_cl_with_gensyms(args, env).map(Some),
            // GNU push/pop/cl-incf/cl-decf are macros; expand them for
            // macroexpand-all consumers (generator.el's CPS transformer)
            // while normal evaluation keeps hitting the native forms.
            BuiltinMacroForm::Push if args.len() == 2 => {
                let value = args[0].clone();
                let place = args[1].clone();
                let setter = if matches!(place, Value::Symbol(_)) {
                    "setq"
                } else {
                    "setf"
                };
                Ok(Some(Value::list([
                    Value::Symbol(setter.into()),
                    place.clone(),
                    Value::list([Value::Symbol("cons".into()), value, place]),
                ])))
            }
            BuiltinMacroForm::Pop if args.len() == 1 => {
                let place = args[0].clone();
                let setter = if matches!(place, Value::Symbol(_)) {
                    "setq"
                } else {
                    "setf"
                };
                Ok(Some(Value::list([
                    Value::Symbol("prog1".into()),
                    Value::list([Value::Symbol("car".into()), place.clone()]),
                    Value::list([
                        Value::Symbol(setter.into()),
                        place.clone(),
                        Value::list([Value::Symbol("cdr".into()), place]),
                    ]),
                ])))
            }
            BuiltinMacroForm::Incf | BuiltinMacroForm::Decf
                if !args.is_empty() && args.len() <= 2 =>
            {
                let place = args[0].clone();
                let delta = args.get(1).cloned().unwrap_or(Value::Integer(1));
                let operator = if form == BuiltinMacroForm::Incf {
                    "+"
                } else {
                    "-"
                };
                let setter = if matches!(place, Value::Symbol(_)) {
                    "setq"
                } else {
                    "setf"
                };
                Ok(Some(Value::list([
                    Value::Symbol(setter.into()),
                    place.clone(),
                    Value::list([Value::Symbol(operator.into()), place, delta]),
                ])))
            }
            // GNU setf is a macro; with plain symbol places it expands to
            // setq (the CPS transformer only understands the expansion).
            BuiltinMacroForm::Setf
                if !args.is_empty()
                    && args.len().is_multiple_of(2)
                    && args
                        .iter()
                        .step_by(2)
                        .all(|place| matches!(place, Value::Symbol(_))) =>
            {
                let mut rebuilt = vec![Value::Symbol("setq".into())];
                rebuilt.extend(args.iter().cloned());
                Ok(Some(Value::list(rebuilt)))
            }
            // prog2 reaches generator.el's CPS transformer, which only
            // understands progn/prog1; give it the equivalent expansion.
            BuiltinMacroForm::Prog2 if args.len() >= 2 => {
                let mut prog1_form = vec![Value::Symbol("prog1".into())];
                prog1_form.extend(args[1..].iter().cloned());
                Ok(Some(Value::list([
                    Value::Symbol("progn".into()),
                    args[0].clone(),
                    Value::list(prog1_form),
                ])))
            }
            // GNU cl-symbol-macrolet substitutes variable references in
            // the body during macroexpansion (generator.el's variable
            // renaming builds on it).
            BuiltinMacroForm::ClSymbolMacrolet if args.len() >= 2 => {
                let mut substitutions = Vec::new();
                for binding in args[0].to_vec().unwrap_or_default() {
                    let parts = binding.to_vec().unwrap_or_default();
                    if let (Some(Value::Symbol(name)), Some(expansion)) =
                        (parts.first(), parts.get(1))
                    {
                        substitutions.push((name.to_string(), expansion.clone()));
                    }
                }
                let macro_environment = self
                    .lookup_var("macroexpand-all-environment", env)
                    .filter(Value::is_truthy);
                let mut forms = vec![Value::Symbol("progn".into())];
                let mut failure = None;
                for body_form in &args[1..] {
                    let substituted = substitute_symbol_macros(body_form, &substitutions);
                    match self.macroexpand_all_form_with_environment(
                        &substituted,
                        macro_environment.as_ref(),
                        env,
                    ) {
                        // A surrounding macro environment can introduce a
                        // reference to one of the symbol macros (obsolete
                        // `labels' turns #'<name> into its generated local
                        // variable).  Apply the substitutions once more to
                        // that fully expanded result, just as GNU's combined
                        // macro/symbol-macro environment does.
                        Ok(expanded) => {
                            forms.push(substitute_symbol_macros(&expanded, &substitutions))
                        }
                        Err(error) => {
                            failure = Some(error);
                            break;
                        }
                    }
                }
                if let Some(error) = failure {
                    return Err(error);
                }
                Ok(Some(Value::list(forms)))
            }
            // GNU cl-macrolet is a macro: its expansion is the body,
            // macroexpanded with the local macros in effect (generator.el's
            // CPS transformer relies on `macroexpand' doing this).
            BuiltinMacroForm::ClMacrolet if args.len() >= 2 => {
                let local_macros = self.parse_cl_macrolet_bindings(&args[0], env)?;
                let (local_start, local_count) = self.push_local_macros(&local_macros);
                let mut forms = vec![Value::Symbol("progn".into())];
                let mut failure = None;
                for body_form in &args[1..] {
                    match self.macroexpand_all_form_with_environment(body_form, None, env) {
                        Ok(expanded) => forms.push(expanded),
                        Err(error) => {
                            failure = Some(error);
                            break;
                        }
                    }
                }
                self.drain_local_macros(local_start, local_count);
                if let Some(error) = failure {
                    return Err(error);
                }
                Ok(Some(Value::list(forms)))
            }
            BuiltinMacroForm::ErtSimulateKeys => self.expand_ert_simulate_keys(args).map(Some),
            BuiltinMacroForm::CLangConst => self.expand_c_lang_const(args, env).map(Some),
            BuiltinMacroForm::CLangDefconstEvalImmediately => self
                .expand_c_lang_defconst_eval_immediately(args, env)
                .map(Some),
            BuiltinMacroForm::Letrec => self.expand_letrec(args).map(Some),
            BuiltinMacroForm::ClDefstruct => {
                // GNU cl-defstruct signals at expansion time when the name
                // fails cl--struct-name-p (nil, keyword, or built-in type).
                let struct_name = args.first().and_then(|spec| match spec {
                    Value::Symbol(name) => Some(name.to_string()),
                    Value::Cons(_) => spec
                        .car()
                        .ok()
                        .and_then(|head| head.as_symbol().ok().map(str::to_string)),
                    _ => None,
                });
                if let Some(name) = struct_name
                    && (name == "nil"
                        || name.starts_with(':')
                        || crate::lisp::primitives::is_builtin_class_name(&name))
                {
                    return Err(LispError::SignalValue(Value::list([
                        Value::Symbol("wrong-type-argument".into()),
                        Value::Symbol("cl-struct-name-p".into()),
                        Value::Symbol(name.into()),
                        Value::Symbol("name".into()),
                    ])));
                }
                // GNU's expansion ultimately defines every generated
                // function through `defalias'; find-func's
                // macro-expanding search looks for those subforms.  Emit
                // GNU-shaped stubs ahead of the native definer.
                Ok(Some(Self::cl_defstruct_expansion_with_stubs(args)))
            }
            BuiltinMacroForm::DefineDerivedMode => {
                // Same shape rationale as cl-defstruct: GNU's expansion
                // carries the mode function `defalias' and the
                // MODE-hook/-map/-syntax-table `defvar's that find-func's
                // macro-expanding search looks for.
                let Some(mode) = args.first().and_then(|m| m.as_symbol().ok()) else {
                    return Ok(None);
                };
                let mut forms = vec![
                    Value::Symbol("progn".into()),
                    Value::list([
                        Value::Symbol("defalias".into()),
                        Value::list([
                            Value::Symbol("quote".into()),
                            Value::Symbol(mode.to_string().into()),
                        ]),
                        Value::list([
                            Value::Symbol("function".into()),
                            Value::Symbol("ignore".into()),
                        ]),
                    ]),
                ];
                for suffix in ["hook", "map", "syntax-table", "abbrev-table"] {
                    forms.push(Value::list([
                        Value::Symbol("defvar".into()),
                        Value::Symbol(format!("{mode}-{suffix}").into()),
                    ]));
                }
                forms.push(Value::list(
                    std::iter::once(Value::Symbol("emaxx--define-derived-mode".into()))
                        .chain(args.iter().cloned())
                        .collect::<Vec<_>>(),
                ));
                Ok(Some(Value::list(forms)))
            }
            BuiltinMacroForm::NamedLet => self.expand_named_let(args).map(Some),
            BuiltinMacroForm::WithWrapperHook => self.expand_with_wrapper_hook(args).map(Some),
            BuiltinMacroForm::SubrWithWrapperHookNoWarnings => {
                self.expand_subr_with_wrapper_hook(args).map(Some)
            }
            BuiltinMacroForm::WithSelectedFrame => {
                if args.is_empty() {
                    return Err(LispError::WrongNumberOfArgs(
                        "with-selected-frame".into(),
                        0,
                    ));
                }
                let body = &args[1..];
                Ok(Some(match body {
                    [] => Value::Nil,
                    [single] => single.clone(),
                    _ => Value::list(
                        std::iter::once(Value::Symbol("progn".into())).chain(body.iter().cloned()),
                    ),
                }))
            }
            _ => Ok(None),
        }
    }

    fn expand_c_lang_defconst_eval_immediately(
        &mut self,
        args: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if args.len() != 1 {
            return Err(LispError::WrongNumberOfArgs(
                "c-lang-defconst-eval-immediately".into(),
                args.len(),
            ));
        }
        self.eval(&args[0], env)
    }

    fn expand_c_lang_const(&mut self, args: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if args.is_empty() || args.len() > 2 {
            return Err(LispError::WrongNumberOfArgs(
                "c-lang-const".into(),
                args.len(),
            ));
        }
        let name = args[0].as_symbol()?;
        let mode = match args.get(1) {
            Some(Value::Nil) | None => None,
            Some(value) => Some(format!("{}-mode", value.as_symbol()?)),
        };
        if self
            .lookup_var("c-lang-const-expansion", env)
            .is_some_and(|value| matches!(value, Value::Symbol(symbol) if symbol == "immediate"))
        {
            let mut call = vec![
                Value::Symbol("c-get-lang-constant".into()),
                quoted_literal(&Value::Symbol(name.into())),
                Value::Nil,
            ];
            if let Some(mode) = mode {
                call.push(quoted_literal(&Value::Symbol(mode.into())));
            }
            let value = self.eval(&Value::list(call), env)?;
            return Ok(quoted_literal(&value));
        }

        let mut call = vec![
            Value::Symbol("c-get-lang-constant".into()),
            quoted_literal(&Value::Symbol(name.into())),
        ];
        if let Some(mode) = mode {
            call.push(Value::Nil);
            call.push(quoted_literal(&Value::Symbol(mode.into())));
        }
        Ok(Value::list(call))
    }

    pub(super) fn expand_with_wrapper_hook(&mut self, args: &[Value]) -> Result<Value, LispError> {
        if args.len() < 2 {
            return Err(LispError::WrongNumberOfArgs(
                "with-wrapper-hook".into(),
                args.len(),
            ));
        }
        Ok(Value::list(
            std::iter::once(Value::Symbol("subr--with-wrapper-hook-no-warnings".into()))
                .chain(args.iter().cloned()),
        ))
    }

    pub(super) fn expand_subr_with_wrapper_hook(
        &mut self,
        args: &[Value],
    ) -> Result<Value, LispError> {
        if args.len() < 2 {
            return Err(LispError::WrongNumberOfArgs(
                "subr--with-wrapper-hook-no-warnings".into(),
                args.len(),
            ));
        }

        let hook = args[0].clone();
        let arg_list = args[1].clone();
        let body = &args[2..];
        let funs = self.make_generated_symbol("funs");
        let global = self.make_generated_symbol("global");
        let argssym = self.make_generated_symbol("args");
        let runrestofhook = self.make_generated_symbol("runrestofhook");

        let lambda_body = Value::list([
            Value::Symbol("if".into()),
            Value::list([Value::Symbol("consp".into()), funs.clone()]),
            Value::list([
                Value::Symbol("if".into()),
                Value::list([
                    Value::Symbol("eq".into()),
                    Value::T,
                    Value::list([Value::Symbol("car".into()), funs.clone()]),
                ]),
                Value::list([
                    Value::Symbol("funcall".into()),
                    runrestofhook.clone(),
                    Value::list([
                        Value::Symbol("append".into()),
                        global.clone(),
                        Value::list([Value::Symbol("cdr".into()), funs.clone()]),
                    ]),
                    Value::Nil,
                    argssym.clone(),
                ]),
                Value::list([
                    Value::Symbol("apply".into()),
                    Value::list([Value::Symbol("car".into()), funs.clone()]),
                    Value::list([
                        Value::Symbol("apply-partially".into()),
                        Value::list([
                            Value::Symbol("lambda".into()),
                            Value::list([
                                funs.clone(),
                                global.clone(),
                                Value::Symbol("&rest".into()),
                                argssym.clone(),
                            ]),
                            Value::list([
                                Value::Symbol("funcall".into()),
                                runrestofhook.clone(),
                                funs.clone(),
                                global.clone(),
                                argssym.clone(),
                            ]),
                        ]),
                        Value::list([Value::Symbol("cdr".into()), funs.clone()]),
                        global.clone(),
                    ]),
                    argssym.clone(),
                ]),
            ]),
            Value::list([
                Value::Symbol("apply".into()),
                Value::list(
                    std::iter::once(Value::Symbol("lambda".into()))
                        .chain(std::iter::once(arg_list))
                        .chain(body.iter().cloned()),
                ),
                argssym.clone(),
            ]),
        ]);

        let global_form = match &hook {
            Value::Symbol(_) => Value::list([
                Value::Symbol("if".into()),
                Value::list([
                    Value::Symbol("local-variable-p".into()),
                    quoted_literal(&hook),
                ]),
                Value::list([Value::Symbol("default-value".into()), quoted_literal(&hook)]),
            ]),
            _ => Value::Nil,
        };

        let wrapper_args = args[1].to_vec()?;

        Ok(Value::list([
            Value::Symbol("letrec".into()),
            Value::list([Value::list([
                runrestofhook.clone(),
                Value::list([
                    Value::Symbol("lambda".into()),
                    Value::list([funs, global, argssym.clone()]),
                    lambda_body,
                ]),
            ])]),
            Value::list([
                Value::Symbol("funcall".into()),
                runrestofhook,
                hook,
                global_form,
                Value::list(std::iter::once(Value::Symbol("list".into())).chain(wrapper_args)),
            ]),
        ]))
    }

    pub(super) fn expand_ert_simulate_keys(&mut self, args: &[Value]) -> Result<Value, LispError> {
        if args.is_empty() {
            return Err(LispError::WrongNumberOfArgs("ert-simulate-keys".into(), 0));
        }
        let bindings = Value::list([
            Value::list([
                Value::Symbol("unread-command-events".into()),
                Value::list([
                    Value::Symbol("append".into()),
                    args[0].clone(),
                    Value::list([
                        Value::Symbol("quote".into()),
                        Value::list([Value::Integer(7), Value::Integer(7), Value::Integer(7)]),
                    ]),
                ]),
            ]),
            Value::list([Value::Symbol("executing-kbd-macro".into()), Value::T]),
        ]);
        Ok(Value::list(
            std::iter::once(Value::Symbol("let".into()))
                .chain(std::iter::once(bindings))
                .chain(args[1..].iter().cloned()),
        ))
    }

    pub(super) fn expand_cl_with_gensyms(
        &mut self,
        args: &[Value],
        _env: &mut Env,
    ) -> Result<Value, LispError> {
        let names = args
            .first()
            .ok_or_else(|| LispError::WrongNumberOfArgs("cl-with-gensyms".into(), 0))?
            .to_vec()?;
        let mut bindings = Vec::with_capacity(names.len());
        for name in names {
            let name = name.as_symbol()?.to_string();
            bindings.push(Value::list([
                Value::Symbol(name.clone().into()),
                Value::list([
                    Value::Symbol("quote".into()),
                    self.make_generated_symbol(&name),
                ]),
            ]));
        }
        Ok(Value::list(
            std::iter::once(Value::Symbol("let".into()))
                .chain(std::iter::once(Value::list(bindings)))
                .chain(args[1..].iter().cloned()),
        ))
    }

    pub(super) fn expand_letrec(&mut self, args: &[Value]) -> Result<Value, LispError> {
        let bindings = args
            .first()
            .ok_or_else(|| LispError::WrongNumberOfArgs("letrec".into(), 0))?
            .to_vec()?;
        let mut lowered_bindings = Vec::with_capacity(bindings.len());
        let mut initializers = Vec::new();

        for binding in bindings {
            match binding {
                Value::Symbol(name) => lowered_bindings.push(Value::Symbol(name)),
                Value::Cons(_) => {
                    let parts = binding.to_vec()?;
                    let Some(name_value) = parts.first() else {
                        return Err(LispError::ReadError("bad letrec binding".into()));
                    };
                    let name = name_value.as_symbol()?.to_string();
                    lowered_bindings.push(Value::Symbol(name.clone().into()));
                    if parts.len() > 1 {
                        initializers.push(Value::list([
                            Value::Symbol("setq".into()),
                            Value::Symbol(name.into()),
                            parts[1].clone(),
                        ]));
                    }
                }
                other => return Err(wrong_type_argument("listp", other)),
            }
        }

        Ok(Value::list(
            std::iter::once(Value::Symbol("let".into()))
                .chain(std::iter::once(Value::list(lowered_bindings)))
                .chain(initializers)
                .chain(args[1..].iter().cloned()),
        ))
    }

    pub(super) fn expand_named_let(&mut self, args: &[Value]) -> Result<Value, LispError> {
        let name = args
            .first()
            .ok_or_else(|| LispError::WrongNumberOfArgs("named-let".into(), 0))?
            .as_symbol()?
            .to_string();
        let bindings = args
            .get(1)
            .ok_or_else(|| LispError::WrongNumberOfArgs("named-let".into(), 1))?
            .to_vec()?;
        let mut params = Vec::with_capacity(bindings.len());
        let mut inits = Vec::with_capacity(bindings.len());
        for binding in bindings {
            match binding {
                Value::Symbol(symbol) => {
                    params.push(Value::Symbol(symbol));
                    inits.push(Value::Nil);
                }
                Value::Cons(_) => {
                    let parts = binding.to_vec()?;
                    let Some(param) = parts.first() else {
                        return Err(LispError::ReadError("bad named-let binding".into()));
                    };
                    params.push(Value::Symbol(param.as_symbol()?.to_string().into()));
                    inits.push(parts.get(1).cloned().unwrap_or(Value::Nil));
                }
                other => return Err(wrong_type_argument("listp", other)),
            }
        }

        if let Some(lowered) = self.try_expand_named_let_loop(&name, &params, &inits, &args[2..])? {
            return Ok(lowered);
        }

        let lambda = Value::list(
            std::iter::once(Value::Symbol("lambda".into()))
                .chain(std::iter::once(Value::list(params)))
                .chain(if args.len() > 2 {
                    args[2..].to_vec()
                } else {
                    vec![Value::Nil]
                }),
        );
        let binding = Value::list([Value::Symbol(name.clone().into()), lambda]);
        let call = Value::list(std::iter::once(Value::Symbol(name.into())).chain(inits));

        Ok(Value::list([
            Value::Symbol("letrec".into()),
            Value::list([binding]),
            call,
        ]))
    }

    pub(super) fn try_expand_named_let_loop(
        &mut self,
        name: &str,
        params: &[Value],
        inits: &[Value],
        body: &[Value],
    ) -> Result<Option<Value>, LispError> {
        let done_tag = self.make_generated_symbol("named-let-done");
        let bindings = Value::list(
            params
                .iter()
                .cloned()
                .zip(inits.iter().cloned())
                .map(|(param, init)| Value::list([param, init])),
        );

        let loop_body = match body {
            [single] => {
                if let Ok(items) = single.to_vec()
                    && matches!(items.first(), Some(Value::Symbol(symbol)) if symbol == "if")
                {
                    let then_forms = items
                        .get(2)
                        .cloned()
                        .map_or_else(Vec::new, |form| vec![form]);
                    let else_forms = if items.len() > 3 {
                        items[3..].to_vec()
                    } else {
                        vec![Value::Nil]
                    };
                    if !named_let_branch_safe_for_loop(name, &then_forms)
                        || !named_let_branch_safe_for_loop(name, &else_forms)
                    {
                        return Ok(None);
                    }
                    self.expand_named_let_loop_if(name, params, &done_tag, &items)?
                } else if let Some((prefix, args)) = named_let_tail_call(name, body) {
                    self.build_named_let_rebind(params, &args, &prefix)?
                } else {
                    return Ok(None);
                }
            }
            _ => {
                if let Some((prefix, args)) = named_let_tail_call(name, body) {
                    self.build_named_let_rebind(params, &args, &prefix)?
                } else {
                    return Ok(None);
                }
            }
        };

        Ok(Some(Value::list([
            Value::Symbol("let".into()),
            bindings,
            Value::list([
                Value::Symbol("catch".into()),
                quoted_literal(&done_tag),
                Value::list([Value::Symbol("while".into()), Value::T, loop_body]),
            ]),
        ])))
    }

    pub(super) fn expand_named_let_loop_if(
        &mut self,
        name: &str,
        params: &[Value],
        done_tag: &Value,
        items: &[Value],
    ) -> Result<Value, LispError> {
        let condition = items.get(1).cloned().unwrap_or(Value::Nil);
        let then_forms = items
            .get(2)
            .cloned()
            .map_or_else(Vec::new, |form| vec![form]);
        let else_forms = if items.len() > 3 {
            items[3..].to_vec()
        } else {
            vec![Value::Nil]
        };
        let then_branch = self.build_named_let_loop_branch(name, params, done_tag, &then_forms)?;
        let else_branch = self.build_named_let_loop_branch(name, params, done_tag, &else_forms)?;
        Ok(Value::list([
            Value::Symbol("if".into()),
            condition,
            then_branch,
            else_branch,
        ]))
    }

    pub(super) fn build_named_let_loop_branch(
        &mut self,
        name: &str,
        params: &[Value],
        done_tag: &Value,
        forms: &[Value],
    ) -> Result<Value, LispError> {
        if let Some((prefix, args)) = named_let_tail_call(name, forms) {
            self.build_named_let_rebind(params, &args, &prefix)
        } else {
            Ok(Value::list([
                Value::Symbol("throw".into()),
                quoted_literal(done_tag),
                forms_to_progn(forms),
            ]))
        }
    }

    pub(super) fn build_named_let_rebind(
        &mut self,
        params: &[Value],
        args: &[Value],
        prefix: &[Value],
    ) -> Result<Value, LispError> {
        if params.len() != args.len() {
            return Err(LispError::WrongNumberOfArgs("named-let".into(), args.len()));
        }
        let temp_symbols = (0..params.len())
            .map(|_| self.make_generated_symbol("named-let-arg"))
            .collect::<Vec<_>>();
        let temp_bindings = Value::list(
            temp_symbols
                .iter()
                .cloned()
                .zip(args.iter().cloned())
                .map(|(temp, arg)| Value::list([temp, arg])),
        );
        let mut setq_items = vec![Value::Symbol("setq".into())];
        for (param, temp) in params.iter().cloned().zip(temp_symbols) {
            setq_items.push(param);
            setq_items.push(temp);
        }
        let rebind = Value::list([
            Value::Symbol("let".into()),
            temp_bindings,
            Value::list(setq_items),
        ]);
        let forms = prefix
            .iter()
            .cloned()
            .chain(std::iter::once(rebind))
            .collect::<Vec<_>>();
        Ok(forms_to_progn(&forms))
    }

    pub(super) fn expand_cl_case(
        &mut self,
        args: &[Value],
        _env: &mut Env,
    ) -> Result<Value, LispError> {
        let expr = args
            .first()
            .ok_or_else(|| LispError::WrongNumberOfArgs("cl-case".into(), 0))?;
        let temp = self.make_generated_symbol("cl-case");
        let mut clauses = Vec::with_capacity(args.len().saturating_sub(1));

        for (index, clause) in args[1..].iter().enumerate() {
            let clause_items = clause.to_vec()?;
            let (keys, body) = match clause_items.split_first() {
                Some((keys, body)) => (keys, body),
                None => (&Value::Nil, &[][..]),
            };
            let test = self.expand_cl_case_clause_test(
                &temp,
                keys,
                index + 1 == args.len().saturating_sub(1),
            )?;
            let body = if body.is_empty() {
                vec![Value::Nil]
            } else {
                body.to_vec()
            };
            clauses.push(Value::list(std::iter::once(test).chain(body)));
        }

        Ok(Value::list([
            Value::Symbol("let".into()),
            Value::list([Value::list([temp.clone(), expr.clone()])]),
            Value::list(std::iter::once(Value::Symbol("cond".into())).chain(clauses)),
        ]))
    }

    pub(super) fn expand_cl_case_clause_test(
        &self,
        temp: &Value,
        keys: &Value,
        final_clause: bool,
    ) -> Result<Value, LispError> {
        if matches!(keys, Value::Symbol(name) if name == "t" || name == "otherwise") {
            if final_clause {
                return Ok(Value::T);
            }
            return Err(LispError::Signal(
                "Misplaced t or `otherwise' clause".into(),
            ));
        }

        if keys.is_nil() {
            return Ok(Value::Nil);
        }

        if let Value::Cons(_) = keys {
            let keys = keys.to_vec()?;
            let mut tests = Vec::with_capacity(keys.len());
            for key in keys {
                tests.push(Self::cl_case_key_test(temp, key));
            }
            return Ok(match tests.as_slice() {
                [] => Value::Nil,
                [single] => single.clone(),
                _ => Value::list(std::iter::once(Value::Symbol("or".into())).chain(tests)),
            });
        }

        Ok(Self::cl_case_key_test(temp, keys.clone()))
    }

    pub(super) fn cl_case_key_test(temp: &Value, key: Value) -> Value {
        Value::list([
            Value::Symbol("eql".into()),
            temp.clone(),
            quoted_literal(&key),
        ])
    }

    pub(super) fn sf_skip_unless(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Ok(Value::Nil);
        }
        // GNU's private `ert--skip-unless' treats evaluation errors like a
        // false result, so both paths skip rather than leaking the error.
        let keep_running = self
            .eval(&items[1], env)
            .is_ok_and(|value| value.is_truthy());
        if keep_running {
            Ok(Value::Nil)
        } else {
            Err(LispError::TestSkipped("Test skipped".into()))
        }
    }

    pub(super) fn sf_skip_when(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Ok(Value::Nil);
        }
        // GNU's private `ert--skip-when' treats evaluation errors like a
        // true result, so an error means skip as well.
        let should_skip = self
            .eval(&items[1], env)
            .map_or(true, |value| value.is_truthy());
        if should_skip {
            Err(LispError::TestSkipped("Test skipped".into()))
        } else {
            Ok(Value::Nil)
        }
    }

    pub(super) fn sf_rx(&mut self, items: &[Value], env: &Env) -> Result<Value, LispError> {
        Ok(Value::String(
            rx::compile_rx_sequence(self, env, &items[1..])?.into(),
        ))
    }

    pub(super) fn sf_rx_define(&mut self, items: &[Value]) -> Result<Value, LispError> {
        if items.len() < 3 {
            return Err(LispError::Signal(
                "rx-define needs name and definition".into(),
            ));
        }
        let name = items[1].as_symbol()?.to_string();
        let binding = match &items[2..] {
            [definition] => Value::list([definition.clone()]),
            [params, definition] => Value::list([params.clone(), definition.clone()]),
            _ => {
                return Err(LispError::Signal(format!(
                    "Bad `rx' definition of {name}: {}",
                    Value::list(items[2..].iter().cloned())
                )));
            }
        };
        self.put_symbol_property(&name, "rx-definition", binding);
        Ok(Value::Symbol(name.into()))
    }

    pub(super) fn sf_rx_let(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() < 3 {
            return Err(LispError::Signal("rx-let needs bindings and a body".into()));
        }
        let bindings = items[1].to_vec()?;
        let mut saved = Vec::new();
        for binding in bindings {
            let parts = binding.to_vec()?;
            if parts.len() < 2 {
                return Err(LispError::Signal("Bad `rx-let' binding".into()));
            }
            let name = parts[0].as_symbol()?.to_string();
            let value = match &parts[1..] {
                [definition] => Value::list([definition.clone()]),
                [params, definition] => Value::list([params.clone(), definition.clone()]),
                _ => {
                    return Err(LispError::Signal(format!(
                        "Bad `rx-let' definition of {name}: {}",
                        Value::list(parts[1..].iter().cloned())
                    )));
                }
            };
            saved.push((
                name.clone(),
                self.get_symbol_property(&name, "rx-definition"),
            ));
            self.put_symbol_property(&name, "rx-definition", value);
        }

        let result = self.sf_progn(&items[2..], env);
        for (name, previous) in saved.into_iter().rev() {
            if let Some(previous) = previous {
                self.put_symbol_property(&name, "rx-definition", previous);
            } else {
                self.remove_symbol_property(&name, "rx-definition");
            }
        }
        result
    }
}

// GNU oclosure-lambda rejects duplicate slot INITIALIZERS at expansion
// time ("Duplicate slot: fst").
fn validate_oclosure_lambda_slots(args: &[Value]) -> Result<(), LispError> {
    let Some(spec) = args.first() else {
        return Ok(());
    };
    let Ok(spec_items) = spec.to_vec() else {
        return Ok(());
    };
    let mut seen: Vec<String> = Vec::new();
    for binding in spec_items.get(1..).unwrap_or(&[]) {
        let slot = match binding {
            Value::Symbol(name) => Some(name.to_string()),
            other => other.to_vec().ok().and_then(|parts| {
                parts
                    .first()
                    .and_then(|v| v.as_symbol().ok().map(String::from))
            }),
        };
        let Some(slot) = slot else { continue };
        if seen.contains(&slot) {
            return Err(LispError::Signal(format!("Duplicate slot: {slot}")));
        }
        seen.push(slot);
    }
    Ok(())
}

impl Interpreter {
    // GNU oclosure-define rejects duplicate slot NAMES — within the new
    // slots and against inherited parent slots ("Duplicate slot name: a").
    pub(super) fn validate_oclosure_define_slots(&self, args: &[Value]) -> Result<(), LispError> {
        let Some(name_form) = args.first() else {
            return Ok(());
        };
        let mut parent: Option<String> = None;
        if let Ok(parts) = name_form.to_vec() {
            for option in parts.get(1..).unwrap_or(&[]) {
                if let Ok(option_parts) = option.to_vec()
                    && matches!(option_parts.first(), Some(Value::Symbol(key)) if key == ":parent")
                {
                    parent = option_parts
                        .get(1)
                        .and_then(|v| v.as_symbol().ok().map(String::from));
                }
            }
        }
        let mut seen: Vec<String> = parent
            .as_ref()
            .and_then(|parent| self.get_symbol_property(parent, "emaxx-oclosure-slots"))
            .and_then(|value| value.to_vec().ok())
            .unwrap_or_default()
            .iter()
            .filter_map(|value| value.as_symbol().ok().map(String::from))
            .collect();
        for slot in args.get(1..).unwrap_or(&[]) {
            let slot_name = match slot {
                Value::Symbol(name) => Some(name.to_string()),
                Value::Cons(_) => slot.to_vec().ok().and_then(|parts| {
                    parts
                        .first()
                        .and_then(|v| v.as_symbol().ok().map(String::from))
                }),
                _ => None,
            };
            let Some(slot_name) = slot_name else { continue };
            if seen.contains(&slot_name) {
                return Err(LispError::Signal(format!(
                    "Duplicate slot name: {slot_name}"
                )));
            }
            seen.push(slot_name);
        }
        Ok(())
    }
}

fn macro_environment_expander(macro_environment: Option<&Value>, name: &str) -> Option<Value> {
    let mut entries = macro_environment?.clone();
    while let Value::Cons(_) = &entries {
        let entry = entries.car().ok()?;
        if let Value::Cons(_) = entry {
            let symbol = entry.car().ok()?;
            if symbol.as_symbol().ok()? == name {
                return entry.cdr().ok();
            }
        }
        entries = entries.cdr().ok()?;
    }
    None
}

impl Interpreter {
    // Compute the function names a cl-defstruct generates and wrap the
    // native definition in a progn of `defalias' stubs so macro-expanded
    // output has GNU's shape (the stubs are immediately overridden by the
    // native definer that follows them).
    pub(super) fn cl_defstruct_expansion_with_stubs(args: &[Value]) -> Value {
        let (name, options) = match args.first() {
            Some(Value::Symbol(name)) => (name.to_string(), Vec::new()),
            Some(spec @ Value::Cons(_)) => {
                let parts = spec.to_vec().unwrap_or_default();
                let name = parts
                    .first()
                    .and_then(|head| head.as_symbol().ok().map(str::to_string))
                    .unwrap_or_default();
                (name, parts[1..].to_vec())
            }
            _ => (String::new(), Vec::new()),
        };
        let mut conc_name = format!("{name}-");
        let mut predicate = Some(format!("{name}-p"));
        let mut copier = Some(format!("copy-{name}"));
        let mut constructors: Vec<String> = Vec::new();
        let mut suppress_default_constructor = false;
        for option in &options {
            let Ok(parts) = option.to_vec() else { continue };
            match parts.first().and_then(|key| key.as_symbol().ok()) {
                Some(":conc-name") => {
                    conc_name = match parts.get(1) {
                        Some(Value::Symbol(prefix)) => prefix.to_string(),
                        Some(Value::String(prefix)) => prefix.to_string(),
                        _ => String::new(),
                    }
                }
                Some(":predicate") => {
                    predicate = parts.get(1).and_then(|v| match v {
                        Value::Symbol(name) => Some(name.to_string()),
                        _ => None,
                    })
                }
                Some(":copier") => {
                    copier = parts.get(1).and_then(|v| match v {
                        Value::Symbol(name) => Some(name.to_string()),
                        _ => None,
                    })
                }
                Some(":constructor") => match parts.get(1) {
                    Some(Value::Symbol(ctor)) => constructors.push(ctor.to_string()),
                    Some(Value::Nil) | None => suppress_default_constructor = true,
                    _ => {}
                },
                _ => {}
            }
        }
        if !suppress_default_constructor {
            constructors.push(format!("make-{name}"));
        }
        let mut generated = constructors;
        generated.extend(predicate);
        generated.extend(copier);
        for slot in args.iter().skip(1) {
            let slot_name = match slot {
                Value::Symbol(slot_name) => Some(slot_name.to_string()),
                Value::Cons(_) => slot
                    .car()
                    .ok()
                    .and_then(|head| head.as_symbol().ok().map(str::to_string)),
                _ => None,
            };
            if let Some(slot_name) = slot_name {
                generated.push(format!("{conc_name}{slot_name}"));
            }
        }
        let mut forms = vec![Value::Symbol("progn".into())];
        for function in generated {
            forms.push(Value::list([
                Value::Symbol("defalias".into()),
                Value::list([
                    Value::Symbol("quote".into()),
                    Value::Symbol(function.into()),
                ]),
                Value::list([
                    Value::Symbol("function".into()),
                    Value::Symbol("ignore".into()),
                ]),
            ]));
        }
        forms.push(Value::list(
            std::iter::once(Value::Symbol("emaxx--cl-defstruct".into()))
                .chain(args.iter().cloned())
                .collect::<Vec<_>>(),
        ));
        Value::list(forms)
    }
}

// Replace non-shadowed variable references to symbol-macro names with
// their expansions (GNU cl-symbol-macrolet semantics, scoped to the
// shapes generator.el produces: let/let*/lambda shadowing, quote
// opacity, and setq name rewriting).
fn substitute_symbol_macros(form: &Value, substitutions: &[(String, Value)]) -> Value {
    if substitutions.is_empty() {
        return form.clone();
    }
    match form {
        Value::Symbol(name) => substitutions
            .iter()
            .find(|(macro_name, _)| macro_name == name)
            .map(|(_, expansion)| expansion.clone())
            .unwrap_or_else(|| form.clone()),
        Value::Cons(_) => {
            let Ok(items) = form.to_vec() else {
                // Dotted pair: substitute both sides.
                if let Some((car, cdr)) = form.cons_values() {
                    return Value::cons(
                        substitute_symbol_macros(&car, substitutions),
                        substitute_symbol_macros(&cdr, substitutions),
                    );
                }
                return form.clone();
            };
            match items.first() {
                Some(Value::Symbol(head)) if head == "quote" => form.clone(),
                Some(Value::Symbol(head)) if head == "lambda" && items.len() >= 2 => {
                    let params: Vec<String> = items[1]
                        .to_vec()
                        .unwrap_or_default()
                        .iter()
                        .filter_map(|p| p.as_symbol().ok().map(str::to_string))
                        .collect();
                    let inner: Vec<(String, Value)> = substitutions
                        .iter()
                        .filter(|(name, _)| !params.contains(name))
                        .cloned()
                        .collect();
                    let mut rebuilt = vec![items[0].clone(), items[1].clone()];
                    for body in &items[2..] {
                        rebuilt.push(substitute_symbol_macros(body, &inner));
                    }
                    Value::list(rebuilt)
                }
                Some(Value::Symbol(head))
                    if (head == "let" || head == "let*") && items.len() >= 2 =>
                {
                    let mut bound = Vec::new();
                    let mut new_bindings = Vec::new();
                    for binding in items[1].to_vec().unwrap_or_default() {
                        match &binding {
                            Value::Symbol(name) => {
                                bound.push(name.to_string());
                                new_bindings.push(binding.clone());
                            }
                            Value::Cons(_) => {
                                let parts = binding.to_vec().unwrap_or_default();
                                let name = parts
                                    .first()
                                    .and_then(|n| n.as_symbol().ok())
                                    .unwrap_or_default()
                                    .to_string();
                                // In let* a later initform sees earlier
                                // shadows; approximate with the outer set
                                // for `let' and the progressive set for
                                // `let*'.
                                let active: Vec<(String, Value)> = if head == "let*" {
                                    substitutions
                                        .iter()
                                        .filter(|(n, _)| !bound.contains(n))
                                        .cloned()
                                        .collect()
                                } else {
                                    substitutions.to_vec()
                                };
                                let mut rebuilt_binding = vec![parts[0].clone()];
                                for init in &parts[1..] {
                                    rebuilt_binding.push(substitute_symbol_macros(init, &active));
                                }
                                bound.push(name);
                                new_bindings.push(Value::list(rebuilt_binding));
                            }
                            _ => new_bindings.push(binding.clone()),
                        }
                    }
                    let inner: Vec<(String, Value)> = substitutions
                        .iter()
                        .filter(|(name, _)| !bound.contains(name))
                        .cloned()
                        .collect();
                    let mut rebuilt = vec![items[0].clone(), Value::list(new_bindings)];
                    for body in &items[2..] {
                        rebuilt.push(substitute_symbol_macros(body, &inner));
                    }
                    Value::list(rebuilt)
                }
                Some(Value::Symbol(head)) if head == "setq" && items.len() >= 3 => {
                    // A symbol macro in SETQ position denotes a generalized
                    // variable.  GNU macroexp-all rewrites that pair through
                    // SETF (with-slots consequently becomes eieio-oset), not
                    // into an invalid `(setq (PLACE ...) VALUE)'.
                    let mut assignments = Vec::new();
                    for pair in items[1..].chunks(2) {
                        let Some(value) = pair.get(1) else { continue };
                        let value = substitute_symbol_macros(value, substitutions);
                        let assignment = pair[0]
                            .as_symbol()
                            .ok()
                            .and_then(|name| {
                                substitutions
                                    .iter()
                                    .find(|(macro_name, _)| macro_name == name)
                                    .map(|(_, expansion)| {
                                        Value::list([
                                            Value::Symbol("setf".into()),
                                            expansion.clone(),
                                            value.clone(),
                                        ])
                                    })
                            })
                            .unwrap_or_else(|| {
                                Value::list([Value::Symbol("setq".into()), pair[0].clone(), value])
                            });
                        assignments.push(assignment);
                    }
                    match assignments.len() {
                        0 => Value::Nil,
                        1 => assignments.pop().unwrap_or(Value::Nil),
                        _ => Value::list(
                            std::iter::once(Value::Symbol("progn".into()))
                                .chain(assignments)
                                .collect::<Vec<_>>(),
                        ),
                    }
                }
                _ => Value::list(
                    items
                        .iter()
                        .map(|item| substitute_symbol_macros(item, substitutions))
                        .collect::<Vec<_>>(),
                ),
            }
        }
        _ => form.clone(),
    }
}

// Turn a backquote template into constructor code, GNU bq_process style:
// `(a b ,x ,@y . z) => (append (list 'a 'b x) y 'z).  Unquoted leaves
// become quoted literals; self-evaluating atoms stay as-is.
fn backquote_template_code(template: &Value) -> Value {
    backquote_template_code_at_depth(template, 0)
}

fn backquote_template_code_at_depth(template: &Value, depth: usize) -> Value {
    fn quote_literal(value: &Value) -> Value {
        match value {
            Value::Nil
            | Value::T
            | Value::Integer(_)
            | Value::BigInteger(_)
            | Value::Float(_)
            | Value::String(_)
            | Value::StringObject(_) => value.clone(),
            _ => Value::list([Value::Symbol("quote".into()), value.clone()]),
        }
    }
    if let Some((kind, inner)) = backquote_unquote_form(template) {
        if depth == 0 {
            if kind == "comma" {
                return inner;
            }
            // A top-level ,@ is invalid; keep it quoted.
            return quote_literal(template);
        }
        let head = template
            .to_vec()
            .ok()
            .and_then(|items| items.first().cloned())
            .unwrap_or_else(|| Value::Symbol(kind.into()));
        return Value::list([
            Value::Symbol("list".into()),
            quote_literal(&head),
            backquote_template_code_at_depth(&inner, depth - 1),
        ]);
    }
    if let Some(body) = nested_backquote_body(template) {
        let head = template
            .to_vec()
            .ok()
            .and_then(|items| items.first().cloned())
            .unwrap_or_else(|| Value::Symbol("backquote".into()));
        return Value::list([
            Value::Symbol("list".into()),
            quote_literal(&head),
            backquote_template_code_at_depth(&body, depth + 1),
        ]);
    }
    if is_vector_literal(template) && template_tree_unquotes(template) {
        let elements = template
            .to_vec()
            .expect("vector literal is a proper internal list")
            .into_iter()
            .skip(1)
            .collect::<Vec<_>>();
        // Construct the element sequence with ordinary backquote list
        // semantics, then let the public sequence primitive restore the
        // vector container.  Carrying the internal `vector-literal' marker
        // through `(append ...)' is ambiguous: `(list 'vector-literal)'
        // itself denotes an empty vector, so append correctly consumes it.
        return Value::list([
            Value::Symbol("vconcat".into()),
            backquote_template_code_at_depth(&Value::list(elements), depth),
        ]);
    }
    // A quoted form (or vector literal) is only opaque when nothing
    // inside it unquotes: `',(f) reads as (quote (comma (f))) and must
    // still evaluate the unquote.
    if is_backquote_atomic_cons_tail(template) && !template_tree_unquotes(template) {
        return quote_literal(template);
    }
    if !matches!(template, Value::Cons(_)) {
        return quote_literal(template);
    }
    // Walk the list spine, batching plain elements into (list ...) chunks
    // and splicing ,@ elements and dotted tails through (append ...).
    let mut segments: Vec<Value> = Vec::new();
    let mut chunk: Vec<Value> = Vec::new();
    let mut tail = template.clone();
    loop {
        if let Some((kind, inner)) = backquote_unquote_form(&tail) {
            // Dotted unquote tail: `(a . ,x)
            if depth == 0 && kind == "comma" {
                if !chunk.is_empty() {
                    segments.push(Value::list(
                        std::iter::once(Value::Symbol("list".into()))
                            .chain(chunk.drain(..))
                            .collect::<Vec<_>>(),
                    ));
                }
                segments.push(inner);
                tail = Value::Nil;
            } else if depth > 0 {
                if !chunk.is_empty() {
                    segments.push(Value::list(
                        std::iter::once(Value::Symbol("list".into()))
                            .chain(chunk.drain(..))
                            .collect::<Vec<_>>(),
                    ));
                }
                segments.push(backquote_template_code_at_depth(&tail, depth));
                tail = Value::Nil;
            }
            break;
        }
        match tail.cons_values() {
            Some((car, cdr)) => {
                if let Some((kind, inner)) = backquote_unquote_form(&car) {
                    if depth > 0 {
                        chunk.push(backquote_template_code_at_depth(&car, depth));
                    } else if kind == "comma-at" {
                        if !chunk.is_empty() {
                            segments.push(Value::list(
                                std::iter::once(Value::Symbol("list".into()))
                                    .chain(chunk.drain(..))
                                    .collect::<Vec<_>>(),
                            ));
                        }
                        segments.push(inner);
                    } else {
                        chunk.push(inner);
                    }
                } else {
                    chunk.push(backquote_template_code_at_depth(&car, depth));
                }
                tail = cdr;
            }
            None => break,
        }
    }
    if !chunk.is_empty() {
        segments.push(Value::list(
            std::iter::once(Value::Symbol("list".into()))
                .chain(chunk.drain(..))
                .collect::<Vec<_>>(),
        ));
    }
    if !tail.is_nil() {
        segments.push(quote_literal(&tail));
    }
    match segments.len() {
        0 => Value::Nil,
        1 => segments.pop().expect("one segment"),
        _ => Value::list(
            std::iter::once(Value::Symbol("append".into()))
                .chain(segments)
                .collect::<Vec<_>>(),
        ),
    }
}

// Whether a template subtree contains a comma/comma-at marker outside
// nested backquotes.
fn template_tree_unquotes(form: &Value) -> bool {
    if backquote_unquote_form(form).is_some() {
        return true;
    }
    if nested_backquote_body(form).is_some() {
        return false;
    }
    let mut tail = form.clone();
    while let Some((car, cdr)) = tail.cons_values() {
        if template_tree_unquotes(&car) {
            return true;
        }
        if backquote_unquote_form(&cdr).is_some() {
            return true;
        }
        tail = cdr;
    }
    false
}
