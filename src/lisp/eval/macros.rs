use super::*;
use crate::lisp::reader::ReadLiteral;
use crate::lisp::types::Kind;

/// lread.c:bytecode_from_rev_list's checks of a closure literal's slots.
fn prepare_closure_literal(
    interp: &mut Interpreter,
    slots: &mut [Value],
    env: &mut Env,
) -> Result<(), LispError> {
    // lread.c accepts both execution modes under the same PVEC_CLOSURE tag.
    if !(3..=6).contains(&slots.len())
        || !matches!(
            slots[0].kind(),
            Kind::Integer(_) | Kind::Cons(_) | Kind::Nil
        )
        || !((matches!(slots[1].kind(), Kind::Cons(_))
            && matches!(slots[2].kind(), Kind::Cons(_) | Kind::Nil))
            || (slots[1].is_string()
                && matches!(slots[2].kind(), Kind::Vector(_))
                && slots
                    .get(3)
                    .is_some_and(|depth| matches!(depth.kind(), Kind::Integer(n) if n >= 0))))
    {
        return Err(LispError::ReadError("Invalid byte-code object".into()));
    }
    if slots[1].is_string() && crate::lisp::primitives::string_argument_multibyte(&slots[1]) {
        slots[1] = crate::lisp::primitives::call(interp, "string-as-unibyte", &[slots[1]], env)?;
    }
    Ok(())
}

/// The reader's view of the Lisp world as `read' and `load' read: symbols
/// interned in the current obarray, literal objects built as their syntax
/// closes.
pub(crate) struct LispReadContext<'i> {
    pub(crate) interp: &'i mut Interpreter,
    pub(crate) env: &'i mut Env,
}

impl<'i> LispReadContext<'i> {
    /// The context of a read now.
    pub(crate) fn current(interp: &'i mut Interpreter, env: &'i mut Env) -> Self {
        Self { interp, env }
    }
}

impl crate::lisp::reader::ReadContext for LispReadContext<'_> {
    fn symbols_with_pos_enabled(&mut self) -> bool {
        crate::lisp::primitives::symbols_with_pos_enabled(self.interp, self.env)
    }

    fn intern(&mut self, name: &str, multibyte: bool) -> Result<Value, LispError> {
        // read0 interns each symbol in `check_obarray (Vobarray)', read
        // as the symbol is: Lisp run in the middle of a read can bind it.
        let obarray = self
            .interp
            .symbol_value_key(cached_symbol!("obarray"))
            .unwrap_or(Value::Nil);
        crate::lisp::primitives::intern_read_symbol(self.interp, &obarray, name, multibyte)
    }

    fn build(&mut self, literal: ReadLiteral) -> Result<Value, LispError> {
        self.interp.build_read_object(literal, self.env)
    }

    fn load_file_name(&mut self) -> Value {
        self.interp.read_load_file_name()
    }

    fn force_doc_strings(&mut self) -> bool {
        self.interp.read_force_doc_strings()
    }

    fn doc_string(&mut self, reference: Value) -> Result<Value, LispError> {
        crate::lisp::primitives::unibyte_doc_string(self.interp, &reference)
    }

    fn char_from_name(&mut self, name: &str) -> Result<Value, LispError> {
        self.interp.read_char_from_name(name, self.env)
    }

    fn read_circle(&mut self) -> bool {
        self.interp.read_circle()
    }

    fn unescaped_character_literal(&mut self, character: u8) -> Result<(), LispError> {
        crate::lisp::primitives::note_unescaped_character_literal(self.interp, character, self.env)
    }
}

impl Interpreter {
    /// lread.c character_name_to_code's `call2 (Qchar_from_name,
    /// make_unibyte_string (name, name_len), Qt)': the code a `\N{NAME}'
    /// escape reads as, NAME's case ignored.
    pub(crate) fn read_char_from_name(
        &mut self,
        name: &str,
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let name = Value::StringObject(crate::lisp::types::StringObjectRef::from_unibyte(
            name.as_bytes().to_vec(),
        ));
        self.call_function_value(
            Value::symbol("char-from-name"),
            Some("char-from-name"),
            &[name, Value::T],
            env,
        )
    }

    /// Vload_file_name as the reader reads `#$'.
    pub(crate) fn read_load_file_name(&self) -> Value {
        self.symbol_value_key(cached_symbol!("load-file-name"))
            .unwrap_or(Value::Nil)
    }

    /// Vread_circle as the reader tests it.
    pub(crate) fn read_circle(&self) -> bool {
        self.symbol_value_key(cached_symbol!("read-circle"))
            .is_some_and(|value| value.is_truthy())
    }

    /// load_force_doc_strings as the reader tests it.
    pub(crate) fn read_force_doc_strings(&self) -> bool {
        self.symbol_value_key(cached_symbol!("load-force-doc-strings"))
            .is_some_and(|value| value.is_truthy())
    }

    /// read0's construction of a literal whose syntax just closed, its
    /// fields already the objects they read as: a record (RE_record), a
    /// closure (bytecode_from_rev_list), a bool vector, a hash table
    /// (hash_table_from_plist) or a char-table.
    pub(crate) fn build_read_object(
        &mut self,
        literal: ReadLiteral,
        env: &mut Env,
    ) -> Result<Value, LispError> {
        match literal {
            ReadLiteral::Record { slots } => {
                let Some(kind) = slots.first() else {
                    return Err(LispError::ReadError("empty record literal".into()));
                };
                crate::lisp::primitives::check_record_data_slots(slots.len() - 1)?;
                Ok(Value::LispRecord(LispRecordRef::new(*kind, &slots[1..])))
            }
            ReadLiteral::Closure { mut slots } => {
                if !(3..=6).contains(&slots.len()) {
                    return Err(LispError::ReadError("Invalid byte-code object".into()));
                }
                prepare_closure_literal(self, &mut slots, env)?;
                let value = Value::Closure(crate::lisp::types::ClosureRef::filled(
                    slots.len(),
                    Value::Nil,
                ));
                let Kind::Closure(closure) = value.kind() else {
                    unreachable!()
                };
                for (index, slot) in slots.into_iter().enumerate() {
                    closure.initialize_slot(index, slot);
                }
                Ok(value)
            }
            ReadLiteral::HashTable { fields } => {
                crate::lisp::primitives::hash_table_from_read_fields(self, &fields, env)
            }
            ReadLiteral::CharTable { fields } => {
                crate::lisp::primitives::char_table_from_read_fields(&fields, false)
            }
            ReadLiteral::SubCharTable { fields } => {
                crate::lisp::primitives::char_table_from_read_fields(&fields, true)
            }
        }
    }

    /// eval.c:Fmake_interpreted_closure: validate only the outer kinds,
    /// then retain the given argument, body and environment objects.
    pub(crate) fn make_interpreted_closure(
        &mut self,
        arguments: Value,
        body: Value,
        environment: Value,
        documentation: Value,
        iform: Value,
    ) -> Result<Value, LispError> {
        if !matches!(body.kind(), Kind::Cons(_)) {
            return Err(LispError::WrongTypeArgument("consp".into(), body));
        }
        if !matches!(arguments.kind(), Kind::Nil | Kind::Cons(_)) {
            return Err(LispError::WrongTypeArgument("listp".into(), arguments));
        }
        if !matches!(iform.kind(), Kind::Nil | Kind::Cons(_)) {
            return Err(LispError::WrongTypeArgument("listp".into(), iform));
        }
        let interactive = crate::lisp::types::ClosureRef::interactive_slot_from_iform(iform)?;
        let length = if !iform.is_nil() {
            6
        } else if !documentation.is_nil() {
            5
        } else {
            3
        };
        let slots = [
            arguments,
            body,
            environment,
            Value::Nil,
            documentation,
            interactive,
        ];
        Ok(Value::allocated_closure(&slots[..length]))
    }

    // ── Macros ──

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
        // GNU 30.2 data.c:Fdefalias uses CHECK_SYMBOL/XSYMBOL.  Preserve the
        // original object for its return value while installing the function
        // on the positioned symbol's bare owner.
        let name = crate::lisp::primitives::checked_symbol_identity(self, &args[0], env)?;
        let mut function = args[1];
        let docstring = args.get(2).cloned().unwrap_or(Value::Nil);
        // No cycle check here: data.c:defalias leaves it to Ffset, which
        // a `defalias-fset-function' (advice's) takes the store from.
        if self
            .symbol_value_key(cached_symbol!("purify-flag"))
            .is_some_and(|value| value.is_truthy())
            && !crate::lisp::primitives::is_keymap_value(self, &function)
        {
            function = crate::lisp::primitives::purecopy_value(self, &function, env)?;
        }
        let old_definition = self.raw_function_binding_symbol(&name, &Env::new());
        // data.c:defalias records no autoload in the dumping process
        // (loaddefs.el's would fill the image's `load-history').
        let autoload = matches!(
            function.cons_values().map(|(head, _)| head.kind()),
            Some(Kind::Symbol(head)) if head.is_named("autoload")
        );
        if !autoload || !self.will_dump() {
            self.record_symbol_definition_in_load_history("defun", &name);
        }
        if let Some(old_definition) = old_definition {
            self.record_function_redefinition(&name, old_definition);
        }
        if self.defalias_fset_function_handles(&name, &function, env) {
        } else {
            // A nil function definition voids the cell.  In particular,
            // loadhist uses `(defalias NAME nil)' while unloading; leaving
            // a literal nil binding here would hide any dumped autoload and
            // turn the next call into `(invalid-function nil)'.
            // Ffset: `NILP (symbol) && !NILP (definition)' signals.
            if args[0].is_nil() && !function.is_nil() {
                return Err(LispError::SignalValue(Value::list([
                    Value::symbol("setting-constant"),
                    args[0],
                ])));
            }
            self.fset_function_symbol(&name, args[0], function, env)?;
        }
        if !docstring.is_nil() {
            self.put_symbol_property_on(&name, "function-documentation", docstring);
        }
        Ok(args[0])
    }

    /// Invoke a macro-environment expander (from cl-flet/cl-labels/
    /// cl-macrolet and friends) in a fresh environment.  Expanders are
    /// closures over their own captured bindings; running them inside the
    /// caller's frames would let same-named caller locals (e.g. the `var'
    /// bound by cl-labels' pcase-let*) shadow the captured ones.
    fn call_macro_environment_expander(
        &mut self,
        expander: Value,
        symbol: &SymbolName,
        args: &[Value],
    ) -> Result<Value, LispError> {
        let mut expander_env = Env::new();
        self.call_expander(expander, symbol, args, &mut expander_env)
    }

    /// The expander called under the macro's symbol.
    fn call_expander(
        &mut self,
        expander: Value,
        symbol: &SymbolName,
        args: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        self.call_function_value_as(expander, symbol, args, env)
    }

    /// Run only the macro expander itself with GNU's temporary
    /// `lexical-binding' value.
    ///
    /// GNU resolves and autoloads a function cell before establishing this
    /// special binding. Restricting the scope likewise keeps ordinary
    /// non-macro probes free of buffer-local writes and watcher events.
    pub(super) fn with_macro_lexical_binding<T>(
        &mut self,
        caller: MacroCaller,
        env: &mut Env,
        operation: impl FnOnce(&mut Self, &mut Env) -> Result<T, LispError>,
    ) -> Result<T, LispError> {
        // Only eval_sub binds `lexical-binding' and `macroexp--dynvars'
        // around the expander; Fmacroexpand applies it as it is (a
        // watcher of `lexical-binding' hears nothing from `macroexpand',
        // and the expander reads the variable's own value, which `load'
        // binds to the file's cookie for the whole readevalloop).
        if caller == MacroCaller::Macroexpand {
            return operation(self, env);
        }
        let lexical = self.interpreter_environment_is_lexical(env);
        // GNU eval.c always specbinds `lexical-binding' while invoking a
        // macro expander.  Binding only the lexical case leaks the loading
        // buffer's lexical-binding=t into `(eval FORM nil)', causing delayed
        // macro expansion inside a dynamic lambda to misclassify every
        // ordinary argument and local as lexical.
        let count = self.specpdl_index();
        cached_symbol!("lexical-binding").with(|symbol| {
            self.specbind_symbol(symbol, if lexical { Value::T } else { Value::Nil }, env)
        })?;
        if !lexical {
            let result = operation(self, env);
            let unbind = self.unbind_to(count, env);
            return match (result, unbind) {
                (Err(error), _) | (Ok(_), Err(error)) => Err(error),
                (Ok(value), Ok(())) => Ok(value),
            };
        }
        // GNU eval.c scans the current hidden interpreter environment in
        // innermost-first order and conses every bare symbol onto the active
        // macroexp--dynvars value.  Preserve live public closure environments
        // when present; typed frames carry the same declarations otherwise.
        let mut dynvars = self
            .symbol_value_key(cached_symbol!("macroexp--dynvars"))
            .unwrap_or(Value::Nil);
        let mut cursor = crate::lisp::types::current_environment_value(env);
        // eval_sub's loop has no cycle check and no QUIT: over a
        // circular environment it never ends, in GNU until a signal's
        // handler ends the session (terminate_due_to_signal runs from the
        // handler).  A fatal signal is looked for here every few thousand
        // entries, more than any environment that ends has.
        let mut steps = 0usize;
        while let Kind::Cons(list_cell) = cursor.kind() {
            let entry = list_cell.car.get();
            if let Kind::Symbol(_) | Kind::T | Kind::Nil = entry.kind() {
                dynvars = Value::cons(entry, dynvars);
            }
            cursor = list_cell.cdr.get();
            steps = steps.wrapping_add(1);
            if steps & 0xfff == 0 && crate::lisp::primitives::fatal_signal_pending() {
                crate::lisp::primitives::run_pending_fatal_signal(self, env)?;
            }
        }
        if let Err(error) = cached_symbol!("macroexp--dynvars")
            .with(|symbol| self.specbind_symbol(symbol, dynvars, env))
        {
            let _ = self.unbind_to(count, env);
            return Err(error);
        }
        let result = operation(self, env);
        let unbind = self.unbind_to(count, env);
        match (result, unbind) {
            (Err(error), _) | (Ok(_), Err(error)) => Err(error),
            (Ok(value), Ok(())) => Ok(value),
        }
    }

    /// eval.c:Fmacroexpand's one step for a form headed by SYMBOL: an
    /// entry of MACRO_ENVIRONMENT for the symbol (Fassq, by EQ), else the
    /// symbol's function, through its aliases, a macro or a macro's
    /// autoload.
    pub(super) fn try_macroexpand_with_environment(
        &mut self,
        symbol: &SymbolName,
        args: &[Value],
        macro_environment: Option<&Value>,
        caller: MacroCaller,
        env: &mut Env,
    ) -> Result<Option<Value>, LispError> {
        if let Some(expander) = macro_environment_expander(macro_environment, symbol) {
            return self
                .with_macro_lexical_binding(caller, env, |interp, _env| {
                    interp.call_macro_environment_expander(expander, symbol, args)
                })
                .map(Some);
        }

        let mut attempted_autoload = false;
        loop {
            let Some(function) = self.macro_position_function_symbol(symbol, env) else {
                return Ok(None);
            };
            let Some((head, expander)) = function.cons_values() else {
                return Ok(None);
            };
            // GNU reads (macro . EXPANDER) from the current function cell.
            // Direct/native stores must be visible without notifying a
            // per-name cache. Resolve once, including symbol aliases.
            if crate::lisp::types::eq_symbol_named(head, "macro") {
                let expanded = self.with_macro_lexical_binding(caller, env, |interp, env| {
                    interp.call_expander(expander, symbol, args, env)
                })?;
                return Ok(Some(expanded));
            }

            if attempted_autoload || !crate::lisp::types::eq_symbol_named(head, "autoload") {
                return Ok(None);
            }
            if crate::lisp::primitives::autoload_parts(&function).is_none() {
                return Ok(None);
            }
            let loads_macro =
                crate::lisp::primitives::autoload_is_macro(self, Some(symbol), &function);
            if !loads_macro {
                return Ok(None);
            }
            // eval.c:Fmacroexpand: Fautoload_do_load with MACRO-ONLY `macro'.
            self.autoload_do_load(
                function,
                Value::Symbol(*symbol),
                Value::symbol("macro"),
                env,
            )?;
            attempted_autoload = true;
        }
    }

    pub(crate) fn macroexpand_1_form_with_environment(
        &mut self,
        form: &Value,
        macro_environment: Option<&Value>,
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let Ok(items) = form.to_vec() else {
            return Ok(*form);
        };
        let Some(Kind::Symbol(symbol)) = items.first().map(|v| v.kind()) else {
            return Ok(*form);
        };
        Ok(self
            .try_macroexpand_with_environment(
                &symbol,
                &items[1..],
                macro_environment,
                MacroCaller::Macroexpand,
                env,
            )?
            .unwrap_or(*form))
    }
}

/// Who applies a macro's expander: eval.c's eval_sub, which binds
/// `lexical-binding' and `macroexp--dynvars' around it, or Fmacroexpand,
/// which does not.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum MacroCaller {
    EvalSub,
    Macroexpand,
}

fn macro_environment_expander(
    macro_environment: Option<&Value>,
    symbol: &SymbolName,
) -> Option<Value> {
    let mut entries = *macro_environment?;
    while let Kind::Cons(_) = entries.kind() {
        let entry = entries.car_if_list()?;
        if let Kind::Cons(_) = entry.kind() {
            // Fassq: the entry's key EQ to SYMBOL.
            if entry.car_if_list()?.symbol_cell() == Some(*symbol) {
                return entry.cdr_if_list();
            }
        }
        entries = entries.cdr_if_list()?;
    }
    None
}
