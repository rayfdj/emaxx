use super::*;
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
    fn intern(&mut self, name: &str) -> Result<Value, LispError> {
        // read0 interns each symbol in `check_obarray (Vobarray)', read
        // as the symbol is: Lisp run in the middle of a read can bind it.
        let obarray = self
            .interp
            .symbol_value_key(cached_symbol!("obarray"))
            .unwrap_or(Value::Nil);
        crate::lisp::primitives::intern_in_obarray(self.interp, &obarray, name)
    }

    fn build(&mut self, form: ReaderForm) -> Result<Value, LispError> {
        self.interp.build_read_object(form, self.env)
    }

    fn load_file_name(&mut self) -> Value {
        self.interp.read_load_file_name(self.env)
    }

    fn force_doc_strings(&mut self) -> bool {
        self.interp.read_force_doc_strings(self.env)
    }

    fn doc_string(&mut self, reference: Value) -> Result<Value, LispError> {
        crate::lisp::primitives::unibyte_doc_string(self.interp, &reference, self.env)
    }

    fn char_from_name(&mut self, name: &str) -> Result<Value, LispError> {
        self.interp.read_char_from_name(name, self.env)
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
    pub(crate) fn read_load_file_name(&self, env: &Env) -> Value {
        self.lookup_var("load-file-name", env).unwrap_or(Value::Nil)
    }

    /// load_force_doc_strings as the reader tests it.
    pub(crate) fn read_force_doc_strings(&self, env: &Env) -> bool {
        self.lookup_var("load-force-doc-strings", env)
            .is_some_and(|value| value.is_truthy())
    }

    /// read0's construction of a literal whose syntax just closed, its
    /// fields already the objects they read as: a record (RE_record), a
    /// closure (bytecode_from_rev_list), a bool vector, a hash table
    /// (hash_table_from_plist) or a char-table.
    pub(crate) fn build_read_object(
        &mut self,
        form: ReaderForm,
        env: &mut Env,
    ) -> Result<Value, LispError> {
        match form {
            ReaderForm::Record { slots } => {
                let Some(kind) = slots.first() else {
                    return Err(LispError::ReadError("empty record literal".into()));
                };
                crate::lisp::primitives::check_record_data_slots(slots.len() - 1)?;
                Ok(Value::LispRecord(LispRecordRef::new(*kind, &slots[1..])))
            }
            ReaderForm::Closure { mut slots, .. } => {
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
            ReaderForm::HashTable { fields } => {
                crate::lisp::primitives::hash_table_from_read_fields(self, &fields, env)
            }
            ReaderForm::CharTable { fields } => {
                crate::lisp::primitives::char_table_from_read_fields(&fields, false)
            }
            ReaderForm::SubCharTable { fields } => {
                crate::lisp::primitives::char_table_from_read_fields(&fields, true)
            }
        }
    }

    /// Finish the Interpreter-dependent part of GNU's reader contract.
    ///
    /// A read without a context leaves identity-bearing literals (`#s(...)',
    /// `#[...]', `#^[...]', `#&') as reader forms until the reader's caller
    /// finishes the object here, so every read constructs one shared graph
    /// before evaluation.
    pub(crate) fn materialize_read_object_literals(
        &mut self,
        value: Value,
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if !crate::lisp::reader::read_object_needs_resolution(&value) {
            return Ok(value);
        }
        let value = self.materialize_read_record_literals(&value, env)?;
        let value =
            crate::lisp::primitives::materialize_read_hash_table_literals(self, &value, env)?;
        crate::lisp::primitives::materialize_read_char_table_literals(self, &value, env)
    }

    /// Materialize `#[...]' and ordinary `#s(...)' reader forms throughout a
    /// freshly-read object.  GNU creates pseudovector objects in the reader, even
    /// below `quote'.  Emaxx keeps parsing independent of an Interpreter, so
    /// perform that object-allocation step at the read/evaluation boundary.
    /// Mutating surrounding cons cells in place preserves reader sharing.
    pub(crate) fn materialize_read_record_literals(
        &mut self,
        value: &Value,
        env: &mut Env,
    ) -> Result<Value, LispError> {
        self.materialize_read_record_literals_inner(
            value,
            env,
            &mut std::collections::HashSet::new(),
            &mut std::collections::HashSet::new(),
            &mut std::collections::HashSet::new(),
            &mut std::collections::HashMap::new(),
        )
    }

    fn materialize_read_record_literals_inner(
        &mut self,
        value: &Value,
        env: &mut Env,
        seen_cons: &mut std::collections::HashSet<usize>,
        seen_vectors: &mut std::collections::HashSet<usize>,
        active_reader_forms: &mut std::collections::HashSet<usize>,
        records: &mut std::collections::HashMap<usize, Value>,
    ) -> Result<Value, LispError> {
        if let Kind::ReaderForm(form) = value.kind()
            && matches!(
                form.as_ref(),
                ReaderForm::Record { .. } | ReaderForm::Closure { .. }
            )
        {
            let identity = form.identity();
            if let Some(record) = records.get(&identity) {
                return Ok(*record);
            }
            if !active_reader_forms.insert(identity) {
                return Err(LispError::ReadError("circular record literal".into()));
            }
            let (slots, closure_kind) = match form.as_ref() {
                ReaderForm::Record { slots } => (slots, None),
                ReaderForm::Closure { kind, slots } => (slots, Some(*kind)),
                _ => unreachable!(),
            };
            // Install the real closure before descending into its slots.
            // A reader label can lead back through any of those objects.
            let closure = if closure_kind.is_some() {
                if !(3..=6).contains(&slots.len()) {
                    return Err(LispError::ReadError("Invalid byte-code object".into()));
                }
                let value = Value::Closure(crate::lisp::types::ClosureRef::filled(
                    slots.len(),
                    Value::Nil,
                ));
                records.insert(identity, value);
                Some(value)
            } else {
                None
            };
            let mut materialized = Vec::with_capacity(slots.len());
            for slot in slots {
                let value = self.materialize_read_record_literals_inner(
                    slot,
                    env,
                    seen_cons,
                    seen_vectors,
                    active_reader_forms,
                    records,
                )?;
                let value = crate::lisp::primitives::materialize_read_hash_table_literals(
                    self, &value, env,
                )?;
                let value = crate::lisp::primitives::materialize_read_char_table_literals(
                    self, &value, env,
                )?;
                materialized.push(value);
            }
            let record = match closure_kind {
                Some(_) => {
                    prepare_closure_literal(self, &mut materialized, env)?;
                    let value = closure.expect("allocated closure");
                    let Kind::Closure(closure) = value.kind() else {
                        unreachable!()
                    };
                    for (index, slot) in materialized.into_iter().enumerate() {
                        closure.initialize_slot(index, slot);
                    }
                    value
                }
                None => {
                    let Some(kind) = materialized.first() else {
                        return Err(LispError::ReadError("empty record literal".into()));
                    };
                    crate::lisp::primitives::check_record_data_slots(materialized.len() - 1)?;
                    Value::LispRecord(LispRecordRef::new(*kind, &materialized[1..]))
                }
            };
            active_reader_forms.remove(&identity);
            records.insert(identity, record);
            return Ok(record);
        }
        if let Kind::Vector(vector) = value.kind() {
            if !seen_vectors.insert(vector.identity()) {
                return Ok(*value);
            }
            let slots = vector.slots().collect::<Vec<_>>();
            for (index, slot) in slots.iter().enumerate() {
                let materialized = self.materialize_read_record_literals_inner(
                    slot,
                    env,
                    seen_cons,
                    seen_vectors,
                    active_reader_forms,
                    records,
                )?;
                vector.set(index, materialized);
            }
            return Ok(*value);
        }
        let Some((car_cell, cdr_cell)) = (value).cons_cells() else {
            return Ok(*value);
        };
        let identity = car_cell.cell_id();
        if !seen_cons.insert(identity) {
            return Ok(*value);
        }
        let car = car_cell.get();
        car_cell.set(self.materialize_read_record_literals_inner(
            &car,
            env,
            seen_cons,
            seen_vectors,
            active_reader_forms,
            records,
        )?);
        let cdr = cdr_cell.get();
        cdr_cell.set(self.materialize_read_record_literals_inner(
            &cdr,
            env,
            seen_cons,
            seen_vectors,
            active_reader_forms,
            records,
        )?);
        Ok(*value)
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
        let name = crate::lisp::primitives::checked_symbol_name(self, &args[0], env)?;
        let mut function = args[1];
        let docstring = args.get(2).cloned().unwrap_or(Value::Nil);
        self.validate_function_binding(&name, &function)?;
        if self
            .lookup_var("purify-flag", env)
            .is_some_and(|value| value.is_truthy())
            && !crate::lisp::primitives::is_keymap_value(self, &function)
        {
            function = crate::lisp::primitives::purecopy_value(self, &function, env)?;
        }
        let old_definition = self.logical_function_binding(&name, &Env::new());
        self.record_definition_in_load_history("defun", &name);
        if let Some(old_definition) = old_definition {
            self.record_function_redefinition(&name, old_definition);
        }
        if self.defalias_fset_function_handles(&name, &function, env) {
        } else {
            // A nil function definition voids the cell.  In particular,
            // loadhist uses `(defalias NAME nil)' while unloading; leaving
            // a literal nil binding here would hide any dumped autoload and
            // turn the next call into `(invalid-function nil)'.
            if !function.is_nil() || !self.defer_unloaded_defsubst(&name, env) {
                // Ffset: `NILP (symbol) && !NILP (definition)' signals.
                if args[0].is_nil() && !function.is_nil() {
                    return Err(LispError::SignalValue(Value::list([
                        Value::symbol("setting-constant"),
                        args[0],
                    ])));
                }
                self.fset_function(&name, function, env)?;
            }
        }
        if !docstring.is_nil() {
            self.put_symbol_property(&name, "function-documentation", docstring);
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
        name: &str,
        args: &[Value],
    ) -> Result<Value, LispError> {
        let mut expander_env = Env::new();
        self.call_expander(expander, name, args, &mut expander_env)
    }

    /// The expander called under the macro's name: by its symbol when the
    /// name is an interned symbol's (no interning per expansion).
    fn call_expander(
        &mut self,
        expander: Value,
        name: &str,
        args: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        match SymbolName::interned_cached(name) {
            Some(symbol) => self.call_function_value_as(expander, &symbol, args, env),
            None => self.call_function_value(expander, Some(name), args, env),
        }
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
            .lookup_var_key(cached_symbol!("macroexp--dynvars"), env)
            .unwrap_or(Value::Nil);
        let mut cursor = crate::lisp::types::current_environment_value(env);
        while let Kind::Cons(list_cell) = cursor.kind() {
            let entry = list_cell.car.get();
            if let Kind::Symbol(_) | Kind::T | Kind::Nil = entry.kind() {
                dynvars = Value::cons(entry, dynvars);
            }
            cursor = list_cell.cdr.get();
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

    pub(super) fn try_macroexpand_with_environment(
        &mut self,
        name: &str,
        args: &[Value],
        macro_environment: Option<&Value>,
        caller: MacroCaller,
        env: &mut Env,
    ) -> Result<Option<Value>, LispError> {
        if let Some(expander) = macro_environment_expander(macro_environment, name) {
            return self
                .with_macro_lexical_binding(caller, env, |interp, _env| {
                    interp.call_macro_environment_expander(expander, name, args)
                })
                .map(Some);
        }

        let mut attempted_autoload = false;
        loop {
            let Some(function) = self.macro_position_function(name, env) else {
                return Ok(None);
            };
            let Some((head, expander)) = function.cons_values() else {
                return Ok(None);
            };
            // GNU reads (macro . EXPANDER) from the current function cell.
            // Direct/native stores must be visible without notifying a
            // per-name cache. Resolve once, including symbol aliases.
            if matches!(head.kind(), Kind::Symbol(name) if name == "macro") {
                let expanded = self.with_macro_lexical_binding(caller, env, |interp, env| {
                    interp.call_expander(expander, name, args, env)
                })?;
                return Ok(Some(expanded));
            }

            if attempted_autoload
                || !matches!(head.kind(), Kind::Symbol(name) if name == "autoload")
            {
                return Ok(None);
            }
            let Some((file, _, _kind)) = crate::lisp::primitives::autoload_parts(&function) else {
                return Ok(None);
            };
            let loads_macro =
                crate::lisp::primitives::autoload_is_macro(self, Some(name), &function);
            if !loads_macro {
                return Ok(None);
            }
            self.load_autoload_target(&file, env)?;
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
        let Some(Kind::Symbol(name)) = items.first().map(|v| v.kind()) else {
            return Ok(*form);
        };
        Ok(self
            .try_macroexpand_with_environment(
                &name,
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

fn macro_environment_expander(macro_environment: Option<&Value>, name: &str) -> Option<Value> {
    let mut entries = *macro_environment?;
    while let Kind::Cons(_) = entries.kind() {
        let entry = entries.car_if_list()?;
        if let Kind::Cons(_) = entry.kind() {
            let symbol = entry.car_if_list()?;
            if symbol.symbol_str()? == name {
                return entry.cdr_if_list();
            }
        }
        entries = entries.cdr_if_list()?;
    }
    None
}
