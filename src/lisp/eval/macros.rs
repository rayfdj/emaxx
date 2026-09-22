use super::*;
use crate::lisp::types::Kind;
use crate::lisp::types::StringPropertySpan;

/// lread.c:bytecode_from_rev_list, after resolving circular reader labels.
fn validate_interpreted_closure_literal(slots: &[Value]) -> Result<(), LispError> {
    if !(3..=6).contains(&slots.len())
        || !matches!(
            slots[0].kind(),
            Kind::Integer(_) | Kind::Cons(_) | Kind::Nil
        )
        || !matches!(slots[1].kind(), Kind::Cons(_))
        || !matches!(slots[2].kind(), Kind::Cons(_) | Kind::Nil)
    {
        return Err(LispError::ReadError("Invalid byte-code object".into()));
    }
    Ok(())
}

struct CircularReadMaterializer<'a> {
    interpreter: &'a mut Interpreter,
    environment: &'a mut Env,
    labels: std::collections::HashMap<u32, Value>,
    records: std::collections::HashMap<usize, Value>,
    resolved_cons: std::collections::HashSet<usize>,
    resolved_vectors: std::collections::HashSet<usize>,
}

impl CircularReadMaterializer<'_> {
    fn invalid() -> LispError {
        LispError::ReadError("invalid-read-syntax".into())
    }

    fn circular_label(value: &Value) -> Option<(u32, Value)> {
        let Kind::ReaderForm(form) = value.kind() else {
            return None;
        };
        match form.as_ref() {
            ReaderForm::CircularLabel { id, payload } => Some((*id, *payload)),
            _ => None,
        }
    }

    fn circular_reference(value: &Value) -> Option<u32> {
        let Kind::ReaderForm(form) = value.kind() else {
            return None;
        };
        match form.as_ref() {
            ReaderForm::CircularReference(id) => Some(*id),
            _ => None,
        }
    }

    fn quoted_hash_table_with_circular_data(value: &Value) -> bool {
        let Some((head, tail)) = value.cons_values() else {
            return false;
        };
        if head.as_symbol().ok() != Some("quote") {
            return false;
        }
        let Some((literal, end)) = tail.cons_values() else {
            return false;
        };
        end.is_nil()
            && matches!(
                literal.kind(),
                Kind::ReaderForm(form)
                    if matches!(form.as_ref(), ReaderForm::HashTable { .. })
            )
            && crate::lisp::reader::contains_circular_read_syntax(&literal)
    }

    fn record_placeholder(
        &mut self,
        form: &crate::lisp::types::ReaderFormRef,
        label: Option<u32>,
    ) -> Result<Option<Value>, LispError> {
        let identity = form.identity();
        if let Some(record) = self.records.get(&identity).cloned() {
            if let Some(label) = label {
                self.labels.insert(label, record);
            }
            return Ok(Some(record));
        }

        let (slots, closure_kind) = match form.as_ref() {
            ReaderForm::Record { slots } => (slots, None),
            ReaderForm::Closure { kind, slots } => (slots, Some(*kind)),
            _ => return Ok(None),
        };
        let ordinary_record = closure_kind.is_none();
        if ordinary_record && slots.is_empty() {
            return Err(LispError::ReadError("empty record literal".into()));
        }

        // lread.c installs the finished object's address in the #N= table
        // before reading its fields.  Allocate the Rust arena object first
        // for the same reason: comp.el's serialized IR contains records whose
        // predecessor/successor slots point back to the record itself.
        let placeholder = if closure_kind == Some(ReaderClosureKind::Interpreted) {
            if !(3..=6).contains(&slots.len()) {
                return Err(LispError::ReadError("Invalid byte-code object".into()));
            }
            Value::allocated_lambda(&[Value::Nil; 6][..slots.len()])
        } else if ordinary_record {
            self.interpreter.create_record_with_type(
                Value::Nil,
                vec![Value::Nil; slots.len().saturating_sub(1)],
            )
        } else {
            self.interpreter.create_pseudovector(
                RecordKind::Closure,
                "byte-code-function",
                vec![Value::Nil; slots.len()],
            )
        };
        self.records.insert(identity, placeholder);
        if let Some(label) = label {
            // Reused label numbers replace the previous mapping, exactly as
            // GNU's reader does; already-built objects retain their links.
            self.labels.insert(label, placeholder);
        }

        let mut resolved = Vec::with_capacity(slots.len());
        for slot in slots {
            let slot = self.resolve(slot)?;
            let slot = self
                .interpreter
                .materialize_read_record_literals(&slot, self.environment)?;
            let slot = crate::lisp::primitives::materialize_read_hash_table_literals(
                self.interpreter,
                &slot,
                self.environment,
            )?;
            let slot = crate::lisp::primitives::materialize_read_char_table_literals(
                self.interpreter,
                &slot,
                self.environment,
            )?;
            resolved.push(slot);
        }

        if let Kind::Lambda(closure) = placeholder.kind() {
            validate_interpreted_closure_literal(&resolved)?;
            for (index, value) in resolved.into_iter().enumerate() {
                closure.initialize_slot(index, value);
            }
            return Ok(Some(placeholder));
        }
        let Kind::Record(record_id) = placeholder.kind() else {
            unreachable!("record placeholder allocation returns a record")
        };
        if ordinary_record {
            let type_tag = resolved.remove(0);
            self.interpreter.retag_record(record_id.id, type_tag)?;
        }
        self.interpreter
            .find_record_mut(record_id)
            .expect("new reader record must remain allocated")
            .slots = resolved;
        Ok(Some(Value::Record(record_id)))
    }

    fn fill_cons(&mut self, template: &Value, target: &Value) -> Result<(), LispError> {
        let Some((template_car, template_cdr)) = template.cons_values() else {
            return Err(Self::invalid());
        };
        let Some((target_car, target_cdr)) = target.cons_cells() else {
            return Err(Self::invalid());
        };
        target_car.set(self.resolve(&template_car)?);
        target_cdr.set(self.resolve(&template_cdr)?);
        Ok(())
    }

    fn resolve(&mut self, value: &Value) -> Result<Value, LispError> {
        if Self::quoted_hash_table_with_circular_data(value) {
            return Err(Self::invalid());
        }
        if let Some(id) = Self::circular_reference(value) {
            return self.labels.get(&id).cloned().ok_or_else(Self::invalid);
        }
        if let Some((id, template)) = Self::circular_label(value) {
            if Self::circular_reference(&template) == Some(id) {
                return Err(LispError::ReadError("nonsensical self-reference".into()));
            }
            if let Kind::ReaderForm(form) = template.kind()
                && let Some(record) = self.record_placeholder(&form, Some(id))?
            {
                return Ok(record);
            }

            if matches!(template.kind(), Kind::Vector(_)) {
                // The inline slots can already refer to this very vector.
                // Publish the existing identity before filling those slots.
                self.labels.insert(id, template);
                return self.resolve(&template);
            }

            // The parser has already allocated the cons tree.  GNU's reader
            // installs that object's address in the #N= table before filling
            // it, so use the existing cons as the placeholder instead of
            // cloning every list in a form that happens to contain a label.
            // Besides matching GNU's identity model, this avoids quadratic
            // list rescans in comp.el's large serialized compiler context.
            let placeholder = matches!(template.kind(), Kind::Cons(_)).then(|| template);
            if let Some(placeholder) = placeholder {
                self.labels.insert(id, placeholder);
                if let Kind::Cons(cell) = placeholder.kind() {
                    self.resolved_cons.insert(ConsCell::identity(&cell));
                }
                self.fill_cons(&template, &placeholder)?;
                return Ok(placeholder);
            }

            let resolved = self.resolve(&template)?;
            self.labels.insert(id, resolved);
            return Ok(resolved);
        }

        match value.kind() {
            Kind::Cons(_) => {
                // Walk ordinary list spines without one Rust frame per
                // element. Labels still enter through resolve, so shared
                // and cyclic tails keep their already-published identity.
                let mut cursor = *value;
                while let Kind::Cons(cell) = cursor.kind() {
                    if !self.resolved_cons.insert(ConsCell::identity(&cell)) {
                        break;
                    }
                    cell.car.set(self.resolve(&cell.car.get())?);
                    let tail = cell.cdr.get();
                    if matches!(tail.kind(), Kind::Cons(_)) {
                        cursor = tail;
                    } else {
                        cell.cdr.set(self.resolve(&tail)?);
                        break;
                    }
                }
                Ok(*value)
            }
            Kind::Vector(vector) => {
                if !self.resolved_vectors.insert(vector.identity()) {
                    return Ok(*value);
                }
                let slots = vector.slots().collect::<Vec<_>>();
                for (index, slot) in slots.iter().enumerate() {
                    vector.set(index, self.resolve(slot)?);
                }
                Ok(*value)
            }
            Kind::StringObject(state) => {
                let spans = state.borrow().props.clone();
                let mut resolved_spans = Vec::with_capacity(spans.len());
                for span in spans {
                    let mut props = Vec::with_capacity(span.props.len());
                    for (key, property) in span.props {
                        props.push((key, self.resolve(&property)?));
                    }
                    resolved_spans.push(StringPropertySpan { props, ..span });
                }
                state.borrow_mut().props = resolved_spans;
                Ok(*value)
            }
            Kind::ReaderForm(form) => {
                if let Some(record) = self.record_placeholder(&form, None)? {
                    return Ok(record);
                }
                let resolve_fields = |this: &mut Self, fields: &[Value]| {
                    fields
                        .iter()
                        .map(|field| this.resolve(field))
                        .collect::<Result<Vec<_>, _>>()
                };
                let resolved = match form.as_ref() {
                    // A #N= label can make the same hash table appear both
                    // as an instruction constant and as its relocation-table
                    // key.  Hash tables compare by identity, so materialize
                    // the table before the label is installed rather than
                    // leaving a ReaderForm that each later graph walk would
                    // independently turn into a different table.
                    ReaderForm::HashTable { fields } => {
                        let fields = resolve_fields(self, fields)?;
                        return crate::lisp::primitives::materialize_read_hash_table_literal_fields(
                            self.interpreter,
                            &fields,
                            self.environment,
                        );
                    }
                    ReaderForm::CharTable { fields } => ReaderForm::CharTable {
                        fields: resolve_fields(self, fields)?,
                    },
                    ReaderForm::SubCharTable { fields } => ReaderForm::SubCharTable {
                        fields: resolve_fields(self, fields)?,
                    },
                    ReaderForm::Record { slots } => ReaderForm::Record {
                        slots: resolve_fields(self, slots)?,
                    },
                    ReaderForm::Closure { kind, slots } => ReaderForm::Closure {
                        kind: *kind,
                        slots: resolve_fields(self, slots)?,
                    },
                    ReaderForm::BoolVector { bits } => {
                        ReaderForm::BoolVector { bits: bits.clone() }
                    }
                    ReaderForm::PositionedSymbol { name, pos } => ReaderForm::PositionedSymbol {
                        name: name.clone(),
                        pos: *pos,
                    },
                    ReaderForm::CircularLabel { .. } | ReaderForm::CircularReference(_) => {
                        unreachable!("circular forms are handled before structural descent")
                    }
                };
                Ok(Value::ReaderForm(
                    crate::lisp::alloc::VectorlikeRef::allocate(resolved),
                ))
            }
            _ => Ok(*value),
        }
    }
}

impl Interpreter {
    /// Finish the Interpreter-dependent part of GNU's reader contract.
    ///
    /// Emaxx deliberately parses without an Interpreter, so circular labels
    /// and identity-bearing `#s(...)' literals remain explicit reader forms
    /// until the reader entry point finishes the object. Keep that allocation
    /// here so every read constructs one shared graph before evaluation.
    pub(crate) fn materialize_read_object_literals(
        &mut self,
        value: Value,
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if !crate::lisp::reader::read_object_needs_resolution(&value) {
            return Ok(value);
        }
        let value = if crate::lisp::reader::contains_circular_read_syntax(&value) {
            CircularReadMaterializer {
                interpreter: self,
                environment: env,
                labels: std::collections::HashMap::new(),
                records: std::collections::HashMap::new(),
                resolved_cons: std::collections::HashSet::new(),
                resolved_vectors: std::collections::HashSet::new(),
            }
            .resolve(&value)?
        } else {
            value
        };
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
            && let ReaderForm::BoolVector { bits } = form.as_ref()
        {
            // GNU's reader allocates the bool vector itself; Emaxx does it
            // here, at the same boundary as `#s(...)' records, so the object
            // -- not a reader marker -- is what quoted structure, bytecode
            // constants and `read' hand to Lisp.
            return Ok(self.create_pseudovector(
                RecordKind::BoolVector,
                "bool-vector",
                bits.iter()
                    .map(|bit| if *bit { Value::T } else { Value::Nil })
                    .collect(),
            ));
        }
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
            let closure = if closure_kind == Some(ReaderClosureKind::Interpreted) {
                if !(3..=6).contains(&slots.len()) {
                    return Err(LispError::ReadError("Invalid byte-code object".into()));
                }
                let value = Value::allocated_lambda(&[Value::Nil; 6][..slots.len()]);
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
                Some(ReaderClosureKind::Interpreted) => {
                    validate_interpreted_closure_literal(&materialized)?;
                    let value = closure.expect("allocated interpreted closure");
                    let Kind::Lambda(closure) = value.kind() else {
                        unreachable!()
                    };
                    for (index, slot) in materialized.into_iter().enumerate() {
                        closure.initialize_slot(index, slot);
                    }
                    value
                }
                Some(ReaderClosureKind::ByteCode) => self.create_pseudovector(
                    RecordKind::Closure,
                    "byte-code-function",
                    materialized,
                ),
                None => {
                    let Some(kind) = materialized.first() else {
                        return Err(LispError::ReadError("empty record literal".into()));
                    };
                    self.create_record_with_type(*kind, materialized[1..].to_vec())
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

    /// Store the reader's interpreted-closure slots verbatim. Parameter
    /// validation belongs to funcall_lambda, not object reconstruction.
    pub(crate) fn make_interpreted_closure_value(
        &mut self,
        slots: &[Value],
    ) -> Result<Value, LispError> {
        if !(3..=6).contains(&slots.len()) || !matches!(slots[1].kind(), Kind::Cons(_)) {
            return Err(LispError::ReadError("Invalid byte-code object".into()));
        }
        Ok(Value::allocated_lambda(slots))
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
        let interactive = crate::lisp::types::LambdaValue::interactive_slot_from_iform(iform)?;
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
        Ok(Value::allocated_lambda(&slots[..length]))
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

    pub(super) fn try_macroexpand_with_environment(
        &mut self,
        name: &str,
        args: &[Value],
        macro_environment: Option<&Value>,
        caller: MacroCaller,
        env: &mut Env,
    ) -> Result<Option<Value>, LispError> {
        // GNU resolves the function cell first and binds `lexical-binding'
        // only after that resolution proves the form is a macro call. A
        // generation-stamped global non-macro verdict gives us the same
        // answer without buffer-local binding, watcher notification, and
        // unwind setup on every ordinary interpreted call. An explicit
        // macro environment must still run.
        if macro_environment.is_none() && self.known_not_macro(name) {
            return Ok(None);
        }
        self.try_macroexpand_with_environment_inner(name, args, macro_environment, caller, env)
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

    fn try_macroexpand_with_environment_inner(
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

        // A cached (and still current) not-a-macro verdict skips the whole
        // probe.  cl-flet frame shadowing can only make a name LESS of a
        // macro, so a global "not a macro" verdict stays correct under any
        // frames; verdicts influenced by frames are never cached.
        if self.known_not_macro(name) {
            return Ok(None);
        }

        let mut attempted_autoload = false;
        loop {
            // GNU keeps global macros in the function cell as
            // (macro . EXPANDER);
            // nadvice fsets advised macros (and advised macro ALIASES) that
            // way, so the cell wins over the native macro table.
            if let Some(expander) = self.function_cell_macro_expander(name, env) {
                let expanded = self.with_macro_lexical_binding(caller, env, |interp, env| {
                    interp.call_expander(expander, name, args, env)
                })?;
                return Ok(Some(expanded));
            }

            if attempted_autoload {
                self.note_not_macro(name);
                return Ok(None);
            }
            // Only global state can hold an autoload stub (env frames
            // never resolve to autoload conses), so probe the macro
            // position without scanning ordinary frames.
            let Some((function, from_frame)) = self.macro_position_function(name, env) else {
                self.note_not_macro(name);
                return Ok(None);
            };
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
        let entry = entries.car().ok()?;
        if let Kind::Cons(_) = entry.kind() {
            let symbol = entry.car().ok()?;
            if symbol.as_symbol().ok()? == name {
                return entry.cdr().ok();
            }
        }
        entries = entries.cdr().ok()?;
    }
    None
}
