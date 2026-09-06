use super::*;
use crate::lisp::types::StringPropertySpan;
use crate::lisp::types::VectorValue;

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
        let Value::ReaderForm(form) = value else {
            return None;
        };
        match form.as_ref() {
            ReaderForm::CircularLabel { id, payload } => Some((*id, payload.clone())),
            _ => None,
        }
    }

    fn circular_reference(value: &Value) -> Option<u32> {
        let Value::ReaderForm(form) = value else {
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
                &literal,
                Value::ReaderForm(form)
                    if matches!(form.as_ref(), ReaderForm::HashTable { .. })
            )
            && crate::lisp::reader::contains_circular_read_syntax(&literal)
    }

    fn record_placeholder(
        &mut self,
        form: &Rc<ReaderForm>,
        label: Option<u32>,
    ) -> Result<Option<Value>, LispError> {
        let identity = Rc::as_ptr(form) as usize;
        if let Some(record) = self.records.get(&identity).cloned() {
            if let Some(label) = label {
                self.labels.insert(label, record.clone());
            }
            return Ok(Some(record));
        }

        let (slots, ordinary_record) = match form.as_ref() {
            ReaderForm::Record { slots }
                if !matches!(
                    slots.first(),
                    Some(kind) if kind.as_symbol().ok() == Some("interpreted-function")
                ) =>
            {
                (slots, true)
            }
            ReaderForm::Closure {
                kind: ReaderClosureKind::ByteCode,
                slots,
            } => (slots, false),
            _ => return Ok(None),
        };
        if ordinary_record && slots.is_empty() {
            return Err(LispError::ReadError("empty record literal".into()));
        }

        // lread.c installs the finished object's address in the #N= table
        // before reading its fields.  Allocate the Rust arena object first
        // for the same reason: comp.el's serialized IR contains records whose
        // predecessor/successor slots point back to the record itself.
        let placeholder = if ordinary_record {
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
        self.records.insert(identity, placeholder.clone());
        if let Some(label) = label {
            // Reused label numbers replace the previous mapping, exactly as
            // GNU's reader does; already-built objects retain their links.
            self.labels.insert(label, placeholder.clone());
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

        let Value::Record(record_id) = placeholder else {
            unreachable!("record placeholder allocation returns a record")
        };
        if ordinary_record {
            let type_tag = resolved.remove(0);
            self.interpreter.retag_record(record_id, type_tag)?;
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
        *target_car.borrow_mut() = self.resolve(&template_car)?;
        *target_cdr.borrow_mut() = self.resolve(&template_cdr)?;
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
            if let Value::ReaderForm(form) = &template
                && let Some(record) = self.record_placeholder(form, Some(id))?
            {
                return Ok(record);
            }

            // The parser has already allocated the cons tree.  GNU's reader
            // installs that object's address in the #N= table before filling
            // it, so use the existing cons as the placeholder instead of
            // cloning every list in a form that happens to contain a label.
            // Besides matching GNU's identity model, this avoids quadratic
            // list rescans in comp.el's large serialized compiler context.
            let placeholder = matches!(template, Value::Cons(_)).then(|| template.clone());
            if let Some(placeholder) = placeholder {
                self.labels.insert(id, placeholder.clone());
                if let Value::Cons(cell) = &placeholder {
                    self.resolved_cons.insert(ConsCell::identity(cell));
                }
                self.fill_cons(&template, &placeholder)?;
                return Ok(placeholder);
            }

            let resolved = self.resolve(&template)?;
            self.labels.insert(id, resolved.clone());
            return Ok(resolved);
        }

        match value {
            Value::Cons(cell) => {
                if !self.resolved_cons.insert(ConsCell::identity(cell)) {
                    return Ok(value.clone());
                }
                let Some((car_cell, cdr_cell)) = value.cons_cells() else {
                    return Err(Self::invalid());
                };
                let car = car_cell.borrow().clone();
                *car_cell.borrow_mut() = self.resolve(&car)?;
                let cdr = cdr_cell.borrow().clone();
                *cdr_cell.borrow_mut() = self.resolve(&cdr)?;
                Ok(value.clone())
            }
            Value::Vector(vector) => {
                if !self.resolved_vectors.insert(VectorValue::identity(vector)) {
                    return Ok(value.clone());
                }
                let slots = vector.slots().clone();
                for (index, slot) in slots.iter().enumerate() {
                    vector.slots_mut()[index] = self.resolve(slot)?;
                }
                Ok(value.clone())
            }
            Value::StringObject(state) => {
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
                Ok(value.clone())
            }
            Value::ReaderForm(form) => {
                if let Some(record) = self.record_placeholder(form, None)? {
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
                Ok(Value::ReaderForm(Rc::new(resolved)))
            }
            _ => Ok(value.clone()),
        }
    }
}

impl Interpreter {
    /// Finish the Interpreter-dependent part of GNU's reader contract.
    ///
    /// Emaxx deliberately parses without an Interpreter, so circular labels
    /// and identity-bearing `#s(...)' literals remain explicit reader forms
    /// until the object crosses into evaluation.  Keep the allocation order
    /// in one place so quoted data, directly evaluated vectors, and future
    /// reader entry points cannot materialize different object graphs.
    pub(crate) fn materialize_read_object_literals(
        &mut self,
        value: Value,
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if !crate::lisp::reader::quote_template_needs_resolution(&value) {
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
        if let Value::ReaderForm(form) = value
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
        if let Value::ReaderForm(form) = value
            && matches!(
                form.as_ref(),
                ReaderForm::Record { .. } | ReaderForm::Closure { .. }
            )
        {
            let identity = Rc::as_ptr(form) as usize;
            if let Some(record) = records.get(&identity) {
                return Ok(record.clone());
            }
            if !active_reader_forms.insert(identity) {
                return Err(LispError::ReadError("circular record literal".into()));
            }
            let (slots, closure_kind) = match form.as_ref() {
                ReaderForm::Record { slots } => (slots, None),
                ReaderForm::Closure { kind, slots } => (slots, Some(*kind)),
                _ => unreachable!(),
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
                    self.make_interpreted_closure_value(&materialized)?
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
                    if kind.as_symbol().ok() == Some("interpreted-function") {
                        self.make_interpreted_closure_value(&materialized[1..])?
                    } else {
                        self.create_record_with_type(kind.clone(), materialized[1..].to_vec())
                    }
                }
            };
            active_reader_forms.remove(&identity);
            records.insert(identity, record.clone());
            return Ok(record);
        }
        if let Value::Vector(vector) = value {
            if !seen_vectors.insert(VectorValue::identity(vector)) {
                return Ok(value.clone());
            }
            let slots = vector.slots().clone();
            for (index, slot) in slots.iter().enumerate() {
                vector.slots_mut()[index] = self.materialize_read_record_literals_inner(
                    slot,
                    env,
                    seen_cons,
                    seen_vectors,
                    active_reader_forms,
                    records,
                )?;
            }
            return Ok(value.clone());
        }
        let Some((car_cell, cdr_cell)) = (value).cons_cells() else {
            return Ok(value.clone());
        };
        let identity = car_cell.cell_id();
        if !seen_cons.insert(identity) {
            return Ok(value.clone());
        }
        let car = car_cell.borrow().clone();
        *car_cell.borrow_mut() = self.materialize_read_record_literals_inner(
            &car,
            env,
            seen_cons,
            seen_vectors,
            active_reader_forms,
            records,
        )?;
        let cdr = cdr_cell.borrow().clone();
        *cdr_cell.borrow_mut() = self.materialize_read_record_literals_inner(
            &cdr,
            env,
            seen_cons,
            seen_vectors,
            active_reader_forms,
            records,
        )?;
        Ok(value.clone())
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

        let public_environment = slots[2].clone();
        let lexical = !public_environment.is_nil();
        enum EnvironmentEntry {
            Binding(SymbolName, Value),
            LocalSpecial(String),
        }
        let mut entries = Vec::new();
        let mut cursor = public_environment.clone();
        let mut seen = std::collections::HashSet::new();
        while let Value::Cons(list_cell) = cursor {
            if !seen.insert(ConsCell::identity(&list_cell)) {
                break;
            }
            let entry = list_cell.car.borrow().clone();
            cursor = list_cell.cdr.borrow().clone();
            match entry {
                Value::Cons(cons_cell) => {
                    let car = &cons_cell.car;
                    let cdr = &cons_cell.cdr;
                    let name = match &*car.borrow() {
                        Value::Symbol(name) => name.clone(),
                        Value::Nil => "nil".into(),
                        Value::T => "t".into(),
                        _ => continue,
                    };
                    // GNU treats each entry as a true alist cell: `(x 1)'
                    // binds x to `(1)', whereas `(x . 1)' binds it to 1.
                    entries.push(EnvironmentEntry::Binding(
                        name,
                        Self::stored_value(cdr.borrow().clone()),
                    ));
                }
                Value::Symbol(name) => {
                    entries.push(EnvironmentEntry::LocalSpecial(name.to_string()));
                }
                // `t' is GNU's empty-lexical-environment sentinel.  Other
                // non-binding entries are ignored by GNU's assq lookup too.
                Value::T | Value::Nil => {}
                _ => {}
            }
        }
        // GNU stores ENV exactly as an innermost-first alist and resolves the
        // first matching binding.  EnvFrame uses the opposite internal
        // convention (bindings are searched from the back), so translate at
        // this boundary.  `interpreted_closure_slots' performs the inverse
        // projection; without this reversal `make-interpreted-closure'
        // permutes OClosure fields every time oclosure--copy rebuilds one.
        entries.reverse();
        let mut bindings = Vec::new();
        let mut local_special_declarations = Vec::new();
        for entry in entries {
            match entry {
                EnvironmentEntry::Binding(name, value) => bindings.push((name, value)),
                EnvironmentEntry::LocalSpecial(name) => {
                    self.note_captured_local_special(&name);
                    local_special_declarations.push((bindings.len(), name));
                }
            }
        }
        let closure_env = shared_env(if public_environment.is_nil() {
            Vec::new()
        } else {
            let mut frame = EnvFrame::from_parts(
                bindings,
                Some(Self::fresh_frame_identity()),
                false,
                local_special_declarations,
            );
            frame.set_lisp_environment(public_environment.clone());
            vec![frame]
        });
        if lexical {
            self.mark_lexical_closure_env(&closure_env);
        } else {
            self.mark_closure_eval_context(&closure_env, false);
        }

        Ok(Value::lambda_with_public_environment(
            params.into(),
            slots[0].clone(),
            body.into(),
            closure_env,
            slots.get(4).cloned(),
            slots.get(5).cloned(),
            public_environment,
        ))
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
        let mut function = args[1].clone();
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
        Ok(args[0].clone())
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
        let _ = name;
        true
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
        // GNU resolves the function cell first and binds `lexical-binding'
        // only after that resolution proves the form is a macro call. A
        // generation-stamped global non-macro verdict gives us the same
        // answer without buffer-local binding, watcher notification, and
        // unwind setup on every ordinary interpreted call. An explicit
        // macro environment must still run.
        if macro_environment.is_none() && self.known_not_macro(name) {
            return Ok(None);
        }
        self.try_macroexpand_with_environment_inner(name, args, macro_environment, env)
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
        let lexical = self.interpreter_environment_is_lexical(env);
        // GNU eval.c always specbinds `lexical-binding' while invoking a
        // macro expander.  Binding only the lexical case leaks the loading
        // buffer's lexical-binding=t into `(eval FORM nil)', causing delayed
        // macro expansion inside a dynamic lambda to misclassify every
        // ordinary argument and local as lexical.
        let restore = self.bind_special_variable(
            "lexical-binding",
            if lexical { Value::T } else { Value::Nil },
            env,
        )?;
        if !lexical {
            let result = operation(self, env);
            let restore_result = self.restore_special_binding(restore, env);
            return match (result, restore_result) {
                (Err(error), _) | (Ok(_), Err(error)) => Err(error),
                (Ok(value), Ok(())) => Ok(value),
            };
        }
        // GNU eval.c scans the current hidden interpreter environment in
        // innermost-first order and conses every bare symbol onto the active
        // macroexp--dynvars value.  Preserve live public closure environments
        // when present; typed frames carry the same declarations otherwise.
        let mut dynvars = self
            .lookup_var("macroexp--dynvars", env)
            .unwrap_or(Value::Nil);
        for frame in env.iter().skip(self.special_scan_floor).rev() {
            if let Some(environment) = frame.lisp_environment() {
                for entry in super::lisp_environment_entries(environment) {
                    if let Value::Symbol(_) | Value::T | Value::Nil = entry {
                        dynvars = Value::cons(entry, dynvars);
                    }
                }
            } else {
                for (_, name) in frame.local_special_declarations().iter().rev() {
                    dynvars = Value::cons(Value::Symbol(name.clone().into()), dynvars);
                }
            }
        }
        let dynvars_restore = match self.bind_special_variable("macroexp--dynvars", dynvars, env) {
            Ok(restore) => restore,
            Err(error) => {
                let _ = self.restore_special_binding(restore, env);
                return Err(error);
            }
        };
        let result = operation(self, env);
        let dynvars_restore_result = self.restore_special_binding(dynvars_restore, env);
        let restore_result = self.restore_special_binding(restore, env);
        match (result, dynvars_restore_result, restore_result) {
            (Err(error), _, _) => Err(error),
            (Ok(_), Err(error), _) | (Ok(_), Ok(()), Err(error)) => Err(error),
            (Ok(value), Ok(()), Ok(())) => Ok(value),
        }
    }

    fn try_macroexpand_with_environment_inner(
        &mut self,
        name: &str,
        args: &[Value],
        macro_environment: Option<&Value>,
        env: &mut Env,
    ) -> Result<Option<Value>, LispError> {
        if let Some(expander) = macro_environment_expander(macro_environment, name) {
            return self
                .with_macro_lexical_binding(env, |interp, _env| {
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
                let expanded = self.with_macro_lexical_binding(env, |interp, env| {
                    interp.call_function_value(expander, Some(name), args, env)
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
            return Ok(form.clone());
        };
        let Some(Value::Symbol(name)) = items.first() else {
            return Ok(form.clone());
        };
        Ok(self
            .try_macroexpand_with_environment(name, &items[1..], macro_environment, env)?
            .unwrap_or_else(|| form.clone()))
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
