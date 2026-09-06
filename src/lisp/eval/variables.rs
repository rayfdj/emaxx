use super::*;
use crate::lisp::types::SymbolName;

impl BacktraceFrame {
    fn function_snapshot(&self) -> Value {
        self.source_form
            .as_ref()
            .and_then(|form| form.car().ok())
            .unwrap_or_else(|| self.function.clone())
    }

    fn args_snapshot(&self) -> Vec<Value> {
        if let Some(words) = &self.native_args {
            return crate::lisp::native_comp::decode_active_backtrace_arguments(words)
                .expect("a native backtrace frame is inspected only during its activation")
                .expect("a native backtrace frame contains valid Lisp words");
        }
        let Some(form) = &self.source_form else {
            return self.args.clone();
        };
        let Ok(tail) = form.cdr() else {
            return Vec::new();
        };
        tail.to_vec().unwrap_or_else(|_| vec![tail])
    }
}

impl Interpreter {
    pub(crate) fn begin_minibuffer_runtime(
        &mut self,
        buffer_id: u64,
        window_id: u64,
        prompt: String,
    ) -> MinibufferRuntimeState {
        let previous = self.minibuffer_runtime.clone();
        self.minibuffer_activation_count = self.minibuffer_activation_count.saturating_add(1);
        self.minibuffer_runtime = MinibufferRuntimeState {
            active_buffer_id: Some(buffer_id),
            active_window_id: Some(window_id),
            activation_id: Some(self.minibuffer_activation_count),
            depth: previous.depth.saturating_add(1),
            prompt: Some(prompt),
        };
        previous
    }

    pub(crate) fn restore_minibuffer_runtime(&mut self, state: MinibufferRuntimeState) {
        self.minibuffer_runtime = state;
    }

    pub(crate) fn active_minibuffer_buffer_id(&self) -> Option<u64> {
        self.minibuffer_runtime.active_buffer_id
    }

    pub(crate) fn active_minibuffer_window_value(&self) -> Option<Value> {
        self.minibuffer_runtime
            .active_window_id
            .filter(|window_id| self.find_record(*window_id).is_some())
            .map(Value::Record)
    }

    pub(crate) fn active_minibuffer_activation_id(&self) -> Option<u64> {
        self.minibuffer_runtime.activation_id
    }

    pub(crate) fn minibuffer_depth(&self) -> usize {
        self.minibuffer_runtime.depth
    }

    pub(crate) fn minibuffer_prompt_text(&self) -> Option<&str> {
        self.minibuffer_runtime.prompt.as_deref()
    }

    /// Set the current buffer's visited name together with metadata derived
    /// from that name.  `buffer-file-name' is one logical state transition:
    /// callers must not have to remember a second remote-visit registration
    /// for modification-time, locking, or supersession policy to work.
    pub(crate) fn set_current_buffer_file_name(&mut self, file: Option<String>) {
        self.buffer.file = file;
    }

    pub(crate) fn buffer_remote_prefix(&self, buffer_id: u64) -> Option<String> {
        self.get_buffer_by_id(buffer_id)
            .and_then(|buffer| buffer.file.as_deref())
            .and_then(primitives::parse_remote_file_name)
            .map(|remote| remote.prefix)
    }

    pub fn buffer_local_hook(&self, buffer_id: u64, hook_name: &str) -> Option<Vec<Value>> {
        self.buffer_local_hooks
            .get(&buffer_id)
            .and_then(|hooks| hooks.get(hook_name))
            .cloned()
    }

    pub fn remove_buffer_local_hook(&mut self, buffer_id: u64, hook_name: &str) {
        let remove_buffer = self
            .buffer_local_hooks
            .get_mut(&buffer_id)
            .is_some_and(|hooks| {
                hooks.remove(hook_name);
                hooks.is_empty()
            });
        if remove_buffer {
            self.buffer_local_hooks.remove(&buffer_id);
        }
    }

    pub fn set_buffer_local_hook(&mut self, buffer_id: u64, hook_name: &str, hooks: Vec<Value>) {
        let local_hooks = self
            .buffer_local_hooks
            .entry(buffer_id)
            .or_insert_with(|| super::ordered_hooks([]));
        if let Some(existing) = local_hooks.get_mut(hook_name) {
            *existing = hooks;
        } else {
            local_hooks.insert(hook_name.to_string(), hooks);
        }
    }

    pub fn buffer_local_value(&self, buffer_id: u64, name: &str) -> Option<Value> {
        self.buffer_locals
            .get(&buffer_id)
            .and_then(|locals| locals.get(name))
            .cloned()
    }

    pub fn set_buffer_local_value(&mut self, buffer_id: u64, name: &str, value: Value) {
        if self.buffer_local_capable_variables.insert(name.to_string()) {
            self.bump_symbol_value_cell_epoch();
        }
        let value = Self::stored_value(self.normalize_forwarded_eval_cell(name, value));
        if buffer_id == self.current_buffer_id() {
            self.update_forwarded_eval_cell(name, &value);
        }
        let locals = self
            .buffer_locals
            .entry(buffer_id)
            .or_insert_with(|| super::ordered_bindings([]));
        if let Some(existing) = locals.get_mut(name) {
            *existing = value;
        } else {
            locals.insert(name.to_string(), value);
        }
    }

    pub fn remove_buffer_local_value(&mut self, buffer_id: u64, name: &str) {
        let remove_buffer = self
            .buffer_locals
            .get_mut(&buffer_id)
            .is_some_and(|locals| {
                locals.remove(name);
                locals.is_empty()
            });
        if remove_buffer {
            self.buffer_locals.remove(&buffer_id);
        }
        if buffer_id == self.current_buffer_id()
            && let Some(value) = self.global_binding_value(name)
        {
            self.update_forwarded_eval_cell(name, &value);
        }
        // Buffer-local hooks are part of the local binding: killing the
        // local variable discards them, as in GNU Emacs.
        self.remove_buffer_local_hook(buffer_id, name);
    }

    pub fn clear_buffer_local_state(&mut self, buffer_id: u64) {
        self.buffer_locals.remove(&buffer_id);
        self.buffer_local_hooks.remove(&buffer_id);
        self.buffer_case_tables.retain(|(id, _)| *id != buffer_id);
    }

    // `kill-all-local-variables' keeps buffer-local hook functions whose
    // hook variable is marked `permanent-local', like `write-file-functions'
    // in an archive member buffer surviving `normal-mode'.
    pub fn clear_buffer_local_state_for_mode_change(&mut self, buffer_id: u64) {
        let permanent_hooks = self
            .buffer_local_hooks
            .get(&buffer_id)
            .into_iter()
            .flat_map(|hooks| hooks.iter())
            .filter(|(name, _)| {
                self.get_symbol_property(name, "permanent-local")
                    .is_some_and(|value| value.is_truthy())
            })
            .map(|(name, values)| (name.clone(), values.clone()))
            .collect::<Vec<_>>();
        self.buffer_locals.remove(&buffer_id);
        if permanent_hooks.is_empty() {
            self.buffer_local_hooks.remove(&buffer_id);
        } else {
            self.buffer_local_hooks
                .insert(buffer_id, super::ordered_hooks(permanent_hooks));
        }
        self.buffer_case_tables.retain(|(id, _)| *id != buffer_id);
    }

    pub fn clone_buffer_local_state(&mut self, from_buffer_id: u64, to_buffer_id: u64) {
        let locals = self
            .buffer_locals
            .get(&from_buffer_id)
            .into_iter()
            .flat_map(|locals| locals.iter())
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect::<Vec<_>>();
        for (name, value) in locals {
            self.set_buffer_local_value(to_buffer_id, &name, value);
        }

        let hooks = self
            .buffer_local_hooks
            .get(&from_buffer_id)
            .into_iter()
            .flat_map(|hooks| hooks.iter())
            .map(|(name, values)| (name.clone(), values.clone()))
            .collect::<Vec<_>>();
        for (name, values) in hooks {
            self.set_buffer_local_hook(to_buffer_id, &name, values);
        }

        if let Some((_, table)) = self
            .buffer_case_tables
            .iter()
            .find(|(id, _)| *id == from_buffer_id)
            .cloned()
        {
            self.buffer_case_tables.push((to_buffer_id, table));
        }
    }

    /// Run the native `make-indirect-buffer' clone hook in the new buffer
    /// while restoring the caller's buffer even when a hook signals.
    pub(crate) fn run_clone_indirect_buffer_hook(
        &mut self,
        new_buffer_id: u64,
        env: &mut Env,
    ) -> Result<(), LispError> {
        let saved_buffer_id = self.current_buffer_id();
        self.set_current_buffer_id(new_buffer_id)?;
        let result = crate::lisp::primitives::run_named_hooks(
            self,
            "clone-indirect-buffer-hook",
            env,
            Some(new_buffer_id),
        );
        let restore = self.set_current_buffer_id(saved_buffer_id);
        match (result, restore) {
            (Err(error), _) => Err(error),
            (Ok(()), Err(error)) => Err(error),
            (Ok(()), Ok(())) => Ok(()),
        }
    }

    pub fn buffer_local_variables(&self, buffer_id: u64) -> Vec<(String, Value)> {
        self.buffer_locals
            .get(&buffer_id)
            .map(|locals| {
                locals
                    .iter()
                    .map(|(name, value)| (name.clone(), value.clone()))
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn mark_auto_buffer_local(&mut self, name: &str) {
        self.auto_buffer_locals.insert(name.to_string());
        if self.buffer_local_capable_variables.insert(name.to_string()) {
            self.bump_symbol_value_cell_epoch();
        }
    }

    pub fn is_auto_buffer_local(&self, name: &str) -> bool {
        self.auto_buffer_locals.contains(name)
    }

    /// Define a global value cell and its dynamic-binding contract together.
    /// Native DEFVAR-style startup state must never require two coordinated
    /// calls at the definition site.
    pub fn define_special_variable(&mut self, name: &str, value: Value) {
        self.set_global_binding(name, value);
        self.mark_special_variable(name);
    }

    /// Define a native per-buffer value and its locality metadata atomically.
    pub fn define_per_buffer_special(&mut self, name: &str, value: Value) {
        self.set_global_binding(name, value);
        self.mark_per_buffer_special(name);
    }

    /// Define a native always-buffer-local value and its locality metadata.
    pub fn define_always_buffer_local_special(&mut self, name: &str, value: Value) {
        self.set_global_binding(name, value);
        self.mark_always_buffer_local_special(name);
    }

    /// Mark a native DEFVAR_PER_BUFFER variable that inherits its default
    /// until assigned locally (a positive buffer_local_flags index in GNU).
    pub fn mark_per_buffer_special(&mut self, name: &str) {
        self.mark_auto_buffer_local(name);
        self.mark_special_variable(name);
        self.per_buffer_specials.insert(name.to_string());
    }

    /// Mark a native DEFVAR_PER_BUFFER variable whose GNU buffer slot has
    /// index -1 and is therefore always local.  A dynamic binding made in one
    /// buffer must not forward into another buffer for this subset.
    pub fn mark_always_buffer_local_special(&mut self, name: &str) {
        self.mark_per_buffer_special(name);
        self.always_buffer_local_specials.insert(name.to_string());
    }

    pub fn is_per_buffer_special(&self, name: &str) -> bool {
        self.per_buffer_specials.contains(name)
    }

    pub fn is_always_buffer_local_special(&self, name: &str) -> bool {
        self.always_buffer_local_specials.contains(name)
    }

    pub fn mark_special_variable(&mut self, name: &str) {
        if self.special_variables_index.insert(name.to_string()) {
            self.special_variables.push(name.to_string());
        }
    }

    pub fn unmark_special_variable(&mut self, name: &str) {
        if let Some(index) = self
            .special_variables
            .iter()
            .rposition(|existing| existing == name)
        {
            self.special_variables.remove(index);
            self.special_variables_index.remove(name);
        }
    }

    /// Record a GNU "locally special" declaration: a bare one-arg `defvar'
    /// evaluated inside a lexical scope makes same-scope `let's of the name
    /// bind dynamically without setting the global special flag.  The
    /// marker is a new persistent environment prefix, so closures created
    /// before and after the declaration share their older tail without
    /// becoming the same environment snapshot.  This is GNU eval.c's exact
    /// `Vinternal_interpreter_environment = Fcons (sym, ...)` ownership
    /// model, represented as typed Rust state.
    pub(crate) fn push_local_special_declaration(&mut self, name: &str, env: &mut Env) {
        let identity = Self::fresh_frame_identity();
        if env.last().is_some_and(EnvFrame::is_local_special_snapshot) {
            let mut names = env
                .last()
                .expect("checked local-special snapshot")
                .local_special_declarations()
                .iter()
                .map(|(_, name)| name.clone())
                .collect::<Vec<_>>();
            names.push(name.to_string());
            *env.last_mut().expect("checked local-special snapshot") =
                EnvFrame::with_local_specials(names, identity);
        } else {
            env.push(EnvFrame::with_local_special(name, identity));
        }
        self.local_special_names.insert(name.to_string());
    }

    /// Reconstitute a bare-symbol entry from GNU's serialized lexical
    /// environment.  Such an entry declares NAME dynamically scoped inside
    /// this closure.  Unlike evaluating a local `defvar', deserialization
    /// must not alter the surrounding macro-expansion environment.
    pub(crate) fn note_captured_local_special(&mut self, name: &str) {
        self.local_special_names.insert(name.to_string());
    }

    /// Whether NAME is declared locally special in the current scope: the
    /// marker must sit in a frame the scope can legitimately see — its own
    /// frames or captured closure frames (at or above the special-reference
    /// floor) — never a caller frame leaking through a shared env chain
    /// (below the floor), mirroring GNU's per-closure interpreter
    /// environment.
    pub(crate) fn local_special_active(&self, name: &str, env: &Env) -> bool {
        if !self.local_special_names.contains(name) {
            return false;
        }
        env.iter().skip(self.special_scan_floor).any(|frame| {
            frame.lisp_environment().is_some_and(|environment| {
                super::bindings::lisp_environment_declares_special(environment, name)
            }) || frame.declares_local_special(name)
        })
    }

    /// Whether GNU's hidden `internal-interpreter-environment' is non-nil.
    /// Explicit evaluator entry points carry the dialect in the override;
    /// direct internal evaluation uses a nonempty typed environment as its
    /// lexical marker.
    pub(crate) fn interpreter_environment_is_lexical(&self, env: &Env) -> bool {
        self.lambda_capture_override().unwrap_or(!env.is_empty())
    }

    /// Whether `let's of NAME bind dynamically and references resolve
    /// dynamically across call boundaries.
    pub(crate) fn is_dynamic_binding_name(&self, name: &str) -> bool {
        self.dlet_active_names.contains_key(name) || self.is_special_variable(name)
    }

    /// Whether a binding form must use GNU's dynamic value-cell semantics.
    /// `(eval FORM)' supplies a nil lexical environment, so bindings made
    /// directly by FORM are dynamic even for undeclared symbols.  Existing
    /// lexical functions called by FORM mask this override at their boundary.
    pub(crate) fn binding_is_dynamic(&self, name: &str, env: &Env) -> bool {
        self.lambda_capture_override() == Some(false)
            || self.is_dynamic_binding_name(name)
            || self.local_special_active(name, env)
    }

    pub fn is_special_variable(&self, name: &str) -> bool {
        // Every value synthesized by `builtin_var_value' represents a
        // dumped/native value cell (or a dumped Lisp defvar) and is therefore
        // dynamically scoped under lexical binding.  Derive that property
        // from the value registry itself so adding a startup default cannot
        // silently omit its binding semantics.
        if self.special_variables_index.contains(name) || self.builtin_var_value(name).is_some() {
            return true;
        }
        if self.variable_aliases_index.is_empty() {
            return false;
        }
        let resolved = self
            .resolve_variable_name(name)
            .unwrap_or_else(|_| name.to_string());
        self.special_variables_index.contains(&resolved)
            || self.builtin_var_value(&resolved).is_some()
    }

    pub fn special_variable_names(&self) -> Vec<String> {
        self.special_variables.clone()
    }

    pub(super) fn symbol_property_index(&self, name: &str) -> Option<usize> {
        self.symbol_properties_index.get(name).copied()
    }

    fn rebuild_symbol_properties_index(&mut self) {
        self.symbol_properties_index = super::ordered_name_index(&self.symbol_properties);
    }

    pub fn get_symbol_property(&self, name: &str, property: &str) -> Option<Value> {
        if property == "choice" {
            match name {
                "vertical-scroll-bar" => {
                    return Some(Value::list([
                        Value::Nil,
                        Value::Symbol("left".into()),
                        Value::Symbol("right".into()),
                    ]));
                }
                "overwrite-mode" => {
                    return Some(Value::list([
                        Value::Nil,
                        Value::Symbol("overwrite-mode-textual".into()),
                        Value::Symbol("overwrite-mode-binary".into()),
                    ]));
                }
                _ => {}
            }
        }
        let index = self.symbol_property_index(name)?;
        let mut tail = self.symbol_properties[index].1.clone();
        let mut seen = HashSet::new();
        while let Value::Cons(cell) = tail {
            if !seen.insert(crate::lisp::types::ConsCell::identity(&cell)) {
                return None;
            }
            let rest = cell.cdr.borrow().clone();
            let (value_cell, next_cell) = rest.cons_cells()?;
            if matches!(&*cell.car.borrow(), Value::Symbol(key) if key == property) {
                return Some(value_cell.borrow().clone());
            }
            tail = next_cell.borrow().clone();
        }
        None
    }

    pub fn put_symbol_property(&mut self, name: &str, property: &str, value: Value) {
        // Lisp macro expanders may consult arbitrary symbol properties.
        // Treat every plist write as a definition change so a previously
        // cached expansion cannot outlive the metadata it depended on.
        self.note_definition_changed();
        let value = Self::stored_value(value);
        if let Some(index) = self.symbol_property_index(name) {
            let plist = self.symbol_properties[index].1.clone();
            let mut tail = plist.clone();
            let mut seen = HashSet::new();
            while let Value::Cons(cell) = tail {
                if !seen.insert(crate::lisp::types::ConsCell::identity(&cell)) {
                    return;
                }
                let rest = cell.cdr.borrow().clone();
                let Some((value_cell, next_cell)) = (rest).cons_cells() else {
                    return;
                };
                if matches!(&*cell.car.borrow(), Value::Symbol(key) if key == property) {
                    *value_cell.borrow_mut() = value;
                    return;
                }
                let next = next_cell.borrow().clone();
                if next.is_nil() {
                    *next_cell.borrow_mut() =
                        Value::list([Value::Symbol(property.to_string().into()), value]);
                    return;
                }
                tail = next;
            }
            if plist.is_nil() {
                self.symbol_properties[index].1 =
                    Value::list([Value::Symbol(property.to_string().into()), value]);
            }
            return;
        }
        let index = self.symbol_properties.len();
        self.symbol_properties.push((
            name.to_string(),
            Value::list([Value::Symbol(property.to_string().into()), value]),
        ));
        self.symbol_properties_index.insert(name.to_string(), index);
    }

    pub fn intern_symbol_name(&mut self, name: &str) {
        self.uninterned_standard_symbol_names.remove(name);
        if self.interned_symbol_names.insert(name.to_string()) {
            self.interned_symbols.push(name.to_string());
        }
    }

    pub(crate) fn unintern_standard_symbol_name(&mut self, name: &str) -> bool {
        if !self.standard_obarray_contains_symbol(name) {
            return false;
        }
        self.uninterned_standard_symbol_names
            .insert(name.to_string());
        if self.interned_symbol_names.remove(name) {
            self.interned_symbols.retain(|candidate| candidate != name);
        }
        true
    }

    /// Register ordinary symbols constructed by the Lisp reader in the
    /// standard obarray.  Reader data may be circular and propertized strings
    /// may hide symbols in their property values, so walk iteratively with an
    /// identity guard instead of assuming a proper tree.
    pub(crate) fn intern_symbols_in_value(&mut self, value: &Value) {
        let mut pending = vec![value.clone()];
        let mut seen_cons_cells = HashSet::new();
        let mut seen_strings = HashSet::new();

        while let Some(current) = pending.pop() {
            match current {
                Value::Symbol(name) => {
                    if crate::lisp::types::visible_symbol_name(&name) == name {
                        self.intern_symbol_name(&name);
                    }
                }
                Value::Cons(cons_cell) => {
                    let car = &cons_cell.car;
                    let cdr = &cons_cell.cdr;
                    if seen_cons_cells.insert(crate::lisp::types::ConsCell::identity(&cons_cell)) {
                        pending.push(cdr.borrow().clone());
                        pending.push(car.borrow().clone());
                    }
                }
                Value::StringObject(state) if seen_strings.insert(Rc::as_ptr(&state) as usize) => {
                    for span in &state.borrow().props {
                        for (property, property_value) in &span.props {
                            self.intern_symbol_name(property);
                            pending.push(property_value.clone());
                        }
                    }
                }
                // GNU's reader interns symbols inside every literal it
                // builds: `#[...]' closure constant vectors, `#s(...)'
                // hash tables/records, char-tables, and circular labels
                // (lread.c read0 interns at each `read_symbol').  Skipping
                // these left every symbol that only occurs in a compiled
                // constant vector out of the standard obarray.
                Value::ReaderForm(form) => match form.as_ref() {
                    crate::lisp::types::ReaderForm::CircularLabel { payload, .. } => {
                        pending.push(payload.clone());
                    }
                    crate::lisp::types::ReaderForm::CircularReference(_) => {}
                    crate::lisp::types::ReaderForm::HashTable { fields }
                    | crate::lisp::types::ReaderForm::CharTable { fields }
                    | crate::lisp::types::ReaderForm::SubCharTable { fields } => {
                        pending.extend(fields.iter().cloned());
                    }
                    crate::lisp::types::ReaderForm::Record { slots }
                    | crate::lisp::types::ReaderForm::Closure { slots, .. } => {
                        pending.extend(slots.iter().cloned());
                    }
                    _ => {}
                },
                _ => {}
            }
        }
    }

    /// Give symbols produced by the Lisp reader the identity selected by the
    /// dynamically active `obarray'.  The parser itself is intentionally
    /// interpreter-free, so ordinary standard-obarray reads only need the
    /// membership walk above.  A private obarray also requires replacing each
    /// parsed symbol object with that table's identity-bearing value.
    pub(crate) fn intern_read_symbols_in_value(
        &mut self,
        value: Value,
        env: &Env,
    ) -> Result<Value, LispError> {
        let obarray = self.lookup_var("obarray", env).unwrap_or(Value::Nil);
        if !matches!(&obarray, Value::Record(id) if !self.is_standard_obarray_id(*id)) {
            self.intern_symbols_in_value(&value);
            return Ok(value);
        }
        self.intern_read_symbols_in_obarray(value, &obarray, &mut HashSet::new())
    }

    fn intern_read_symbols_in_obarray(
        &mut self,
        value: Value,
        obarray: &Value,
        seen_cons_cells: &mut HashSet<usize>,
    ) -> Result<Value, LispError> {
        match value {
            Value::Symbol(name) if crate::lisp::types::visible_symbol_name(&name) == name => {
                crate::lisp::primitives::intern_in_obarray(self, obarray, &name)
            }
            Value::Cons(cell) => {
                if seen_cons_cells.insert(crate::lisp::types::ConsCell::identity(&cell)) {
                    let car = cell.car.borrow().clone();
                    let cdr = cell.cdr.borrow().clone();
                    let car = self.intern_read_symbols_in_obarray(car, obarray, seen_cons_cells)?;
                    let cdr = self.intern_read_symbols_in_obarray(cdr, obarray, seen_cons_cells)?;
                    *cell.car.borrow_mut() = car;
                    *cell.cdr.borrow_mut() = cdr;
                }
                Ok(Value::Cons(cell))
            }
            Value::StringObject(state) => {
                let mut borrowed = state.borrow_mut();
                for span in &mut borrowed.props {
                    for (_, property_value) in &mut span.props {
                        *property_value = self.intern_read_symbols_in_obarray(
                            property_value.clone(),
                            obarray,
                            seen_cons_cells,
                        )?;
                    }
                }
                drop(borrowed);
                Ok(Value::StringObject(state))
            }
            Value::ReaderForm(form) => {
                use crate::lisp::types::ReaderForm;

                let mapped = match form.as_ref() {
                    ReaderForm::CircularLabel { id, payload } => ReaderForm::CircularLabel {
                        id: *id,
                        payload: self.intern_read_symbols_in_obarray(
                            payload.clone(),
                            obarray,
                            seen_cons_cells,
                        )?,
                    },
                    ReaderForm::CircularReference(id) => ReaderForm::CircularReference(*id),
                    ReaderForm::HashTable { fields } => ReaderForm::HashTable {
                        fields: self.intern_read_symbol_fields(fields, obarray, seen_cons_cells)?,
                    },
                    ReaderForm::CharTable { fields } => ReaderForm::CharTable {
                        fields: self.intern_read_symbol_fields(fields, obarray, seen_cons_cells)?,
                    },
                    ReaderForm::SubCharTable { fields } => ReaderForm::SubCharTable {
                        fields: self.intern_read_symbol_fields(fields, obarray, seen_cons_cells)?,
                    },
                    ReaderForm::Record { slots } => ReaderForm::Record {
                        slots: self.intern_read_symbol_fields(slots, obarray, seen_cons_cells)?,
                    },
                    ReaderForm::Closure { kind, slots } => ReaderForm::Closure {
                        kind: *kind,
                        slots: self.intern_read_symbol_fields(slots, obarray, seen_cons_cells)?,
                    },
                    ReaderForm::BoolVector { bits } => {
                        ReaderForm::BoolVector { bits: bits.clone() }
                    }
                    ReaderForm::PositionedSymbol { name, pos } => {
                        // lread.c interns the bare symbol through the
                        // active obarray even when LOCATE_SYMS wraps the
                        // occurrence with a position.
                        crate::lisp::primitives::intern_in_obarray(self, obarray, name)?;
                        ReaderForm::PositionedSymbol {
                            name: name.clone(),
                            pos: *pos,
                        }
                    }
                };
                Ok(Value::ReaderForm(Rc::new(mapped)))
            }
            other => Ok(other),
        }
    }

    fn intern_read_symbol_fields(
        &mut self,
        fields: &[Value],
        obarray: &Value,
        seen_cons_cells: &mut HashSet<usize>,
    ) -> Result<Vec<Value>, LispError> {
        fields
            .iter()
            .cloned()
            .map(|field| self.intern_read_symbols_in_obarray(field, obarray, seen_cons_cells))
            .collect()
    }

    pub(crate) fn is_standard_obarray_id(&self, id: u64) -> bool {
        id == self.standard_obarray_id
    }

    pub fn remove_symbol_property(&mut self, name: &str, property: &str) {
        let Some(index) = self.symbol_property_index(name) else {
            return;
        };
        let mut tail = self.symbol_properties[index].1.clone();
        let mut previous_value_cell: Option<Value> = None;
        let mut seen = HashSet::new();
        while let Value::Cons(cell) = tail {
            if !seen.insert(crate::lisp::types::ConsCell::identity(&cell)) {
                return;
            }
            let rest = cell.cdr.borrow().clone();
            let Some((_, next_cell)) = rest.cons_cells() else {
                return;
            };
            let next = next_cell.borrow().clone();
            if matches!(&*cell.car.borrow(), Value::Symbol(key) if key == property) {
                self.note_definition_changed();
                if let Some(previous) = previous_value_cell {
                    previous
                        .set_cdr(next)
                        .expect("a tracked plist value cell is a cons");
                } else if next.is_nil() {
                    self.symbol_properties.remove(index);
                    self.rebuild_symbol_properties_index();
                } else {
                    self.symbol_properties[index].1 = next;
                }
                return;
            }
            previous_value_cell = Some(rest);
            tail = next;
        }
    }

    pub fn symbol_plist(&self, name: &str) -> Value {
        self.symbol_property_index(name)
            .map(|index| self.symbol_properties[index].1.clone())
            .unwrap_or(Value::Nil)
    }

    pub fn set_symbol_plist(&mut self, name: &str, plist: Value) -> Result<Value, LispError> {
        // Replacing the whole plist has the same cache-coherence contract as
        // `put' and `remprop', including when the new plist is empty.
        self.note_definition_changed();
        if plist.is_nil() {
            if let Some(existing) = self.symbol_property_index(name) {
                self.symbol_properties.remove(existing);
                self.rebuild_symbol_properties_index();
            }
        } else if let Some(existing) = self.symbol_property_index(name) {
            self.symbol_properties[existing].1 = Self::stored_value(plist.clone());
        } else {
            let index = self.symbol_properties.len();
            self.symbol_properties
                .push((name.to_string(), Self::stored_value(plist.clone())));
            self.symbol_properties_index.insert(name.to_string(), index);
        }
        Ok(plist)
    }

    pub(super) fn variable_watcher_index(&self, name: &str) -> Option<usize> {
        self.variable_watchers
            .iter()
            .rposition(|(symbol, _)| symbol == name)
    }

    pub fn variable_watchers(&self, name: &str) -> Vec<Value> {
        let resolved = self
            .resolve_variable_name(name)
            .unwrap_or_else(|_| name.to_string());
        self.variable_watcher_index(&resolved)
            .map(|index| self.variable_watchers[index].1.clone())
            .unwrap_or_default()
    }

    pub fn add_variable_watcher(&mut self, name: &str, watcher: Value) -> Result<Value, LispError> {
        let resolved = self.resolve_variable_name(name)?;
        if let Some(index) = self.variable_watcher_index(&resolved) {
            if !self.variable_watchers[index]
                .1
                .iter()
                .any(|existing| existing == &watcher)
            {
                self.variable_watchers[index].1.push(watcher.clone());
            }
        } else {
            self.variable_watchers
                .push((resolved.clone(), vec![watcher.clone()]));
        }
        Ok(watcher)
    }

    pub fn remove_variable_watcher(
        &mut self,
        name: &str,
        watcher: &Value,
    ) -> Result<Value, LispError> {
        let resolved = self.resolve_variable_name(name)?;
        if let Some(index) = self.variable_watcher_index(&resolved) {
            self.variable_watchers[index]
                .1
                .retain(|existing| existing != watcher);
            if self.variable_watchers[index].1.is_empty() {
                self.variable_watchers.remove(index);
            }
        }
        Ok(watcher.clone())
    }

    pub fn clear_variable_watchers(&mut self, name: &str) {
        if let Some(index) = self.variable_watcher_index(name) {
            self.variable_watchers.remove(index);
        }
    }

    pub fn notify_variable_watchers(
        &mut self,
        name: &str,
        value: Value,
        action: &str,
        buffer_id: Option<u64>,
        env: &mut Env,
    ) -> Result<(), LispError> {
        let Some(index) = self.variable_watcher_index(name) else {
            return Ok(());
        };
        let watchers = self.variable_watchers[index].1.clone();
        let buffer = buffer_id
            .and_then(|id| self.buffer_identity_value(id))
            .unwrap_or(Value::Nil);
        for watcher in watchers {
            self.call_function_value(
                watcher,
                None,
                &[
                    Value::Symbol(name.to_string().into()),
                    value.clone(),
                    Value::Symbol(action.to_string().into()),
                    buffer.clone(),
                ],
                env,
            )?;
        }
        Ok(())
    }

    pub(super) fn direct_variable_alias(&self, name: &str) -> Option<String> {
        self.variable_aliases_index.get(name).cloned()
    }

    pub fn resolve_variable_name(&self, name: &str) -> Result<String, LispError> {
        // Overwhelmingly common: not an alias — skip the cycle
        // bookkeeping (this runs on every global variable reference).
        let Some(first) = self.direct_variable_alias(name) else {
            return Ok(name.to_string());
        };
        let mut seen = vec![name.to_string(), first.clone()];
        let mut current = first;
        while let Some(target) = self.direct_variable_alias(&current) {
            if seen.iter().any(|existing| existing == &target) {
                return Err(LispError::SignalValue(Value::list([
                    Value::Symbol("cyclic-variable-indirection".into()),
                    Value::Symbol(name.to_string().into()),
                ])));
            }
            seen.push(target.clone());
            current = target;
        }
        Ok(current)
    }

    pub fn set_variable_alias(&mut self, alias: &str, target: &str) -> Result<(), LispError> {
        let target = self.resolve_variable_name(target)?;
        if target == alias {
            return Err(LispError::SignalValue(Value::list([
                Value::Symbol("cyclic-variable-indirection".into()),
                Value::Symbol(alias.to_string().into()),
            ])));
        }
        self.variable_aliases_index
            .insert(alias.to_string(), target.clone());
        if let Some(index) = self
            .variable_aliases
            .iter()
            .rposition(|(existing, _)| existing == alias)
        {
            self.variable_aliases[index].1 = target;
        } else {
            self.variable_aliases.push((alias.to_string(), target));
        }
        self.bump_symbol_value_cell_epoch();
        Ok(())
    }

    pub fn remove_variable_alias(&mut self, name: &str) -> bool {
        if let Some(index) = self
            .variable_aliases
            .iter()
            .rposition(|(alias, _)| alias == name)
        {
            self.variable_aliases.remove(index);
            self.variable_aliases_index.remove(name);
            self.bump_symbol_value_cell_epoch();
            true
        } else {
            false
        }
    }

    pub fn indirect_variable_name(&self, name: &str) -> Result<String, LispError> {
        self.resolve_variable_name(name)
    }

    pub(super) fn global_value(&self, name: &str) -> Option<Value> {
        self.global_binding_value(name)
    }

    pub fn default_value(&self, name: &str) -> Option<Value> {
        let resolved = self
            .resolve_variable_name(name)
            .unwrap_or_else(|_| name.to_string());
        self.global_value(&resolved)
            .or_else(|| self.builtin_var_value(&resolved))
    }

    pub fn is_default_bound(&self, name: &str) -> bool {
        let resolved = self
            .resolve_variable_name(name)
            .unwrap_or_else(|_| name.to_string());
        self.globals.contains_key(&resolved)
    }

    pub fn remove_global_binding(&mut self, name: &str) {
        if self.globals.remove(name).is_some() {
            self.bump_symbol_value_cell_epoch();
        }
    }

    pub(crate) fn symbol_value_cell_epoch(&self) -> u64 {
        self.symbol_value_cell_epoch
    }

    fn bump_symbol_value_cell_epoch(&mut self) {
        self.symbol_value_cell_epoch = self.symbol_value_cell_epoch.wrapping_add(1);
    }

    fn normalize_forwarded_eval_cell(&self, name: &str, value: Value) -> Value {
        match name {
            // lread.c:defvar_bool exposes only t or nil from the forwarded
            // C bool even when Lisp stores an arbitrary non-nil object.
            "debug-on-next-call" | "symbols-with-pos-enabled"
                if !self.detached_forwarded_variables.contains_key(name) =>
            {
                if value.is_nil() {
                    Value::Nil
                } else {
                    Value::T
                }
            }
            // data.c:store_symval_forwarding stores an intmax_t and
            // do_symval_forwarding recreates the corresponding Lisp integer.
            "max-lisp-eval-depth" if !self.detached_forwarded_variables.contains_key(name) => value
                .as_integer()
                .map(crate::lisp::primitives::normalize_integer_value)
                .unwrap_or(value),
            _ => value,
        }
    }

    fn update_forwarded_eval_cell(&mut self, name: &str, value: &Value) {
        // data.c:set_internal turns a voided forwarded symbol into a plain
        // symbol. Later stores cannot reconnect it to the C variable.
        if self.detached_forwarded_variables.contains_key(name) {
            return;
        }
        match name {
            "quit-flag" => self.quit_flag = value.clone(),
            "inhibit-quit" => self.inhibit_quit = value.clone(),
            "throw-on-input" => self.throw_on_input = value.clone(),
            "overriding-plist-environment" => self.overriding_plist_environment = value.clone(),
            "load-path" => self.load_path = value.clone(),
            "max-lisp-eval-depth" => {
                if let Ok(depth) = value.as_integer() {
                    self.max_lisp_eval_depth = depth;
                }
            }
            "debug-on-next-call" => self.debug_on_next_call = value.is_truthy(),
            "symbols-with-pos-enabled" => {
                self.symbols_with_positions_enabled.set(value.is_truthy());
            }
            _ => {}
        }
    }

    pub(crate) fn forwarded_eval_cell_value(&self, name: &str) -> Option<Value> {
        match name {
            "quit-flag" => Some(self.quit_flag.clone()),
            "inhibit-quit" => Some(self.inhibit_quit.clone()),
            "throw-on-input" => Some(self.throw_on_input.clone()),
            "overriding-plist-environment" => Some(self.overriding_plist_environment.clone()),
            "load-path" => Some(self.load_path.clone()),
            "max-lisp-eval-depth" => Some(crate::lisp::primitives::normalize_integer_value(
                self.max_lisp_eval_depth,
            )),
            "debug-on-next-call" => Some(if self.debug_on_next_call {
                Value::T
            } else {
                Value::Nil
            }),
            "symbols-with-pos-enabled" => Some(if self.symbols_with_positions_enabled() {
                Value::T
            } else {
                Value::Nil
            }),
            _ => None,
        }
    }

    pub(super) fn refresh_forwarded_eval_cells(&mut self) {
        for name in [
            "quit-flag",
            "inhibit-quit",
            "throw-on-input",
            "overriding-plist-environment",
            "load-path",
            "max-lisp-eval-depth",
            "debug-on-next-call",
            "symbols-with-pos-enabled",
        ] {
            if let Some(value) = self
                .buffer_local_value(self.current_buffer_id(), name)
                .or_else(|| self.global_binding_value(name))
            {
                self.update_forwarded_eval_cell(name, &value);
            }
        }
    }

    pub(crate) fn symbols_with_positions_enabled(&self) -> bool {
        self.symbols_with_positions_enabled.get()
    }

    /// comp.c connects loaded code directly to the same bool that
    /// store_symval_forwarding changes. There is no per-native-call snapshot.
    pub(crate) fn symbols_with_positions_relocation(&self) -> *mut bool {
        self.symbols_with_positions_enabled.as_ptr()
    }

    pub(crate) fn quit_flag_is_nil(&self) -> bool {
        self.quit_flag.is_nil()
    }

    pub(crate) fn quit_flag_value(&self) -> Value {
        self.quit_flag.clone()
    }

    pub(crate) fn inhibit_quit_is_truthy(&self) -> bool {
        self.inhibit_quit.is_truthy()
    }

    pub(crate) fn throw_on_input_value(&self) -> Value {
        self.throw_on_input.clone()
    }

    pub(crate) fn overriding_plist_environment_value(&self) -> Value {
        self.overriding_plist_environment.clone()
    }

    pub(crate) fn max_lisp_eval_depth_value(&self) -> i64 {
        self.max_lisp_eval_depth
    }

    pub(crate) fn debug_on_next_call(&self) -> bool {
        self.debug_on_next_call
    }

    /// eval.c:call_debugger clears the forwarded C flag before invoking the
    /// debugger, so the debugger itself does not immediately re-enter.
    pub(crate) fn clear_debug_on_next_call(&mut self) {
        self.debug_on_next_call = false;
    }

    pub(crate) fn global_binding_value(&self, name: &str) -> Option<Value> {
        self.globals.get(name).cloned()
    }

    pub(crate) fn global_binding_value_symbol(&self, name: &SymbolName) -> Option<Value> {
        self.globals
            .raw_entry()
            .from_key_hashed_nocheck(name.ordered_binding_hash(), name.as_str())
            .map(|(_, value)| value.clone())
    }

    pub fn set_global_binding(&mut self, name: &str, value: Value) {
        let name = self
            .resolve_variable_name(name)
            .unwrap_or_else(|_| name.to_string());
        let value = Self::stored_value(self.normalize_forwarded_eval_cell(&name, value));
        if name == "features" {
            self.provided_features = value
                .to_vec()
                .unwrap_or_default()
                .into_iter()
                .filter_map(|feature| feature.as_symbol().ok().map(str::to_string))
                .collect();
        }
        if name == "ascii-case-table"
            && let Value::CharTable(id) = &value
        {
            self.mark_ascii_case_table(*id);
        }
        if self
            .buffer_local_value(self.current_buffer_id(), &name)
            .is_none()
        {
            self.update_forwarded_eval_cell(&name, &value);
        }
        self.bump_symbol_value_cell_epoch();
        if let Some(existing) = self.globals.get_mut(&name) {
            *existing = value;
        } else {
            self.globals.insert(name, value);
        }
    }

    pub fn buffer_identity_value(&self, buffer_id: u64) -> Option<Value> {
        self.buffer_list
            .iter()
            .find(|(id, _)| *id == buffer_id)
            .map(|(id, name)| Value::buffer(*id, name.clone()))
    }

    pub(super) fn active_special_assignment_scope(
        &self,
        name: &str,
    ) -> Option<SpecialBindingScope> {
        let index = self
            .active_special_restores
            .iter()
            .rposition(|restore| !restore.local_binding_killed && restore.name == name)?;
        let restore = &self.active_special_restores[index];
        match restore.scope {
            SpecialBindingScope::Global
                if self.is_auto_buffer_local(name)
                    && restore.binding_buffer_id != Some(self.current_buffer_id()) =>
            {
                None
            }
            // A let-binding made in another buffer must not capture setq
            // from this buffer: always-buffer-local variables (GNU
            // default-directory) get this buffer's own local instead.
            SpecialBindingScope::BufferLocal(id)
                if self.is_auto_buffer_local(name) && id != self.current_buffer_id() =>
            {
                None
            }
            _ => Some(restore.scope.clone()),
        }
    }

    /// Return the active global special binding as seen from the current
    /// buffer.  GNU's ordinary SPECPDL_LET already lives in the symbol's
    /// current value cell, so looking through the restore stack would only
    /// rediscover `global_value'.  The bridge-specific scan is needed solely
    /// for always-buffer-local slots when the current buffer differs from the
    /// buffer that established a global restore record.
    pub(super) fn active_global_special_value(&self, name: &str) -> Option<Option<Value>> {
        if !self.is_always_buffer_local_special(name) {
            return None;
        }
        let mut value = self.global_value(name);
        let current_buffer_id = self.current_buffer_id();
        let mut found = false;
        for restore in self.active_special_restores.iter().rev().filter(|restore| {
            !restore.local_binding_killed
                && restore.name == name
                && matches!(restore.scope, SpecialBindingScope::Global)
        }) {
            found = true;
            if restore
                .binding_buffer_id
                .is_some_and(|buffer_id| buffer_id != current_buffer_id)
            {
                value = restore.previous.clone();
            } else {
                break;
            }
        }
        found.then_some(value)
    }

    pub(super) fn active_global_toplevel_value(&self, name: &str) -> Option<Option<Value>> {
        self.active_special_restores
            .iter()
            .find(|restore| {
                !restore.local_binding_killed
                    && restore.name == name
                    && matches!(restore.scope, SpecialBindingScope::Global)
            })
            .map(|restore| restore.previous.clone())
    }

    pub(super) fn active_buffer_local_toplevel_value(
        &self,
        buffer_id: u64,
        name: &str,
    ) -> Option<Option<Value>> {
        self.active_special_restores
            .iter()
            .find(|restore| {
                !restore.local_binding_killed
                    && restore.name == name
                    && matches!(restore.scope, SpecialBindingScope::BufferLocal(id) if id == buffer_id)
            })
            .map(|restore| restore.previous.clone())
    }

    pub(super) fn set_active_global_toplevel_value(
        &mut self,
        name: &str,
        value: Option<Value>,
    ) -> bool {
        let Some(index) = self.active_special_restores.iter().position(|restore| {
            !restore.local_binding_killed
                && restore.name == name
                && matches!(restore.scope, SpecialBindingScope::Global)
        }) else {
            return false;
        };
        self.active_special_restores[index].previous = value.map(Self::stored_value);
        true
    }

    pub(super) fn set_active_buffer_local_toplevel_value(
        &mut self,
        buffer_id: u64,
        name: &str,
        value: Option<Value>,
    ) -> bool {
        let Some(index) = self.active_special_restores.iter().position(|restore| {
            !restore.local_binding_killed
                && restore.name == name
                && matches!(restore.scope, SpecialBindingScope::BufferLocal(id) if id == buffer_id)
        }) else {
            return false;
        };
        self.active_special_restores[index].previous = value.map(Self::stored_value);
        true
    }

    /// Whether NAME has a real global default binding, ignoring the
    /// synthesized builtin fallback table.  `defvar' consults this: a table
    /// answer is not a binding and must not suppress a loaded file's
    /// init form.
    pub fn global_default_binding_exists(&self, name: &str) -> bool {
        let resolved = self
            .resolve_variable_name(name)
            .unwrap_or_else(|_| name.to_string());
        if let Some(previous) = self.active_global_toplevel_value(&resolved) {
            return previous.is_some();
        }
        self.global_value(&resolved).is_some()
    }

    pub fn default_toplevel_value(&self, name: &str) -> Option<Value> {
        let resolved = self
            .resolve_variable_name(name)
            .unwrap_or_else(|_| name.to_string());
        if let Some(previous) = self.active_global_toplevel_value(&resolved) {
            return previous.or_else(|| self.builtin_var_value(&resolved));
        }
        self.global_value(&resolved)
            .or_else(|| self.builtin_var_value(&resolved))
    }

    pub fn set_default_toplevel_value(&mut self, name: &str, value: Value) {
        let resolved = self
            .resolve_variable_name(name)
            .unwrap_or_else(|_| name.to_string());
        if !self.set_active_global_toplevel_value(&resolved, Some(value.clone())) {
            self.set_global_binding(&resolved, value);
        }
    }

    pub fn buffer_local_toplevel_value(&self, buffer_id: u64, name: &str) -> Option<Value> {
        let resolved = self
            .resolve_variable_name(name)
            .unwrap_or_else(|_| name.to_string());
        if let Some(previous) = self.active_buffer_local_toplevel_value(buffer_id, &resolved) {
            return previous;
        }
        self.buffer_local_value(buffer_id, &resolved)
    }

    pub fn set_buffer_local_toplevel_value(&mut self, buffer_id: u64, name: &str, value: Value) {
        let resolved = self
            .resolve_variable_name(name)
            .unwrap_or_else(|_| name.to_string());
        if !self.set_active_buffer_local_toplevel_value(buffer_id, &resolved, Some(value.clone())) {
            self.set_buffer_local_value(buffer_id, &resolved, value);
        }
    }

    pub(super) fn assignment_scope(&self, name: &str) -> Option<SpecialBindingScope> {
        let resolved = self
            .resolve_variable_name(name)
            .unwrap_or_else(|_| name.to_string());
        if self
            .buffer_local_value(self.current_buffer_id(), &resolved)
            .is_some()
        {
            return Some(SpecialBindingScope::BufferLocal(self.current_buffer_id()));
        }
        if let Some(scope) = self.active_special_assignment_scope(&resolved) {
            return Some(scope);
        }
        if self.is_auto_buffer_local(&resolved) {
            return Some(SpecialBindingScope::BufferLocal(self.current_buffer_id()));
        }
        None
    }

    pub fn assignment_buffer_id(&self, name: &str) -> Option<u64> {
        match self.assignment_scope(name) {
            Some(SpecialBindingScope::BufferLocal(buffer_id)) => Some(buffer_id),
            _ => None,
        }
    }

    pub fn prepare_variable_assignment(
        &self,
        name: &str,
        value: Value,
    ) -> Result<Value, LispError> {
        if matches!(
            name,
            "nil" | "t" | "most-positive-fixnum" | "most-negative-fixnum"
        ) {
            return Err(LispError::SignalValue(Value::list([
                Value::Symbol("setting-constant".into()),
                Value::Symbol(name.to_string().into()),
            ])));
        }
        if name.starts_with(':') {
            return if value == Value::Symbol(name.to_string().into()) {
                Ok(value)
            } else {
                Err(LispError::SignalValue(Value::list([
                    Value::Symbol("setting-constant".into()),
                    Value::Symbol(name.to_string().into()),
                ])))
            };
        }
        // CHECK_SYMBOL/constant checks still apply, but the symbol no longer
        // forwards through a typed C slot after data.c:set_internal voids it.
        if self.detached_forwarded_variables.contains_key(name) {
            return Ok(value);
        }
        match name {
            "max-lisp-eval-depth" => match value.as_integer() {
                Ok(_) => Ok(value),
                Err(_) if value.is_integer() => Err(LispError::SignalValue(Value::list([
                    Value::Symbol("overflow-error".into()),
                    value,
                ]))),
                Err(error) => Err(error),
            },
            "display-hourglass" => Ok(if value.is_nil() { Value::Nil } else { Value::T }),
            "gc-cons-threshold" => match value {
                Value::Integer(_) | Value::BigInteger(_) => Ok(value),
                other => Err(wrong_type_argument("integerp", other)),
            },
            "scroll-up-aggressively" => match value {
                Value::Nil => Ok(Value::Nil),
                Value::Integer(number) if (0..=1).contains(&number) => Ok(Value::Integer(number)),
                Value::Float(number) if (0.0..=1.0).contains(&number.get()) => {
                    Ok(Value::Float(number))
                }
                other => Err(wrong_type_argument("numberp", other)),
            },
            "vertical-scroll-bar" => match value {
                Value::Nil => Ok(Value::Nil),
                Value::Symbol(ref symbol) if matches!(symbol.as_str(), "left" | "right") => {
                    Ok(value)
                }
                other => Err(wrong_type_argument("symbolp", other)),
            },
            "overwrite-mode" => Ok(value),
            _ => {
                // data.c store_symval_forwarding: a DEFVAR_BOOL slot stores
                // `!NILP (newval)', so every store path (setq, set,
                // set-default, let) reads back t or nil -- unless
                // `makunbound' has detached the symbol from its slot.
                if crate::lisp::primitives::generated_gnu_c_bool_variables::is_gnu_c_bool_variable(
                    name,
                ) {
                    return Ok(if value.is_nil() { Value::Nil } else { Value::T });
                }
                Ok(value)
            }
        }
    }

    pub(crate) fn bind_special_variable(
        &mut self,
        name: &str,
        value: Value,
        env: &mut Env,
    ) -> Result<SpecialBindingRestore, LispError> {
        let name = self.resolve_variable_name(name)?;
        let value = self.prepare_variable_assignment(&name, value)?;
        let buffer_id = self.current_buffer_id();
        let binding_id = self.next_special_binding_id;
        self.next_special_binding_id += 1;
        if name == "buffer-undo-list" {
            let previous = crate::lisp::primitives::buffer_undo_list_value(&self.buffer);
            self.notify_variable_watchers(&name, value.clone(), "let", Some(buffer_id), env)?;
            let previous_undo_state = self.buffer.take_undo_state();
            self.set_symbol_value_cell(&name, value);
            let restore = SpecialBindingRestore {
                binding_id,
                name,
                scope: SpecialBindingScope::BufferLocal(buffer_id),
                binding_buffer_id: None,
                previous: Some(previous),
                previous_undo_state: Some(previous_undo_state),
                local_binding_killed: false,
            };
            self.active_special_restores.push(restore.clone());
            return Ok(restore);
        }
        let restore = if self.buffer_local_value(buffer_id, &name).is_some() {
            let previous = self.buffer_local_value(buffer_id, &name);
            self.notify_variable_watchers(&name, value.clone(), "let", Some(buffer_id), env)?;
            self.set_buffer_local_value(buffer_id, &name, value);
            SpecialBindingRestore {
                binding_id,
                name,
                scope: SpecialBindingScope::BufferLocal(buffer_id),
                binding_buffer_id: None,
                previous,
                previous_undo_state: None,
                local_binding_killed: false,
            }
        } else {
            let previous = self.global_value(&name);
            let binding_buffer_id = if self.is_auto_buffer_local(&name) {
                Some(buffer_id)
            } else {
                None
            };
            self.notify_variable_watchers(&name, value.clone(), "let", None, env)?;
            self.set_global_binding(&name, value);
            SpecialBindingRestore {
                binding_id,
                name,
                scope: SpecialBindingScope::Global,
                binding_buffer_id,
                previous,
                previous_undo_state: None,
                local_binding_killed: false,
            }
        };
        self.active_special_restores.push(restore.clone());
        Ok(restore)
    }

    /// Public wrappers so primitives outside the eval module can make
    /// real dynamic bindings (GNU specbind) instead of pushing lexical
    /// frames for special variables.
    pub(crate) fn bind_special_dynamic(
        &mut self,
        name: &str,
        value: Value,
        env: &mut Env,
    ) -> Result<SpecialBindingRestore, LispError> {
        self.bind_special_variable(name, value, env)
    }

    pub(crate) fn restore_special_dynamic(
        &mut self,
        restore: SpecialBindingRestore,
        env: &mut Env,
    ) -> Result<(), LispError> {
        self.restore_special_binding(restore, env)
    }

    /// Detach every active dynamic binding from a local cell removed by
    /// `kill-all-local-variables'.  The restore record remains on the stack:
    /// if the body creates a fresh local cell before unwinding, GNU restores
    /// the pre-binding value into that cell; otherwise unwind is a no-op.
    pub(crate) fn mark_buffer_local_special_binding_killed(&mut self, buffer_id: u64, name: &str) {
        for restore in &mut self.active_special_restores {
            if restore.name == name && restore.scope == SpecialBindingScope::BufferLocal(buffer_id)
            {
                restore.local_binding_killed = true;
            }
        }
    }

    /// GNU `unbind_for_thread_switch' / `rebind_for_thread_switch'
    /// (thread.c:87-100): each thread owns its dynamic bindings, so before
    /// another thread's body runs, every live special binding is SWAPPED out
    /// -- the cell gets the pre-binding value back, and the binding record
    /// keeps the current cell value for the swap back.  The swap is two-way
    /// on purpose: a child `setq' writes the real global, and when the
    /// parent's `let' later exits, the value it restores is whatever the
    /// child left there (probed: GNU's cell ends `child-wrote', not the
    /// pre-let value).  `restore_special_binding' already prefers the LIVE
    /// stack entry by binding_id, so updating `previous' in place is exactly
    /// what let-exit will consult.
    ///
    /// GNU performs these swaps with SET_INTERNAL_THREAD_SWITCH, which skips
    /// variable watchers; cells are therefore written directly here.
    /// Only records from START onward are swapped: those below belong to
    /// already-suspended ancestor threads, whose values must stay swapped out
    /// while a descendant runs.  GNU gets this for free by walking one
    /// thread's own specpdl (thread.c:94-100); the shared stack needs the
    /// boundary made explicit.
    pub(crate) fn swap_special_bindings_for_thread_switch(&mut self, start: usize, rebind: bool) {
        let mut records = std::mem::take(&mut self.active_special_restores);
        let end = records.len();
        let start = start.min(end);
        let indices: Vec<usize> = if rebind {
            (start..end).collect()
        } else {
            (start..end).rev().collect()
        };
        for index in indices {
            let record = &mut records[index];
            if record.local_binding_killed {
                continue;
            }
            match record.scope {
                SpecialBindingScope::Global => {
                    if let Some(undo_state) = record.previous_undo_state.take() {
                        // buffer-undo-list binds through the buffer's undo
                        // machinery rather than a value cell.
                        let buffer_id = self.current_buffer_id();
                        if let Some(buffer) = self.get_buffer_by_id_mut(buffer_id) {
                            let current = buffer.take_undo_state();
                            buffer.restore_undo_state(undo_state);
                            record.previous_undo_state = Some(current);
                        } else {
                            record.previous_undo_state = Some(undo_state);
                        }
                        continue;
                    }
                    let current = self.global_value(&record.name);
                    match record.previous.take() {
                        Some(value) => self.set_global_binding(&record.name, value),
                        None => {
                            self.remove_global_binding(&record.name);
                        }
                    }
                    record.previous = current;
                }
                SpecialBindingScope::BufferLocal(buffer_id) => {
                    if let Some(undo_state) = record.previous_undo_state.take() {
                        if let Some(buffer) = self.get_buffer_by_id_mut(buffer_id) {
                            let current = buffer.take_undo_state();
                            buffer.restore_undo_state(undo_state);
                            record.previous_undo_state = Some(current);
                        } else {
                            record.previous_undo_state = Some(undo_state);
                        }
                        continue;
                    }
                    let current = self.buffer_local_value(buffer_id, &record.name);
                    match record.previous.take() {
                        Some(value) => self.set_buffer_local_value(buffer_id, &record.name, value),
                        None => self.remove_buffer_local_value(buffer_id, &record.name),
                    }
                    record.previous = current;
                }
            }
        }
        self.active_special_restores = records;
    }

    pub(crate) fn restore_special_binding(
        &mut self,
        restore: SpecialBindingRestore,
        env: &mut Env,
    ) -> Result<(), LispError> {
        let restore = if let Some(index) = self
            .active_special_restores
            .iter()
            .rposition(|active| active.binding_id == restore.binding_id)
        {
            self.active_special_restores.remove(index)
        } else {
            restore
        };
        if restore.local_binding_killed
            && matches!(restore.scope, SpecialBindingScope::BufferLocal(buffer_id)
                if self.buffer_local_value(buffer_id, &restore.name).is_none())
        {
            return Ok(());
        }
        if let Some(previous_undo_state) = restore.previous_undo_state {
            let buffer_id = match restore.scope {
                SpecialBindingScope::BufferLocal(buffer_id) => buffer_id,
                SpecialBindingScope::Global => self.current_buffer_id(),
            };
            self.notify_variable_watchers(
                &restore.name,
                restore.previous.unwrap_or(Value::Nil),
                "unlet",
                Some(buffer_id),
                env,
            )?;
            if let Some(buffer) = self.get_buffer_by_id_mut(buffer_id) {
                buffer.restore_undo_state(previous_undo_state);
            }
            return Ok(());
        }
        match restore.scope {
            SpecialBindingScope::Global => {
                self.notify_variable_watchers(
                    &restore.name,
                    restore.previous.clone().unwrap_or(Value::Nil),
                    "unlet",
                    None,
                    env,
                )?;
                if let Some(value) = restore.previous {
                    self.set_global_binding(&restore.name, value);
                } else {
                    self.remove_global_binding(&restore.name);
                }
            }
            SpecialBindingScope::BufferLocal(buffer_id) => {
                self.notify_variable_watchers(
                    &restore.name,
                    restore.previous.clone().unwrap_or(Value::Nil),
                    "unlet",
                    Some(buffer_id),
                    env,
                )?;
                if let Some(value) = restore.previous {
                    self.set_buffer_local_value(buffer_id, &restore.name, value);
                } else {
                    self.remove_buffer_local_value(buffer_id, &restore.name);
                }
            }
        }
        Ok(())
    }

    pub fn push_backtrace_frame(&mut self, function: Value, args: &[Value]) {
        let mut pooled = self.backtrace_args_pool.pop().unwrap_or_default();
        pooled.extend_from_slice(args);
        self.push_backtrace_frame_with_evald(function, pooled, true);
    }

    pub(crate) fn push_native_backtrace_frame(&mut self, function: Value, args: &[usize]) {
        self.backtrace_frames.push(BacktraceFrame {
            function,
            args: Vec::new(),
            native_args: Some(args.iter().copied().collect()),
            source_form: None,
            locals: Vec::new(),
            lexical_context: None,
            evald: true,
            debug_on_exit: false,
        });
    }

    pub fn push_backtrace_frame_with_evald(
        &mut self,
        function: Value,
        args: Vec<Value>,
        evald: bool,
    ) {
        self.push_backtrace_frame_with_locals(function, args, Vec::new(), evald);
    }

    pub(super) fn push_unevaluated_backtrace_frame(&mut self, source_form: &Value) {
        self.backtrace_frames.push(BacktraceFrame {
            function: Value::Nil,
            args: Vec::new(),
            native_args: None,
            source_form: Some(source_form.clone()),
            locals: Vec::new(),
            lexical_context: None,
            evald: false,
            debug_on_exit: false,
        });
    }

    pub fn push_backtrace_frame_with_locals(
        &mut self,
        function: Value,
        args: Vec<Value>,
        locals: Vec<(SymbolName, Value)>,
        evald: bool,
    ) {
        self.backtrace_frames.push(BacktraceFrame {
            function,
            args,
            native_args: None,
            source_form: None,
            locals,
            lexical_context: None,
            evald,
            debug_on_exit: false,
        });
    }

    /// Preserve the current evaluator environment for debugger operations.
    ///
    /// Cloning every environment at every call would be prohibitively
    /// expensive.  GNU only needs this context while a debugger is active;
    /// `backtrace-eval' itself also captures its immediate caller so direct
    /// users of that primitive get the same activation semantics.
    /// Cheap probe for the debugger flag consulted on every builtin call.
    /// `edebug-entered' can only carry a dynamic binding once edebug's
    /// defvar has marked it special, so until then a set-membership check
    /// replaces the full variable lookup (whose builtin-variable fallback
    /// tables are a measurable per-call cost); with edebug loaded, defer
    /// to the real lookup, buffer-local bindings included.
    fn edebug_entered_active(&self, env: &Env) -> bool {
        // `edebug-entered' can only carry a binding once edebug's defvar
        // has marked it special, so this single set probe is the whole
        // cost until edebug is actually loaded.
        self.special_variables_index.contains("edebug-entered")
            && self
                .lookup_var("edebug-entered", env)
                .is_some_and(|value| value.is_truthy())
    }

    pub fn capture_current_backtrace_context(
        &mut self,
        function_name: Option<&str>,
        env: &Env,
        activation_frame: Option<&[(SymbolName, Value)]>,
    ) {
        if function_name != Some("backtrace-eval") && !self.edebug_entered_active(env) {
            return;
        }
        let mut context = env.clone();
        if let Some(frame) = activation_frame {
            context.push(frame.to_vec().into());
        }
        if let Some(backtrace) = self.backtrace_frames.last_mut() {
            backtrace.lexical_context = Some(context);
        }
    }

    pub fn pop_backtrace_frame(&mut self) {
        if let Some(frame) = self.backtrace_frames.pop() {
            let mut args = frame.args;
            if args.capacity() > 0 && self.backtrace_args_pool.len() < 64 {
                args.clear();
                self.backtrace_args_pool.push(args);
            }
        }
    }

    pub fn backtrace_frames_len(&self) -> usize {
        self.backtrace_frames.len()
    }

    /// Pop frames down to LEN.  The bytecode VM records a signaling byte
    /// op as a frame (bytecode.c's record_in_backtrace) that outlives the
    /// op itself; the VM balances the stack with this once the signal has
    /// been dispatched.
    pub fn truncate_backtrace_frames(&mut self, len: usize) {
        while self.backtrace_frames.len() > len {
            self.pop_backtrace_frame();
        }
    }

    pub fn set_current_backtrace_debug(&mut self, enabled: bool) {
        if let Some(frame) = self.backtrace_frames.last_mut() {
            frame.debug_on_exit = enabled;
        }
    }

    /// eval.c:backtrace_debug_on_exit reads the flag on the active
    /// Ffuncall frame after the callee returns.
    pub(crate) fn current_backtrace_debug_on_exit(&self) -> bool {
        self.backtrace_frames
            .last()
            .is_some_and(|frame| frame.debug_on_exit)
    }

    pub fn current_backtrace_frame(&self) -> Option<(bool, Value, Vec<Value>, bool)> {
        self.backtrace_frames.last().map(|frame| {
            (
                frame.evald,
                frame.function_snapshot(),
                frame.args_snapshot(),
                frame.debug_on_exit,
            )
        })
    }

    pub fn backtrace_frames_snapshot(&self) -> Vec<(bool, Value, Vec<Value>, bool)> {
        self.backtrace_frames
            .iter()
            .rev()
            .map(|frame| {
                (
                    frame.evald,
                    frame.function_snapshot(),
                    frame.args_snapshot(),
                    frame.debug_on_exit,
                )
            })
            .collect()
    }

    pub(crate) fn capture_batch_error_backtrace(&mut self, error: &LispError, env: &Env) {
        if matches!(error, LispError::Throw(_, _) | LispError::Terminate(_)) {
            return;
        }
        let frames = self.backtrace_frames_snapshot();
        if self
            .batch_error_backtrace
            .as_ref()
            .is_some_and(|snapshot| snapshot.frames.len() >= frames.len())
        {
            return;
        }
        self.batch_error_backtrace = Some(BatchErrorBacktrace {
            enabled: self
                .lookup_var("backtrace-on-error-noninteractive", env)
                .is_some_and(|value| value.is_truthy()),
            frames,
        });
    }

    pub(crate) fn take_batch_error_backtrace(&mut self) -> Option<BatchErrorBacktrace> {
        self.batch_error_backtrace.take()
    }

    /// Non-consuming view for diagnostics: the toplevel batch reporter
    /// still needs the snapshot after a load-path trace peeked at it.
    pub(crate) fn peek_batch_error_backtrace(&self) -> Option<&BatchErrorBacktrace> {
        self.batch_error_backtrace.as_ref()
    }

    pub(crate) fn clear_batch_error_backtrace(&mut self) {
        self.batch_error_backtrace = None;
    }

    pub fn backtrace_frame_locals_snapshot(
        &self,
        index: usize,
    ) -> Option<Vec<(SymbolName, Value)>> {
        self.backtrace_frames
            .iter()
            .rev()
            .nth(index)
            .map(|frame| frame.locals.clone())
    }

    pub fn backtrace_frame_locals_snapshot_with_base(
        &self,
        index: usize,
        base: Option<&Value>,
    ) -> Option<Vec<(SymbolName, Value)>> {
        let frames: Vec<&BacktraceFrame> = self.backtrace_frames.iter().rev().collect();
        let start = base
            .and_then(|base| {
                frames
                    .iter()
                    .position(|frame| frame.function_snapshot() == *base)
            })
            .unwrap_or(0);
        frames
            .into_iter()
            .skip(start)
            .nth(index)
            .map(|frame| frame.locals.clone())
    }

    // The lexical context visible at an activation frame.  While Edebug is
    // active this is the evaluator environment captured at the call
    // boundary, including active `let' frames and their identity stamps.
    // Fall back to the older argument-only view for ordinary backtraces.
    pub fn backtrace_frame_context_env(&self, index: usize, base: Option<&Value>) -> Env {
        let frames: Vec<&BacktraceFrame> = self.backtrace_frames.iter().rev().collect();
        let start = base
            .and_then(|base| {
                frames
                    .iter()
                    .position(|frame| frame.function_snapshot() == *base)
            })
            .unwrap_or(0);
        if let Some(context) = frames
            .get(start + index)
            .and_then(|frame| frame.lexical_context.clone())
        {
            return context;
        }
        let mut merged: Vec<(SymbolName, Value)> = Vec::new();
        for frame in frames.into_iter().skip(start + index) {
            for (name, value) in &frame.locals {
                if !merged.iter().any(|(existing, _)| existing == name) {
                    merged.push((name.clone(), value.clone()));
                }
            }
        }
        // Innermost bindings must win: binding lookup scans a frame back to
        // front, so store outer entries first.
        merged.reverse();
        vec![merged.into()]
    }

    pub fn set_window_margins(&mut self, window_id: u64, left: Option<i64>, right: Option<i64>) {
        if let Some(entry) = self
            .window_margins
            .iter_mut()
            .find(|(id, _, _)| *id == window_id)
        {
            entry.1 = left;
            entry.2 = right;
        } else {
            self.window_margins.push((window_id, left, right));
        }
    }

    pub fn window_margins(&self, window_id: u64) -> (Option<i64>, Option<i64>) {
        self.window_margins
            .iter()
            .find(|(id, _, _)| *id == window_id)
            .map(|(_, left, right)| (*left, *right))
            .unwrap_or((None, None))
    }

    pub fn push_handler_bindings(&mut self, bindings: &[(Vec<String>, Value)]) -> usize {
        let start = self.active_handlers.len();
        self.active_handlers.extend(
            bindings.iter().map(|(conditions, handler)| {
                ActiveHandler::Bind(conditions.clone(), handler.clone())
            }),
        );
        start
    }

    pub(crate) fn push_condition_case_handler(&mut self, heads: Vec<Value>) -> usize {
        let start = self.active_handlers.len();
        self.active_handlers.push(ActiveHandler::Case(heads));
        start
    }

    pub fn pop_handler_bindings(&mut self, start: usize) {
        self.active_handlers.truncate(start);
    }

    /// The `error-conditions' of CONDITION, or empty when undefined.
    pub(crate) fn error_condition_names(&mut self, condition: &str) -> Vec<String> {
        self.get_symbol_property(condition, "error-conditions")
            .and_then(|value| value.to_vec().ok())
            .map(|items| {
                items
                    .iter()
                    .filter_map(|item| item.as_symbol().ok().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// GNU handler matching (`eval.c:find_handler_clause' ->
    /// `signal_or_quit''s conditions walk): `t' matches anything; otherwise the
    /// handler symbol must be `memq' in the signaled symbol's
    /// `error-conditions' property.  A symbol with NO `error-conditions' — one
    /// that never went through `define-error' — is caught only by `t'.  GNU
    /// probe: (condition-case nil (signal 'undefined-cond nil)
    /// (error 'as-error) (t 'as-t)) => as-t.  Treating an unregistered
    /// condition as `error' would let `ignore-errors' and `should-error'
    /// silently absorb conditions Emaxx forgot to register.
    pub(super) fn condition_symbol_matches(
        symbol: &str,
        error_type: &str,
        condition_list: &[String],
    ) -> bool {
        if symbol == "t" {
            return true;
        }
        if condition_list.is_empty() {
            return symbol == error_type;
        }
        condition_list.iter().any(|entry| entry == symbol)
    }

    pub(crate) fn clause_head_matches(
        head: &Value,
        error_type: &str,
        condition_list: &[String],
    ) -> bool {
        match head {
            Value::T => true,
            Value::Symbol(symbol) => {
                Self::condition_symbol_matches(symbol, error_type, condition_list)
            }
            Value::Cons(_) => head.to_vec().ok().is_some_and(|items| {
                items.iter().any(|item| {
                    matches!(item, Value::T)
                        || symbol_name(item).is_some_and(|symbol| {
                            Self::condition_symbol_matches(&symbol, error_type, condition_list)
                        })
                })
            }),
            _ => false,
        }
    }

    pub(super) fn take_condition_case_suspend(&mut self) -> bool {
        if self.suspend_condition_case_count == 0 {
            false
        } else {
            self.suspend_condition_case_count -= 1;
            true
        }
    }

    /// Whether an active `condition-case'/`handler-bind' frame would catch
    /// an error of this condition type.  The EMAXX_TRACE_LOAD_ERRORS
    /// diagnostic consults this: an error a live handler is about to
    /// absorb is invisible in GNU, so the trace must stay silent for it
    /// too and speak only for errors that will reach the toplevel report.
    pub(crate) fn some_active_handler_matches(&mut self, error: &LispError) -> bool {
        if matches!(error, LispError::Throw(_, _) | LispError::Terminate(_)) {
            return true;
        }
        let error_type = error.condition_type();
        let condition_list = self.error_condition_names(&error_type);
        self.active_handlers.iter().any(|handler| match handler {
            ActiveHandler::Case(heads) => heads
                .iter()
                .any(|head| Self::clause_head_matches(head, &error_type, &condition_list)),
            ActiveHandler::Bind(conditions, _) => conditions
                .iter()
                .any(|symbol| Self::condition_symbol_matches(symbol, &error_type, &condition_list)),
        })
    }

    pub(crate) fn dispatch_handler_bindings(
        &mut self,
        error: LispError,
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if matches!(error, LispError::Terminate(_)) {
            return Err(error);
        }
        if self.handler_dispatch_depth > 0 {
            return Err(error);
        }
        let error_value = error_condition_value(&error);
        let error_type = error.condition_type();
        let condition_list = self.error_condition_names(&error_type);
        let mut handled = false;
        self.handler_dispatch_depth += 1;
        let snapshot = self.active_handlers.clone();
        for (index, entry) in snapshot.iter().enumerate().rev() {
            match entry {
                // A matching `condition-case' between the signal point and
                // any outer `handler-bind' handles the error itself; stop
                // searching like GNU's signal_or_quit.
                ActiveHandler::Case(heads) => {
                    if heads
                        .iter()
                        .any(|head| Self::clause_head_matches(head, &error_type, &condition_list))
                    {
                        break;
                    }
                }
                ActiveHandler::Bind(conditions, handler) => {
                    if !conditions.iter().any(|condition| {
                        Self::condition_symbol_matches(condition, &error_type, &condition_list)
                    }) {
                        continue;
                    }
                    let result = self.call_function_value(
                        handler.clone(),
                        None,
                        std::slice::from_ref(&error_value),
                        env,
                    );
                    match result {
                        Ok(_) => handled = true,
                        Err(next) => {
                            self.handler_dispatch_depth =
                                self.handler_dispatch_depth.saturating_sub(1);
                            if !matches!(next, LispError::Throw(_, _) | LispError::Terminate(_)) {
                                // An error signaled by the handler propagates
                                // from the `handler-bind' frame outward, so
                                // every `condition-case' inside it must let
                                // the new error pass through untouched.
                                self.suspend_condition_case_count = snapshot[index + 1..]
                                    .iter()
                                    .filter(|inner| matches!(inner, ActiveHandler::Case(_)))
                                    .count();
                            }
                            return Err(next);
                        }
                    }
                }
            }
        }
        self.handler_dispatch_depth = self.handler_dispatch_depth.saturating_sub(1);
        if handled {
            Err(LispError::SignalValue(error_value))
        } else {
            Err(error)
        }
    }

    pub fn effective_labeled_restriction(
        &self,
        buffer_id: u64,
        skip_label: Option<&Value>,
    ) -> Option<(usize, usize)> {
        let mut result: Option<(usize, usize)> = None;
        for restriction in &self.labeled_restrictions {
            if restriction.buffer_id != buffer_id
                || skip_label.is_some_and(|skip| {
                    restriction
                        .label
                        .as_ref()
                        .is_some_and(|label| crate::lisp::primitives::values_eql(skip, label))
                })
            {
                continue;
            }
            let Some(start) = self.marker_position(restriction.beg_marker_id) else {
                continue;
            };
            let Some(end) = self.marker_position(restriction.end_marker_id) else {
                continue;
            };
            result = Some(match result {
                Some((cur_start, cur_end)) => (cur_start.max(start), cur_end.min(end)),
                None => (start, end),
            });
        }
        result
    }

    pub(crate) fn labeled_restrictions_snapshot(&self, buffer_id: u64) -> Vec<LabeledRestriction> {
        self.labeled_restrictions
            .iter()
            .filter(|restriction| restriction.buffer_id == buffer_id)
            .cloned()
            .collect()
    }

    pub(crate) fn restore_labeled_restrictions(
        &mut self,
        buffer_id: u64,
        snapshot: Vec<LabeledRestriction>,
    ) {
        self.labeled_restrictions
            .retain(|restriction| restriction.buffer_id != buffer_id);
        self.labeled_restrictions.extend(snapshot);
    }

    pub(crate) fn push_labeled_restriction(
        &mut self,
        buffer_id: u64,
        label: Value,
        start: usize,
        end: usize,
        outermost: (usize, usize),
    ) -> Result<(), LispError> {
        if !self
            .labeled_restrictions
            .iter()
            .any(|restriction| restriction.buffer_id == buffer_id)
        {
            let (beg_marker_id, end_marker_id) =
                self.labeled_restriction_markers(buffer_id, outermost.0, outermost.1)?;
            self.labeled_restrictions.push(LabeledRestriction {
                buffer_id,
                label: None,
                beg_marker_id,
                end_marker_id,
            });
        }
        let (beg_marker_id, end_marker_id) =
            self.labeled_restriction_markers(buffer_id, start, end)?;
        self.labeled_restrictions.push(LabeledRestriction {
            buffer_id,
            label: Some(label),
            beg_marker_id,
            end_marker_id,
        });
        Ok(())
    }

    pub(crate) fn pop_labeled_restriction(
        &mut self,
        buffer_id: u64,
        label: &Value,
    ) -> Option<(usize, usize)> {
        let top = self
            .labeled_restrictions
            .iter()
            .rposition(|restriction| restriction.buffer_id == buffer_id)?;
        if self.labeled_restrictions[top]
            .label
            .as_ref()
            .is_some_and(|active| crate::lisp::primitives::values_eql(active, label))
        {
            self.labeled_restrictions.remove(top);
        }
        let next = self
            .labeled_restrictions
            .iter()
            .rposition(|restriction| restriction.buffer_id == buffer_id)?;
        let restriction = self.labeled_restrictions[next].clone();
        let start = self.marker_position(restriction.beg_marker_id)?;
        let end = self.marker_position(restriction.end_marker_id)?;
        if restriction.label.is_none() {
            self.labeled_restrictions.remove(next);
        }
        Some((start, end))
    }

    fn labeled_restriction_markers(
        &mut self,
        buffer_id: u64,
        start: usize,
        end: usize,
    ) -> Result<(u64, u64), LispError> {
        let Value::Marker(beg_marker_id) = self.make_marker() else {
            unreachable!("make_marker returns a marker")
        };
        let Value::Marker(end_marker_id) = self.make_marker() else {
            unreachable!("make_marker returns a marker")
        };
        self.set_marker(beg_marker_id, Some(start), Some(buffer_id))?;
        self.set_marker(end_marker_id, Some(end), Some(buffer_id))?;
        // Point-max markers, including the labeled-restriction endpoints in
        // editfns.c, move after text inserted exactly at their position.
        self.set_marker_insertion_type(end_marker_id, true);
        Ok((beg_marker_id, end_marker_id))
    }
}
