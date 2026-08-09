use super::*;

impl BacktraceFrame {
    fn function_snapshot(&self) -> Value {
        self.source_form
            .as_ref()
            .and_then(|form| form.car().ok())
            .unwrap_or_else(|| self.function.clone())
    }

    fn args_snapshot(&self) -> Vec<Value> {
        let Some(form) = &self.source_form else {
            return self.args.clone();
        };
        let Ok(tail) = form.cdr() else {
            return Vec::new();
        };
        tail.to_vec().unwrap_or_else(|_| vec![tail])
    }

    fn first_arg_snapshot(&self) -> Option<Value> {
        if let Some(form) = &self.source_form {
            return form.cdr().ok()?.car().ok();
        }
        self.args.first().cloned()
    }
}

impl Interpreter {
    /// Set the current buffer's visited name together with metadata derived
    /// from that name.  `buffer-file-name' is one logical state transition:
    /// callers must not have to remember a second remote-visit registration
    /// for modification-time, locking, or supersession policy to work.
    pub(crate) fn set_current_buffer_file_name(&mut self, file: Option<String>) {
        let buffer_id = self.current_buffer_id();
        let remote_prefix = file
            .as_deref()
            .and_then(primitives::parse_remote_file_name)
            .map(|remote| remote.prefix);
        self.buffer.file = file;
        if let Some(prefix) = remote_prefix {
            self.set_buffer_local_value(
                buffer_id,
                "emaxx--visited-remote-prefix",
                Value::String(prefix.into()),
            );
        } else {
            self.remove_buffer_local_value(buffer_id, "emaxx--visited-remote-prefix");
        }
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
        let value = Self::stored_value(value);
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
        self.put_symbol_property(name, "emaxx-per-buffer-special", Value::T);
    }

    /// Mark a native DEFVAR_PER_BUFFER variable whose GNU buffer slot has
    /// index -1 and is therefore always local.  A dynamic binding made in one
    /// buffer must not forward into another buffer for this subset.
    pub fn mark_always_buffer_local_special(&mut self, name: &str) {
        self.mark_per_buffer_special(name);
        self.put_symbol_property(name, "emaxx-always-buffer-local-special", Value::T);
    }

    pub fn is_per_buffer_special(&self, name: &str) -> bool {
        self.get_symbol_property(name, "emaxx-per-buffer-special")
            .is_some_and(|value| value.is_truthy())
    }

    pub fn is_always_buffer_local_special(&self, name: &str) -> bool {
        self.get_symbol_property(name, "emaxx-always-buffer-local-special")
            .is_some_and(|value| value.is_truthy())
    }

    pub fn mark_special_variable(&mut self, name: &str) {
        if self.special_variables_index.insert(name.to_string()) {
            self.special_variables.push(name.to_string());
        }
    }

    pub fn unmark_special_variable(&mut self, name: &str) {
        self.soft_special_names.remove(name);
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
    /// marker lives in the innermost env frame, so it pops with its scope
    /// and is captured by closures created in the scope (GNU stores a
    /// `(defvar . NAME)' entry in the interpreter environment, which
    /// closures inherit).
    pub(crate) fn push_local_special_marker(&mut self, name: &str, env: &mut Env) {
        let marker_key = format!("--emaxx-local-special--{name}");
        let activation = Value::Integer(self.current_activation_id as i64);
        if let Some(frame) = env.last_mut() {
            frame.push((marker_key.clone(), activation));
        }
        if self.local_special_names.insert(name.to_string()) {
            // GNU's load-time macroexpansion records the declaration in
            // `macroexp--dynvars' for the rest of the enclosing form, so
            // expansion-time predicates (cl-macs tail-call elimination,
            // &key argument renaming) treat the name as dynamic.
            let existing = self.global_value("macroexp--dynvars").unwrap_or(Value::Nil);
            self.set_global_binding(
                "macroexp--dynvars",
                Value::cons(Value::Symbol(name.to_string().into()), existing),
            );
        }
    }

    /// Reconstitute a bare-symbol entry from GNU's serialized lexical
    /// environment.  Such an entry declares NAME dynamically scoped inside
    /// this closure.  Unlike evaluating a local `defvar', deserialization
    /// must not alter the surrounding macro-expansion environment.
    pub(crate) fn captured_local_special_marker(&mut self, name: &str) -> (String, Value) {
        self.local_special_names.insert(name.to_string());
        (
            format!("--emaxx-local-special--{name}"),
            Value::Integer(self.current_activation_id as i64),
        )
    }

    /// Whether NAME has ever been declared locally special (one-arg
    /// `defvar' in a lexical scope).  GNU's eager load-time macroexpansion
    /// records such names in `macroexp--dynvars' for the rest of the
    /// enclosing top-level form; expansion-time predicates use this set as
    /// the equivalent signal.
    pub(crate) fn local_special_declared(&self, name: &str) -> bool {
        self.local_special_names.contains(name)
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
        let marker_key = format!("--emaxx-local-special--{name}");
        env.iter()
            .skip(self.special_scan_floor)
            .any(|frame| frame.iter().any(|(key, _)| key == &marker_key))
    }

    /// Record a top-level one-arg `defvar': dynamic-binding treatment
    /// without the official special flag (GNU file-scoped declaration).
    /// The name is also exposed through `macroexp--dynvars' so GNU
    /// cl-macs gives same-named function arguments lexical aliases
    /// (bug#47552) exactly as it does during a file compile.
    pub(crate) fn mark_soft_special(&mut self, name: &str) {
        if self.soft_special_names.insert(name.to_string()) {
            let existing = self.global_value("macroexp--dynvars").unwrap_or(Value::Nil);
            self.set_global_binding(
                "macroexp--dynvars",
                Value::cons(Value::Symbol(name.to_string().into()), existing),
            );
        }
    }

    /// Whether `let's of NAME bind dynamically and references resolve
    /// dynamically across call boundaries: officially special variables
    /// plus file-scoped (soft) declarations.
    pub(crate) fn is_dynamic_binding_name(&self, name: &str) -> bool {
        self.soft_special_names.contains(name)
            || self.dlet_active_names.contains_key(name)
            || self.is_special_variable(name)
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

    pub(crate) fn enter_dlet_name(&mut self, name: &str) {
        *self.dlet_active_names.entry(name.to_string()).or_insert(0) += 1;
    }

    pub(crate) fn leave_dlet_name(&mut self, name: &str) {
        if let Some(count) = self.dlet_active_names.get_mut(name) {
            if *count <= 1 {
                self.dlet_active_names.remove(name);
            } else {
                *count -= 1;
            }
        }
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
        if self.interned_symbol_names.insert(name.to_string()) {
            self.interned_symbols.push(name.to_string());
        }
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
                _ => {}
            }
        }
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
        self.globals.remove(name);
    }

    pub(crate) fn global_binding_value(&self, name: &str) -> Option<Value> {
        self.globals.get(name).cloned()
    }

    pub fn set_global_binding(&mut self, name: &str, value: Value) {
        let name = self
            .resolve_variable_name(name)
            .unwrap_or_else(|_| name.to_string());
        let value = Self::stored_value(value);
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
            .rposition(|restore| restore.name == name)?;
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
    /// buffer.  Automatically buffer-local variables can have nested global
    /// specbind layers belonging to other buffers; peel those layers until
    /// this buffer's binding or the top-level value is reached.
    pub(super) fn active_global_special_value(&self, name: &str) -> Option<Option<Value>> {
        let mut value = self.global_value(name);
        let current_buffer_id = self.current_buffer_id();
        let mut found = false;
        for restore in self.active_special_restores.iter().rev().filter(|restore| {
            restore.name == name && matches!(restore.scope, SpecialBindingScope::Global)
        }) {
            found = true;
            if self.is_always_buffer_local_special(name)
                && restore
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
                restore.name == name && matches!(restore.scope, SpecialBindingScope::Global)
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
                restore.name == name
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
            restore.name == name && matches!(restore.scope, SpecialBindingScope::Global)
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
            restore.name == name
                && matches!(restore.scope, SpecialBindingScope::BufferLocal(id) if id == buffer_id)
        }) else {
            return false;
        };
        self.active_special_restores[index].previous = value.map(Self::stored_value);
        true
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
        match name {
            "display-hourglass" => Ok(if value.is_nil() { Value::Nil } else { Value::T }),
            "gc-cons-threshold" => match value {
                Value::Integer(_) | Value::BigInteger(_) => Ok(value),
                other => Err(wrong_type_argument("integerp", other)),
            },
            "scroll-up-aggressively" => match value {
                Value::Nil => Ok(Value::Nil),
                Value::Integer(number) if (0..=1).contains(&number) => Ok(Value::Integer(number)),
                Value::Float(number) if (0.0..=1.0).contains(&number) => Ok(Value::Float(number)),
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
            _ => Ok(value),
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

    pub(crate) fn has_active_buffer_local_special_binding(
        &self,
        buffer_id: u64,
        name: &str,
    ) -> bool {
        self.active_special_restores.iter().any(|restore| {
            restore.name == name && restore.scope == SpecialBindingScope::BufferLocal(buffer_id)
        })
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
        locals: Vec<(String, Value)>,
        evald: bool,
    ) {
        self.backtrace_frames.push(BacktraceFrame {
            function,
            args,
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
        activation_frame: Option<&[(String, Value)]>,
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

    // GNU `called-interactively-p' walks the backtrace and only skips
    // frames it recognizes: nadvice's advice OBJECTS, apply/funcall
    // plumbing, and the interactive dispatch itself.  A USER advice lambda
    // (the body of an :around advice) is not skippable, so the function it
    // wraps does not count as called interactively even under
    // `call-interactively' (nadvice-tests encodes this as expected
    // failures).
    pub(crate) fn called_interactively_by_backtrace(&self) -> bool {
        if !self.in_interactive_call() {
            return false;
        }
        let frame_is_oclosure = |function: &Value| -> bool {
            match function {
                Value::Lambda(_) => crate::lisp::primitives::oclosure_type_of(function).is_some(),
                Value::Symbol(symbol) => self
                    .functions
                    .iter()
                    .rev()
                    .find(|(name, _)| name == symbol)
                    .is_some_and(|(_, value)| {
                        crate::lisp::primitives::oclosure_type_of(value).is_some()
                    }),
                _ => false,
            }
        };
        let frame_name = |function: &Value| -> Option<String> {
            match function {
                Value::Symbol(name) | Value::BuiltinFunc(name) => Some(name.to_string()),
                _ => None,
            }
        };
        if std::env::var_os("EMAXX_DBG_CIP").is_some() {
            for (index, frame) in self.backtrace_frames.iter().rev().enumerate().take(14) {
                let function = frame.function_snapshot();
                eprintln!(
                    "EMAXX-DBG cip frame[{index}] evald={} fn={:.60}",
                    frame.evald,
                    format!("{function:?}")
                );
            }
        }
        let mut frames = self.backtrace_frames.iter().rev().peekable();
        // Drop this call's own frames (the called-interactively-p builtin
        // plus the unevald list frame recording it).
        while frames.peek().is_some_and(|frame| {
            frame_name(&frame.function_snapshot()).as_deref() == Some("called-interactively-p")
        }) {
            frames.next();
        }
        // Skip the current function's in-progress body forms (unevald list
        // frames whose applications have not happened yet).
        while frames.peek().is_some_and(|frame| !frame.evald) {
            frames.next();
        }
        // Drop the current function's frame(s): the evald application and,
        // when it was called by name, the unevald list frame for the call.
        let Some(current) = frames.next() else {
            return true;
        };
        let current_function = current.function_snapshot();
        if let Some(next) = frames.peek()
            && !next.evald
            && next.function_snapshot() == current_function
        {
            frames.next();
        }
        // GNU's advice--called-interactively-skip pairs an :around advice's
        // user lambda with the (apply INNER-ADVICE args) call in its body:
        // the lambda frame is skippable exactly when control continued DOWN
        // the advice chain through it.  The innermost :around (whose apply
        // dispatches the plain original function) is not skippable — that
        // is nadvice's documented broken case.
        let mut descended_through_advice = false;
        for frame in frames {
            let function = frame.function_snapshot();
            let name = frame_name(&function);
            match name.as_deref() {
                Some("apply") | Some("funcall") => {
                    descended_through_advice = frame
                        .first_arg_snapshot()
                        .as_ref()
                        .is_some_and(&frame_is_oclosure);
                    continue;
                }
                Some("call-interactively")
                | Some("funcall-interactively")
                | Some("command-execute") => return true,
                _ => {}
            }
            if frame_is_oclosure(&function) {
                continue;
            }
            // An unevald list frame duplicates the evald application that
            // follows it; judge on the evald one.
            if !frame.evald {
                continue;
            }
            if descended_through_advice {
                descended_through_advice = false;
                continue;
            }
            return false;
        }
        // The interactive dispatch (native command loop, kmacro) may invoke
        // commands without a visible call-interactively frame.
        true
    }

    pub fn set_current_backtrace_debug(&mut self, enabled: bool) {
        if let Some(frame) = self.backtrace_frames.last_mut() {
            frame.debug_on_exit = enabled;
        }
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

    pub fn backtrace_frame_locals_snapshot(&self, index: usize) -> Option<Vec<(String, Value)>> {
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
    ) -> Option<Vec<(String, Value)>> {
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
        let mut merged: Vec<(String, Value)> = Vec::new();
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

    pub fn push_handler_bindings(&mut self, bindings: &[(String, Value)]) -> usize {
        let start = self.active_handlers.len();
        self.active_handlers.extend(
            bindings.iter().map(|(condition, handler)| {
                ActiveHandler::Bind(condition.clone(), handler.clone())
            }),
        );
        start
    }

    pub(super) fn push_condition_case_handler(&mut self, heads: Vec<Value>) -> usize {
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

    /// GNU handler matching: `t' matches anything; otherwise the handler
    /// symbol must be `memq' in the signaled symbol's `error-conditions',
    /// falling back to the condition-or-`error' rule when none is defined.
    pub(super) fn condition_symbol_matches(
        symbol: &str,
        error_type: &str,
        condition_list: &[String],
    ) -> bool {
        if symbol == "t" {
            true
        } else if condition_list.is_empty() {
            symbol == error_type || symbol == "error"
        } else {
            condition_list.iter().any(|entry| entry == symbol)
        }
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

    pub(super) fn dispatch_handler_bindings(
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
                ActiveHandler::Bind(condition, handler) => {
                    if !Self::condition_symbol_matches(condition, &error_type, &condition_list) {
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
        skip_label: Option<&str>,
    ) -> Option<(usize, usize)> {
        let mut result: Option<(usize, usize)> = None;
        for (id, label, start, end) in &self.labeled_restrictions {
            if *id != buffer_id || skip_label == Some(label.as_str()) {
                continue;
            }
            result = Some(match result {
                Some((cur_start, cur_end)) => (cur_start.max(*start), cur_end.min(*end)),
                None => (*start, *end),
            });
        }
        result
    }
}
