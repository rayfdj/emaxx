use super::symbol_cells::{
    ALWAYS_LOCAL, FORWARDED, FWD_BOOL, FWD_INT, LOCAL_IF_SET, LOCALIZED, PER_BUFFER, SPECIAL,
};
use super::*;
use crate::lisp::types::Kind;
use crate::lisp::types::LispErrorKind;
use crate::lisp::types::SymbolName;

#[derive(Default)]
struct ReadSymbolContainers {
    cons: HashSet<usize>,
    vectors: HashSet<usize>,
}

impl BacktraceFrame {
    pub(super) fn function_snapshot(&self) -> Value {
        self.source_form()
            .and_then(|form| form.car().ok())
            .unwrap_or_else(|| self.function.value())
    }

    pub(super) fn args_snapshot(&self) -> Vec<Value> {
        if let Some(words) = self.args.native_words() {
            return crate::lisp::native_comp::decode_active_backtrace_arguments(words)
                .expect("a native backtrace frame is inspected only during its activation")
                .expect("a native backtrace frame contains valid Lisp words");
        }
        let Some(form) = self.source_form() else {
            return self.args.lisp_values().unwrap_or(&[]).to_vec();
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
            .map(|id| self.record_value(id))
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

    /// The bound local value of NAME in BUFFER_ID; a void local (data.c's
    /// `Qunbound' in the alist cell) answers None like no local at all.
    /// Callers that must tell the two apart use `buffer_local_binding'.
    pub fn buffer_local_value(&self, buffer_id: u64, name: &str) -> Option<Value> {
        self.buffer_locals
            .get(&buffer_id)
            .and_then(|locals| locals.binding_by_name(name))
            .flatten()
            .cloned()
    }

    /// `buffer_local_value' of a symbol held through `cached_symbol!'.
    pub(crate) fn buffer_local_value_key(
        &self,
        buffer_id: u64,
        key: &'static std::thread::LocalKey<SymbolName>,
    ) -> Option<Value> {
        key.with(|symbol| {
            self.buffer_locals
                .get(&buffer_id)
                .and_then(|locals| locals.binding(symbol))
                .flatten()
                .cloned()
        })
    }

    /// `assq_no_quit (symbol, BVAR (buffer, local_var_alist))': `Some(None)'
    /// is a binding whose value is void.
    pub(crate) fn buffer_local_binding(&self, buffer_id: u64, name: &str) -> Option<Option<Value>> {
        self.buffer_locals
            .get(&buffer_id)
            .and_then(|locals| locals.binding_by_name(name))
            .map(|value| value.cloned())
    }

    pub(crate) fn buffer_local_binding_symbol(
        &self,
        buffer_id: u64,
        symbol: &SymbolName,
    ) -> Option<Option<Value>> {
        self.buffer_locals
            .get(&buffer_id)
            .and_then(|locals| locals.binding(symbol))
            .map(|value| value.cloned())
    }

    pub(crate) fn has_buffer_local_binding(&self, buffer_id: u64, name: &str) -> bool {
        self.buffer_local_binding(buffer_id, name).is_some()
    }

    pub fn set_buffer_local_value(&mut self, buffer_id: u64, name: &str, value: Value) {
        self.globals.set_flag_by_name(name, LOCALIZED);
        let value = if matches!(value.kind(), Kind::Unbound) {
            value
        } else {
            let value = Self::stored_value(self.normalize_forwarded_eval_cell(name, value));
            if buffer_id == self.current_buffer_id() {
                self.update_forwarded_eval_cell(name, &value);
            }
            value
        };
        let symbol = SymbolName::intern_str(name);
        self.buffer_locals
            .entry(buffer_id)
            .or_default()
            .insert(&symbol, value);
    }

    /// `set_buffer_local_value' for the symbol in hand: the flag and the
    /// buffer's cell by id, the forwarded C cell only for a symbol that
    /// has one (the name path matched every store against the forwarded
    /// names and interned the name for the cell).
    pub(crate) fn set_buffer_local_value_symbol(
        &mut self,
        buffer_id: u64,
        symbol: &SymbolName,
        value: Value,
    ) {
        self.globals.set_flag(symbol, LOCALIZED);
        let value = if matches!(value.kind(), Kind::Unbound) {
            value
        } else if self.has_c_slot_symbol(symbol) {
            let value =
                Self::stored_value(self.normalize_forwarded_eval_cell(symbol.as_str(), value));
            if buffer_id == self.current_buffer_id() {
                self.update_forwarded_eval_cell(symbol.as_str(), &value);
            }
            value
        } else {
            Self::stored_value(value)
        };
        self.buffer_locals
            .entry(buffer_id)
            .or_default()
            .insert(symbol, value);
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
            .map(|(name, value)| (name.as_str().to_owned(), *value))
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

    /// Every local binding of BUFFER_ID in first-binding order; a void
    /// local carries `Value::Unbound'.
    pub fn buffer_local_variables(&self, buffer_id: u64) -> Vec<(String, Value)> {
        self.buffer_locals
            .get(&buffer_id)
            .map(|locals| {
                locals
                    .iter()
                    .map(|(name, value)| (name.as_str().to_owned(), *value))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// `blv->local_if_set = 1' (data.c:Fmake_variable_buffer_local).
    pub fn mark_auto_buffer_local(&mut self, name: &str) {
        self.globals
            .set_flag_by_name(name, LOCAL_IF_SET | LOCALIZED);
    }

    pub fn is_auto_buffer_local(&self, name: &str) -> bool {
        self.globals.has_flag_by_name(name, LOCAL_IF_SET)
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
        self.globals.set_flag_by_name(name, PER_BUFFER);
    }

    /// Mark a native DEFVAR_PER_BUFFER variable whose GNU buffer slot has
    /// index -1 and is therefore always local.  A dynamic binding made in one
    /// buffer must not forward into another buffer for this subset.
    pub fn mark_always_buffer_local_special(&mut self, name: &str) {
        self.mark_per_buffer_special(name);
        self.globals.set_flag_by_name(name, ALWAYS_LOCAL);
    }

    pub fn is_per_buffer_special(&self, name: &str) -> bool {
        self.globals.has_flag_by_name(name, PER_BUFFER)
    }

    pub fn is_always_buffer_local_special(&self, name: &str) -> bool {
        self.globals.has_flag_by_name(name, ALWAYS_LOCAL)
    }

    /// data.c:let_shadows_buffer_binding_p: a `SPECPDL_LET_LOCAL' or
    /// `SPECPDL_LET_DEFAULT' record for NAME made in the current buffer.
    pub(crate) fn let_shadows_buffer_binding(&self, name: &str) -> bool {
        let current = self.current_buffer_id();
        self.active_special_restores.iter().any(|restore| {
            restore.name == name
                && match restore.scope {
                    SpecialBindingScope::BufferLocal(buffer_id) => buffer_id == current,
                    SpecialBindingScope::Global => restore.binding_buffer_id == Some(current),
                }
        })
    }

    pub fn mark_special_variable(&mut self, name: &str) {
        if self.globals.set_flag_by_name(name, SPECIAL) {
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
            self.globals.clear_flag_by_name(name, SPECIAL);
        }
    }

    /// Fdefvar without a value under lexical binding:
    /// `Vinternal_interpreter_environment = Fcons (sym, ...)' -- the bare
    /// symbol stored into the current environment, no specpdl entry, so
    /// the declaration lasts to the end of the enclosing scope (the file,
    /// at top level).  `let's of the name in that scope bind dynamically
    /// (Flet's Fmemq) without the global special flag.
    pub(crate) fn push_local_special_declaration(&mut self, name: &str, env: &mut Env) {
        let declared = Value::cons(
            Value::Symbol(name.into()),
            crate::lisp::types::current_environment_value(env),
        );
        match env.last_mut() {
            Some(frame) => frame.set_environment(declared),
            None => env.push(EnvFrame::from_alist(declared)),
        }
        self.local_special_names.insert(name.to_string());
    }

    /// A bare-symbol entry of a closure's environment declares NAME
    /// locally special within it; note the name so the `let' check runs.
    pub(crate) fn note_captured_local_special(&mut self, name: &str) {
        self.local_special_names.insert(name.to_string());
    }

    /// Flet's `!NILP (Fmemq (var, Vinternal_interpreter_environment))':
    /// whether NAME is declared locally special in the current
    /// environment.  The process-wide set of names ever declared is a
    /// filter in front of the walk (a name never declared skips it).
    pub(crate) fn local_special_active(&self, name: &str, env: &Env) -> bool {
        if !self.local_special_names.contains(name) {
            return false;
        }
        crate::lisp::types::current_environment(env).is_some_and(|environment| {
            crate::lisp::types::environment_declares_special(environment, name)
        })
    }

    /// Whether GNU's hidden `internal-interpreter-environment' is non-nil.
    /// Explicit evaluator entry points carry the dialect in the override;
    /// direct internal evaluation uses a nonempty typed environment as its
    /// lexical marker.
    pub(crate) fn interpreter_environment_is_lexical(&self, env: &Env) -> bool {
        self.lambda_capture_override()
            .unwrap_or_else(|| crate::lisp::types::environment_is_lexical(env))
    }

    /// `is_special_variable' for a symbol in hand: the flag by id, the
    /// C-slot registry by name.
    pub(crate) fn is_special_variable_symbol(&self, symbol: &SymbolName) -> bool {
        if self.globals.has_flag(symbol, SPECIAL) || self.has_c_slot_symbol(symbol) {
            return true;
        }
        if !self.globals.has_aliases() {
            return false;
        }
        let resolved = self.resolve_variable_symbol(symbol).unwrap_or(*symbol);
        self.globals.has_flag(&resolved, SPECIAL)
            || self.builtin_var_value(resolved.as_str()).is_some()
    }

    /// Whether a binding form must use GNU's dynamic value-cell semantics.
    /// `(eval FORM)' supplies a nil lexical environment, so bindings made
    /// directly by FORM are dynamic even for undeclared symbols.  Existing
    /// lexical functions called by FORM mask this override at their boundary.
    pub(crate) fn binding_is_dynamic_symbol(&self, symbol: &SymbolName, env: &Env) -> bool {
        // The name-keyed tables are probed only when they hold anything
        // (each probe hashed the name per `let' binding of interpreted
        // code, over tables that are nearly always empty).
        self.lambda_capture_override() == Some(false)
            || self.is_special_variable_symbol(symbol)
            || (!self.local_special_names.is_empty()
                && self.local_special_active(symbol.as_str(), env))
    }

    /// Whether SYMBOL names a C-owned value cell (`builtin_var_value'
    /// synthesizes one).  The registry is a match over the names; a name
    /// outside it is outside it for good, and that verdict is kept per
    /// symbol id so an interpreted `let' of a lexical variable does not
    /// walk the match on every binding (a fifth of mule-tests' ucs-names
    /// cases).  A detached forwarded variable answers false without
    /// entering the memo, as detachment is undone by a store.
    pub(crate) fn has_c_slot_symbol(&self, symbol: &SymbolName) -> bool {
        thread_local! {
            static NO_C_SLOT: RefCell<HashSet<u32, crate::lisp::types::IdentityBuildHasher>> =
                RefCell::new(HashSet::default());
        }
        if NO_C_SLOT.with_borrow(|known| known.contains(&symbol.id())) {
            return false;
        }
        if self.builtin_var_value(symbol.as_str()).is_some() {
            return true;
        }
        if !self
            .detached_forwarded_variables
            .contains_key(symbol.as_str())
        {
            NO_C_SLOT.with_borrow_mut(|known| {
                known.insert(symbol.id());
            });
        }
        false
    }

    pub fn is_special_variable(&self, name: &str) -> bool {
        // Every value synthesized by `builtin_var_value' represents a
        // dumped/native value cell (or a dumped Lisp defvar) and is therefore
        // dynamically scoped under lexical binding.  Derive that property
        // from the value registry itself so adding a startup default cannot
        // silently omit its binding semantics.
        if self.globals.has_flag_by_name(name, SPECIAL) || self.builtin_var_value(name).is_some() {
            return true;
        }
        if !self.globals.has_aliases() {
            return false;
        }
        let resolved = self
            .resolve_variable_name(name)
            .unwrap_or_else(|_| name.to_string());
        self.globals.has_flag_by_name(&resolved, SPECIAL)
            || self.builtin_var_value(&resolved).is_some()
    }

    pub fn special_variable_names(&self) -> Vec<String> {
        self.special_variables.clone()
    }

    pub(super) fn symbol_property_index(&self, name: &str) -> Option<usize> {
        self.symbol_properties_index.get(name).copied()
    }

    /// The plist position of SYMBOL by its id, learning it from the name
    /// index on the first lookup.
    fn symbol_property_index_of(&self, symbol: &SymbolName) -> Option<usize> {
        if let Some(cached) = self.symbol_properties_by_id.borrow().get(&symbol.id()) {
            return *cached;
        }
        // A symbol without a plist (most) is remembered as such; a new
        // plist clears the cache.
        let index = self.symbol_property_index(symbol.as_str());
        self.symbol_properties_by_id
            .borrow_mut()
            .insert(symbol.id(), index);
        index
    }

    /// A symbol's first plist entry: the position cache learns it (a
    /// cached "no plist" for the symbol would otherwise stand).
    fn note_symbol_plist_added(&mut self, name: &str, index: usize) {
        if let Some(id) = SymbolName::id_of(name) {
            self.symbol_properties_by_id
                .borrow_mut()
                .insert(id, Some(index));
        }
    }

    fn rebuild_symbol_properties_index(&mut self) {
        self.symbol_properties_index = super::ordered_name_index(&self.symbol_properties);
        self.symbol_properties_by_id.borrow_mut().clear();
    }

    /// fns.c:Fget's plist_get on the symbol's own plist: SYMBOL addresses
    /// its plist directly and PROPERTY is compared as a symbol.
    pub fn get_symbol_property_of(
        &self,
        symbol: &SymbolName,
        property: &SymbolName,
    ) -> Option<Value> {
        let index = self.symbol_property_index_of(symbol)?;
        // fns.c:plist_get: the walk reads each cell in place -- no Lisp
        // object is copied per pair (a clone of every car and cdr made the
        // walk three refcount round trips a pair) -- with
        // FOR_EACH_TAIL_SAFE's Brent cycle check on the cell identities.
        let mut cell = match self.symbol_properties[index].1.kind() {
            Kind::Cons(cell) => cell,
            _ => return None,
        };
        let mut tortoise = crate::lisp::types::ConsCell::identity(&cell);
        let (mut power, mut steps) = (2usize, 0usize);
        loop {
            let matches = match cell.car.get().kind() {
                Kind::Symbol(key) => key == *property,
                Kind::Nil => property == "nil",
                Kind::T => property == "t",
                _ => false,
            };
            let next = {
                let rest = cell.cdr.get();
                let Kind::Cons(value_cell) = (rest).kind() else {
                    return None;
                };
                if matches {
                    return Some(value_cell.car.get());
                }
                let after = value_cell.cdr.get();
                match (after).kind() {
                    Kind::Cons(next) => next,
                    _ => return None,
                }
            };
            cell = next;
            steps += 1;
            let identity = crate::lisp::types::ConsCell::identity(&cell);
            if identity == tortoise {
                return None;
            }
            if steps == power {
                tortoise = identity;
                power <<= 1;
                steps = 0;
            }
        }
    }

    pub fn get_symbol_property(&self, name: &str, property: &str) -> Option<Value> {
        let index = self.symbol_property_index(name)?;
        let mut tail = self.symbol_properties[index].1;
        // fns.c:plist_get walks with FOR_EACH_TAIL_SAFE: Brent's cycle
        // detection (a tortoise moved at powers of two), no allocation.
        let mut tortoise = Brent::new(&tail);
        while let Kind::Cons(cell) = tail.kind() {
            let rest = cell.cdr.get();
            let (value_cell, next_cell) = rest.cons_cells()?;
            if matches!(cell.car.get().kind(), Kind::Symbol(key) if key == property) {
                return Some(value_cell.get());
            }
            tail = next_cell.get();
            if tortoise.cycle(&tail) {
                return None;
            }
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
            let plist = self.symbol_properties[index].1;
            let mut tail = plist;
            let mut tortoise = Brent::new(&tail);
            while let Kind::Cons(cell) = tail.kind() {
                let rest = cell.cdr.get();
                let Some((value_cell, next_cell)) = (rest).cons_cells() else {
                    return;
                };
                if matches!(cell.car.get().kind(), Kind::Symbol(key) if key == property) {
                    value_cell.set(value);
                    return;
                }
                let next = next_cell.get();
                if next.is_nil() {
                    next_cell.set(Value::list([
                        Value::Symbol(property.to_string().into()),
                        value,
                    ]));
                    return;
                }
                tail = next;
                if tortoise.cycle(&tail) {
                    return;
                }
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
        self.note_symbol_plist_added(name, index);
    }

    pub fn intern_symbol_name(&mut self, name: &str) {
        self.uninterned_standard_symbol_names.remove(name);
        if self.interned_symbol_names.insert(name.to_string()) {
            self.interned_symbols
                .push(crate::lisp::types::SymbolName::intern_str(name));
        }
    }

    pub(crate) fn unintern_standard_symbol_name(&mut self, name: &str) -> bool {
        if !self.standard_obarray_contains_symbol(name) {
            return false;
        }
        self.uninterned_standard_symbol_names
            .insert(name.to_string());
        self.note_obarray_removal();
        if self.interned_symbol_names.remove(name) {
            self.interned_symbols
                .retain(|candidate| candidate.as_str() != name);
        }
        true
    }

    /// Register ordinary symbols constructed by the Lisp reader in the
    /// standard obarray.  Reader data may be circular and propertized strings
    /// may hide symbols in their property values, so walk iteratively with an
    /// identity guard instead of assuming a proper tree.
    pub(crate) fn intern_symbols_in_value(&mut self, value: &Value) {
        let mut pending = vec![*value];
        let mut seen_cons_cells = HashSet::new();
        let mut seen_strings = HashSet::new();
        let mut seen_vectors = HashSet::new();

        while let Some(current) = pending.pop() {
            match current.kind() {
                Kind::Symbol(name) => {
                    if crate::lisp::types::visible_symbol_name(&name) == name {
                        self.intern_symbol_name(&name);
                    }
                }
                Kind::Cons(cons_cell) => {
                    let car = &cons_cell.car;
                    let cdr = &cons_cell.cdr;
                    if seen_cons_cells.insert(crate::lisp::types::ConsCell::identity(&cons_cell)) {
                        pending.push(cdr.get());
                        pending.push(car.get());
                    }
                }
                Kind::Vector(vector) if seen_vectors.insert(vector.identity()) => {
                    pending.extend(vector.slots().iter().cloned());
                }
                Kind::StringObject(state) if seen_strings.insert(state.identity()) => {
                    for span in &state.borrow().props {
                        for (property, property_value) in &span.props {
                            self.intern_symbol_name(property);
                            pending.push(*property_value);
                        }
                    }
                }
                // GNU's reader interns symbols inside every literal it
                // builds: `#[...]' closure constant vectors, `#s(...)'
                // hash tables/records, char-tables, and circular labels
                // (lread.c read0 interns at each `read_symbol').  Skipping
                // these left every symbol that only occurs in a compiled
                // constant vector out of the standard obarray.
                Kind::ReaderForm(form) => match form.as_ref() {
                    crate::lisp::types::ReaderForm::CircularLabel { payload, .. } => {
                        pending.push(*payload);
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
        if !matches!(obarray.kind(), Kind::Record(id) if !self.is_standard_obarray_id(id.id)) {
            self.intern_symbols_in_value(&value);
            return Ok(value);
        }
        self.intern_read_symbols_in_obarray(value, &obarray, &mut ReadSymbolContainers::default())
    }

    fn intern_read_symbols_in_obarray(
        &mut self,
        value: Value,
        obarray: &Value,
        seen: &mut ReadSymbolContainers,
    ) -> Result<Value, LispError> {
        match value.kind() {
            Kind::Symbol(name) if crate::lisp::types::visible_symbol_name(&name) == name => {
                crate::lisp::primitives::intern_in_obarray(self, obarray, &name)
            }
            Kind::Cons(cell) => {
                if seen
                    .cons
                    .insert(crate::lisp::types::ConsCell::identity(&cell))
                {
                    let car = cell.car.get();
                    let cdr = cell.cdr.get();
                    let car = self.intern_read_symbols_in_obarray(car, obarray, seen)?;
                    let cdr = self.intern_read_symbols_in_obarray(cdr, obarray, seen)?;
                    cell.car.set(car);
                    cell.cdr.set(cdr);
                }
                Ok(Value::Cons(cell))
            }
            Kind::Vector(vector) => {
                if seen.vectors.insert(vector.identity()) {
                    let slots = vector.slots().to_vec();
                    let mapped = self.intern_read_symbol_fields(&slots, obarray, seen)?;
                    vector.slots_mut().clone_from_slice(&mapped);
                }
                Ok(Value::Vector(vector))
            }
            Kind::StringObject(state) => {
                let mut borrowed = state.borrow_mut();
                for span in &mut borrowed.props {
                    for (_, property_value) in &mut span.props {
                        *property_value =
                            self.intern_read_symbols_in_obarray(*property_value, obarray, seen)?;
                    }
                }
                drop(borrowed);
                Ok(Value::StringObject(state))
            }
            Kind::ReaderForm(form) => {
                use crate::lisp::types::ReaderForm;

                let mapped = match form.as_ref() {
                    ReaderForm::CircularLabel { id, payload } => ReaderForm::CircularLabel {
                        id: *id,
                        payload: self.intern_read_symbols_in_obarray(*payload, obarray, seen)?,
                    },
                    ReaderForm::CircularReference(id) => ReaderForm::CircularReference(*id),
                    ReaderForm::HashTable { fields } => ReaderForm::HashTable {
                        fields: self.intern_read_symbol_fields(fields, obarray, seen)?,
                    },
                    ReaderForm::CharTable { fields } => ReaderForm::CharTable {
                        fields: self.intern_read_symbol_fields(fields, obarray, seen)?,
                    },
                    ReaderForm::SubCharTable { fields } => ReaderForm::SubCharTable {
                        fields: self.intern_read_symbol_fields(fields, obarray, seen)?,
                    },
                    ReaderForm::Record { slots } => ReaderForm::Record {
                        slots: self.intern_read_symbol_fields(slots, obarray, seen)?,
                    },
                    ReaderForm::Closure { kind, slots } => ReaderForm::Closure {
                        kind: *kind,
                        slots: self.intern_read_symbol_fields(slots, obarray, seen)?,
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
                Ok(Value::ReaderForm(
                    crate::lisp::alloc::VectorlikeRef::allocate(mapped),
                ))
            }
            other => Ok(other.value()),
        }
    }

    fn intern_read_symbol_fields(
        &mut self,
        fields: &[Value],
        obarray: &Value,
        seen: &mut ReadSymbolContainers,
    ) -> Result<Vec<Value>, LispError> {
        fields
            .iter()
            .cloned()
            .map(|field| self.intern_read_symbols_in_obarray(field, obarray, seen))
            .collect()
    }

    pub(crate) fn is_standard_obarray_id(&self, id: u64) -> bool {
        id == self.standard_obarray_id
    }

    pub fn remove_symbol_property(&mut self, name: &str, property: &str) {
        let Some(index) = self.symbol_property_index(name) else {
            return;
        };
        let mut tail = self.symbol_properties[index].1;
        let mut previous_value_cell: Option<Value> = None;
        let mut seen = HashSet::new();
        while let Kind::Cons(cell) = tail.kind() {
            if !seen.insert(crate::lisp::types::ConsCell::identity(&cell)) {
                return;
            }
            let rest = cell.cdr.get();
            let Some((_, next_cell)) = rest.cons_cells() else {
                return;
            };
            let next = next_cell.get();
            if matches!(cell.car.get().kind(), Kind::Symbol(key) if key == property) {
                self.note_definition_changed();
                if let Some(previous) = previous_value_cell {
                    previous
                        .set_cdr(next)
                        .expect("a tracked plist value cell is a cons");
                } else if next.is_nil() {
                    self.symbol_properties.remove(index);
                    self.rebuild_symbol_properties_index();
                    self.note_obarray_removal();
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
            .map(|index| self.symbol_properties[index].1)
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
                self.note_obarray_removal();
            }
        } else if let Some(existing) = self.symbol_property_index(name) {
            self.symbol_properties[existing].1 = Self::stored_value(plist);
        } else {
            let index = self.symbol_properties.len();
            self.symbol_properties
                .push((name.to_string(), Self::stored_value(plist)));
            self.symbol_properties_index.insert(name.to_string(), index);
            self.note_symbol_plist_added(name, index);
        }
        Ok(plist)
    }

    pub(super) fn variable_watcher_index(&self, name: &str) -> Option<usize> {
        self.variable_watchers
            .iter()
            .rposition(|(symbol, _)| symbol.as_str() == name)
    }

    /// data.c's SYMBOL_TRAPPED_WRITE test for the symbol in hand: whether
    /// any watcher is registered under it, by id.
    fn variable_is_watched(&self, symbol: &SymbolName) -> bool {
        self.variable_watchers
            .iter()
            .any(|(watched, _)| watched.id() == symbol.id())
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
                self.variable_watchers[index].1.push(watcher);
            }
        } else {
            self.variable_watchers
                .push((SymbolName::intern_str(&resolved), vec![watcher]));
        }
        // data.c:Fadd_variable_watcher sets SYMBOL_TRAPPED_WRITE: the
        // symbol's stores notify from now on.
        self.globals
            .set_plain_store(&SymbolName::intern_str(&resolved), false);
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
        Ok(*watcher)
    }

    pub fn clear_variable_watchers(&mut self, name: &str) {
        if let Some(index) = self.variable_watcher_index(name) {
            self.variable_watchers.remove(index);
        }
    }

    /// `notify_variable_watchers' for the symbol in hand: nothing to do,
    /// decided by id, unless a watcher is registered under it.
    pub(crate) fn notify_variable_watchers_symbol(
        &mut self,
        symbol: &SymbolName,
        value: Value,
        action: &str,
        buffer_id: Option<u64>,
        env: &mut Env,
    ) -> Result<(), LispError> {
        if !self.variable_is_watched(symbol) {
            return Ok(());
        }
        self.notify_variable_watchers(symbol.as_str(), value, action, buffer_id, env)
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
                    value,
                    Value::Symbol(action.to_string().into()),
                    buffer,
                ],
                env,
            )?;
        }
        Ok(())
    }

    pub(super) fn direct_variable_alias(&self, name: &str) -> Option<String> {
        self.globals
            .alias_by_name(name)
            .map(|target| target.as_str().to_owned())
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

    /// `resolve_variable_name' for a symbol in hand: the alias chain is
    /// followed by the symbols' ids, no name hashed and no text copied.
    /// An assignment used to resolve its variable's name to its id seven
    /// times over, hashing the text each time, and copy the name three
    /// times; the symbol-keyed path below reads every cell by id.
    pub(crate) fn resolve_variable_symbol(
        &self,
        symbol: &SymbolName,
    ) -> Result<SymbolName, LispError> {
        let Some(first) = self.globals.alias(symbol) else {
            return Ok(*symbol);
        };
        let mut seen = vec![*symbol, *first];
        let mut current = *first;
        while let Some(target) = self.globals.alias(&current) {
            if seen.contains(target) {
                return Err(LispError::SignalValue(Value::list([
                    Value::Symbol("cyclic-variable-indirection".into()),
                    Value::Symbol(*symbol),
                ])));
            }
            seen.push(*target);
            current = *target;
        }
        Ok(current)
    }

    /// eval.c:Fdefvaralias's non-circularity loop: walk BASE's redirect
    /// chain, and signal with BASE if it reaches ALIAS.
    pub(crate) fn check_variable_alias_cycle(
        &self,
        alias: &str,
        base: &str,
    ) -> Result<(), LispError> {
        let mut current = base.to_owned();
        loop {
            if current == alias {
                return Err(LispError::SignalValue(Value::list([
                    Value::Symbol("cyclic-variable-indirection".into()),
                    Value::Symbol(base.to_string().into()),
                ])));
            }
            match self.direct_variable_alias(&current) {
                Some(next) => current = next,
                None => return Ok(()),
            }
        }
    }

    /// `SET_SYMBOL_ALIAS (sym, XSYMBOL (base_variable))': the redirect
    /// names BASE itself, not the end of BASE's chain, so re-pointing BASE
    /// later re-points every alias of it (data.c:indirect_variable walks
    /// the chain on each read).
    pub fn set_variable_alias(&mut self, alias: &str, target: &str) -> Result<(), LispError> {
        self.check_variable_alias_cycle(alias, target)?;
        self.globals.set_alias(
            &SymbolName::intern_str(alias),
            SymbolName::intern_str(target),
        );
        if let Some(index) = self
            .variable_aliases
            .iter()
            .rposition(|(existing, _)| existing == alias)
        {
            self.variable_aliases[index].1 = target.to_owned();
        } else {
            self.variable_aliases
                .push((alias.to_string(), target.to_owned()));
        }
        Ok(())
    }

    /// `SYMBOL_CONSTANT_P': `trapped_write == SYMBOL_NOWRITE'.  nil, t and
    /// keywords are made constant at intern; every other C-made constant is
    /// a `make_symbol_constant' call in the pinned sources (data.c's fixnum
    /// bounds, buffer.c's `enable-multibyte-characters', font.c's three
    /// tables).
    pub(crate) fn is_constant_symbol(&self, name: &str) -> bool {
        matches!(
            name,
            "nil"
                | "t"
                | "most-positive-fixnum"
                | "most-negative-fixnum"
                | "enable-multibyte-characters"
                | "font-weight-table"
                | "font-slant-table"
                | "font-width-table"
        ) || name.starts_with(':')
    }

    /// `SYMBOL_LOCALIZED': the symbol has ever acquired a buffer-local
    /// binding (data.c never clears the redirect).
    pub(crate) fn is_localized_variable(&self, name: &str) -> bool {
        self.globals.has_flag_by_name(name, LOCALIZED)
    }

    /// `SYMBOL_FORWARDED': a DEFVAR_* the pinned oracle build carries, and
    /// that `makunbound' has not detached from its C slot.
    pub(crate) fn is_forwarded_variable(&self, name: &str) -> bool {
        self.globals.has_flag_by_name(name, FORWARDED)
    }

    /// lread.c:defvar_lisp/defvar_bool/defvar_int at interpreter
    /// construction: every name the contracted oracle build forwards gets
    /// its redirect kind in the symbol cell, so stores coerce or check as
    /// data.c:store_symval_forwarding does for that kind.
    pub(crate) fn mark_forwarded_variables(&mut self) {
        for name in crate::lisp::primitives::gnu_c_forwarded_variables() {
            let mut flags = FORWARDED;
            if crate::lisp::primitives::generated_gnu_c_bool_variables::is_gnu_c_bool_variable(name)
            {
                flags |= FWD_BOOL;
            }
            if crate::lisp::primitives::generated_gnu_c_int_variables::is_gnu_c_int_variable(name) {
                flags |= FWD_INT;
            }
            self.globals.set_flag_by_name(name, flags);
        }
    }

    /// data.c:set_internal storing Qunbound into a forwarded symbol:
    /// `sym->u.s.redirect = SYMBOL_PLAINVAL' -- later stores cannot
    /// reconnect it to the C variable.
    pub(crate) fn detach_forwarded_variable(&mut self, name: &str, slot_value: Value) {
        self.globals
            .clear_flag_by_name(name, FORWARDED | FWD_BOOL | FWD_INT);
        self.detached_forwarded_variables
            .insert(name.to_owned(), slot_value);
    }

    /// A forwarded name the oracle build already reports `SYMBOL_LOCALIZED'
    /// at `-Q --batch' (buffer.c/keyboard.c localize it at initialization).
    pub(crate) fn is_localized_at_startup_in_gnu(&self, name: &str) -> bool {
        crate::lisp::primitives::gnu_c_localized_forwarded_variables()
            .binary_search(&name)
            .is_ok()
    }

    /// Whether a `SPECPDL_LET*' record for NAME is on the binding stack.
    pub(crate) fn is_let_bound_special(&self, name: &str) -> bool {
        self.active_special_restores
            .iter()
            .any(|restore| restore.name == name)
    }

    pub fn remove_variable_alias(&mut self, name: &str) -> bool {
        if let Some(index) = self
            .variable_aliases
            .iter()
            .rposition(|(alias, _)| alias == name)
        {
            self.variable_aliases.remove(index);
            self.note_obarray_removal();
            self.globals.clear_alias_by_name(name);
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
        self.globals.is_bound_name(&resolved)
    }

    pub fn remove_global_binding(&mut self, name: &str) {
        for terminal in &mut self.terminals {
            terminal.keyboard.remove(name);
        }
        if self.globals.remove_by_name(name).is_some() {
            self.note_obarray_removal();
        }
    }

    /// `remove_global_binding' for a symbol in hand.
    pub(crate) fn remove_global_binding_symbol(&mut self, symbol: &SymbolName) {
        for terminal in &mut self.terminals {
            terminal.keyboard.remove(symbol.as_str());
        }
        self.globals.remove(symbol);
        self.note_obarray_removal();
    }

    /// The native word of SYMBOL's plain value, if the cell still holds
    /// one produced under STAMP (see `SymbolCells::native_word').
    pub(crate) fn cached_native_symbol_word(
        &self,
        symbol: &SymbolName,
        stamp: u64,
    ) -> Option<usize> {
        if self.terminal_keyboard_value(symbol.as_str()).is_some() {
            return None;
        }
        self.globals.native_word(symbol, stamp)
    }

    pub(crate) fn cache_native_symbol_word(&self, symbol: &SymbolName, stamp: u64, word: usize) {
        if self.terminal_keyboard_value(symbol.as_str()).is_some() {
            return;
        }
        self.globals.set_native_word(symbol, stamp, word);
    }

    #[cfg(test)]
    pub(crate) fn native_symbol_words_under(&self, stamp: u64) -> usize {
        self.globals.native_words_under(stamp)
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

    pub(super) fn update_forwarded_eval_cell(&mut self, name: &str, value: &Value) {
        // data.c:set_internal turns a voided forwarded symbol into a plain
        // symbol. Later stores cannot reconnect it to the C variable.
        if self.detached_forwarded_variables.contains_key(name) {
            return;
        }
        match name {
            "quit-flag" => self.quit_flag = *value,
            "inhibit-quit" => self.inhibit_quit = *value,
            "throw-on-input" => self.throw_on_input = *value,
            "overriding-plist-environment" => self.overriding_plist_environment = *value,
            "load-path" => self.load_path = *value,
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
            "quit-flag" => Some(self.quit_flag),
            "inhibit-quit" => Some(self.inhibit_quit),
            "throw-on-input" => Some(self.throw_on_input),
            "overriding-plist-environment" => Some(self.overriding_plist_environment),
            "load-path" => Some(self.load_path),
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
        self.quit_flag
    }

    pub(crate) fn inhibit_quit_is_truthy(&self) -> bool {
        self.inhibit_quit.is_truthy()
    }

    pub(crate) fn throw_on_input_value(&self) -> Value {
        self.throw_on_input
    }

    pub(crate) fn overriding_plist_environment_value(&self) -> Value {
        self.overriding_plist_environment
    }

    /// fns.c:Fget's `NILP (Voverriding_plist_environment)' read.
    #[inline]
    pub(crate) fn overriding_plist_environment_is_nil(&self) -> bool {
        self.overriding_plist_environment.is_nil()
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
        self.terminal_keyboard_value(name)
            .or_else(|| self.globals.value_by_name(name).cloned())
    }

    pub(crate) fn global_binding_value_symbol(&self, name: &SymbolName) -> Option<Value> {
        if Self::is_keyboard_variable(name)
            && let Some(value) = self.terminal_keyboard_value(name.as_str())
        {
            return Some(value);
        }
        self.globals.value(name).cloned()
    }

    pub fn set_global_binding(&mut self, name: &str, value: Value) {
        let name = self
            .resolve_variable_name(name)
            .unwrap_or_else(|_| name.to_string());
        let symbol = SymbolName::intern_str(&name);
        self.set_global_binding_resolved(&symbol, value);
    }

    /// `set_global_binding' for a symbol whose alias chain is resolved:
    /// the buffer-local and global cells are read and written by id.
    pub(crate) fn set_global_binding_resolved(&mut self, symbol: &SymbolName, value: Value) {
        let name = symbol.as_str();
        let value = Self::stored_value(self.normalize_forwarded_eval_cell(name, value));
        if self.set_terminal_keyboard_value(name, &value) {
            return;
        }
        if name == "features" {
            self.provided_features = value
                .to_vec()
                .unwrap_or_default()
                .into_iter()
                .filter_map(|feature| feature.as_symbol().ok().map(str::to_string))
                .collect();
        }
        if name == "ascii-case-table"
            && let Kind::CharTable(id) = value.kind()
        {
            self.mark_ascii_case_table(id);
        }
        if self
            .buffer_locals
            .get(&self.current_buffer_id())
            .and_then(|locals| locals.binding(symbol))
            .flatten()
            .is_none()
        {
            self.update_forwarded_eval_cell(name, &value);
        }
        if let Some(existing) = self.globals.value_mut(symbol) {
            *existing = value;
        } else {
            self.globals.insert(symbol, value);
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

    /// `active_special_assignment_scope' for a symbol in hand: the
    /// restore records carry symbols, so the match is by identity first,
    /// and the auto-local flag is read by id.
    pub(super) fn active_special_assignment_scope_symbol(
        &self,
        symbol: &SymbolName,
    ) -> Option<SpecialBindingScope> {
        let index = self
            .active_special_restores
            .iter()
            .rposition(|restore| !restore.local_binding_killed && restore.name == *symbol)?;
        let restore = &self.active_special_restores[index];
        let auto_local = || self.globals.has_flag(symbol, LOCAL_IF_SET);
        match restore.scope {
            SpecialBindingScope::Global
                if auto_local() && restore.binding_buffer_id != Some(self.current_buffer_id()) =>
            {
                None
            }
            SpecialBindingScope::BufferLocal(id)
                if auto_local() && id != self.current_buffer_id() =>
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
                value = restore.previous;
            } else {
                break;
            }
        }
        found.then_some(value)
    }

    /// `active_global_special_value' for a symbol in hand: the flag and
    /// the records by id.
    pub(super) fn active_global_special_value_symbol(
        &self,
        symbol: &SymbolName,
    ) -> Option<Option<Value>> {
        if !self.globals.has_flag(symbol, ALWAYS_LOCAL) {
            return None;
        }
        let mut value = self.global_binding_value_symbol(symbol);
        let current_buffer_id = self.current_buffer_id();
        let mut found = false;
        let id = symbol.id();
        for restore in self.active_special_restores.iter().rev().filter(|restore| {
            !restore.local_binding_killed
                && restore.name.id() == id
                && matches!(restore.scope, SpecialBindingScope::Global)
        }) {
            found = true;
            if restore
                .binding_buffer_id
                .is_some_and(|buffer_id| buffer_id != current_buffer_id)
            {
                value = restore.previous;
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
            .map(|restore| restore.previous)
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
            .map(|restore| restore.previous)
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
        if !self.set_active_global_toplevel_value(&resolved, Some(value)) {
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
        if !self.set_active_buffer_local_toplevel_value(buffer_id, &resolved, Some(value)) {
            self.set_buffer_local_value(buffer_id, &resolved, value);
        }
    }

    pub(super) fn assignment_scope(&self, name: &str) -> Option<SpecialBindingScope> {
        let resolved = self
            .resolve_variable_name(name)
            .unwrap_or_else(|_| name.to_string());
        // data.c:set_internal SYMBOL_LOCALIZED: an existing alist cell (bound
        // or void) receives the store; otherwise a let made for this buffer
        // keeps the default, and only then does local_if_set create a cell.
        if self.has_buffer_local_binding(self.current_buffer_id(), &resolved) {
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

    /// `assignment_scope' for a symbol whose alias chain is resolved: the
    /// buffer-local cell and the auto-local flag are read by id.
    pub(super) fn assignment_scope_symbol(
        &self,
        resolved: &SymbolName,
    ) -> Option<SpecialBindingScope> {
        let buffer_id = self.current_buffer_id();
        // data.c:set_internal dispatches on the redirect tag: only a
        // SYMBOL_LOCALIZED symbol can have a buffer-local binding, so the
        // buffer's binding table is probed for those alone (the read path
        // gates the same way).
        if self.globals.has_flag(resolved, LOCALIZED)
            && self
                .buffer_locals
                .get(&buffer_id)
                .is_some_and(|locals| locals.binding(resolved).is_some())
        {
            return Some(SpecialBindingScope::BufferLocal(buffer_id));
        }
        if let Some(scope) = self.active_special_assignment_scope_symbol(resolved) {
            return Some(scope);
        }
        if self.globals.has_flag(resolved, LOCAL_IF_SET) {
            return Some(SpecialBindingScope::BufferLocal(buffer_id));
        }
        None
    }

    /// `assignment_buffer_id' for a resolved symbol.
    pub(crate) fn assignment_buffer_id_symbol(&self, resolved: &SymbolName) -> Option<u64> {
        match self.assignment_scope_symbol(resolved) {
            Some(SpecialBindingScope::BufferLocal(buffer_id)) => Some(buffer_id),
            _ => None,
        }
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
        self.prepare_variable_assignment_with(name, value, |flag| {
            self.globals.has_flag_by_name(name, flag)
        })
    }

    /// `prepare_variable_assignment' for a symbol in hand: the forwarding
    /// flags are read by id.
    pub(crate) fn prepare_variable_assignment_symbol(
        &self,
        symbol: &SymbolName,
        value: Value,
    ) -> Result<Value, LispError> {
        self.prepare_variable_assignment_with(symbol.as_str(), value, |flag| {
            self.globals.has_flag(symbol, flag)
        })
    }

    fn prepare_variable_assignment_with(
        &self,
        name: &str,
        value: Value,
        has_flag: impl Fn(u8) -> bool,
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
        // data.c:store_symval_forwarding by the slot's kind.  Lisp_Fwd_Int:
        // CHECK_INTEGER, then integer_to_intmax or `overflow-error'.
        if has_flag(FWD_INT) {
            return match value.as_integer() {
                Ok(_) => Ok(value),
                Err(_) if value.is_integer() => Err(LispError::SignalValue(Value::list([
                    Value::Symbol("overflow-error".into()),
                    value,
                ]))),
                Err(_) => Err(wrong_type_argument("integerp", value)),
            };
        }
        // Lisp_Fwd_Bool: `!NILP (newval)', so every store path (setq, set,
        // set-default, let) reads back t or nil.
        if has_flag(FWD_BOOL) {
            return Ok(if value.is_nil() { Value::Nil } else { Value::T });
        }
        match name {
            "display-hourglass" => Ok(if value.is_nil() { Value::Nil } else { Value::T }),
            "scroll-up-aggressively" => match value.kind() {
                Kind::Nil => Ok(Value::Nil),
                Kind::Integer(number) if (0..=1).contains(&number) => Ok(Value::Integer(number)),
                Kind::Float(number) if (0.0..=1.0).contains(&number.get()) => {
                    Ok(Value::Float(number))
                }
                other => Err(wrong_type_argument("numberp", other.value())),
            },
            "vertical-scroll-bar" => match value.kind() {
                Kind::Nil => Ok(Value::Nil),
                Kind::Symbol(ref symbol) if matches!(symbol.as_str(), "left" | "right") => {
                    Ok(value)
                }
                other => Err(wrong_type_argument("symbolp", other.value())),
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
        self.bind_special_symbol(&SymbolName::intern_str(name), value, env)
    }

    /// `case-fold-search' as search.c reads it: the DEFVAR_PER_BUFFER slot
    /// of the current buffer when it has one, else the default, by the
    /// symbol interned once per thread (a name lookup per search hashed
    /// the name and walked the alias, flag and buffer-local tables by it).
    pub(crate) fn case_fold_search_active(&self, env: &Env) -> bool {
        thread_local! {
            static CASE_FOLD_SEARCH: SymbolName = SymbolName::intern_str("case-fold-search");
        }
        CASE_FOLD_SEARCH.with(|symbol| {
            if self.globals.alias(symbol).is_some() {
                return self
                    .lookup_var("case-fold-search", env)
                    .is_some_and(|value| value.is_truthy());
            }
            match self.buffer_local_binding_symbol(self.current_buffer_id(), symbol) {
                Some(Some(local)) => local.is_truthy(),
                // A void local cell reads as void: not a true value.
                Some(None) => false,
                None => match self.globals.value(symbol) {
                    Some(value) => value.is_truthy(),
                    None => self
                        .builtin_var_value("case-fold-search")
                        .is_some_and(|value| value.is_truthy()),
                },
            }
        })
    }

    /// A name whose store is not the symbol's value cell alone: the
    /// constants, keywords, the buffer's own fields, the forwarded eval
    /// cells, the keyboard's per-terminal variables and the frame
    /// parameters with a normalizing store (every name the assignment
    /// paths below match on).
    fn dedicated_store_name(name: &str) -> bool {
        name.starts_with(':')
            || matches!(
                name,
                "nil"
                    | "t"
                    | "most-positive-fixnum"
                    | "most-negative-fixnum"
                    | "buffer-file-name"
                    | "buffer-file-truename"
                    | "mark-active"
                    | "buffer-undo-list"
                    | "features"
                    | "ascii-case-table"
                    | "initial-window-system"
                    | "quit-flag"
                    | "inhibit-quit"
                    | "throw-on-input"
                    | "overriding-plist-environment"
                    | "load-path"
                    | "max-lisp-eval-depth"
                    | "debug-on-next-call"
                    | "symbols-with-pos-enabled"
                    | "display-hourglass"
                    | "scroll-up-aggressively"
                    | "vertical-scroll-bar"
                    | "overwrite-mode"
                    | "overriding-terminal-local-map"
                    | "last-command"
                    | "real-last-command"
                    | "keyboard-translate-table"
                    | "last-repeatable-command"
                    | "prefix-arg"
                    | "last-prefix-arg"
                    | "defining-kbd-macro"
                    | "last-kbd-macro"
                    | "system-key-alist"
                    | "window-system"
                    | "default-minibuffer-frame"
                    | "input-decode-map"
                    | "local-function-key-map"
            )
    }

    /// Learn whether SYMBOL's assignments are a plain store (data.c's
    /// SYMBOL_PLAINVAL, untrapped): no alias, none of the localizing or
    /// forwarding flags, no watcher, not a name with a dedicated store.
    /// Recorded on the cell after a full assignment has run once, read by
    /// the fast paths of `set_internal_symbol', `bind_special_symbol' and
    /// `restore_special_binding'.
    pub(crate) fn learn_plain_store(&mut self, symbol: &SymbolName) {
        let name = symbol.as_str();
        let plain = self.globals.alias(symbol).is_none()
            && !self.globals.has_flag(
                symbol,
                LOCALIZED
                    | LOCAL_IF_SET
                    | PER_BUFFER
                    | ALWAYS_LOCAL
                    | FORWARDED
                    | FWD_BOOL
                    | FWD_INT,
            )
            && self.variable_watcher_index(name).is_none()
            && !self.detached_forwarded_variables.contains_key(name)
            && !Self::dedicated_store_name(name);
        self.globals.set_plain_store(symbol, plain);
    }

    /// data.c:set_internal's SYMBOL_PLAINVAL store for a symbol known to be
    /// plain: the value into the cell, nothing else read or notified.
    /// False when the symbol is not known plain or is void (the full path
    /// creates the binding and learns).
    #[inline]
    pub(crate) fn assign_plain_global(&mut self, symbol: &SymbolName, value: Value) -> bool {
        if !self.globals.plain_store(symbol) {
            return false;
        }
        match self.globals.value_mut(symbol) {
            Some(existing) => {
                *existing = Self::stored_value(value);
                true
            }
            None => false,
        }
    }

    /// eval.c's SPECPDL_INDEX: the depth of the binding stack, for
    /// `unbind_to'.
    #[inline]
    pub(crate) fn specpdl_index(&self) -> usize {
        self.active_special_restores.len()
    }

    /// eval.c's specbind for the symbol in hand: the record is left on
    /// the binding stack and nothing is returned; the caller unwinds to
    /// the index it took with `unbind_to' (a copy of the record was
    /// handed back and searched for on the way out before).
    #[inline]
    pub(crate) fn specbind_symbol(
        &mut self,
        symbol: &SymbolName,
        value: Value,
        env: &mut Env,
    ) -> Result<(), LispError> {
        self.specbind_symbol_record(symbol, value, env).map(|_| ())
    }

    /// eval.c's unbind_to: every record above COUNT popped and restored,
    /// innermost first; the first restore error is reported after all
    /// have run.
    pub(crate) fn unbind_to(&mut self, count: usize, env: &mut Env) -> Result<(), LispError> {
        let mut first_error = None;
        while self.active_special_restores.len() > count {
            let restore = self.active_special_restores.pop().expect("above count");
            if let Err(error) = self.apply_special_restore(restore, env)
                && first_error.is_none()
            {
                first_error = Some(error);
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    /// `specbind_symbol' with a copy of the record handed back, for the
    /// callers outside the evaluator that keep it (the VM's unwind list,
    /// the native runtime, the loaders).
    pub(crate) fn bind_special_symbol(
        &mut self,
        symbol: &SymbolName,
        value: Value,
        env: &mut Env,
    ) -> Result<SpecialBindingRestore, LispError> {
        self.specbind_symbol_record(symbol, value, env)?;
        Ok(self
            .active_special_restores
            .last()
            .cloned()
            .expect("the record just pushed"))
    }

    /// eval.c:specbind for the symbol in hand: the alias chain, the
    /// forwarding flags, the buffer-local cell and the global cell are
    /// read and written by id; the record, holding the symbol, is pushed
    /// on the binding stack and its id returned.
    fn specbind_symbol_record(
        &mut self,
        symbol: &SymbolName,
        value: Value,
        env: &mut Env,
    ) -> Result<u64, LispError> {
        // SPECPDL_LET on a plain symbol: save the cell, store the value.
        if self.globals.plain_store(symbol) {
            let binding_id = self.next_special_binding_id;
            self.next_special_binding_id += 1;
            let previous = self.globals.value(symbol).cloned();
            let value = Self::stored_value(value);
            match self.globals.value_mut(symbol) {
                Some(existing) => *existing = value,
                None => {
                    self.globals.insert(symbol, value);
                }
            }
            let restore = SpecialBindingRestore {
                binding_id,
                name: *symbol,
                scope: SpecialBindingScope::Global,
                binding_buffer_id: None,
                keyboard_terminal_id: None,
                previous,
                previous_undo_state: None,
                let_default: false,
                local_binding_killed: false,
            };
            self.active_special_restores.push(restore);
            return Ok(binding_id);
        }
        let resolved = self.resolve_variable_symbol(symbol)?;
        let unaliased = resolved.id() == symbol.id();
        let value = self.prepare_variable_assignment_symbol(&resolved, value)?;
        let name = resolved.as_str();
        let buffer_id = self.current_buffer_id();
        let binding_id = self.next_special_binding_id;
        self.next_special_binding_id += 1;
        if name == "buffer-undo-list" {
            let previous = crate::lisp::primitives::buffer_undo_list_value(&self.buffer);
            self.notify_variable_watchers(name, value, "let", Some(buffer_id), env)?;
            let previous_undo_state = self.buffer.take_undo_state();
            self.set_symbol_value_cell_resolved(&resolved, value);
            let restore = SpecialBindingRestore {
                binding_id,
                name: resolved,
                scope: SpecialBindingScope::BufferLocal(buffer_id),
                binding_buffer_id: None,
                keyboard_terminal_id: None,
                previous: Some(previous),
                previous_undo_state: Some(previous_undo_state),
                let_default: false,
                local_binding_killed: false,
            };
            self.active_special_restores.push(restore);
            return Ok(binding_id);
        }
        // eval.c:specbind SYMBOL_LOCALIZED: a binding cell in this buffer,
        // bound or void, makes the let SPECPDL_LET_LOCAL.
        let restore = if let Some(local) = self.buffer_local_binding_symbol(buffer_id, &resolved) {
            let previous = Some(local.unwrap_or(Value::Unbound));
            self.notify_variable_watchers_symbol(&resolved, value, "let", Some(buffer_id), env)?;
            self.set_buffer_local_value_symbol(buffer_id, &resolved, value);
            SpecialBindingRestore {
                binding_id,
                name: resolved,
                scope: SpecialBindingScope::BufferLocal(buffer_id),
                binding_buffer_id: None,
                keyboard_terminal_id: None,
                previous,
                previous_undo_state: None,
                let_default: false,
                local_binding_killed: false,
            }
        } else {
            let previous = self.global_binding_value_symbol(&resolved);
            let binding_buffer_id = if self.globals.has_flag(&resolved, LOCAL_IF_SET) {
                Some(buffer_id)
            } else {
                None
            };
            // eval.c:specbind: a localized symbol (or a per-buffer one)
            // without a cell here makes the let SPECPDL_LET_DEFAULT.  The
            // store is set_internal's, whose watchers hear `let' in the
            // current buffer when the symbol is local if set there
            // (data.c:notify_variable_watchers).
            let let_default = self.globals.has_flag(
                &resolved,
                LOCALIZED | LOCAL_IF_SET | ALWAYS_LOCAL | PER_BUFFER,
            );
            let where_heard = self
                .globals
                .has_flag(&resolved, LOCAL_IF_SET | PER_BUFFER)
                .then_some(buffer_id);
            self.notify_variable_watchers_symbol(&resolved, value, "let", where_heard, env)?;
            self.set_global_binding_resolved(&resolved, value);
            SpecialBindingRestore {
                binding_id,
                keyboard_terminal_id: if Self::is_keyboard_variable(&resolved) {
                    self.keyboard_binding_terminal(name)
                } else {
                    None
                },
                name: resolved,
                scope: SpecialBindingScope::Global,
                binding_buffer_id,
                previous,
                previous_undo_state: None,
                let_default,
                local_binding_killed: false,
            }
        };
        self.active_special_restores.push(restore);
        if unaliased {
            self.learn_plain_store(symbol);
        }
        Ok(binding_id)
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
        for thread in &mut self.thread_states {
            if let Some(context) = &mut thread.context {
                for restore in &mut context.active_special_restores {
                    if restore.name == name
                        && restore.scope == SpecialBindingScope::BufferLocal(buffer_id)
                    {
                        restore.local_binding_killed = true;
                    }
                }
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
            if let Some(terminal) = record.keyboard_terminal_id {
                let current = self.keyboard_binding_value_on(terminal, &record.name);
                self.set_keyboard_binding_on(
                    terminal,
                    &record.name,
                    record.previous.take().unwrap_or(Value::Unbound),
                );
                record.previous = current;
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
        // Bindings unwind in order: the record is the last one nearly
        // always (eval.c's specpdl pops), the scan is for the rest.
        let restore = if self
            .active_special_restores
            .last()
            .is_some_and(|active| active.binding_id == restore.binding_id)
        {
            self.active_special_restores.pop().expect("checked last")
        } else if let Some(index) = self
            .active_special_restores
            .iter()
            .rposition(|active| active.binding_id == restore.binding_id)
        {
            self.active_special_restores.remove(index)
        } else {
            restore
        };
        self.apply_special_restore(restore, env)
    }

    /// The unbind of one record taken off the binding stack.
    fn apply_special_restore(
        &mut self,
        restore: SpecialBindingRestore,
        env: &mut Env,
    ) -> Result<(), LispError> {
        // The unbind of a plain symbol's SPECPDL_LET: the saved value back
        // into the cell (a watcher added since cleared the bit).
        if restore.scope == SpecialBindingScope::Global
            && restore.previous_undo_state.is_none()
            && restore.keyboard_terminal_id.is_none()
            && self.globals.plain_store(&restore.name)
        {
            match restore.previous {
                Some(value) => {
                    let value = Self::stored_value(value);
                    match self.globals.value_mut(&restore.name) {
                        Some(existing) => *existing = value,
                        None => {
                            self.globals.insert(&restore.name, value);
                        }
                    }
                }
                None => self.remove_global_binding_symbol(&restore.name),
            }
            return Ok(());
        }
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
                // eval.c:unbind_to: SPECPDL_LET_DEFAULT writes the default
                // back through set_default_internal (watchers hear `set',
                // no buffer); SPECPDL_LET through set_internal (`unlet',
                // the current buffer when the symbol is local if set there).
                let (action, where_heard) = if restore.let_default {
                    ("set", None)
                } else {
                    (
                        "unlet",
                        self.globals
                            .has_flag(&restore.name, LOCAL_IF_SET | PER_BUFFER)
                            .then(|| self.current_buffer_id()),
                    )
                };
                self.notify_variable_watchers_symbol(
                    &restore.name,
                    restore.previous.unwrap_or(Value::Nil),
                    action,
                    where_heard,
                    env,
                )?;
                if let Some(terminal) = restore.keyboard_terminal_id {
                    self.set_keyboard_binding_on(
                        terminal,
                        &restore.name,
                        restore.previous.unwrap_or(Value::Unbound),
                    );
                } else if let Some(value) = restore.previous {
                    self.set_global_binding_resolved(&restore.name, value);
                } else {
                    self.remove_global_binding_symbol(&restore.name);
                }
            }
            SpecialBindingScope::BufferLocal(buffer_id) => {
                self.notify_variable_watchers_symbol(
                    &restore.name,
                    restore.previous.unwrap_or(Value::Nil),
                    "unlet",
                    Some(buffer_id),
                    env,
                )?;
                if let Some(value) = restore.previous {
                    self.set_buffer_local_value_symbol(buffer_id, &restore.name, value);
                } else {
                    self.remove_buffer_local_value(buffer_id, &restore.name);
                }
            }
        }
        Ok(())
    }

    /// An explicitly retained frame owns its arguments. Callers of this
    /// public API may release ARGS before popping the frame.
    #[inline]
    pub fn push_backtrace_frame(&mut self, function: Value, args: &[Value]) {
        self.push_plain_backtrace_frame(function, FrameArgs::Owned(args.to_vec()), true);
    }

    /// eval.c:record_in_backtrace borrows the activation's arguments.
    /// Keep that borrow inside this scope, including Rust panic unwinding.
    #[inline]
    pub(crate) fn with_backtrace_frame<R>(
        &mut self,
        function: Value,
        args: &[Value],
        body: impl FnOnce(&mut Self) -> R,
    ) -> R {
        self.with_backtrace_arguments(function, FrameArgs::borrowed(args), body)
    }

    #[inline]
    fn with_backtrace_arguments<R>(
        &mut self,
        function: Value,
        args: FrameArgs,
        body: impl FnOnce(&mut Self) -> R,
    ) -> R {
        let depth = self.backtrace_frames.len();
        self.push_plain_backtrace_frame(function, args, true);
        // The frame is popped on every exit, a Rust unwind included, by a
        // guard's drop rather than a catch_unwind around the body (the
        // catch cost a call of its own on every Lisp call).
        struct FramePop {
            interpreter: *mut Interpreter,
            depth: usize,
        }
        impl Drop for FramePop {
            fn drop(&mut self) {
                // SAFETY: the guard lives inside this method's borrow of
                // `self' and is dropped before the borrow ends; the body's
                // reborrow has ended by then (normally or by unwinding).
                unsafe { &mut *self.interpreter }.truncate_backtrace_frames(self.depth);
            }
        }
        let guard = FramePop {
            interpreter: std::ptr::from_mut(self),
            depth,
        };
        let result = body(self);
        drop(guard);
        result
    }

    /// The four-word frame record_in_backtrace writes.
    #[inline(always)]
    fn push_plain_backtrace_frame(&mut self, function: Value, args: FrameArgs, evald: bool) {
        Self::write_backtrace_frame(
            &mut self.backtrace_frames,
            FrameFunction::Owned(function),
            args,
            evald,
        );
    }

    /// record_in_backtrace's stores into `specpdl_ptr': the frame's words
    /// written into the vector's next slot (a frame built on the stack
    /// and copied in was a stall per interpreted call).
    #[inline(always)]
    fn write_backtrace_frame(
        frames: &mut Vec<BacktraceFrame>,
        function: FrameFunction,
        args: FrameArgs,
        evald: bool,
    ) {
        frames.reserve(1);
        let len = frames.len();
        // SAFETY: one slot past the length was reserved above; every field
        // of the frame is written before the length covers the slot, and
        // nothing reads the slot in between.
        unsafe {
            let slot = frames.as_mut_ptr().add(len);
            std::ptr::addr_of_mut!((*slot).function).write(function);
            std::ptr::addr_of_mut!((*slot).args).write(args);
            std::ptr::addr_of_mut!((*slot).evald).write(evald);
            std::ptr::addr_of_mut!((*slot).debug_on_exit).write(false);
            std::ptr::addr_of_mut!((*slot).detail).write(None);
            frames.set_len(len + 1);
        }
    }

    /// The same, for native Ffuncall's word vector.
    pub(crate) fn with_native_backtrace_frame<R>(
        &mut self,
        function: Value,
        args: &[usize],
        body: impl FnOnce(&mut Self) -> R,
    ) -> R {
        self.with_backtrace_arguments(
            function,
            FrameArgs::NativeWords {
                ptr: args.as_ptr(),
                len: args.len(),
            },
            body,
        )
    }

    /// A frame that outlives the scope holding its arguments (a byte op
    /// recording itself for handlers that run after the op's step) owns them.
    pub fn push_backtrace_frame_with_evald(
        &mut self,
        function: Value,
        args: Vec<Value>,
        evald: bool,
    ) {
        self.push_plain_backtrace_frame(function, FrameArgs::Owned(args), evald);
    }

    /// record_in_backtrace for a byte-code call whose arguments stay on
    /// the thread's bytecode stack: the frame holds them by address.
    #[inline(always)]
    pub(crate) fn push_backtrace_frame_borrowed(&mut self, function: Value, args: &[Value]) {
        self.push_plain_backtrace_frame(function, FrameArgs::borrowed(args), true);
    }

    /// eval.c's set_backtrace_args on the frame eval_sub recorded before
    /// the arguments were evaluated: the frame now names the function and
    /// holds the evaluated arguments (nargs no longer UNEVALLED).
    pub(super) fn set_backtrace_args(&mut self, function: Value, args: &[Value]) {
        if let Some(frame) = self.backtrace_frames.last_mut() {
            frame.function = FrameFunction::Owned(function);
            frame.args = FrameArgs::borrowed(args);
            frame.evald = true;
        }
    }

    #[inline(always)]
    pub(super) fn push_unevaluated_backtrace_frame(&mut self, source_form: &Value) {
        // eval.c's eval_sub records the form itself (nargs UNEVALLED):
        // the four-word frame holds it, and the debugger's projections
        // read its head and tail when asked.  A boxed detail per
        // interpreted call was a heap allocation and release per call.
        // The word is the caller's form, borrowed for the frame's life
        // as `bt.function' holds it; a non-cons form (evaluated through
        // a path that records it) is owned.
        let function = match source_form.kind() {
            Kind::Cons(cell) => FrameFunction::Form(cell.as_ptr()),
            other => FrameFunction::Owned(other.value()),
        };
        Self::write_backtrace_frame(
            &mut self.backtrace_frames,
            function,
            FrameArgs::borrowed(&[]),
            false,
        );
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
    #[inline]
    fn edebug_entered_active(&self, env: &Env) -> bool {
        // `edebug-entered' can only carry a binding once edebug's defvar
        // has marked it special, so this single flag read, by the id of
        // the symbol interned once per thread, is the whole cost until
        // edebug is actually loaded (it ran on every function call, and
        // hashed the name each time).
        // Symbol ids are process-wide: the id read once, then one flag
        // test per call (a thread-local symbol handle was fetched on
        // every call before).
        static EDEBUG_ENTERED_ID: std::sync::OnceLock<u32> = std::sync::OnceLock::new();
        let id = *EDEBUG_ENTERED_ID.get_or_init(|| SymbolName::intern_str("edebug-entered").id());
        self.globals.has_flag_id(id, SPECIAL)
            && self
                .lookup_var("edebug-entered", env)
                .is_some_and(|value| value.is_truthy())
    }

    #[inline]
    pub fn capture_current_backtrace_context(
        &mut self,
        function_name: Option<&str>,
        env: &Env,
        activation_frame: Option<&EnvFrame>,
    ) {
        if !self.edebug_entered_active(env) && function_name != Some("backtrace-eval") {
            return;
        }
        self.capture_backtrace_context(env, activation_frame);
    }

    #[cold]
    fn capture_backtrace_context(&mut self, env: &Env, activation_frame: Option<&EnvFrame>) {
        let mut context = env.clone();
        if let Some(frame) = activation_frame {
            context.push(frame.clone());
        }
        if let Some(backtrace) = self.backtrace_frames.last_mut() {
            backtrace.detail_mut().lexical_context = Some(context);
        }
    }

    #[inline]
    pub fn pop_backtrace_frame(&mut self) {
        self.backtrace_frames.pop();
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
            // The common frame -- an immediate function object (a byte-code
            // record) with no detail -- owns nothing to drop.
            if let Some(frame) = self.backtrace_frames.pop()
                && frame.function.is_immediate()
                && frame.detail.is_none()
            {
                std::mem::forget(frame);
            }
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
        if matches!(
            error.kind(),
            LispErrorKind::Throw(_, _) | LispErrorKind::Terminate(_)
        ) {
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
            .map(|frame| frame.locals().to_vec())
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
            .map(|frame| frame.locals().to_vec())
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
            .and_then(|frame| frame.lexical_context().cloned())
        {
            return context;
        }
        let mut merged: Vec<(SymbolName, Value)> = Vec::new();
        for frame in frames.into_iter().skip(start + index) {
            for (name, value) in frame.locals() {
                if !merged.iter().any(|(existing, _)| existing == name) {
                    merged.push((*name, *value));
                }
            }
        }
        // Innermost bindings first: Flet conses in order, so the outer
        // entries are consed first and the inner ones end up in front.
        merged.reverse();
        crate::lisp::types::Env::from_vec(vec![EnvFrame::bindings(merged, &Value::Nil)])
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
            bindings
                .iter()
                .map(|(conditions, handler)| ActiveHandler::Bind(conditions.clone(), *handler)),
        );
        start
    }

    pub(crate) fn push_condition_case_handler(&mut self, heads: Vec<Value>) -> usize {
        let start = self.active_handlers.len();
        self.active_handlers.push(ActiveHandler::Case(heads));
        start
    }

    pub(crate) fn push_module_handler(&mut self) -> usize {
        let start = self.active_handlers.len();
        self.active_handlers.push(ActiveHandler::Module);
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
        match head.kind() {
            Kind::T => true,
            Kind::Symbol(symbol) => {
                Self::condition_symbol_matches(&symbol, error_type, condition_list)
            }
            Kind::Cons(_) => head.to_vec().ok().is_some_and(|items| {
                items.iter().any(|item| {
                    matches!(item.kind(), Kind::T)
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
        if matches!(
            error.kind(),
            LispErrorKind::Throw(_, _) | LispErrorKind::Terminate(_)
        ) {
            return true;
        }
        let error_type = error.condition_type();
        let condition_list = self.error_condition_names(&error_type);
        self.active_handlers.iter().any(|handler| match handler {
            ActiveHandler::Module => true,
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
        if matches!(
            error.kind(),
            LispErrorKind::Terminate(_) | LispErrorKind::Throw(_, _)
        ) {
            return Err(error);
        }
        if self.handler_dispatch_depth > 0 {
            return Err(error);
        }
        // signal_or_quit searches the handlers once per signal.  Every frame
        // boundary the error unwinds through dispatches here, so the object
        // the handlers already ran for (the search rewrites the error to
        // that object) passes outward untouched.  A fresh `signal' of the
        // same symbol and data is a new object and runs them again, as in
        // GNU.
        if let LispErrorKind::SignalValue(value) = error.kind()
            && self.dispatched_signal.as_ref().is_some_and(|seen| {
                crate::lisp::primitives::values_eq_in_env(self, seen, value, env)
            })
        {
            return Err(error);
        }
        if !self
            .active_handlers
            .iter()
            .any(|handler| matches!(handler, ActiveHandler::Bind(..)))
        {
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
                ActiveHandler::Module => break,
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
                        *handler,
                        None,
                        std::slice::from_ref(&error_value),
                        env,
                    );
                    match result {
                        Ok(_) => handled = true,
                        Err(next) => {
                            self.handler_dispatch_depth =
                                self.handler_dispatch_depth.saturating_sub(1);
                            if !matches!(
                                next.kind(),
                                LispErrorKind::Throw(_, _) | LispErrorKind::Terminate(_)
                            ) {
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
            self.dispatched_signal = Some(error_value);
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
        let Kind::Marker(beg_marker_id) = self.make_marker().kind() else {
            unreachable!("make_marker returns a marker")
        };
        let Kind::Marker(end_marker_id) = self.make_marker().kind() else {
            unreachable!("make_marker returns a marker")
        };
        self.set_marker(beg_marker_id, Some(start), Some(buffer_id))?;
        self.set_marker(end_marker_id, Some(end), Some(buffer_id))?;
        // editfns.c's Finternal__labeled_narrow_to_region records
        // `point-min-marker' and `point-max-marker': plain markers, so an
        // insertion at the end bound falls outside the restriction.  (Only
        // save_restriction_save's end marker has insertion type t.)
        Ok((beg_marker_id, end_marker_id))
    }
}

/// lisp.h's FOR_EACH_TAIL_INTERNAL cycle detection (Brent's algorithm):
/// the tortoise is moved to the current cell at every power-of-two step
/// count, and a tail that is `eq' to it closes a cycle.
struct Brent {
    tortoise: Value,
    power: usize,
    steps: usize,
}

impl Brent {
    fn new(head: &Value) -> Self {
        Self {
            tortoise: *head,
            power: 2,
            steps: 0,
        }
    }

    /// Note one step to TAIL; true when TAIL is the tortoise's cell.
    fn cycle(&mut self, tail: &Value) -> bool {
        self.steps += 1;
        if let (Kind::Cons(a), Kind::Cons(b)) = (tail.kind(), self.tortoise.kind())
            && crate::lisp::types::SharedCons::ptr_eq(&a, &b)
        {
            return true;
        }
        if self.steps == self.power {
            self.tortoise = *tail;
            self.power <<= 1;
            self.steps = 0;
        }
        false
    }
}
