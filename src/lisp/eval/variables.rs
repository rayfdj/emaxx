use super::*;

impl Interpreter {
    pub fn buffer_local_hook(&self, buffer_id: u64, hook_name: &str) -> Option<Vec<Value>> {
        self.buffer_local_hooks
            .iter()
            .find(|(id, name, _)| *id == buffer_id && name == hook_name)
            .map(|(_, _, hooks)| hooks.clone())
    }

    pub fn set_buffer_local_hook(&mut self, buffer_id: u64, hook_name: &str, hooks: Vec<Value>) {
        if let Some((_, _, existing)) = self
            .buffer_local_hooks
            .iter_mut()
            .find(|(id, name, _)| *id == buffer_id && name == hook_name)
        {
            *existing = hooks;
        } else {
            self.buffer_local_hooks
                .push((buffer_id, hook_name.to_string(), hooks));
        }
    }

    pub fn buffer_local_value(&self, buffer_id: u64, name: &str) -> Option<Value> {
        self.buffer_locals
            .iter()
            .rev()
            .find(|(id, var, _)| *id == buffer_id && var == name)
            .map(|(_, _, value)| value.clone())
    }

    pub fn set_buffer_local_value(&mut self, buffer_id: u64, name: &str, value: Value) {
        let value = Self::stored_value(value);
        for (id, var, existing) in self.buffer_locals.iter_mut().rev() {
            if *id == buffer_id && var == name {
                *existing = value;
                return;
            }
        }
        self.buffer_locals
            .push((buffer_id, name.to_string(), value));
    }

    pub fn remove_buffer_local_value(&mut self, buffer_id: u64, name: &str) {
        if let Some(index) = self
            .buffer_locals
            .iter()
            .rposition(|(id, var, _)| *id == buffer_id && var == name)
        {
            self.buffer_locals.remove(index);
        }
    }

    pub fn clear_buffer_local_state(&mut self, buffer_id: u64) {
        self.buffer_locals.retain(|(id, _, _)| *id != buffer_id);
        self.buffer_local_hooks
            .retain(|(id, _, _)| *id != buffer_id);
        self.buffer_case_tables.retain(|(id, _)| *id != buffer_id);
    }

    pub fn clone_buffer_local_state(&mut self, from_buffer_id: u64, to_buffer_id: u64) {
        let locals = self
            .buffer_locals
            .iter()
            .filter(|(id, _, _)| *id == from_buffer_id)
            .map(|(_, name, value)| (name.clone(), value.clone()))
            .collect::<Vec<_>>();
        for (name, value) in locals {
            self.set_buffer_local_value(to_buffer_id, &name, value);
        }

        let hooks = self
            .buffer_local_hooks
            .iter()
            .filter(|(id, _, _)| *id == from_buffer_id)
            .map(|(_, name, values)| (name.clone(), values.clone()))
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
        let mut vars = Vec::new();
        for (id, name, value) in &self.buffer_locals {
            if *id == buffer_id && !vars.iter().any(|(existing, _)| existing == name) {
                vars.push((name.clone(), value.clone()));
            }
        }
        vars
    }

    pub fn mark_auto_buffer_local(&mut self, name: &str) {
        if !self
            .auto_buffer_locals
            .iter()
            .any(|existing| existing == name)
        {
            self.auto_buffer_locals.push(name.to_string());
        }
    }

    pub fn is_auto_buffer_local(&self, name: &str) -> bool {
        self.auto_buffer_locals
            .iter()
            .any(|existing| existing == name)
    }

    pub fn mark_special_variable(&mut self, name: &str) {
        if !self
            .special_variables
            .iter()
            .any(|existing| existing == name)
        {
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
        }
    }

    pub fn is_special_variable(&self, name: &str) -> bool {
        let resolved = self
            .resolve_variable_name(name)
            .unwrap_or_else(|_| name.to_string());
        self.special_variables
            .iter()
            .any(|existing| existing == &resolved)
    }

    pub fn special_variable_names(&self) -> Vec<String> {
        self.special_variables.clone()
    }

    pub(super) fn symbol_property_index(&self, name: &str) -> Option<usize> {
        self.symbol_properties
            .iter()
            .rposition(|(symbol, _)| symbol == name)
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
        self.symbol_property_index(name).and_then(|index| {
            self.symbol_properties[index]
                .1
                .iter()
                .rposition(|(key, _)| key == property)
                .map(|prop_index| self.symbol_properties[index].1[prop_index].1.clone())
        })
    }

    pub fn put_symbol_property(&mut self, name: &str, property: &str, value: Value) {
        let value = Self::stored_value(value);
        if let Some(index) = self.symbol_property_index(name) {
            if let Some(prop_index) = self.symbol_properties[index]
                .1
                .iter()
                .rposition(|(key, _)| key == property)
            {
                self.symbol_properties[index].1[prop_index].1 = value;
            } else {
                self.symbol_properties[index]
                    .1
                    .push((property.to_string(), value));
            }
            return;
        }
        self.symbol_properties
            .push((name.to_string(), vec![(property.to_string(), value)]));
    }

    pub fn remove_symbol_property(&mut self, name: &str, property: &str) {
        let Some(index) = self.symbol_property_index(name) else {
            return;
        };
        if let Some(prop_index) = self.symbol_properties[index]
            .1
            .iter()
            .rposition(|(key, _)| key == property)
        {
            self.symbol_properties[index].1.remove(prop_index);
        }
        if self.symbol_properties[index].1.is_empty() {
            self.symbol_properties.remove(index);
        }
    }

    pub fn symbol_plist(&self, name: &str) -> Value {
        let Some(index) = self.symbol_property_index(name) else {
            return Value::Nil;
        };
        let mut items = Vec::new();
        for (property, value) in &self.symbol_properties[index].1 {
            items.push(Value::Symbol(property.clone()));
            items.push(value.clone());
        }
        Value::list(items)
    }

    pub fn set_symbol_plist(&mut self, name: &str, plist: Value) -> Result<Value, LispError> {
        let items = plist.to_vec()?;
        let mut props = Vec::new();
        let mut index = 0usize;
        while index + 1 < items.len() {
            props.push((
                items[index].as_symbol()?.to_string(),
                Self::stored_value(items[index + 1].clone()),
            ));
            index += 2;
        }
        if props.is_empty() {
            if let Some(existing) = self.symbol_property_index(name) {
                self.symbol_properties.remove(existing);
            }
        } else if let Some(existing) = self.symbol_property_index(name) {
            self.symbol_properties[existing].1 = props;
        } else {
            self.symbol_properties.push((name.to_string(), props));
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
                    Value::Symbol(name.to_string()),
                    value.clone(),
                    Value::Symbol(action.to_string()),
                    buffer.clone(),
                ],
                env,
            )?;
        }
        Ok(())
    }

    pub(super) fn direct_variable_alias(&self, name: &str) -> Option<String> {
        self.variable_aliases
            .iter()
            .rposition(|(alias, _)| alias == name)
            .map(|index| self.variable_aliases[index].1.clone())
    }

    pub fn resolve_variable_name(&self, name: &str) -> Result<String, LispError> {
        let mut seen = vec![name.to_string()];
        let mut current = name.to_string();
        while let Some(target) = self.direct_variable_alias(&current) {
            if seen.iter().any(|existing| existing == &target) {
                return Err(LispError::SignalValue(Value::list([
                    Value::Symbol("cyclic-variable-indirection".into()),
                    Value::Symbol(name.to_string()),
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
                Value::Symbol(alias.to_string()),
            ])));
        }
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
            true
        } else {
            false
        }
    }

    pub fn indirect_variable_name(&self, name: &str) -> Result<String, LispError> {
        self.resolve_variable_name(name)
    }

    pub(super) fn global_value(&self, name: &str) -> Option<Value> {
        self.globals
            .iter()
            .rposition(|(symbol, _)| symbol == name)
            .map(|index| self.globals[index].1.clone())
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
        self.globals.iter().any(|(symbol, _)| symbol == &resolved)
    }

    pub fn remove_global_binding(&mut self, name: &str) {
        if let Some(index) = self.globals.iter().rposition(|(symbol, _)| symbol == name) {
            self.globals.remove(index);
        }
    }

    pub fn set_global_binding(&mut self, name: &str, value: Value) {
        let name = self
            .resolve_variable_name(name)
            .unwrap_or_else(|_| name.to_string());
        let value = Self::stored_value(value);
        if let Some(index) = self.globals.iter().rposition(|(symbol, _)| symbol == &name) {
            self.globals[index].1 = value;
        } else {
            self.globals.push((name, value));
        }
    }

    pub fn buffer_identity_value(&self, buffer_id: u64) -> Option<Value> {
        self.buffer_list
            .iter()
            .find(|(id, _)| *id == buffer_id)
            .map(|(id, name)| Value::Buffer(*id, name.clone()))
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
            _ => Some(restore.scope.clone()),
        }
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
                Value::Symbol(name.to_string()),
            ])));
        }
        if name.starts_with(':') {
            return if value == Value::Symbol(name.to_string()) {
                Ok(value)
            } else {
                Err(LispError::SignalValue(Value::list([
                    Value::Symbol("setting-constant".into()),
                    Value::Symbol(name.to_string()),
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
            "overwrite-mode" => match value {
                Value::Nil => Ok(Value::Nil),
                Value::Symbol(ref symbol)
                    if matches!(
                        symbol.as_str(),
                        "overwrite-mode-textual" | "overwrite-mode-binary"
                    ) =>
                {
                    Ok(value)
                }
                other => Err(wrong_type_argument("symbolp", other)),
            },
            _ => Ok(value),
        }
    }

    pub(super) fn bind_special_variable(
        &mut self,
        name: &str,
        value: Value,
        env: &mut Env,
    ) -> Result<SpecialBindingRestore, LispError> {
        let name = self.resolve_variable_name(name)?;
        let value = self.prepare_variable_assignment(&name, value)?;
        let buffer_id = self.current_buffer_id();
        let restore = if self.buffer_local_value(buffer_id, &name).is_some() {
            let previous = self.buffer_local_value(buffer_id, &name);
            self.notify_variable_watchers(&name, value.clone(), "let", Some(buffer_id), env)?;
            self.set_buffer_local_value(buffer_id, &name, value);
            SpecialBindingRestore {
                name,
                scope: SpecialBindingScope::BufferLocal(buffer_id),
                binding_buffer_id: None,
                previous,
            }
        } else {
            let previous = self.global_value(&name);
            let binding_buffer_id = if self.is_auto_buffer_local(&name) {
                Some(buffer_id)
            } else {
                None
            };
            self.notify_variable_watchers(&name, value.clone(), "let", None, env)?;
            let value = Self::stored_value(value);
            if let Some(index) = self.globals.iter().rposition(|(symbol, _)| symbol == &name) {
                self.globals[index].1 = value;
            } else {
                self.globals.push((name.clone(), value));
            }
            SpecialBindingRestore {
                name,
                scope: SpecialBindingScope::Global,
                binding_buffer_id,
                previous,
            }
        };
        self.active_special_restores.push(restore.clone());
        Ok(restore)
    }

    pub(super) fn restore_special_binding(
        &mut self,
        restore: SpecialBindingRestore,
        env: &mut Env,
    ) -> Result<(), LispError> {
        let restore = if let Some(index) = self
            .active_special_restores
            .iter()
            .rposition(|active| active.name == restore.name && active.scope == restore.scope)
        {
            self.active_special_restores.remove(index)
        } else {
            restore
        };
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
                    let value = Self::stored_value(value);
                    if let Some(index) = self
                        .globals
                        .iter()
                        .rposition(|(symbol, _)| symbol == &restore.name)
                    {
                        self.globals[index].1 = value;
                    } else {
                        self.globals.push((restore.name.clone(), value));
                    }
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

    pub fn push_backtrace_frame(&mut self, function: Value, args: Vec<Value>) {
        self.backtrace_frames.push(BacktraceFrame {
            function,
            args,
            debug_on_exit: false,
        });
    }

    pub fn pop_backtrace_frame(&mut self) {
        self.backtrace_frames.pop();
    }

    pub fn set_current_backtrace_debug(&mut self, enabled: bool) {
        if let Some(frame) = self.backtrace_frames.last_mut() {
            frame.debug_on_exit = enabled;
        }
    }

    pub fn current_backtrace_frame(&self) -> Option<(Value, Vec<Value>, bool)> {
        self.backtrace_frames.last().map(|frame| {
            (
                frame.function.clone(),
                frame.args.clone(),
                frame.debug_on_exit,
            )
        })
    }

    pub fn backtrace_frames_snapshot(&self) -> Vec<(Value, Vec<Value>, bool)> {
        self.backtrace_frames
            .iter()
            .rev()
            .map(|frame| {
                (
                    frame.function.clone(),
                    frame.args.clone(),
                    frame.debug_on_exit,
                )
            })
            .collect()
    }

    pub fn push_handler_bindings(&mut self, bindings: &[(String, Value)]) -> usize {
        let start = self.active_handlers.len();
        self.active_handlers.extend_from_slice(bindings);
        start
    }

    pub fn pop_handler_bindings(&mut self, start: usize) {
        self.active_handlers.truncate(start);
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
        if self.handler_dispatch_depth > 0 {
            return Err(error);
        }
        let error_value = error_condition_value(&error);
        let error_type = error.condition_type();
        let mut handled = false;
        self.handler_dispatch_depth += 1;
        for (condition, handler) in self.active_handlers.clone().into_iter().rev() {
            if condition != "error" && condition != error_type {
                continue;
            }
            let result =
                self.call_function_value(handler, None, std::slice::from_ref(&error_value), env);
            match result {
                Ok(_) => handled = true,
                Err(next) => {
                    self.handler_dispatch_depth = self.handler_dispatch_depth.saturating_sub(1);
                    if !matches!(next, LispError::Throw(_, _)) && self.condition_case_depth > 1 {
                        self.suspend_condition_case_count = 1;
                    }
                    return Err(next);
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
