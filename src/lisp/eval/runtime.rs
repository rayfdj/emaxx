use super::*;

impl Interpreter {
    pub fn alloc_buffer_id(&mut self) -> u64 {
        let id = self.next_buffer_id;
        self.next_buffer_id += 1;
        id
    }

    /// Check if a buffer name exists in the buffer list.
    pub fn has_buffer(&self, name: &str) -> bool {
        self.buffer_list.iter().any(|(_, n)| n == name)
    }

    /// Check if a buffer ID exists in the live buffer list.
    pub fn has_buffer_id(&self, id: u64) -> bool {
        self.buffer_list
            .iter()
            .any(|(buffer_id, _)| *buffer_id == id)
    }

    /// Find a buffer by name, returning (id, name).
    pub fn find_buffer(&self, name: &str) -> Option<(u64, String)> {
        self.buffer_list.iter().find(|(_, n)| n == name).cloned()
    }

    /// Return the current buffer ID.
    pub fn current_buffer_id(&self) -> u64 {
        self.current_buffer_id
    }

    pub fn current_buffer(&self) -> &crate::buffer::Buffer {
        &self.buffer
    }

    pub fn set_current_load_file(&mut self, path: Option<String>) -> Option<String> {
        std::mem::replace(&mut self.current_load_file, path)
    }

    pub fn current_load_file(&self) -> Option<&str> {
        self.current_load_file.as_deref()
    }

    pub(super) fn stored_value(value: Value) -> Value {
        match value {
            Value::String(_) => {
                let string = primitives::string_like(&value).expect("string_like handles strings");
                primitives::make_shared_string_value_with_multibyte(
                    string.text,
                    string.props,
                    string.multibyte,
                )
            }
            other => other,
        }
    }

    pub(super) fn make_generated_symbol(&mut self, prefix: &str) -> Value {
        let id = self.next_generated_symbol_id;
        self.next_generated_symbol_id += 1;
        Value::Symbol(format!("{prefix}--emaxx-gensym-{id}"))
    }

    pub(crate) fn resolve_load_target(&self, target: &str) -> Option<PathBuf> {
        let direct = PathBuf::from(target);
        if direct.is_file() {
            return Some(direct);
        }

        let with_el = if target.ends_with(".el") || target.ends_with(".elc") {
            None
        } else {
            Some(format!("{target}.el"))
        };
        if let Some(with_el) = &with_el {
            let candidate = PathBuf::from(with_el);
            if candidate.is_file() {
                if load_source_stub_prefers_elc(&candidate)
                    && let Some(with_elc) = if target.ends_with(".el") || target.ends_with(".elc") {
                        None
                    } else {
                        Some(PathBuf::from(format!("{target}.elc")))
                    }
                    && with_elc.is_file()
                {
                    return Some(with_elc);
                }
                return Some(candidate);
            }
        }
        let with_elc = if target.ends_with(".el") || target.ends_with(".elc") {
            None
        } else {
            Some(format!("{target}.elc"))
        };
        if let Some(with_elc) = &with_elc {
            let candidate = PathBuf::from(with_elc);
            if candidate.is_file() {
                return Some(candidate);
            }
        }
        for root in &self.load_path {
            let candidate = root.join(target);
            if candidate.is_file() {
                return Some(candidate);
            }
            if let Some(with_el) = &with_el {
                let candidate = root.join(with_el);
                if candidate.is_file() {
                    if load_source_stub_prefers_elc(&candidate)
                        && let Some(with_elc) = &with_elc
                    {
                        let elc_candidate = root.join(with_elc);
                        if elc_candidate.is_file() {
                            return Some(elc_candidate);
                        }
                    }
                    return Some(candidate);
                }
            }
            if let Some(with_elc) = &with_elc {
                let candidate = root.join(with_elc);
                if candidate.is_file() {
                    return Some(candidate);
                }
            }
        }
        if let Some(alias) = repeated_directory_load_alias(target) {
            let alias_with_el = if alias.ends_with(".el") || alias.ends_with(".elc") {
                None
            } else {
                Some(format!("{alias}.el"))
            };
            let alias_with_elc = if alias.ends_with(".el") || alias.ends_with(".elc") {
                None
            } else {
                Some(format!("{alias}.elc"))
            };
            for root in &self.load_path {
                let candidate = root.join(&alias);
                if candidate.is_file() {
                    return Some(candidate);
                }
                if let Some(alias_with_el) = &alias_with_el {
                    let candidate = root.join(alias_with_el);
                    if candidate.is_file() {
                        if load_source_stub_prefers_elc(&candidate)
                            && let Some(alias_with_elc) = &alias_with_elc
                        {
                            let elc_candidate = root.join(alias_with_elc);
                            if elc_candidate.is_file() {
                                return Some(elc_candidate);
                            }
                        }
                        return Some(candidate);
                    }
                }
                if let Some(alias_with_elc) = &alias_with_elc {
                    let candidate = root.join(alias_with_elc);
                    if candidate.is_file() {
                        return Some(candidate);
                    }
                }
            }
        }
        None
    }

    pub fn load_target(&mut self, target: &str) -> Result<PathBuf, LispError> {
        let Some(path) = self.resolve_load_target(target) else {
            return Err(load_file_missing_error(target));
        };
        crate::lisp::load_file_strict(self, &path)?;
        Ok(path)
    }

    pub(super) fn require_feature_with_target(
        &mut self,
        feature: &str,
        target: Option<&str>,
        env: &Env,
    ) -> Result<Value, LispError> {
        if self.has_feature(feature) || self.loading_features.iter().any(|name| name == feature) {
            return Ok(Value::Symbol(feature.to_string()));
        }
        if is_compat_preloaded_feature(feature) {
            return self.provide_feature_with_after_load(feature);
        }
        if feature == "cus-edit" || target == Some("cus-edit") {
            for dependency in ["custom", "lisp-mode", "pp", "tabify"] {
                if !self.has_feature(dependency) {
                    self.require_feature_with_target(dependency, None, &Env::new())?;
                }
            }
        }
        let load_target = target.unwrap_or(feature);
        let Some(path) =
            crate::lisp::primitives::resolve_load_target_in_env(self, load_target, env)
        else {
            return Err(load_file_missing_error(load_target));
        };
        self.loading_features.push(feature.to_string());
        let load_result = crate::lisp::load_file_strict(self, &path);
        self.loading_features.pop();
        load_result?;
        if (feature == "semantic/symref" || load_target == "semantic/symref")
            && let Some(grep_path) = self.resolve_load_target("semantic/symref/grep")
        {
            crate::lisp::load_file_strict(self, &grep_path)?;
        }
        if !self.has_feature(feature) && target.is_some() {
            return Err(LispError::Signal(format!(
                "Loading file {load_target} failed to provide feature {feature}"
            )));
        }
        if !self.has_feature(feature) {
            return self.provide_feature_with_after_load(feature);
        }
        Ok(Value::Symbol(feature.to_string()))
    }

    /// Resolve a Lisp string-or-buffer value to a live buffer ID.
    pub fn resolve_buffer_id(&self, value: &Value) -> Result<u64, LispError> {
        match value {
            Value::Buffer(id, _) if self.has_buffer_id(*id) => Ok(*id),
            Value::Buffer(_, name) => self
                .find_buffer(name)
                .map(|(id, _)| id)
                .ok_or_else(|| LispError::Signal(format!("No buffer named {}", name))),
            _ => Err(LispError::TypeError(
                "string-or-buffer".into(),
                value.type_name(),
            )),
        }
        .or_else(|error| {
            primitives::string_like(value)
                .and_then(|string| self.find_buffer(&string.text).map(|(id, _)| id))
                .ok_or(error)
        })
    }

    /// Create and register a new empty buffer.
    pub fn create_buffer(&mut self, name: &str) -> (u64, String) {
        let id = self.alloc_buffer_id();
        self.inactive_buffers
            .push((id, crate::buffer::Buffer::new(name)));
        self.buffer_list.push((id, name.to_string()));
        (id, name.to_string())
    }

    /// Switch the current buffer to a different live buffer ID.
    pub fn switch_to_buffer_id(&mut self, id: u64) -> Result<(), LispError> {
        self.switch_to_buffer_id_with_window_history(id, true)
    }

    pub fn switch_to_buffer_id_preserving_window_history(
        &mut self,
        id: u64,
    ) -> Result<(), LispError> {
        self.switch_to_buffer_id_with_window_history(id, false)
    }

    pub(super) fn switch_to_buffer_id_with_window_history(
        &mut self,
        id: u64,
        record_previous_window_buffer: bool,
    ) -> Result<(), LispError> {
        if id == self.current_buffer_id {
            return Ok(());
        }
        let pos = self
            .inactive_buffers
            .iter()
            .position(|(buffer_id, _)| *buffer_id == id)
            .ok_or_else(|| LispError::Signal(format!("No buffer with id {}", id)))?;
        let (_, next_buffer) = self.inactive_buffers.swap_remove(pos);
        let current_id = self.current_buffer_id;
        let current_buffer = std::mem::replace(&mut self.buffer, next_buffer);
        self.inactive_buffers.push((current_id, current_buffer));
        self.current_buffer_id = id;
        if let Some(index) = self
            .buffer_list
            .iter()
            .position(|(buffer_id, _)| *buffer_id == id)
        {
            let entry = self.buffer_list.remove(index);
            self.buffer_list.insert(0, entry);
        }
        let point_min = self.buffer.point_min() as i64;
        if let Some(window) = self.find_record_mut(self.selected_window_id) {
            let previous = window
                .slots
                .first()
                .and_then(|value| value.as_integer().ok())
                .unwrap_or(current_id as i64);
            window.slots[0] = Value::Integer(id as i64);
            if window.slots.len() < 2 {
                window.slots.push(Value::Integer(point_min));
            } else {
                window.slots[1] = Value::Integer(point_min);
            }
            if record_previous_window_buffer && previous != id as i64 {
                if window.slots.len() < 3 {
                    window.slots.resize(3, Value::Nil);
                }
                window.slots[2] = Value::Integer(previous);
            }
        }
        Ok(())
    }

    pub fn selected_window_value(&self) -> Value {
        Value::Record(self.selected_window_id)
    }

    pub fn selected_window_id(&self) -> u64 {
        self.selected_window_id
    }

    pub(crate) fn set_selected_window_id(&mut self, id: u64) {
        self.selected_window_id = id;
    }

    pub fn selected_window_buffer_id(&self) -> u64 {
        self.find_record(self.selected_window_id)
            .and_then(|record| record.slots.first())
            .and_then(|value| value.as_integer().ok())
            .map(|value| value.max(0) as u64)
            .unwrap_or(self.current_buffer_id)
    }

    pub fn selected_window_previous_buffer_id(&self) -> Option<u64> {
        self.find_record(self.selected_window_id)
            .and_then(|record| record.slots.get(2))
            .and_then(|value| value.as_integer().ok())
            .map(|value| value.max(0) as u64)
            .filter(|id| self.has_buffer_id(*id))
    }

    pub fn buffer_bounds_by_id(&self, id: u64) -> Option<(usize, usize)> {
        self.get_buffer_by_id(id)
            .map(|buffer| (buffer.point_min(), buffer.point_max()))
    }

    pub fn selected_window_start(&self) -> usize {
        let (point_min, point_max) = self
            .buffer_bounds_by_id(self.selected_window_buffer_id())
            .unwrap_or((self.buffer.point_min(), self.buffer.point_max()));
        self.find_record(self.selected_window_id)
            .and_then(|record| record.slots.get(1))
            .and_then(|value| value.as_integer().ok())
            .map(|value| value.clamp(point_min as i64, point_max as i64) as usize)
            .unwrap_or(point_min)
    }

    pub fn set_selected_window_start(&mut self, start: usize) {
        let (point_min, point_max) = self
            .buffer_bounds_by_id(self.selected_window_buffer_id())
            .unwrap_or((self.buffer.point_min(), self.buffer.point_max()));
        let start = start.clamp(point_min, point_max) as i64;
        if let Some(window) = self.find_record_mut(self.selected_window_id) {
            if window.slots.len() < 2 {
                window.slots.resize(2, Value::Nil);
            }
            window.slots[1] = Value::Integer(start);
        }
    }

    pub fn set_selected_window_buffer_id(&mut self, buffer_id: u64) {
        let (point_min, _) = self
            .buffer_bounds_by_id(buffer_id)
            .unwrap_or((self.buffer.point_min(), self.buffer.point_max()));
        let current_buffer_id = self.current_buffer_id as i64;
        if let Some(window) = self.find_record_mut(self.selected_window_id) {
            let previous = window
                .slots
                .first()
                .and_then(|value| value.as_integer().ok())
                .unwrap_or(current_buffer_id);
            if window.slots.len() < 2 {
                window.slots.resize(2, Value::Nil);
            }
            window.slots[0] = Value::Integer(buffer_id as i64);
            window.slots[1] = Value::Integer(point_min as i64);
            if previous != buffer_id as i64 {
                if window.slots.len() < 3 {
                    window.slots.resize(3, Value::Nil);
                }
                window.slots[2] = Value::Integer(previous);
            }
        }
    }

    pub fn frame_width(&self) -> i64 {
        self.frame_width.max(1)
    }

    pub fn set_frame_width(&mut self, width: i64) {
        self.frame_width = width.max(1);
    }

    pub fn frame_height(&self) -> i64 {
        self.frame_height.max(1)
    }

    pub fn set_frame_height(&mut self, height: i64) {
        self.frame_height = height.max(1);
    }

    pub(super) fn snapshot_window_configuration(&self) -> WindowConfigurationSnapshot {
        WindowConfigurationSnapshot {
            current_buffer_id: self.current_buffer_id(),
            selected_window_id: self.selected_window_id,
            selected_window_slots: self
                .find_record(self.selected_window_id)
                .map(|record| record.slots.clone())
                .unwrap_or_default(),
            frame_width: self.frame_width,
            frame_height: self.frame_height,
        }
    }

    pub(crate) fn window_configuration_value(&mut self) -> Value {
        let snapshot = self.snapshot_window_configuration();
        self.create_record(
            "window-configuration",
            vec![
                Value::Integer(snapshot.current_buffer_id as i64),
                Value::Integer(snapshot.selected_window_id as i64),
                Value::list(snapshot.selected_window_slots),
                Value::Integer(snapshot.frame_width),
                Value::Integer(snapshot.frame_height),
            ],
        )
    }

    pub(crate) fn apply_window_configuration_value(
        &mut self,
        value: &Value,
    ) -> Result<bool, LispError> {
        let Value::Record(id) = value else {
            return Ok(false);
        };
        let Some(record) = self.find_record(*id) else {
            return Ok(false);
        };
        if record.type_name != "window-configuration" {
            return Ok(false);
        }
        let slots = record.slots.clone();
        let snapshot = WindowConfigurationSnapshot {
            current_buffer_id: slots
                .first()
                .and_then(|value| value.as_integer().ok())
                .unwrap_or_default() as u64,
            selected_window_id: slots
                .get(1)
                .and_then(|value| value.as_integer().ok())
                .unwrap_or_default() as u64,
            selected_window_slots: slots
                .get(2)
                .and_then(|value| value.to_vec().ok())
                .unwrap_or_default(),
            frame_width: slots
                .get(3)
                .and_then(|value| value.as_integer().ok())
                .unwrap_or(80),
            frame_height: slots
                .get(4)
                .and_then(|value| value.as_integer().ok())
                .unwrap_or(24),
        };
        self.restore_window_configuration(snapshot)?;
        Ok(true)
    }

    pub(crate) fn is_window_configuration_value(&self, value: &Value) -> bool {
        matches!(
            value,
            Value::Record(id)
                if self
                    .find_record(*id)
                    .is_some_and(|record| record.type_name == "window-configuration")
        )
    }

    pub(super) fn restore_window_configuration(
        &mut self,
        snapshot: WindowConfigurationSnapshot,
    ) -> Result<(), LispError> {
        if self.has_buffer_id(snapshot.current_buffer_id) {
            self.switch_to_buffer_id(snapshot.current_buffer_id)?;
        }
        self.selected_window_id = snapshot.selected_window_id;
        self.frame_width = snapshot.frame_width.max(1);
        self.frame_height = snapshot.frame_height.max(1);
        if let Some(window) = self.find_record_mut(snapshot.selected_window_id) {
            window.slots = snapshot.selected_window_slots;
        }
        Ok(())
    }

    pub(super) fn find_class_state(&self, name: &str) -> Option<&ClassState> {
        self.class_states.iter().find(|state| state.name == name)
    }

    pub(super) fn find_class_state_mut(&mut self, name: &str) -> Option<&mut ClassState> {
        self.class_states
            .iter_mut()
            .find(|state| state.name == name)
    }

    pub(super) fn find_class_state_by_record_id(&self, record_id: u64) -> Option<&ClassState> {
        self.class_states
            .iter()
            .find(|state| state.record_id == record_id)
    }

    pub(crate) fn class_name_from_value(&self, value: &Value) -> Option<String> {
        match value {
            Value::T => Some("t".into()),
            Value::Symbol(symbol) => Some(symbol.clone()),
            Value::Record(record_id) => self
                .find_class_state_by_record_id(*record_id)
                .map(|state| state.name.clone()),
            _ => None,
        }
    }

    pub(crate) fn class_value(&self, name: &str) -> Option<Value> {
        self.find_class_state(name)
            .map(|state| Value::Record(state.record_id))
    }

    pub(crate) fn class_parents_value(&self, class: &Value) -> Result<Value, LispError> {
        if let Some(name) = self.class_name_from_value(class)
            && primitives::is_builtin_class_name(&name)
        {
            return Ok(Value::list(
                primitives::builtin_class_parents(&name)
                    .iter()
                    .map(|parent| crate::lisp::types::interned_symbol_value((*parent).into())),
            ));
        }
        let Value::Record(record_id) = class else {
            return Err(LispError::TypeError("class".into(), class.type_name()));
        };
        if let Some(state) = self.find_class_state_by_record_id(*record_id) {
            return Ok(Value::list(state.parents.iter().map(|parent| {
                self.class_value(parent)
                    .unwrap_or_else(|| Value::Symbol(parent.clone()))
            })));
        }
        if let Some((_, parents)) = self
            .class_parent_overrides
            .iter()
            .find(|(stored_id, _)| stored_id == record_id)
        {
            return Ok(Value::list(parents.iter().map(|parent| {
                self.class_value(parent)
                    .unwrap_or_else(|| Value::Symbol(parent.clone()))
            })));
        }
        Ok(Value::Nil)
    }

    pub(crate) fn set_class_record(&mut self, name: &str, class: Value) -> Result<(), LispError> {
        let Value::Record(record_id) = class.clone() else {
            return Err(LispError::TypeError("class".into(), class.type_name()));
        };
        let parents = self
            .class_parent_overrides
            .iter()
            .find(|(stored_id, _)| *stored_id == record_id)
            .map(|(_, parents)| parents.clone())
            .unwrap_or_default();
        if let Some(existing) = self.find_class_state_mut(name) {
            existing.record_id = record_id;
            existing.parents = parents.clone();
        } else {
            self.class_states.push(ClassState {
                name: name.to_string(),
                record_id,
                parents: parents.clone(),
                slot_specs: Vec::new(),
                options: Vec::new(),
                children: Vec::new(),
            });
        }
        self.put_symbol_property(name, "cl--class", Value::Record(record_id));
        self.put_symbol_property(
            name,
            "emaxx-class-parents",
            Value::list(parents.into_iter().map(Value::Symbol)),
        );
        Ok(())
    }

    pub(crate) fn set_class_parents_value(
        &mut self,
        class: &Value,
        parents: Value,
    ) -> Result<(), LispError> {
        let Value::Record(record_id) = class else {
            return Err(LispError::TypeError("class".into(), class.type_name()));
        };
        let parent_names = parents
            .to_vec()
            .unwrap_or_default()
            .into_iter()
            .filter_map(|parent| self.class_name_from_value(&parent))
            .collect::<Vec<_>>();
        match self
            .class_parent_overrides
            .iter_mut()
            .find(|(stored_id, _)| stored_id == record_id)
        {
            Some((_, stored_parents)) => *stored_parents = parent_names.clone(),
            None => self
                .class_parent_overrides
                .push((*record_id, parent_names.clone())),
        }
        let Some(class_name) = self.class_name_from_value(class) else {
            return Ok(());
        };
        self.register_class(&class_name, parent_names, Vec::new(), Vec::new());
        Ok(())
    }

    pub(super) fn register_class(
        &mut self,
        name: &str,
        parents: Vec<String>,
        slot_specs: Vec<Value>,
        options: Vec<Value>,
    ) -> Value {
        let record_value = if let Some(existing) = self.find_class_state(name) {
            Value::Record(existing.record_id)
        } else {
            self.create_record(
                "eieio--class",
                vec![
                    Value::Symbol(name.to_string()),
                    Value::list(parents.iter().cloned().map(Value::Symbol)),
                    Value::list(slot_specs.iter().cloned()),
                    Value::list(options.iter().cloned()),
                ],
            )
        };
        let Value::Record(record_id) = record_value.clone() else {
            unreachable!("class registration uses class records");
        };

        let old_parents = self
            .find_class_state(name)
            .map(|state| state.parents.clone())
            .unwrap_or_default();
        for parent in &old_parents {
            if let Some(parent_state) = self.find_class_state_mut(parent) {
                parent_state.children.retain(|child| child != name);
            }
        }

        if let Some(record) = self.find_record_mut(record_id) {
            record.slots = vec![
                Value::Symbol(name.to_string()),
                Value::list(parents.iter().cloned().map(Value::Symbol)),
                Value::list(slot_specs.iter().cloned()),
                Value::list(options.iter().cloned()),
            ];
        }

        if let Some(existing) = self.find_class_state_mut(name) {
            existing.parents = parents.clone();
            existing.slot_specs = slot_specs.clone();
            existing.options = options.clone();
        } else {
            self.class_states.push(ClassState {
                name: name.to_string(),
                record_id,
                parents: parents.clone(),
                slot_specs: slot_specs.clone(),
                options: options.clone(),
                children: Vec::new(),
            });
        }

        for parent in &parents {
            if let Some(parent_state) = self.find_class_state_mut(parent)
                && !parent_state.children.iter().any(|child| child == name)
            {
                parent_state.children.push(name.to_string());
            }
        }

        self.put_symbol_property(name, "cl--class", record_value.clone());
        self.put_symbol_property(
            name,
            "emaxx-class-parents",
            Value::list(parents.into_iter().map(Value::Symbol)),
        );
        self.put_symbol_property(name, "emaxx-class-slots", Value::list(slot_specs));
        self.put_symbol_property(name, "emaxx-class-options", Value::list(options));
        record_value
    }

    pub(crate) fn class_allparents(&self, name: &str) -> Vec<Value> {
        if let Some(parents) = primitives::builtin_class_allparents(name) {
            return parents
                .iter()
                .map(|parent| crate::lisp::types::interned_symbol_value((*parent).into()))
                .collect();
        }

        fn visit(
            interp: &Interpreter,
            name: &str,
            output: &mut Vec<Value>,
            seen: &mut std::collections::HashSet<String>,
        ) {
            if !seen.insert(name.to_string()) {
                return;
            }
            output.push(crate::lisp::types::interned_symbol_value(name.to_string()));
            let builtin_parents = primitives::builtin_class_parents(name);
            if !builtin_parents.is_empty() {
                for parent in builtin_parents {
                    visit(interp, parent, output, seen);
                }
            } else if let Some(state) = interp.find_class_state(name) {
                if state.parents.is_empty() {
                    if name != "t" {
                        visit(interp, "t", output, seen);
                    }
                } else {
                    for parent in &state.parents {
                        visit(interp, parent, output, seen);
                    }
                }
            } else if name != "t" {
                visit(interp, "t", output, seen);
            }
        }

        let mut output = Vec::new();
        let mut seen = std::collections::HashSet::new();
        visit(self, name, &mut output, &mut seen);
        output
    }

    pub(crate) fn value_is_instance_of_class(&self, value: &Value, class_name: &str) -> bool {
        let Value::Record(record_id) = value else {
            return false;
        };
        let Some(record) = self.find_record(*record_id) else {
            return false;
        };
        self.class_allparents(&record.type_name)
            .iter()
            .any(|parent| matches!(parent, Value::Symbol(name) if name == class_name))
    }

    pub(crate) fn class_children(&self, name: &str) -> Vec<Value> {
        self.find_class_state(name)
            .map(|state| {
                state
                    .children
                    .iter()
                    .cloned()
                    .map(Value::Symbol)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default()
    }

    pub(super) fn register_generic_generalizer(
        &mut self,
        name: &str,
        priority: i64,
        tagcode_function: Value,
        specializers_function: Value,
    ) -> Value {
        let record_value = if let Some(existing) = self
            .generalizer_states
            .iter()
            .find(|state| state.name == name)
        {
            Value::Record(existing.record_id)
        } else {
            self.create_record(
                "cl--generic-generalizer",
                vec![
                    Value::Symbol(name.to_string()),
                    Value::Integer(priority),
                    tagcode_function.clone(),
                    specializers_function.clone(),
                ],
            )
        };
        let Value::Record(record_id) = record_value.clone() else {
            unreachable!("generalizer registration uses generalizer records");
        };

        if let Some(record) = self.find_record_mut(record_id) {
            record.slots = vec![
                Value::Symbol(name.to_string()),
                Value::Integer(priority),
                tagcode_function.clone(),
                specializers_function.clone(),
            ];
        }

        if let Some(existing) = self
            .generalizer_states
            .iter_mut()
            .find(|state| state.name == name)
        {
            existing.priority = priority;
            existing.tagcode_function = tagcode_function.clone();
            existing.specializers_function = specializers_function.clone();
        } else {
            self.generalizer_states.push(GenericGeneralizerState {
                name: name.to_string(),
                record_id,
                priority,
                tagcode_function: tagcode_function.clone(),
                specializers_function: specializers_function.clone(),
            });
        }

        self.set_global_binding(name, record_value.clone());
        self.put_symbol_property(name, "emaxx-generic-generalizer", record_value.clone());
        record_value
    }

    pub fn terminal_parameter(&self, name: &str) -> Option<Value> {
        self.terminal_parameters
            .iter()
            .rfind(|(parameter, _)| parameter == name)
            .map(|(_, value)| value.clone())
    }

    pub fn set_terminal_parameter(&mut self, name: &str, value: Value) {
        let value = Self::stored_value(value);
        if let Some(index) = self
            .terminal_parameters
            .iter()
            .rposition(|(parameter, _)| parameter == name)
        {
            self.terminal_parameters[index].1 = value;
        } else {
            self.terminal_parameters.push((name.to_string(), value));
        }
    }

    /// Remove a non-current buffer from the live buffer list.
    pub fn remove_buffer_id(&mut self, id: u64) -> Option<crate::buffer::Buffer> {
        if id == self.current_buffer_id {
            return None;
        }
        self.buffer_list.retain(|(buffer_id, _)| *buffer_id != id);
        self.inactive_buffers
            .iter()
            .position(|(buffer_id, _)| *buffer_id == id)
            .map(|pos| self.inactive_buffers.swap_remove(pos).1)
    }

    /// Kill a buffer by ID, switching away if it is current.
    pub fn kill_buffer_id(&mut self, id: u64) {
        let selected_window_showed_buffer = self.selected_window_buffer_id() == id;
        self.detach_markers_for_buffer(id);
        self.buffer_locals
            .retain(|(buffer_id, _, _)| *buffer_id != id);
        self.indirect_buffers
            .retain(|(buffer_id, base_id)| *buffer_id != id && *base_id != id);
        if id == self.current_buffer_id {
            self.buffer_list.retain(|(buffer_id, _)| *buffer_id != id);
            if let Some((next_id, next_buffer)) = self.inactive_buffers.pop() {
                self.buffer = next_buffer;
                self.current_buffer_id = next_id;
            } else {
                let scratch_id = self.alloc_buffer_id();
                self.buffer = crate::buffer::Buffer::new("*scratch*");
                self.current_buffer_id = scratch_id;
                self.buffer_list.push((scratch_id, "*scratch*".to_string()));
            }
        } else {
            let _ = self.remove_buffer_id(id);
        }
        if selected_window_showed_buffer {
            self.set_selected_window_buffer_id(self.current_buffer_id);
        }
    }

    /// Allocate a new unique overlay ID.
    pub fn alloc_overlay_id(&mut self) -> u64 {
        let id = self.next_overlay_id;
        self.next_overlay_id += 1;
        id
    }

    pub fn alloc_record_id(&mut self) -> u64 {
        let id = self.next_record_id;
        self.next_record_id += 1;
        id
    }

    pub fn alloc_finalizer_id(&mut self) -> u64 {
        let id = self.next_finalizer_id;
        self.next_finalizer_id += 1;
        id
    }

    /// Allocate a new marker.
    pub fn make_marker(&mut self) -> Value {
        let id = self.next_marker_id;
        self.next_marker_id += 1;
        self.markers.push(MarkerState {
            id,
            buffer_id: None,
            position: None,
            last_position: None,
            insertion_type: false,
        });
        Value::Marker(id)
    }

    pub fn find_marker(&self, id: u64) -> Option<&MarkerState> {
        self.markers.iter().find(|marker| marker.id == id)
    }

    pub fn find_marker_mut(&mut self, id: u64) -> Option<&mut MarkerState> {
        self.markers.iter_mut().find(|marker| marker.id == id)
    }

    pub fn marker_position(&self, id: u64) -> Option<usize> {
        self.find_marker(id).and_then(|marker| marker.position)
    }

    pub fn marker_buffer_id(&self, id: u64) -> Option<u64> {
        self.find_marker(id).and_then(|marker| marker.buffer_id)
    }

    pub fn marker_last_position(&self, id: u64) -> Option<usize> {
        self.find_marker(id).and_then(|marker| marker.last_position)
    }

    pub fn marker_insertion_type(&self, id: u64) -> Option<bool> {
        self.find_marker(id).map(|marker| marker.insertion_type)
    }

    pub fn set_marker_insertion_type(&mut self, id: u64, insertion_type: bool) {
        if let Some(marker) = self.find_marker_mut(id) {
            marker.insertion_type = insertion_type;
        }
    }

    pub fn set_marker(
        &mut self,
        id: u64,
        position: Option<usize>,
        buffer_id: Option<u64>,
    ) -> Result<(), LispError> {
        let marker = self
            .find_marker_mut(id)
            .ok_or_else(|| LispError::TypeError("marker".into(), format!("marker<{}>", id)))?;
        marker.buffer_id = buffer_id;
        marker.position = position;
        if let Some(pos) = position {
            marker.last_position = Some(pos);
        }
        Ok(())
    }

    pub fn copy_marker_value(
        &mut self,
        value: &Value,
        insertion_type: bool,
    ) -> Result<Value, LispError> {
        let marker_value = self.make_marker();
        let Value::Marker(marker_id) = marker_value else {
            unreachable!("make_marker always returns a marker")
        };
        match value {
            Value::Nil => {
                self.set_marker(marker_id, None, None)?;
            }
            Value::Marker(source_id) => {
                let source = self.find_marker(*source_id).cloned().ok_or_else(|| {
                    LispError::TypeError("marker".into(), format!("marker<{}>", source_id))
                })?;
                self.set_marker(marker_id, source.position, source.buffer_id)?;
            }
            Value::Integer(position) => {
                self.set_marker(
                    marker_id,
                    Some(*position as usize),
                    Some(self.current_buffer_id()),
                )?;
            }
            _ => {
                return Err(LispError::TypeError(
                    "integer-or-marker-p".into(),
                    value.type_name(),
                ));
            }
        }
        self.set_marker_insertion_type(marker_id, insertion_type);
        Ok(marker_value)
    }

    pub fn make_char_table(&mut self, subtype: Option<String>, default: Value) -> Value {
        let id = self.next_char_table_id;
        self.next_char_table_id += 1;
        self.char_tables.push(CharTableState {
            id,
            subtype,
            default,
            parent: None,
            extra_slots: Vec::new(),
            entries: Vec::new(),
            category_docs: Vec::new(),
        });
        Value::CharTable(id)
    }

    pub fn find_char_table(&self, id: u64) -> Option<&CharTableState> {
        self.char_tables.iter().find(|table| table.id == id)
    }

    pub fn find_char_table_mut(&mut self, id: u64) -> Option<&mut CharTableState> {
        self.char_tables.iter_mut().find(|table| table.id == id)
    }

    pub fn char_table_set(&mut self, id: u64, key: u32, value: Value) -> Result<(), LispError> {
        self.char_table_set_range(id, key, key, value)
    }

    pub fn char_table_set_range(
        &mut self,
        id: u64,
        start: u32,
        end: u32,
        value: Value,
    ) -> Result<(), LispError> {
        let table = self.find_char_table_mut(id).ok_or_else(|| {
            LispError::TypeError("char-table".into(), format!("char-table<{id}>"))
        })?;
        table.entries.push(CharTableEntry {
            start: start.min(end),
            end: start.max(end),
            value,
        });
        Ok(())
    }

    pub fn char_table_set_default(&mut self, id: u64, value: Value) -> Result<(), LispError> {
        let table = self.find_char_table_mut(id).ok_or_else(|| {
            LispError::TypeError("char-table".into(), format!("char-table<{id}>"))
        })?;
        table.default = value;
        Ok(())
    }

    pub fn char_table_get(&self, id: u64, key: u32) -> Option<Value> {
        let table = self.find_char_table(id)?;
        if let Some(entry) = table
            .entries
            .iter()
            .rev()
            .find(|entry| entry.start <= key && key <= entry.end)
        {
            return Some(entry.value.clone());
        }
        if let Some(parent_id) = table.parent
            && let Some(value) = self.char_table_get(parent_id, key)
        {
            return Some(value);
        }
        if table.default.is_nil()
            && let Some(value) = primitives::case_table_default_value(table.subtype.as_deref(), key)
        {
            return Some(value);
        }
        Some(table.default.clone())
    }

    pub fn char_table_range(&self, id: u64, start: u32, end: u32) -> Option<Value> {
        let table = self.find_char_table(id)?;
        if let Some(entry) = table
            .entries
            .iter()
            .rev()
            .find(|entry| entry.start == start.min(end) && entry.end == start.max(end))
        {
            return Some(entry.value.clone());
        }
        if let Some(parent_id) = table.parent
            && let Some(value) = self.char_table_range(parent_id, start, end)
        {
            return Some(value);
        }
        Some(table.default.clone())
    }

    pub fn char_table_subtype(&self, id: u64) -> Option<Option<String>> {
        self.find_char_table(id).map(|table| table.subtype.clone())
    }

    pub fn char_table_parent(&self, id: u64) -> Option<Option<u64>> {
        self.find_char_table(id).map(|table| table.parent)
    }

    pub fn char_table_explicit_get(&self, id: u64, key: u32) -> Option<Value> {
        let table = self.find_char_table(id)?;
        if let Some(entry) = table
            .entries
            .iter()
            .rev()
            .find(|entry| entry.start <= key && key <= entry.end)
        {
            return Some(entry.value.clone());
        }
        if let Some(parent_id) = table.parent {
            return self.char_table_explicit_get(parent_id, key);
        }
        None
    }

    pub fn set_char_table_parent(&mut self, id: u64, parent: Option<u64>) -> Result<(), LispError> {
        let table = self.find_char_table_mut(id).ok_or_else(|| {
            LispError::TypeError("char-table".into(), format!("char-table<{id}>"))
        })?;
        table.parent = parent;
        Ok(())
    }

    pub fn char_table_extra_slot(&self, id: u64, slot: usize) -> Option<Value> {
        self.find_char_table(id)
            .and_then(|table| table.extra_slots.get(slot).cloned())
    }

    pub fn set_char_table_extra_slot(
        &mut self,
        id: u64,
        slot: usize,
        value: Value,
    ) -> Result<(), LispError> {
        let table = self.find_char_table_mut(id).ok_or_else(|| {
            LispError::TypeError("char-table".into(), format!("char-table<{id}>"))
        })?;
        while table.extra_slots.len() <= slot {
            table.extra_slots.push(Value::Nil);
        }
        table.extra_slots[slot] = value;
        Ok(())
    }

    pub fn char_table_purpose(&self, id: u64) -> Option<&str> {
        self.find_char_table(id)
            .and_then(|table| table.subtype.as_deref())
    }

    pub fn clone_char_table(&mut self, id: u64) -> Result<Value, LispError> {
        let source = self.find_char_table(id).cloned().ok_or_else(|| {
            LispError::TypeError("char-table".into(), format!("char-table<{id}>"))
        })?;
        let new_id = self.next_char_table_id;
        self.next_char_table_id += 1;
        self.char_tables.push(CharTableState {
            id: new_id,
            ..source
        });
        Ok(Value::CharTable(new_id))
    }

    pub fn create_record(&mut self, type_name: &str, slots: Vec<Value>) -> Value {
        let id = self.alloc_record_id();
        self.records.push(RecordState {
            id,
            type_name: type_name.to_string(),
            slots,
        });
        Value::Record(id)
    }

    pub fn find_record(&self, id: u64) -> Option<&RecordState> {
        self.records.iter().find(|record| record.id == id)
    }

    pub fn find_record_mut(&mut self, id: u64) -> Option<&mut RecordState> {
        self.records.iter_mut().find(|record| record.id == id)
    }

    pub(crate) fn record_ids_by_type(&self, type_name: &str) -> Vec<u64> {
        self.records
            .iter()
            .filter(|record| record.type_name == type_name)
            .map(|record| record.id)
            .collect()
    }

    pub fn register_sqlite_handle(&mut self, id: u64, state: SqliteHandleState) {
        if let Some((_, existing)) = self
            .sqlite_handles
            .iter_mut()
            .find(|(record_id, _)| *record_id == id)
        {
            *existing = state;
        } else {
            self.sqlite_handles.push((id, state));
        }
    }

    pub fn find_sqlite_handle(&self, id: u64) -> Option<&SqliteHandleState> {
        self.sqlite_handles
            .iter()
            .find(|(record_id, _)| *record_id == id)
            .map(|(_, state)| state)
    }

    pub fn find_sqlite_handle_mut(&mut self, id: u64) -> Option<&mut SqliteHandleState> {
        self.sqlite_handles
            .iter_mut()
            .find(|(record_id, _)| *record_id == id)
            .map(|(_, state)| state)
    }

    pub fn copy_record(&mut self, id: u64) -> Result<Value, LispError> {
        let record = self
            .find_record(id)
            .cloned()
            .ok_or_else(|| LispError::TypeError("record".into(), format!("record<{id}>")))?;
        Ok(self.create_record(&record.type_name, record.slots))
    }

    pub fn provide_feature(&mut self, feature: &str) {
        if !self.provided_features.iter().any(|name| name == feature) {
            self.provided_features.push(feature.to_string());
        }
        self.set_global_binding("features", self.features_value());
        if feature == "abbrev" {
            primitives::ensure_standard_abbrev_tables(self);
        }
    }

    pub(super) fn provide_feature_with_after_load(
        &mut self,
        feature: &str,
    ) -> Result<Value, LispError> {
        self.provide_feature(feature);
        let mut pending = Vec::new();
        let mut index = 0usize;
        while index < self.after_load_forms.len() {
            if self.after_load_forms[index].0 == feature {
                let (_, body, env) = self.after_load_forms.remove(index);
                pending.push((body, env));
            } else {
                index += 1;
            }
        }
        for (body, mut env) in pending {
            self.sf_progn(&body, &mut env)?;
        }
        Ok(Value::Symbol(feature.to_string()))
    }

    pub fn has_feature(&self, feature: &str) -> bool {
        self.global_value("features")
            .unwrap_or_else(|| self.features_value())
            .to_vec()
            .is_ok_and(|features| {
                features
                    .iter()
                    .any(|value| matches!(value, Value::Symbol(name) if name == feature))
            })
    }

    pub(super) fn features_value(&self) -> Value {
        Value::list(
            self.provided_features
                .iter()
                .cloned()
                .map(Value::Symbol)
                .collect::<Vec<_>>(),
        )
    }
}

fn repeated_directory_load_alias(target: &str) -> Option<String> {
    let (directory, file) = target.rsplit_once('/')?;
    let directory_name = directory.rsplit('/').next()?;
    let alias_file = file.strip_prefix(&format!("{directory_name}-"))?;
    Some(format!("{directory}/{alias_file}"))
}

fn load_source_stub_prefers_elc(path: &std::path::Path) -> bool {
    fs::metadata(path).is_ok_and(|metadata| metadata.len() == 0)
}
