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

    fn current_load_history_file(&self) -> Option<String> {
        self.lookup_var("current-load-list", &Env::new())
            .and_then(|value| value.to_vec().ok())
            .and_then(|items| items.last().cloned())
            .and_then(|value| primitives::string_text(&value).ok())
    }

    pub(crate) fn current_load_history_is_suppressed(&self) -> bool {
        self.current_load_history_file().is_some_and(|file| {
            self.load_history_suppressed_files
                .iter()
                .any(|suppressed| suppressed == &file)
        })
    }

    pub(crate) fn with_current_load_history_suppressed<T>(
        &mut self,
        operation: impl FnOnce(&mut Self) -> T,
    ) -> T {
        let file = self.current_load_history_file();
        if let Some(file) = &file {
            self.load_history_suppressed_files.push(file.clone());
        }
        let result = operation(self);
        if file.is_some() {
            self.load_history_suppressed_files.pop();
        }
        result
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
        self.materialize_cl_macs_runtime_dependency(target)?;
        crate::lisp::load_file_strict(self, &path)?;
        Ok(path)
    }

    fn materialize_cl_macs_runtime_dependency(&mut self, target: &str) -> Result<(), LispError> {
        // GNU cl-macs.el starts with `(require 'cl-lib)'.  emaxx treats
        // cl-lib as preloaded, so that require is a no-op here; when the
        // runtime environment lacks cl-lib's Lisp-level helpers (e.g. a bare
        // interpreter that never loaded simple_compat.el), load the real
        // cl-lib.el first so cl-macs macro expanders can call them.  This
        // must cover both explicit `load' and `require': cl-preloaded.el's
        // eager compile-time require reaches the latter path.
        if target == "cl-macs"
            && self.lookup_function("cl-copy-list", &Env::new()).is_err()
            && !self.loading_features.iter().any(|name| name == "cl-lib")
        {
            self.load_target("cl-lib")?;
        }
        Ok(())
    }

    pub(crate) fn require_feature_with_target(
        &mut self,
        feature: &str,
        target: Option<&str>,
        env: &Env,
    ) -> Result<Value, LispError> {
        // GNU records a file's dependency even when FEATURE was loaded
        // already.  `file-dependents' and `unload-feature' derive their
        // dependency graph from these entries.
        self.record_require_in_load_history(feature);
        if self.has_feature(feature) || self.loading_features.iter().any(|name| name == feature) {
            return Ok(Value::Symbol(feature.to_string()));
        }
        // GNU does not preload map.el; its cl-generic definitions
        // (map-put!, map-insert, ...) and the map-elt gv-expander only
        // exist after the real library loads.  Prefer the real file over
        // the native compat subset whenever it is on the load-path.
        let compat_shim = is_compat_preloaded_feature(feature)
            && !(feature == "map"
                && crate::lisp::primitives::resolve_load_target_in_env(self, feature, env)
                    .is_some());
        if compat_shim {
            // GNU cl-lib.el ends with `(load "cl-loaddefs" ...)': the
            // autoloads for the rest of the cl- namespace must still be
            // registered even though the library itself is preloaded.
            if matches!(feature, "cl-lib" | "cl-extra" | "cl-generic")
                && !self.has_feature("cl-loaddefs")
            {
                if let Some(path) = self.resolve_load_target("cl-loaddefs") {
                    crate::lisp::load_file_strict(self, &path)?;
                }
                self.provide_feature_with_after_load("cl-loaddefs")?;
            }
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
        self.materialize_cl_macs_runtime_dependency(load_target)?;
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
            Value::Buffer(_, name) => self.find_buffer(name).map(|(id, _)| id).ok_or_else(|| {
                if std::env::var_os("EMAXX_DEBUG_SEMANTIC").is_some() {
                    eprintln!(
                        "[buf] resolve failed for dead buffer {name} (current {})",
                        self.buffer.name
                    );
                }
                LispError::Signal(format!("No buffer named {}", name))
            }),
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
        // GNU reset_buffer initializes a new buffer's directory from the
        // current buffer, while other per-buffer state such as read-only is
        // reset.  Materialize that inherited value on the new buffer instead
        // of making unrelated dynamic per-buffer bindings leak at lookup.
        let inherited_directory = self.lookup_var("default-directory", &Env::new());
        let id = self.alloc_buffer_id();
        self.inactive_buffers
            .push((id, crate::buffer::Buffer::new(name)));
        self.buffer_list.push((id, name.to_string()));
        if let Some(directory) = inherited_directory {
            self.set_buffer_local_value(id, "default-directory", directory);
        }
        // GNU reset_buffer materializes buffer-read-only as a local nil in
        // every new buffer.  This keeps a process-wide dynamic binding in
        // the creating buffer from making a fresh temporary buffer read-only.
        self.set_buffer_local_value(id, "buffer-read-only", Value::Nil);
        (id, name.to_string())
    }

    /// Switch the current buffer and display it in the selected window.
    pub fn switch_to_buffer_id(&mut self, id: u64) -> Result<(), LispError> {
        self.switch_to_buffer_id_with_window_history(id, true)
    }

    /// Change the current buffer without displaying it in any window.
    pub fn set_current_buffer_id(&mut self, id: u64) -> Result<(), LispError> {
        if id == self.current_buffer_id {
            return Ok(());
        }
        let pos = self
            .inactive_buffers
            .iter()
            .position(|(buffer_id, _)| *buffer_id == id)
            .ok_or_else(|| LispError::Signal(format!("No buffer with id {id}")))?;
        let (_, next_buffer) = self.inactive_buffers.swap_remove(pos);
        let current_id = self.current_buffer_id;
        let current_buffer = std::mem::replace(&mut self.buffer, next_buffer);
        self.inactive_buffers.push((current_id, current_buffer));
        self.current_buffer_id = id;
        Ok(())
    }

    /// Move a buffer to the front of the buffer list (GNU record_buffer).
    pub fn record_buffer_front(&mut self, id: u64) {
        if let Some(index) = self
            .buffer_list
            .iter()
            .position(|(buffer_id, _)| *buffer_id == id)
        {
            let entry = self.buffer_list.remove(index);
            self.buffer_list.insert(0, entry);
        }
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
        let current_id = self.current_buffer_id;
        let selected_window_already_displays_target = self
            .find_record(self.selected_window_id)
            .and_then(|window| window.slots.first())
            .and_then(|value| value.as_integer().ok())
            == Some(id as i64);
        if id == current_id && selected_window_already_displays_target {
            return Ok(());
        }
        if id != current_id {
            self.set_current_buffer_id(id)?;
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

    pub(crate) fn window_cursor_visible(&self, id: u64) -> bool {
        self.window_cursor_visibility
            .get(&id)
            .copied()
            .unwrap_or(true)
    }

    pub(crate) fn set_window_cursor_visible(&mut self, id: u64, visible: bool) {
        self.window_cursor_visibility.insert(id, visible);
    }

    pub(crate) fn old_selected_window_value(&self) -> Value {
        Value::Record(self.old_selected_window_id)
    }

    pub(crate) fn frame_old_selected_window_value(&self) -> Value {
        self.frame_old_selected_window_id
            .map(Value::Record)
            .unwrap_or(Value::Nil)
    }

    pub(crate) fn window_use_time(&self, id: u64) -> i64 {
        self.find_record(id)
            .and_then(|record| record.slots.get(primitives::WINDOW_USE_TIME_SLOT))
            .and_then(|value| value.as_integer().ok())
            .unwrap_or_default()
    }

    pub(crate) fn record_window_selection(&mut self, id: u64) {
        self.window_select_count = self.window_select_count.saturating_add(1);
        let use_time = self.window_select_count;
        if let Some(window) = self.find_record_mut(id) {
            window.slots[primitives::WINDOW_USE_TIME_SLOT] = Value::Integer(use_time);
        }
    }

    pub(crate) fn bump_window_use_time(&mut self, id: u64) -> Option<i64> {
        if id == self.selected_window_id
            || self.window_use_time(self.selected_window_id) != self.window_select_count
        {
            return None;
        }
        let bumped = self.window_select_count;
        if let Some(window) = self.find_record_mut(id) {
            window.slots[primitives::WINDOW_USE_TIME_SLOT] = Value::Integer(bumped);
        }
        self.window_select_count = self.window_select_count.saturating_add(1);
        let selected_time = self.window_select_count;
        if let Some(selected) = self.find_record_mut(self.selected_window_id) {
            selected.slots[primitives::WINDOW_USE_TIME_SLOT] = Value::Integer(selected_time);
        }
        Some(bumped)
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

    pub(crate) fn frame_state(&self, id: u64) -> Option<&super::FrameState> {
        self.frame_states.iter().find(|frame| frame.id == id)
    }

    pub(crate) fn frame_state_mut(&mut self, id: u64) -> Option<&mut super::FrameState> {
        self.frame_states.iter_mut().find(|frame| frame.id == id)
    }

    pub(crate) fn selected_frame_state(&self) -> Option<&super::FrameState> {
        self.frame_state(self.selected_frame_id)
    }

    pub(crate) fn selected_frame_state_mut(&mut self) -> Option<&mut super::FrameState> {
        self.frame_state_mut(self.selected_frame_id)
    }

    pub(crate) fn selected_frame_value(&self) -> Value {
        Value::Frame(self.selected_frame_id)
    }

    pub(crate) fn old_selected_frame_value(&self) -> Value {
        Value::Frame(self.old_selected_frame_id)
    }

    pub(crate) fn frame_is_live(&self, id: u64) -> bool {
        self.terminal_live() && self.frame_state(id).is_some_and(|frame| frame.live)
    }

    pub fn frame_width(&self) -> i64 {
        self.selected_frame_state()
            .map(|frame| frame.width)
            .unwrap_or(1)
            .max(1)
    }

    pub fn set_frame_width(&mut self, width: i64) {
        let width = width.max(1);
        if let Some(frame) = self.selected_frame_state_mut() {
            frame.width = width;
        }
        self.resize_frame_window_records();
    }

    pub fn frame_height(&self) -> i64 {
        self.selected_frame_state()
            .map(|frame| frame.height)
            .unwrap_or(1)
            .max(1)
    }

    pub fn set_frame_height(&mut self, height: i64) {
        let text_height = height.max(1);
        if let Some(frame) = self.selected_frame_state_mut() {
            frame.text_height = text_height;
            frame.height = text_height.saturating_add(1);
        }
        self.resize_frame_window_records();
    }

    pub(crate) fn frame_text_height(&self) -> i64 {
        self.selected_frame_state()
            .map(|frame| frame.text_height)
            .unwrap_or(1)
            .max(1)
    }

    pub(crate) fn frame_parameter_width(&self) -> i64 {
        self.selected_frame_state()
            .map(|frame| frame.parameter_width)
            .unwrap_or(1)
            .max(1)
    }

    pub(crate) fn frame_parameter_height(&self) -> i64 {
        self.selected_frame_state()
            .map(|frame| frame.parameter_height)
            .unwrap_or(1)
            .max(1)
    }

    fn window_record_geometry(&self, id: u64) -> (i64, i64, i64, i64) {
        let integer_slot = |slot: usize, fallback: i64| {
            self.find_record(id)
                .and_then(|record| record.slots.get(slot))
                .and_then(|value| value.as_integer().ok())
                .unwrap_or(fallback)
        };
        (
            integer_slot(primitives::WINDOW_PIXEL_WIDTH_SLOT, self.frame_width()),
            integer_slot(primitives::WINDOW_PIXEL_HEIGHT_SLOT, self.frame_height()),
            integer_slot(primitives::WINDOW_PIXEL_LEFT_SLOT, 0),
            integer_slot(primitives::WINDOW_PIXEL_TOP_SLOT, 0),
        )
    }

    fn set_window_record_geometry(&mut self, id: u64, geometry: (i64, i64, i64, i64)) {
        let Some(record) = self.find_record_mut(id) else {
            return;
        };
        if record.slots.len() <= primitives::WINDOW_PIXEL_TOP_SLOT {
            record
                .slots
                .resize(primitives::WINDOW_PIXEL_TOP_SLOT + 1, Value::Nil);
        }
        for (slot, value) in [
            (primitives::WINDOW_PIXEL_WIDTH_SLOT, geometry.0),
            (primitives::WINDOW_PIXEL_HEIGHT_SLOT, geometry.1),
            (primitives::WINDOW_PIXEL_LEFT_SLOT, geometry.2),
            (primitives::WINDOW_PIXEL_TOP_SLOT, geometry.3),
        ] {
            record.slots[slot] = Value::Integer(value);
        }
    }

    fn window_record_link(&self, id: u64, slot: usize) -> Option<u64> {
        match self
            .find_record(id)
            .and_then(|record| record.slots.get(slot))
        {
            Some(Value::Record(link)) => Some(*link),
            _ => None,
        }
    }

    fn resize_window_record_tree(&mut self, id: u64, geometry: (i64, i64, i64, i64)) {
        let old_geometry = self.window_record_geometry(id);
        let kind = self
            .find_record(id)
            .and_then(|record| record.slots.get(primitives::WINDOW_KIND_SLOT))
            .cloned()
            .unwrap_or(Value::Nil);
        let horizontal = matches!(
            kind,
            Value::Symbol(ref kind) if kind == primitives::INTERNAL_HORIZONTAL_WINDOW_KIND
        );
        let vertical = matches!(
            kind,
            Value::Symbol(ref kind) if kind == primitives::INTERNAL_VERTICAL_WINDOW_KIND
        );
        let mut children = Vec::new();
        let mut child = self.window_record_link(id, primitives::WINDOW_FIRST_CHILD_SLOT);
        while let Some(child_id) = child {
            if children.contains(&child_id) {
                break;
            }
            children.push(child_id);
            child = self.window_record_link(child_id, primitives::WINDOW_NEXT_SIBLING_SLOT);
        }

        self.set_window_record_geometry(id, geometry);
        if children.is_empty() || (!horizontal && !vertical) {
            return;
        }

        let old_total = if horizontal {
            old_geometry.0
        } else {
            old_geometry.1
        }
        .max(1);
        let new_total = if horizontal { geometry.0 } else { geometry.1 }.max(0);
        let mut old_offset = 0;
        for (index, child_id) in children.iter().enumerate() {
            let child_geometry = self.window_record_geometry(*child_id);
            let old_size = if horizontal {
                child_geometry.0
            } else {
                child_geometry.1
            }
            .max(0);
            let new_start = new_total.saturating_mul(old_offset) / old_total;
            old_offset = old_offset.saturating_add(old_size);
            let new_end = if index + 1 == children.len() {
                new_total
            } else {
                new_total.saturating_mul(old_offset) / old_total
            };
            let child_new_geometry = if horizontal {
                (
                    new_end.saturating_sub(new_start),
                    geometry.1,
                    geometry.2.saturating_add(new_start),
                    geometry.3,
                )
            } else {
                (
                    geometry.0,
                    new_end.saturating_sub(new_start),
                    geometry.2,
                    geometry.3.saturating_add(new_start),
                )
            };
            self.resize_window_record_tree(*child_id, child_new_geometry);
        }
    }

    fn resize_frame_window_records(&mut self) {
        let width = self.frame_width();
        let total_height = self.frame_height();
        let text_height = self.frame_text_height();
        let top_margin = total_height.saturating_sub(text_height);
        let root_height = text_height.saturating_sub(1).max(1);
        if let Some(Value::Record(root_id)) = self.global_binding_value("emaxx-root-window") {
            self.resize_window_record_tree(root_id, (width, root_height, 0, top_margin));
        }
        if let Some(Value::Record(minibuffer_id)) =
            self.global_binding_value("emaxx-minibuffer-window")
        {
            let old_minibuffer = self.window_record_geometry(minibuffer_id);
            self.set_window_record_geometry(
                minibuffer_id,
                (
                    width,
                    old_minibuffer.1,
                    old_minibuffer.2,
                    top_margin.saturating_add(root_height),
                ),
            );
        }
    }

    pub(crate) fn frame_parameter_override(&self, name: &str) -> Option<Value> {
        self.selected_frame_state()?
            .parameter_overrides
            .iter()
            .find(|(parameter, _)| parameter == name)
            .map(|(_, value)| value.clone())
    }

    pub(crate) fn frame_name_value(&self) -> Value {
        self.selected_frame_state()
            .map(|frame| frame.name.clone())
            .unwrap_or(Value::Nil)
    }

    pub(crate) fn frame_and_buffer_state(&self) -> Value {
        self.frame_and_buffer_state.clone()
    }

    pub(crate) fn set_frame_and_buffer_state(&mut self, state: Value) {
        self.frame_and_buffer_state = state;
    }

    pub(crate) fn snapshot_window_configuration(&self) -> WindowConfigurationSnapshot {
        WindowConfigurationSnapshot {
            current_buffer_id: self.current_buffer_id(),
            selected_window_id: self.selected_window_id,
            selected_window_slots: self
                .find_record(self.selected_window_id)
                .map(|record| record.slots.clone())
                .unwrap_or_default(),
            window_records: self
                .records
                .iter()
                .filter(|record| record.type_name == "window")
                .map(|record| (record.id, record.slots.clone()))
                .collect(),
            root_window: self
                .global_binding_value("emaxx-root-window")
                .unwrap_or(Value::Nil),
            frame_width: self.frame_width(),
            frame_height: self.frame_height(),
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
                snapshot.root_window,
                Value::list(
                    snapshot.window_records.into_iter().map(|(id, slots)| {
                        Value::cons(Value::Integer(id as i64), Value::list(slots))
                    }),
                ),
            ],
        )
    }

    pub(crate) fn apply_window_configuration_value(
        &mut self,
        value: &Value,
    ) -> Result<bool, LispError> {
        let Some(snapshot) = self.window_configuration_snapshot_from_value(value) else {
            return Ok(false);
        };
        self.restore_window_configuration(snapshot)?;
        Ok(true)
    }

    fn window_configuration_snapshot_from_value(
        &self,
        value: &Value,
    ) -> Option<WindowConfigurationSnapshot> {
        let Value::Record(id) = value else {
            return None;
        };
        let record = self
            .find_record(*id)
            .filter(|record| record.type_name == "window-configuration")?;
        let slots = &record.slots;
        let selected_window_id = slots
            .get(1)
            .and_then(|value| value.as_integer().ok())
            .unwrap_or_default() as u64;
        let selected_window_slots = slots
            .get(2)
            .and_then(|value| value.to_vec().ok())
            .unwrap_or_default();
        let window_records = slots
            .get(6)
            .and_then(|value| value.to_vec().ok())
            .map(|records| {
                records
                    .into_iter()
                    .filter_map(|record| {
                        let (id, slots) = record.cons_values()?;
                        Some((id.as_integer().ok()?.max(0) as u64, slots.to_vec().ok()?))
                    })
                    .collect()
            })
            .unwrap_or_else(|| vec![(selected_window_id, selected_window_slots.clone())]);
        Some(WindowConfigurationSnapshot {
            current_buffer_id: slots
                .first()
                .and_then(|value| value.as_integer().ok())
                .unwrap_or_default() as u64,
            selected_window_id,
            selected_window_slots,
            window_records,
            root_window: slots
                .get(5)
                .cloned()
                .unwrap_or(Value::Record(selected_window_id)),
            frame_width: slots
                .get(3)
                .and_then(|value| value.as_integer().ok())
                .unwrap_or(80),
            frame_height: slots
                .get(4)
                .and_then(|value| value.as_integer().ok())
                .unwrap_or(24),
        })
    }

    pub(crate) fn is_window_configuration_value(&self, value: &Value) -> bool {
        self.window_configuration_snapshot_from_value(value)
            .is_some()
    }

    pub(crate) fn window_configurations_equal(
        &self,
        left: &Value,
        right: &Value,
    ) -> Result<bool, LispError> {
        let snapshot = |value: &Value| {
            self.window_configuration_snapshot_from_value(value)
                .ok_or_else(|| {
                    LispError::TypeError("window-configuration-p".into(), value.type_name())
                })
        };
        let left = snapshot(left)?;
        let right = snapshot(right)?;
        // GNU compares layout, buffers, and frame dimensions, while
        // deliberately ignoring point and scrolling positions.  Slots 1
        // and 2 are Emaxx's window-start/history state; slot 0 is the
        // displayed buffer and later slots describe window structure.
        let layout_slots = |slots: &[Value]| {
            slots
                .first()
                .into_iter()
                .chain(slots.iter().skip(3))
                .cloned()
                .collect::<Vec<_>>()
        };
        Ok(left.current_buffer_id == right.current_buffer_id
            && left.frame_width == right.frame_width
            && left.frame_height == right.frame_height
            && layout_slots(&left.selected_window_slots)
                == layout_slots(&right.selected_window_slots))
    }

    pub(crate) fn restore_window_configuration(
        &mut self,
        snapshot: WindowConfigurationSnapshot,
    ) -> Result<(), LispError> {
        let saved_windows = snapshot
            .window_records
            .into_iter()
            .collect::<std::collections::HashMap<_, _>>();
        for record in self
            .records
            .iter_mut()
            .filter(|record| record.type_name == "window")
        {
            if let Some(slots) = saved_windows.get(&record.id) {
                record.slots.clone_from(slots);
            } else {
                record
                    .slots
                    .resize(primitives::WINDOW_FIRST_CHILD_SLOT + 1, Value::Nil);
                record.slots[primitives::WINDOW_BUFFER_SLOT] = Value::Nil;
                record.slots[primitives::WINDOW_KIND_SLOT] =
                    Value::Symbol(primitives::DELETED_WINDOW_KIND.into());
                for slot in [
                    primitives::WINDOW_PARENT_SLOT,
                    primitives::WINDOW_PREV_SIBLING_SLOT,
                    primitives::WINDOW_NEXT_SIBLING_SLOT,
                    primitives::WINDOW_FIRST_CHILD_SLOT,
                ] {
                    record.slots[slot] = Value::Nil;
                }
            }
        }
        self.set_global_binding("emaxx-root-window", snapshot.root_window);
        self.selected_window_id = snapshot.selected_window_id;
        if self.has_buffer_id(snapshot.current_buffer_id) {
            self.set_current_buffer_id(snapshot.current_buffer_id)?;
        }
        // GNU records frame dimensions for configuration equality, but
        // `set-window-configuration' does not rewind a frame resize.  Restore
        // the saved tree into the frame's current geometry instead.
        self.resize_frame_window_records();
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
                .map(|state| state.name.clone())
                .or_else(|| {
                    // eieio-core constructs an `eieio--class' record, fills
                    // its inherited cl--class slots, and only then installs
                    // it with `(setf (cl--find-class NAME) RECORD)'.  GNU's
                    // accessors work throughout that construction phase.
                    let record = self.find_record(*record_id)?;
                    (record.type_name == "eieio--class")
                        .then(|| record.slots.first()?.as_symbol().ok().map(str::to_string))
                        .flatten()
                }),
            _ => None,
        }
    }

    pub(crate) fn class_value(&self, name: &str) -> Option<Value> {
        self.find_class_state(name)
            .map(|state| Value::Record(state.record_id))
            // Loaded cl-preloaded.el defines `cl--find-class' as this public
            // symbol property.  A GNU EIEIO class can therefore exist
            // without ever passing through the bootstrap ClassState path.
            .or_else(|| self.get_symbol_property(name, "cl--class"))
    }

    pub(crate) fn raw_eieio_class_slot(&self, class: &Value, index: usize) -> Option<Value> {
        let Value::Record(record_id) = class else {
            return None;
        };
        let record = self.find_record(*record_id)?;
        // A class built by GNU's eieio-core.el has the complete `cl--class'
        // plus `eieio--class' record layout (slots 0..=10).  Native
        // bootstrap classes deliberately keep their canonical metadata in
        // ClassState and use a compact four-slot facade.  Do not interpret
        // facade fields using the GNU record layout.
        (record.type_name == "eieio--class" && record.slots.len() >= 11)
            .then(|| record.slots.get(index).cloned())
            .flatten()
    }

    fn raw_eieio_class_parents(&self, class: &Value) -> Option<Value> {
        self.raw_eieio_class_slot(class, 2).or_else(|| {
            let Value::Record(record_id) = class else {
                return None;
            };
            let record = self.find_record(*record_id)?;
            // Unregistered short records are raw cl--class values used
            // during Lisp construction and by focused compatibility tests.
            (record.type_name == "eieio--class"
                && self.find_class_state_by_record_id(*record_id).is_none())
            .then(|| record.slots.get(2).cloned())
            .flatten()
        })
    }

    pub(crate) fn raw_eieio_class_slot_by_name(&self, name: &str, index: usize) -> Option<Value> {
        let class = self.class_value(name)?;
        self.raw_eieio_class_slot(&class, index)
    }

    pub(crate) fn class_default_object_cache(&self, name: &str) -> Option<Value> {
        let class = self.class_value(name)?;
        self.raw_eieio_class_slot(&class, 9).or_else(|| {
            let Value::Record(record_id) = class else {
                return None;
            };
            // Native bootstrap classes use the compact
            // (name parents slots options cache) facade.
            self.find_record(record_id)
                .and_then(|record| record.slots.get(4).cloned())
        })
    }

    pub(crate) fn eieio_unbound_form(&self) -> Option<Value> {
        self.global_value("eieio--unbound-form")
    }

    pub(crate) fn eieio_unbound_value(&self) -> Option<Value> {
        self.global_value("eieio--unbound")
    }

    pub(crate) fn value_is_eieio_unbound(&self, value: &Value) -> bool {
        matches!(value, Value::Unbound)
            || self
                .eieio_unbound_value()
                .is_some_and(|marker| primitives::values_equal(self, value, &marker))
    }

    fn raw_eieio_class_parent_names(&self, name: &str) -> Option<Vec<String>> {
        let class = self
            .class_value(name)
            .or_else(|| self.get_symbol_property(name, "cl--class"))?;
        Some(
            self.raw_eieio_class_parents(&class)?
                .to_vec()
                .unwrap_or_default()
                .iter()
                .filter_map(|parent| self.class_name_from_value(parent))
                .collect(),
        )
    }

    pub(crate) fn class_is_autoload_stub(&self, name: &str) -> bool {
        let Some(Value::Record(record_id)) = self.class_value(name) else {
            return false;
        };
        let Some(record) = self.find_record(record_id) else {
            return false;
        };
        // eieio--full-class-object uses the nil default-object cache to
        // distinguish the dummy made by eieio-defclass-autoload from a
        // completed class.  The cache is field 9 of the full record.
        record.type_name == "eieio--class"
            && record.slots.len() >= 11
            && record.slots.get(9).is_none_or(Value::is_nil)
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
        // GNU registers a new class before it fills the record's parent
        // field.  The completed Lisp record is therefore authoritative over
        // the initially empty ClassState snapshot.
        if let Some(parents) = self.raw_eieio_class_parents(class) {
            return Ok(parents);
        }
        if let Some(state) = self.find_class_state_by_record_id(*record_id) {
            return Ok(Value::list(state.parents.iter().map(|parent| {
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
        let overridden_parents = self
            .class_parent_overrides
            .iter()
            .find(|(stored_id, _)| *stored_id == record_id)
            .map(|(_, parents)| parents.clone());
        let raw_parents = self
            .find_record(record_id)
            .and_then(|record| {
                (record.type_name == "eieio--class")
                    .then(|| record.slots.get(2).cloned())
                    .flatten()
            })
            .and_then(|parents| parents.to_vec().ok())
            .unwrap_or_default();
        let parents = overridden_parents.unwrap_or_else(|| {
            raw_parents
                .iter()
                .filter_map(|parent| self.class_name_from_value(parent))
                .collect()
        });
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

        fn parent_names(interp: &Interpreter, name: &str) -> Vec<String> {
            let builtin_parents = primitives::builtin_class_parents(name);
            if !builtin_parents.is_empty() {
                builtin_parents
                    .iter()
                    .map(|parent| (*parent).to_string())
                    .collect()
            } else if name == "oclosure" {
                vec!["closure".into()]
            } else if interp.class_is_oclosure_type(name) {
                vec![
                    interp
                        .get_symbol_property(name, "emaxx-oclosure-parent")
                        .and_then(|value| value.as_symbol().ok().map(String::from))
                        .unwrap_or_else(|| "oclosure".into()),
                ]
            } else if let Some(parents) = interp.raw_eieio_class_parent_names(name) {
                parents
            } else {
                interp
                    .find_class_state(name)
                    .map(|state| state.parents.clone())
                    .unwrap_or_default()
            }
        }

        // GNU's `cl--class-allparents' merges the already ordered ancestry
        // of every direct parent.  A depth-first walk is observably wrong for
        // multiple inheritance: shared ancestors of the first parent must not
        // precede the second direct parent.
        fn merge_ordered(mut lists: Vec<Vec<String>>) -> Vec<String> {
            lists.retain(|list| !list.is_empty());
            let mut merged = Vec::new();
            while lists.len() > 1 {
                let candidate = lists.iter().find_map(|list| {
                    let head = list.first()?;
                    lists
                        .iter()
                        .all(|other| !other.iter().skip(1).any(|item| item == head))
                        .then(|| head.clone())
                });
                // GNU's general class merge resolves an inconsistent graph
                // by taking the first available head.  EIEIO's explicit C3
                // validator is the layer that signals inconsistent ancestry.
                let candidate = candidate.unwrap_or_else(|| lists[0][0].clone());
                merged.push(candidate.clone());
                for list in &mut lists {
                    if list.first() == Some(&candidate) {
                        list.remove(0);
                    }
                }
                lists.retain(|list| !list.is_empty());
            }
            if let Some(last) = lists.pop() {
                merged.extend(last);
            }
            merged
        }

        fn precedence(
            interp: &Interpreter,
            name: &str,
            active: &mut std::collections::HashSet<String>,
        ) -> Vec<String> {
            if let Some(parents) = primitives::builtin_class_allparents(name) {
                return parents.iter().map(|parent| (*parent).to_string()).collect();
            }
            if !active.insert(name.to_string()) {
                return vec![name.to_string()];
            }
            let parents = parent_names(interp, name);
            let mut result = vec![name.to_string()];
            if parents.is_empty() {
                if name != "t" {
                    result.extend(precedence(interp, "t", active));
                }
            } else {
                let parent_lists = parents
                    .iter()
                    .map(|parent| precedence(interp, parent, active))
                    .collect();
                result.extend(merge_ordered(parent_lists));
            }
            active.remove(name);
            result
        }

        precedence(self, name, &mut std::collections::HashSet::new())
            .into_iter()
            .map(crate::lisp::types::interned_symbol_value)
            .collect()
    }

    pub(crate) fn class_is_oclosure_type(&self, name: &str) -> bool {
        name == "oclosure"
            || self
                .get_symbol_property(name, "emaxx-oclosure-slots")
                .is_some()
    }

    // Sibling classes (neither inherits the other) have no global
    // specificity order; CLOS resolves them through the class precedence
    // list of the instance's class. A common subclass fixes that order
    // statically: its precedence list mentions both, most-specific-first.
    pub(crate) fn class_sibling_precedes(&self, left: &str, right: &str) -> bool {
        for state in &self.class_states {
            let parents = self.class_allparents(&state.name);
            let left_position = parents
                .iter()
                .position(|parent| matches!(parent, Value::Symbol(name) if name == left));
            let right_position = parents
                .iter()
                .position(|parent| matches!(parent, Value::Symbol(name) if name == right));
            if let (Some(left_position), Some(right_position)) = (left_position, right_position) {
                return left_position < right_position;
            }
        }
        false
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

    pub(crate) fn value_is_eieio_object(&self, value: &Value) -> bool {
        // Several host-backed values (hash tables, processes, markers, ...)
        // use Value::Record too.  GNU's `recordp' accepts only actual record
        // objects here, and `eieio--class-p' further requires their tag to
        // resolve to an EIEIO class.  Model that semantic boundary through
        // ancestry instead of leaking Emaxx's shared Rust representation.
        let Value::Record(record_id) = value else {
            return false;
        };
        let Some(record) = self.find_record(*record_id) else {
            return false;
        };
        self.get_symbol_property(&record.type_name, "emaxx-eieio-class")
            .is_some_and(|marker| marker.is_truthy())
            || self.value_is_instance_of_class(value, "eieio-default-superclass")
    }

    pub(crate) fn callable_is_ignore(&self, value: &Value) -> bool {
        fn resolves_to_ignore(
            interp: &Interpreter,
            value: &Value,
            seen_records: &mut std::collections::HashSet<u64>,
        ) -> bool {
            match value {
                Value::BuiltinFunc(name) | Value::Symbol(name) => name == "ignore",
                Value::Record(record_id) if seen_records.insert(*record_id) => interp
                    .find_record(*record_id)
                    .filter(|record| record.type_name == "byte-code-function")
                    .and_then(|record| record.slots.first())
                    .is_some_and(|callable| resolves_to_ignore(interp, callable, seen_records)),
                _ => false,
            }
        }

        // `symbol-function' exposes a GNU-preloaded Lisp function through an
        // observable byte-code-function facade, while Emaxx retains the host
        // callable in slot zero.  Generic dispatch compares callable identity,
        // so normalize that representation boundary before testing the
        // `ignore' end-of-chain sentinel.
        resolves_to_ignore(self, value, &mut std::collections::HashSet::new())
    }

    pub(crate) fn class_children(&self, name: &str) -> Vec<Value> {
        // GNU mutates the live eieio--class record when each subclass is
        // defined.  A ClassState entry may have been installed earlier,
        // before those child names were pushed, so a complete Lisp record is
        // authoritative just as it is for parents, slots, and options.
        if let Some(children) = self
            .class_value(name)
            .and_then(|class| self.raw_eieio_class_slot(&class, 5))
            .and_then(|children| children.to_vec().ok())
        {
            return children;
        }
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

    pub fn terminal_parameter(&self, parameter: &Value) -> Option<Value> {
        self.terminal_parameters
            .iter()
            .rfind(|(key, _)| key == parameter)
            .map(|(_, value)| value.clone())
    }

    pub fn terminal_live(&self) -> bool {
        self.terminal_live
    }

    pub(crate) fn terminal_value(&self) -> Value {
        Value::Terminal(0)
    }

    pub fn delete_terminal_state(&mut self) {
        self.terminal_live = false;
        self.terminal_parameters.clear();
    }

    pub fn set_terminal_parameter(&mut self, parameter: Value, value: Value) -> Value {
        let parameter = Self::stored_value(parameter);
        let value = Self::stored_value(value);
        if let Some(index) = self
            .terminal_parameters
            .iter()
            .rposition(|(key, _)| key == &parameter)
        {
            let previous = self.terminal_parameters[index].1.clone();
            self.terminal_parameters[index].1 = value;
            previous
        } else {
            self.terminal_parameters.push((parameter, value));
            Value::Nil
        }
    }

    pub fn terminal_parameters(&self) -> Value {
        Value::list(
            self.terminal_parameters
                .iter()
                .rev()
                .map(|(parameter, value)| Value::cons(parameter.clone(), value.clone())),
        )
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
        // GNU's kill-buffer path replaces the dying buffer in every window
        // before tearing the buffer down.  Keep that invariant here as the
        // low-level backstop too: Emaxx's temporary-buffer and ERT cleanup
        // paths call kill_buffer_id directly, so updating only the selected
        // window leaves non-selected windows looking live while their buffer
        // slot names a dead ID.
        let replacement_id = self.current_buffer_id;
        let replacement_start = self
            .buffer_bounds_by_id(replacement_id)
            .map(|(point_min, _)| point_min)
            .unwrap_or(1);
        let window_ids = self.record_ids_by_type("window");
        for window_id in window_ids {
            let showed_killed_buffer = self
                .find_record(window_id)
                .and_then(|window| window.slots.get(primitives::WINDOW_BUFFER_SLOT))
                .and_then(|value| value.as_integer().ok())
                .is_some_and(|buffer_id| buffer_id.max(0) as u64 == id);
            if !showed_killed_buffer {
                continue;
            }
            if let Some(window) = self.find_record_mut(window_id) {
                window.slots[primitives::WINDOW_BUFFER_SLOT] =
                    Value::Integer(replacement_id as i64);
                window.slots[primitives::WINDOW_START_SLOT] =
                    Value::Integer(replacement_start as i64);
            }
        }
        if selected_window_showed_buffer {
            self.set_selected_window_buffer_id(replacement_id);
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

    pub fn buffer_mark_marker_value(&mut self) -> Value {
        let buffer_id = self.current_buffer_id();
        let mark = self.buffer.mark();
        let marker_id = match self.buffer_mark_marker_ids.get(&buffer_id).copied() {
            Some(marker_id) => marker_id,
            None => {
                let Value::Marker(marker_id) = self.make_marker() else {
                    unreachable!("make_marker always returns a marker")
                };
                self.buffer_mark_marker_ids.insert(buffer_id, marker_id);
                marker_id
            }
        };
        if let Some(marker) = self.find_marker_mut(marker_id) {
            marker.position = mark;
            marker.buffer_id = mark.map(|_| buffer_id);
            if let Some(mark) = mark {
                marker.last_position = Some(mark);
            }
        }
        Value::Marker(marker_id)
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
        let mark_buffer_id = self
            .buffer_mark_marker_ids
            .iter()
            .find_map(|(buffer_id, marker_id)| (*marker_id == id).then_some(*buffer_id));
        let marker = self
            .find_marker_mut(id)
            .ok_or_else(|| LispError::TypeError("marker".into(), format!("marker<{}>", id)))?;
        marker.buffer_id = buffer_id;
        marker.position = position;
        if let Some(pos) = position {
            marker.last_position = Some(pos);
        }
        if let Some(mark_buffer_id) = mark_buffer_id
            && let Some(buffer) = self.get_buffer_by_id_mut(mark_buffer_id)
        {
            if buffer_id == Some(mark_buffer_id) {
                if let Some(position) = position {
                    buffer.set_mark(position);
                } else {
                    buffer.clear_mark();
                }
            } else {
                buffer.clear_mark();
            }
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

    pub fn unicode_property_table(&mut self, property: &str) -> (Value, bool) {
        if let Some(id) = self.unicode_property_table_ids.get(property).copied()
            && self.find_char_table(id).is_some()
        {
            return (Value::CharTable(id), false);
        }
        let table = self.make_char_table(Some("char-code-property-table".into()), Value::Nil);
        let Value::CharTable(id) = table else {
            unreachable!("make_char_table always returns a character table")
        };
        // GNU Unicode property tables keep the property name in extra slot
        // zero and a value-description function in slot three.  Emaxx
        // computes the generated Unicode data natively, but must expose the
        // same table contract to the Lisp-owned mule-cmds.el accessors.
        let state = self
            .find_char_table_mut(id)
            .expect("new Unicode property table must exist");
        let description = if property == "general-category" {
            Value::BuiltinFunc("emaxx--general-category-description".into())
        } else {
            Value::Nil
        };
        state.extra_slots = vec![
            Value::Symbol(property.to_string()),
            Value::Nil,
            Value::Nil,
            description,
            Value::Nil,
        ];
        self.unicode_property_table_ids
            .insert(property.to_string(), id);
        (Value::CharTable(id), true)
    }

    pub fn replace_hash_table_runtime_entries(
        &mut self,
        id: u64,
        test: &str,
        entries: Vec<(Value, Value)>,
    ) {
        let test = match test {
            "eq" => RuntimeHashTest::Eq,
            "eql" => RuntimeHashTest::Eql,
            "equal" => RuntimeHashTest::Equal,
            _ => {
                self.equal_hash_tables.remove(&id);
                return;
            }
        };
        let mut key_index: HashMap<
            Option<i64>,
            Vec<usize>,
            crate::lisp::primitives::FnvBuildHasher,
        > = HashMap::default();
        for (index, (key, _)) in entries.iter().enumerate() {
            let hash = crate::lisp::primitives::runtime_hash_bucket_key(self, test, key);
            key_index.entry(hash).or_default().push(index);
        }
        self.equal_hash_tables.insert(
            id,
            EqualHashTableState {
                test,
                entries,
                key_index,
            },
        );
    }

    pub fn hash_table_runtime_entries(&self, id: u64) -> Option<&Vec<(Value, Value)>> {
        self.equal_hash_tables.get(&id).map(|state| &state.entries)
    }

    fn runtime_hash_keys_match(
        &self,
        test: RuntimeHashTest,
        stored: &Value,
        probe: &Value,
        env: &Env,
    ) -> bool {
        match test {
            RuntimeHashTest::Eq => {
                crate::lisp::primitives::values_eq_in_env(self, stored, probe, env)
            }
            RuntimeHashTest::Eql => crate::lisp::primitives::values_eql(stored, probe),
            RuntimeHashTest::Equal => crate::lisp::primitives::values_equal(self, stored, probe),
        }
    }

    pub fn equal_hash_lookup(&self, id: u64, key: &Value, env: &Env) -> Option<Option<Value>> {
        let state = self.equal_hash_tables.get(&id)?;
        let hash = crate::lisp::primitives::runtime_hash_bucket_key(self, state.test, key);
        Some(
            state
                .key_index
                .get(&hash)
                .into_iter()
                .flatten()
                .filter_map(|index| state.entries.get(*index))
                .find(|(existing, _)| self.runtime_hash_keys_match(state.test, existing, key, env))
                .map(|(_, value)| value.clone()),
        )
    }

    pub fn equal_hash_put(&mut self, id: u64, key: Value, value: Value, env: &Env) -> bool {
        let Some(state) = self.equal_hash_tables.get(&id) else {
            return false;
        };
        let test = state.test;
        let hash = crate::lisp::primitives::runtime_hash_bucket_key(self, test, &key);
        let existing_index = state
            .key_index
            .get(&hash)
            .into_iter()
            .flatten()
            .copied()
            .find(|index| {
                state.entries.get(*index).is_some_and(|(existing, _)| {
                    self.runtime_hash_keys_match(test, existing, &key, env)
                })
            });

        let state = self
            .equal_hash_tables
            .get_mut(&id)
            .expect("equal hash table disappeared during lookup");
        if let Some(index) = existing_index {
            state.entries[index].1 = value;
        } else {
            let index = state.entries.len();
            state.entries.push((key, value));
            state.key_index.entry(hash).or_default().push(index);
        }
        true
    }

    pub fn equal_hash_remove(&mut self, id: u64, key: &Value, env: &Env) -> Option<bool> {
        let state = self.equal_hash_tables.get(&id)?;
        let test = state.test;
        let hash = crate::lisp::primitives::runtime_hash_bucket_key(self, test, key);
        let existing_index = state
            .key_index
            .get(&hash)
            .into_iter()
            .flatten()
            .copied()
            .find(|index| {
                state.entries.get(*index).is_some_and(|(existing, _)| {
                    self.runtime_hash_keys_match(test, existing, key, env)
                })
            });
        let Some(existing_index) = existing_index else {
            return Some(false);
        };
        self.equal_hash_tables
            .get_mut(&id)
            .expect("equal hash table disappeared during removal")
            .entries
            .remove(existing_index);

        let mut key_index: HashMap<
            Option<i64>,
            Vec<usize>,
            crate::lisp::primitives::FnvBuildHasher,
        > = HashMap::default();
        for (index, (entry_key, _)) in self
            .equal_hash_tables
            .get(&id)
            .expect("equal hash table disappeared while rebuilding")
            .entries
            .iter()
            .enumerate()
        {
            let hash = crate::lisp::primitives::runtime_hash_bucket_key(self, test, entry_key);
            key_index.entry(hash).or_default().push(index);
        }
        self.equal_hash_tables
            .get_mut(&id)
            .expect("equal hash table disappeared after rebuilding")
            .key_index = key_index;
        Some(true)
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
        if table.id == self.standard_syntax_table_id
            && table.default.is_nil()
            && let Some(value) = primitives::standard_syntax_table_default_value(key)
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

    /// Enumerate the effective explicit ranges in an append-only character
    /// table.  Newer writes mask older ones; nil writes mask without being
    /// reported as values.
    pub(crate) fn char_table_effective_ranges(&self, id: u64) -> Option<Vec<CharTableEntry>> {
        let entries = &self.find_char_table(id)?.entries;
        let mut covered = Vec::<(u32, u32)>::new();
        let mut effective = Vec::new();

        for entry in entries.iter().rev() {
            let mut pieces = vec![(entry.start, entry.end)];
            for &(covered_start, covered_end) in &covered {
                let mut remaining = Vec::with_capacity(pieces.len() + 1);
                for (piece_start, piece_end) in pieces {
                    if covered_end < piece_start || covered_start > piece_end {
                        remaining.push((piece_start, piece_end));
                    } else {
                        if piece_start < covered_start {
                            remaining.push((piece_start, covered_start - 1));
                        }
                        if piece_end > covered_end {
                            remaining.push((covered_end + 1, piece_end));
                        }
                    }
                }
                pieces = remaining;
                if pieces.is_empty() {
                    break;
                }
            }
            for &(start, end) in &pieces {
                covered.push((start, end));
                if !entry.value.is_nil() {
                    effective.push(CharTableEntry {
                        start,
                        end,
                        value: entry.value.clone(),
                    });
                }
            }
        }

        effective.sort_by_key(|entry| (entry.start, entry.end));
        Some(effective)
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
        if self.is_ascii_case_table(id) {
            self.mark_ascii_case_table(new_id);
        }
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
        // Ids are handed out densely starting at 1 (main thread, standard
        // obarray, then next_record_id onward) and records are never
        // removed, so the id doubles as an index; the linear scan is a
        // safety net in case that invariant ever changes.
        let index = (id as usize).checked_sub(1)?;
        match self.records.get(index) {
            Some(record) if record.id == id => Some(record),
            _ => self.records.iter().find(|record| record.id == id),
        }
    }

    pub fn find_record_mut(&mut self, id: u64) -> Option<&mut RecordState> {
        // The caller may rewrite the slots, so a decoded byte-code program
        // for this record can no longer be trusted (see bytecode::vm).
        if let Some(slot) = (id as usize)
            .checked_sub(1)
            .and_then(|index| self.bytecode_program_cache.get_mut(index))
        {
            *slot = None;
        }
        let index = (id as usize).checked_sub(1)?;
        match self.records.get(index) {
            Some(record) if record.id == id => self.records.get_mut(index),
            _ => self.records.iter_mut().find(|record| record.id == id),
        }
    }

    pub(crate) fn create_treesit_query(&mut self, language: Value, source: Value) -> Value {
        let query = self.create_record("tree-sitter-compiled-query", Vec::new());
        let Value::Record(record_id) = query else {
            unreachable!("Tree-sitter queries use opaque record identities");
        };
        self.treesit_queries.push(TreeSitterQueryState {
            record_id,
            language,
            source,
            query: None,
        });
        Value::Record(record_id)
    }

    pub(crate) fn treesit_query_state(&self, value: &Value) -> Option<&TreeSitterQueryState> {
        let Value::Record(record_id) = value else {
            return None;
        };
        self.treesit_queries
            .iter()
            .find(|query| query.record_id == *record_id)
    }

    pub(crate) fn cache_treesit_query(
        &mut self,
        value: &Value,
        query: std::rc::Rc<tree_sitter::Query>,
    ) {
        let Value::Record(record_id) = value else {
            unreachable!("only compiled Tree-sitter query records are cached");
        };
        self.treesit_queries
            .iter_mut()
            .find(|state| state.record_id == *record_id)
            .expect("compiled Tree-sitter query state exists")
            .query = Some(query);
    }

    // GNU eieio objects carry the class OBJECT as their record tag unless
    // `make-instance' downgrades it to the class symbol for backward
    // compatibility; such records print with the class object expanded in
    // place of the type symbol.
    pub(crate) fn mark_class_object_tagged_record(&mut self, id: u64) {
        self.class_object_tagged_records.insert(id);
    }

    pub(crate) fn is_class_object_tagged_record(&self, id: u64) -> bool {
        self.class_object_tagged_records.contains(&id)
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
        let hash_entries = self.hash_table_runtime_entries(id).cloned();
        let record = self
            .find_record(id)
            .cloned()
            .ok_or_else(|| LispError::TypeError("record".into(), format!("record<{id}>")))?;
        let mut slots = record.slots;
        if let Some(entries) = &hash_entries
            && slots.len() >= 2
        {
            slots[1] = Value::list(
                entries
                    .iter()
                    .cloned()
                    .map(|(key, value)| Value::cons(key, value)),
            );
        }
        let test = slots
            .first()
            .and_then(|value| value.as_symbol().ok())
            .unwrap_or("eql")
            .to_string();
        let copy = self.create_record(&record.type_name, slots);
        if let (Some(entries), Value::Record(copy_id)) = (hash_entries, &copy) {
            self.replace_hash_table_runtime_entries(*copy_id, &test, entries);
        }
        // GNU `copy-sequence' copies the record verbatim, including a
        // class-object type tag.
        if self.is_class_object_tagged_record(id)
            && let Value::Record(copy_id) = &copy
        {
            self.mark_class_object_tagged_record(*copy_id);
        }
        Ok(copy)
    }

    pub(crate) fn find_class_state_name_by_record_id(&self, record_id: u64) -> Option<String> {
        self.find_class_state_by_record_id(record_id)
            .map(|state| state.name.clone())
    }

    // `aset' on a record's type slot: a symbol clears the class-object tag,
    // a class record sets it (GNU stores the tag value directly).
    pub(crate) fn retag_record(
        &mut self,
        id: u64,
        type_name: &str,
        class_object_tagged: bool,
    ) -> Result<(), LispError> {
        let Some(record) = self.find_record_mut(id) else {
            return Err(LispError::TypeError(
                "record".into(),
                format!("record<{id}>"),
            ));
        };
        record.type_name = type_name.to_string();
        if class_object_tagged {
            self.class_object_tagged_records.insert(id);
        } else {
            self.class_object_tagged_records.remove(&id);
        }
        Ok(())
    }

    pub(crate) fn has_lisp_macro(&self, name: &str) -> bool {
        self.macros_name_counts.contains_key(name)
            && self.macros.iter().any(|binding| binding.name == name)
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

    fn record_provide_in_load_history(&mut self, feature: &str) {
        if self.current_load_history_is_suppressed() {
            return;
        }
        let Some(current_load_list) = self.lookup_var("current-load-list", &Env::new()) else {
            return;
        };
        if current_load_list.is_nil() {
            return;
        }
        let entry = Value::cons(
            Value::Symbol("provide".into()),
            Value::Symbol(feature.to_string()),
        );
        let entries = current_load_list.to_vec().unwrap_or_default();
        if entries.iter().any(|item| item == &entry) {
            return;
        }
        // GNU LOADHIST_ATTACH conses onto the front: the source file name
        // stays the LAST element (`macroexp-file-name' reads it there).
        self.set_global_binding("current-load-list", Value::cons(entry, current_load_list));
    }

    fn record_require_in_load_history(&mut self, feature: &str) {
        if self.current_load_history_is_suppressed() {
            return;
        }
        let Some(current_load_list) = self.lookup_var("current-load-list", &Env::new()) else {
            return;
        };
        let Ok(entries) = current_load_list.to_vec() else {
            return;
        };
        // `require' only records dependencies while reading a file.  GNU
        // recognizes that state by the final string in current-load-list.
        if !matches!(
            entries.last(),
            Some(Value::String(_) | Value::StringObject(_))
        ) {
            return;
        }
        let entry = Value::cons(
            Value::Symbol("require".into()),
            Value::Symbol(feature.to_string()),
        );
        if entries.iter().any(|item| item == &entry) {
            return;
        }
        self.set_global_binding("current-load-list", Value::cons(entry, current_load_list));
    }

    pub(crate) fn record_definition_in_load_history(&mut self, kind: &str, name: &str) {
        if self.current_load_history_is_suppressed() {
            return;
        }
        let Some(current_load_list) = self.lookup_var("current-load-list", &Env::new()) else {
            return;
        };
        if current_load_list.is_nil() {
            return;
        }
        let entry = if kind == "defvar" {
            Value::Symbol(name.to_string())
        } else {
            Value::cons(
                Value::Symbol(kind.to_string()),
                Value::Symbol(name.to_string()),
            )
        };
        if current_load_list
            .to_vec()
            .is_ok_and(|items| items.iter().any(|item| item == &entry))
        {
            return;
        }
        // GNU's LOADHIST_ATTACH conses definitions onto the front.  The
        // source-file string therefore remains last until build_load_history
        // reverses the completed entry.
        self.set_global_binding("current-load-list", Value::cons(entry, current_load_list));
    }

    /// Remember the definition hidden by a `defalias'-style operation.
    ///
    /// GNU stores alternating (FILE DEFINITION) pairs.  When the same file
    /// defines a symbol repeatedly, unloading it must restore the definition
    /// from before that file first touched the symbol, not an intermediate
    /// definition from the same load.
    pub(crate) fn record_function_redefinition(&mut self, name: &str, old_definition: Value) {
        if old_definition.is_nil() || self.current_load_history_is_suppressed() {
            return;
        }
        let file = self
            .lookup_var("current-load-list", &Env::new())
            .and_then(|value| value.to_vec().ok())
            .and_then(|items| items.last().cloned())
            .filter(|value| matches!(value, Value::String(_) | Value::StringObject(_)))
            .unwrap_or(Value::Nil);
        let past = self
            .get_symbol_property(name, "function-history")
            .unwrap_or(Value::Nil);
        let Ok(mut entries) = past.to_vec() else {
            self.put_symbol_property(
                name,
                "function-history",
                Value::cons(file, Value::cons(old_definition, past)),
            );
            return;
        };

        if let Some(index) = (0..entries.len())
            .step_by(2)
            .find(|&index| entries[index] == file)
        {
            if index == 0 {
                return;
            }
            // (... OTHER-FILE DEF3 THIS-FILE DEF2 ...) becomes
            // (... OTHER-FILE DEF2 ...), matching add_to_function_history.
            entries.drain(index - 1..=index);
        }
        entries.insert(0, old_definition);
        entries.insert(0, file);
        self.put_symbol_property(name, "function-history", Value::list(entries));
    }

    /// Commit the definitions accumulated in `current-load-list`.
    ///
    /// GNU's `build_load_history' replaces every older entry for an entire
    /// file evaluation.  Keeping duplicate entries makes `unload-feature'
    /// remove only the newest one and leaves the previous definitions live.
    pub(crate) fn commit_entire_load_history(&mut self, filename: &str, current: Value) {
        let mut entry = current.to_vec().unwrap_or_default();
        entry.reverse();
        if entry.is_empty() {
            return;
        }

        let filename = Value::String(filename.to_string());
        let mut history = self
            .lookup_var("load-history", &Env::new())
            .and_then(|value| value.to_vec().ok())
            .unwrap_or_default();
        history.retain(|existing| match existing.car() {
            Ok(existing_filename) => existing_filename != filename,
            Err(_) => true,
        });
        history.insert(0, Value::list(entry));
        self.set_global_binding("load-history", Value::list(history));
    }

    pub fn unprovide_feature(&mut self, feature: &str) {
        self.provided_features.retain(|name| name != feature);
        self.set_global_binding("features", self.features_value());
    }

    pub(crate) fn provide_feature_with_after_load(
        &mut self,
        feature: &str,
    ) -> Result<Value, LispError> {
        self.provide_feature(feature);
        self.record_provide_in_load_history(feature);
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

    // Whether NAME has an interpreted (Lisp-defined) function binding,
    // as opposed to only a native dispatch arm.
    pub(crate) fn has_lisp_function(&self, name: &str) -> bool {
        self.functions_index.contains_key(name)
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

    pub(crate) fn combined_after_change(&self) -> Option<&CombinedAfterChangeState> {
        self.combined_after_change.as_ref()
    }

    pub(crate) fn record_combined_after_change(&mut self, buffer_id: u64, change: (i64, i64, i64)) {
        match self.combined_after_change.as_mut() {
            Some(pending) if pending.buffer_id == buffer_id => pending.changes.push(change),
            _ => {
                self.combined_after_change = Some(CombinedAfterChangeState {
                    buffer_id,
                    changes: vec![change],
                });
            }
        }
    }

    pub(crate) fn take_combined_after_change(&mut self) -> Option<CombinedAfterChangeState> {
        self.combined_after_change.take()
    }
}

fn repeated_directory_load_alias(target: &str) -> Option<String> {
    let (directory, file) = target.rsplit_once('/')?;
    let directory_name = directory.rsplit('/').next()?;
    let alias_file = file.strip_prefix(&format!("{directory_name}-"))?;
    Some(format!("{directory}/{alias_file}"))
}

pub(crate) fn load_source_stub_prefers_elc(path: &std::path::Path) -> bool {
    fs::metadata(path).is_ok_and(|metadata| metadata.len() == 0)
}
