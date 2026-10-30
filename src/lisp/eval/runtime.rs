use super::*;
use crate::lisp::types::Kind;
use crate::lisp::types::LispErrorKind;
use crate::lisp::types::RecordRef;

/// A standard hash table's entry found for a key: its index, key and value.
type HashEntry = (usize, Value, Value);

/// What names a record to `find_record': the object, or an id in this
/// state's registry (the side tables' key, until they live in the
/// object).
pub trait RecordKey {
    fn record_ref(self, interp: &Interpreter) -> Option<RecordRef>;
}

impl RecordKey for u64 {
    fn record_ref(self, interp: &Interpreter) -> Option<RecordRef> {
        interp.record_ref(self)
    }
}

impl RecordKey for &u64 {
    fn record_ref(self, interp: &Interpreter) -> Option<RecordRef> {
        interp.record_ref(*self)
    }
}

impl RecordKey for RecordRef {
    fn record_ref(self, _interp: &Interpreter) -> Option<RecordRef> {
        Some(self)
    }
}

impl RecordKey for &RecordRef {
    fn record_ref(self, _interp: &Interpreter) -> Option<RecordRef> {
        Some(*self)
    }
}

impl Interpreter {
    pub(crate) fn inhibit_garbage_collection(&mut self) {
        self.garbage_collection_inhibited = self.garbage_collection_inhibited.saturating_add(1);
    }

    pub(crate) fn allow_garbage_collection(&mut self) {
        debug_assert!(self.garbage_collection_inhibited > 0);
        self.garbage_collection_inhibited = self.garbage_collection_inhibited.saturating_sub(1);
    }

    pub(crate) fn garbage_collection_is_inhibited(&self) -> bool {
        self.garbage_collection_inhibited != 0
    }

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

    /// The id of the live buffer named NAME, without a copy of the name.
    pub(crate) fn find_buffer_id(&self, name: &str) -> Option<u64> {
        self.buffer_list
            .iter()
            .find(|(_, n)| n == name)
            .map(|(id, _)| *id)
    }

    /// Return the current buffer ID.
    pub fn current_buffer_id(&self) -> u64 {
        self.current_buffer_id
    }

    pub fn current_buffer(&self) -> std::cell::Ref<'_, crate::buffer::Buffer> {
        self.buffer.borrow()
    }

    pub fn set_current_load_file(&mut self, path: Option<String>) -> Option<String> {
        std::mem::replace(&mut self.current_load_file, path)
    }

    pub(crate) fn set_load_source_provenance_remap(
        &mut self,
        physical_root: PathBuf,
        provenance_root: PathBuf,
    ) {
        self.load_source_provenance_remap = Some((physical_root, provenance_root));
    }

    pub(crate) fn load_source_provenance_path(&self, path: &std::path::Path) -> PathBuf {
        let Some((physical_root, provenance_root)) = &self.load_source_provenance_remap else {
            return path.to_path_buf();
        };
        path.strip_prefix(physical_root)
            .map(|relative| provenance_root.join(relative))
            .unwrap_or_else(|_| path.to_path_buf())
    }

    pub(crate) fn resolve_load_target(
        &mut self,
        target: &str,
    ) -> Result<Option<PathBuf>, LispError> {
        crate::lisp::primitives::resolve_load_target_in_env(self, target, &Env::new())
    }

    #[cfg(test)]
    pub fn load_target(&mut self, target: &str) -> Result<PathBuf, LispError> {
        self.load_target_with_env(target, &Env::new())
    }

    /// lread.c save_match_data_load, the load Fautoload_do_load performs:
    /// the file the autoload names is loaded with the match data saved
    /// around it, so an autoload triggered between a `string-match' and
    /// its `match-end' (dired-aux's `cl-member' inside
    /// `dired--need-confirm-positions') leaves the caller's match data
    /// intact.
    pub(crate) fn load_autoload_target(
        &mut self,
        target: &str,
        env: &Env,
    ) -> Result<PathBuf, LispError> {
        let mut call_env = env.clone();
        let saved = crate::lisp::primitives::call(self, "match-data", &[], &mut call_env)?;
        // The saved data holds markers (a buffer match), and lread.c keeps
        // it on the specpdl (record_unwind_save_match_data) where the
        // collector reaches it; held here through the load, it is a
        // registered root, or a collection inside the load unchains its
        // markers and the restore signals.
        let result =
            self.with_lisp_stack_roots(&saved, |interp| interp.load_target_with_env(target, env));
        crate::lisp::primitives::call(self, "set-match-data", &[saved, Value::T], &mut call_env)?;
        result
    }

    pub(crate) fn load_target_with_env(
        &mut self,
        target: &str,
        env: &Env,
    ) -> Result<PathBuf, LispError> {
        let (_, found) = primitives::load_file(
            self,
            &[Value::string(target), Value::Nil, Value::T],
            &mut env.clone(),
        )?;
        Ok(PathBuf::from(primitives::string_text(&found)?))
    }

    /// Fload's native branch, after descriptor closure and the outer load
    /// bindings. The common Fload owner runs the final hook after unwinding.
    pub(crate) fn load_native_resolved_path(
        &mut self,
        path: &std::path::Path,
        history_filename: &str,
        env: &Env,
    ) -> Result<Value, LispError> {
        self.load_foreign_resolved_path(path, history_filename, env, true)
    }

    pub(crate) fn load_foreign_resolved_path(
        &mut self,
        path: &std::path::Path,
        history_filename: &str,
        env: &Env,
        native: bool,
    ) -> Result<Value, LispError> {
        let filename = path.to_str().ok_or_else(|| {
            LispError::SignalValue(Value::list([
                Value::symbol("file-error"),
                Value::string("Invalid native Lisp filename"),
            ]))
        })?;
        let previous_file = self.set_current_load_file(Some(history_filename.to_owned()));
        let mut load_environment = env.clone();
        let mut restores = Vec::with_capacity(5);
        for (name, value) in [
            ("load-file-name", Value::string(history_filename)),
            ("load-true-file-name", Value::string(filename)),
            ("inhibit-file-name-operation", Value::Nil),
            ("load-in-progress", Value::T),
            (
                "current-load-list",
                Value::list([Value::string(history_filename)]),
            ),
        ] {
            match self.bind_special_variable(name, value, &mut load_environment) {
                Ok(restore) => restores.push(restore),
                Err(error) => {
                    while let Some(restore) = restores.pop() {
                        let _ = self.restore_special_binding(restore, &mut load_environment);
                    }
                    self.set_current_load_file(previous_file);
                    return Err(error);
                }
            }
        }
        let mut result = if native {
            primitives::native_elisp_load(
                self,
                &Value::string(filename),
                false,
                &mut load_environment,
            )
        } else {
            crate::lisp::modules::load(self, &Value::string(filename), &mut load_environment)
        };
        if result.is_ok() {
            let current = self
                .lookup_var("current-load-list", &load_environment)
                .unwrap_or_else(|| Value::list([Value::string(history_filename)]));
            self.commit_entire_load_history(&Value::string(history_filename), current);
        }
        while let Some(restore) = restores.pop() {
            if let Err(error) = self.restore_special_binding(restore, &mut load_environment) {
                result = Err(error);
            }
        }
        self.set_current_load_file(previous_file);
        result.map(|_| Value::T)
    }

    /// The quote characters `error' will requote a message with, per the
    /// effective `text-quoting-style' (doc.c:679, doprnt.c:490).
    fn effective_quote_pair(&self) -> (char, char) {
        match crate::lisp::primitives::values::effective_text_quoting_style(self) {
            "curve" => ('\u{2018}', '\u{2019}'),
            "straight" => ('\'', '\''),
            _ => ('`', '\''),
        }
    }

    fn with_require_nesting<T>(
        &mut self,
        feature: &str,
        quotes: (char, char),
        load: impl FnOnce(&mut Self) -> Result<T, LispError>,
    ) -> Result<T, LispError> {
        // GNU permits the same feature to re-enter four active `require'
        // loads.  The fifth call sees four existing entries and signals.
        // Legitimate libraries use this bounded recursion with an early
        // `provide' to break dependency cycles.
        let nesting = self
            .require_nesting
            .iter()
            .filter(|nested| nested.as_str() == feature)
            .count();
        if nesting > 3 {
            // fns.c:3762 signals through `error' too, so this message is
            // requoted by the same rule as the one below.
            let (open, close) = quotes;
            return Err(LispError::Signal(format!(
                "Recursive {open}require{close} for feature {open}{feature}{close}"
            )));
        }

        self.require_nesting.push(feature.to_string());
        let result = load(self);
        let popped = self.require_nesting.pop();
        debug_assert_eq!(popped.as_deref(), Some(feature));
        result
    }

    pub(crate) fn require_feature_with_target(
        &mut self,
        symbol: SymbolName,
        target: Option<&str>,
        env: &mut Env,
    ) -> Result<Value, LispError> {
        // GNU records a file's dependency even when FEATURE was loaded
        // already.  `file-dependents' and `unload-feature' derive their
        // dependency graph from these entries.
        self.record_require_in_load_history(symbol);
        if self.has_feature_symbol(symbol) {
            return Ok(Value::Symbol(symbol));
        }
        let feature = symbol.as_str();
        let load_target = target.unwrap_or(feature);
        let (open, close) = self.effective_quote_pair();
        let loaded = self.with_require_nesting(feature, (open, close), |interp| {
            primitives::load_file(
                interp,
                &[
                    Value::string(load_target),
                    Value::Nil,
                    Value::T,
                    Value::Nil,
                    if target.is_none() {
                        Value::T
                    } else {
                        Value::Nil
                    },
                ],
                env,
            )
            .map(|(value, _)| value)
        })?;
        if loaded.is_nil() {
            return Ok(Value::Nil);
        }
        // fns.c Frequire signals through `error', whose format string is
        // processed with `format-message' semantics: the quotes follow the
        // effective `text-quoting-style', which is grave in a non-UTF-8
        // locale (the compatibility harness runs LANG=C).
        if !self.has_feature_symbol(symbol) {
            // GNU fns.c:Frequire signals unconditionally when the loaded
            // file did not provide FEATURE, naming the file `load-history'
            // records last (the resolved path, not the feature), or
            // "Required feature ... was not provided" when there is none.
            let loaded_file = self
                .lookup_var("load-history", env)
                .and_then(|history| history.car_if_list())
                .and_then(|entry| entry.car_if_list())
                .and_then(|file| crate::lisp::primitives::string_like(&file).map(|s| s.text));
            return Err(LispError::Signal(match loaded_file {
                Some(file) => {
                    format!("Loading file {file} failed to provide feature {open}{feature}{close}")
                }
                None => format!("Required feature {open}{feature}{close} was not provided"),
            }));
        }
        Ok(Value::Symbol(symbol))
    }

    /// buffer.c:decode_buffer: the current buffer for nil, else BUFFER
    /// itself, live or killed (CHECK_BUFFER).
    pub(crate) fn decode_buffer(
        &self,
        buffer: Option<&Value>,
    ) -> Result<crate::lisp::types::BufferRef, LispError> {
        match buffer.map(|value| value.kind()) {
            None | Some(Kind::Nil) => Ok(self.buffer),
            Some(Kind::Buffer(buffer)) => Ok(buffer),
            Some(_) => Err(LispError::WrongTypeArgument(
                "bufferp".into(),
                *buffer.expect("a value was given"),
            )),
        }
    }

    /// buffer.c:Fget_buffer: BUFFER-OR-NAME itself when it is a buffer,
    /// live or killed, else the live buffer of that name (CHECK_STRING),
    /// or None.
    pub(crate) fn get_buffer(
        &self,
        buffer_or_name: &Value,
    ) -> Result<Option<crate::lisp::types::BufferRef>, LispError> {
        if let Kind::Buffer(buffer) = buffer_or_name.kind() {
            return Ok(Some(buffer));
        }
        let id = primitives::with_string_text(buffer_or_name, |name| self.find_buffer_id(name))
            .ok_or_else(|| LispError::WrongTypeArgument("stringp".into(), *buffer_or_name))?;
        Ok(id.and_then(|id| self.buffer_object(id)))
    }

    /// Fget_buffer, with buffer.c:nsberror when there is no such buffer.
    pub(crate) fn get_buffer_or_error(
        &self,
        buffer_or_name: &Value,
    ) -> Result<crate::lisp::types::BufferRef, LispError> {
        self.get_buffer(buffer_or_name)?
            .ok_or_else(|| nsberror(buffer_or_name))
    }

    /// BUFFER's id when it is live (BUFFER_LIVE_P), else None.
    pub(crate) fn live_buffer_id(&self, buffer: crate::lisp::types::BufferRef) -> Option<u64> {
        self.buffer_object(buffer.id)
            .is_some_and(|live| live.ptr_eq(&buffer))
            .then_some(buffer.id)
    }

    /// Resolve a Lisp string-or-buffer value to a live buffer ID.
    pub fn resolve_buffer_id(&self, value: &Value) -> Result<u64, LispError> {
        match value.kind() {
            Kind::Buffer(buffer)
                if self
                    .buffer_object(buffer.id)
                    .is_some_and(|live| live.ptr_eq(&buffer)) =>
            {
                Ok(buffer.id)
            }
            // buffer.c:Fset_buffer checks the given object's liveness.
            // A reused name must never select a different allocation.
            Kind::Buffer(_) => Err(LispError::Signal("Selecting deleted buffer".into())),
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
        // current buffer's slot (a `let' of it there included).
        let inherited_directory = self
            .symbol_value_key(cached_symbol!("default-directory"))
            .unwrap_or(Value::Nil);
        let id = self.alloc_buffer_id();
        let mut buffer = crate::buffer::Buffer::new(name);
        // buffer.c:Fget_buffer_create: `bset_last_name (b, name)'.
        buffer.last_name = Some(name.to_string());
        buffer.text_conversion_style = self.native_text_conversion_style_default();
        // buffer.c:Fget_buffer_create: `bset_undo_list (b, SREF (name, 0)
        // != ' ' ? Qnil : Qt)', no undo kept in a buffer whose name starts
        // with a space (`with-temp-buffer's, the printer's, `load's).
        if name.starts_with(' ') {
            buffer.set_undo_list(Value::T);
        }
        self.inactive_buffers
            .push((id, crate::lisp::types::BufferRef::new(id, buffer)));
        self.buffer_list.push((id, name.to_string()));
        // Every always-local slot is the new buffer's own from the start,
        // so neither a `let' in the creating buffer nor a `setq-default'
        // (buffer_defaults) reaches it.
        self.reset_always_local_slots(id, inherited_directory);
        primitives::casetab::reset_buffer_case_tables(self, id);
        (id, name.to_string())
    }

    /// Switch the current buffer and display it in the selected window.
    pub fn switch_to_buffer_id(&mut self, id: u64) -> Result<(), LispError> {
        self.show_buffer_in_selected_window(id)
    }

    /// indent.c:invalidate_current_column: no recorded column answers.
    pub(crate) fn invalidate_current_column(&mut self) {
        self.last_known_column.1 = 0;
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
        // buffer.c:set_buffer_internal_2 invalidates the indentation cache.
        self.invalidate_current_column();
        let current_id = self.current_buffer_id;
        let current_point = self.buffer.borrow().point();
        if self.selected_window_buffer_id() == current_id
            && let Some(window) = self.find_record_mut(self.selected_window_id)
        {
            if window.slots.len() <= primitives::WINDOW_POINT_SLOT {
                window
                    .slots
                    .resize(primitives::WINDOW_POINT_SLOT + 1, Value::Nil);
            }
            window.slots[primitives::WINDOW_POINT_SLOT] = Value::Integer(current_point as i64);
        }
        let (_, next_buffer) = self.inactive_buffers.swap_remove(pos);
        let current_buffer = std::mem::replace(&mut self.buffer, next_buffer);
        self.inactive_buffers.push((current_id, current_buffer));
        self.current_buffer_id = id;
        if !self.indirect_buffers.is_empty() {
            self.share_undo_list_with_base(current_id, id);
        }
        // A localized GNU forwarding symbol loads the newly current
        // buffer's cell into its C variable during a buffer switch.
        self.refresh_forwarded_eval_cells_after_switch(current_id);
        Ok(())
    }

    /// buffer.c:set_buffer_internal_2's sharing of a base buffer's undo
    /// list: the list of OLD, when indirect, goes back into its base, and
    /// NEW, when indirect, takes its base's.
    fn share_undo_list_with_base(&mut self, old: u64, new: u64) {
        if let Some(base) = self.buffer_base_id(old) {
            let list = self.get_buffer_by_id(old).map(|buffer| buffer.undo_list());
            if let Some(list) = list
                && let Some(mut base) = self.get_buffer_by_id_mut(base)
            {
                base.set_undo_list(list);
            }
        }
        if let Some(base) = self.buffer_base_id(new) {
            let list = self.get_buffer_by_id(base).map(|buffer| buffer.undo_list());
            if let Some(list) = list {
                self.buffer.borrow_mut().set_undo_list(list);
            }
        }
    }

    /// buffer.c:record_buffer: a buffer to the front of the buffer list,
    /// and of the selected frame's, out of the frame's buried buffers.
    pub fn record_buffer_front(&mut self, id: u64) {
        if let Some(index) = self
            .buffer_list
            .iter()
            .position(|(buffer_id, _)| *buffer_id == id)
        {
            let entry = self.buffer_list.remove(index);
            self.buffer_list.insert(0, entry);
        }
        let mut frame = self.selected_frame_id.borrow_mut();
        frame.buffer_list.retain(|buffer| *buffer != id);
        frame.buffer_list.insert(0, id);
        frame.buried_buffer_list.retain(|buffer| *buffer != id);
    }

    /// Fbury_buffer_internal's update of the selected frame's lists: the
    /// buffer leaves the frame's buffers for the front of its buried ones.
    pub(crate) fn bury_buffer_in_selected_frame(&mut self, id: u64) {
        let mut frame = self.selected_frame_id.borrow_mut();
        frame.buffer_list.retain(|buffer| *buffer != id);
        frame.buried_buffer_list.retain(|buffer| *buffer != id);
        frame.buried_buffer_list.insert(0, id);
    }

    /// frame.c:frames_discard_buffer: a killed buffer leaves every frame's
    /// lists.
    fn frames_discard_buffer(&mut self, id: u64) {
        for frame in &self.frame_states {
            let mut frame = frame.borrow_mut();
            frame.buffer_list.retain(|buffer| *buffer != id);
            frame.buried_buffer_list.retain(|buffer| *buffer != id);
        }
    }

    pub fn switch_to_buffer_id_preserving_window_history(
        &mut self,
        id: u64,
    ) -> Result<(), LispError> {
        self.show_buffer_in_selected_window(id)
    }

    /// Make ID current and show it in the selected window.
    fn show_buffer_in_selected_window(&mut self, id: u64) -> Result<(), LispError> {
        let current_id = self.current_buffer_id;
        let selected_window_already_displays_target = self
            .find_record(self.selected_window_id)
            .and_then(|window| window.slots.first())
            .and_then(|value| value.integer_value())
            == Some(id as i64);
        if id == current_id && selected_window_already_displays_target {
            return Ok(());
        }
        if id != current_id {
            self.set_current_buffer_id(id)?;
        }
        if selected_window_already_displays_target {
            return Ok(());
        }
        let window_id = self.selected_window_id;
        self.unshow_window_buffer(window_id);
        self.show_buffer_in_window(window_id, id, false);
        Ok(())
    }

    pub fn selected_window_value(&self) -> Value {
        self.record_value(self.selected_window_id)
    }

    pub fn selected_window_id(&self) -> u64 {
        self.selected_window_id
    }

    pub(crate) fn set_selected_window_id(&mut self, id: u64) {
        self.selected_window_id = id;
        self.selected_frame_id.borrow_mut().selected_window = self.record_ref(id);
    }

    pub(crate) fn root_window_value(&self) -> Value {
        self.selected_frame_id
            .borrow()
            .root_window
            .map(Value::Record)
            .unwrap_or(Value::Nil)
    }

    pub(crate) fn set_root_window_id(&mut self, id: u64) {
        self.selected_frame_id.borrow_mut().root_window = self.record_ref(id);
    }

    pub(crate) fn minibuffer_window_id(&self) -> u64 {
        self.selected_frame_id.borrow().minibuffer_window_id()
    }

    pub(crate) fn minibuffer_window_value(&self) -> Value {
        self.selected_frame_id
            .borrow()
            .minibuffer_window
            .map(Value::Record)
            .unwrap_or(Value::Nil)
    }

    pub(crate) fn set_minibuffer_window_id(&mut self, id: u64) {
        self.selected_frame_id.borrow_mut().minibuffer_window = self.record_ref(id);
    }

    pub(crate) fn minibuffer_selected_window_id(&self) -> Option<u64> {
        self.minibuffer_selected_window_id
    }

    pub(crate) fn replace_minibuffer_selected_window_id(
        &mut self,
        window_id: Option<u64>,
    ) -> Option<u64> {
        std::mem::replace(&mut self.minibuffer_selected_window_id, window_id)
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
        self.record_value(self.old_selected_window_id)
    }

    pub(crate) fn window_use_time(&self, id: u64) -> i64 {
        self.find_record(id)
            .and_then(|record| record.slots.get(primitives::WINDOW_USE_TIME_SLOT))
            .and_then(|value| value.integer_value())
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
            .and_then(|value| value.integer_value())
            .map(|value| value.max(0) as u64)
            .unwrap_or(self.current_buffer_id)
    }

    pub fn buffer_bounds_by_id(&self, id: u64) -> Option<(usize, usize)> {
        self.get_buffer_by_id(id)
            .map(|buffer| (buffer.point_min(), buffer.point_max()))
    }

    /// The selected window's start marker position: Fwindow_start reads
    /// the marker as is, so a start outside a later narrowing stays there
    /// until redisplay moves it.
    pub fn selected_window_start(&self) -> usize {
        let buffer_id = self.selected_window_buffer_id();
        let point_min = self.buffer_bounds_by_id(buffer_id).map_or_else(
            || self.buffer.borrow().point_min(),
            |(point_min, _)| point_min,
        );
        let z = if buffer_id == self.current_buffer_id {
            self.buffer.borrow().size_total() + 1
        } else {
            self.get_buffer_by_id(buffer_id)
                .map_or(point_min, |buffer| buffer.size_total() + 1)
        };
        self.find_record(self.selected_window_id)
            .and_then(|record| record.slots.get(1))
            .and_then(|value| value.integer_value())
            .map(|value| value.clamp(1, z as i64) as usize)
            .unwrap_or(point_min)
    }

    /// xdisp.c redisplay_other_windows: the next redisplay considers
    /// every window, not just the selected one.
    pub(crate) fn redisplay_other_windows(&mut self) {
        self.windows_or_buffers_changed = true;
    }

    /// buffer.c buffer_window_count: the live windows showing BUFFER_ID.
    pub(crate) fn buffer_window_count(&self, buffer_id: u64) -> usize {
        primitives::live_ordinary_window_ids(self)
            .into_iter()
            .filter(|window_id| self.window_record_buffer_id(*window_id) == Some(buffer_id))
            .count()
    }

    /// xdisp.c wset_redisplay: marking a window other than the selected
    /// one for redisplay makes redisplay consider all windows.
    pub(crate) fn wset_redisplay(&mut self, window_id: u64) {
        if window_id != self.selected_window_id {
            self.redisplay_other_windows();
        }
    }

    pub fn set_selected_window_start(&mut self, start: usize) {
        let (point_min, point_max) = self
            .buffer_bounds_by_id(self.selected_window_buffer_id())
            .unwrap_or((
                self.buffer.borrow().point_min(),
                self.buffer.borrow().point_max(),
            ));
        let start = start.clamp(point_min, point_max) as i64;
        if let Some(window) = self.find_record_mut(self.selected_window_id) {
            if window.slots.len() < 2 {
                window.slots.resize(2, Value::Nil);
            }
            window.slots[1] = Value::Integer(start);
        }
    }

    /// Show BUFFER_ID in the selected window (set-window-buffer without
    /// its Lisp bookkeeping); showing the buffer it already shows changes
    /// nothing.
    pub fn set_selected_window_buffer_id(&mut self, buffer_id: u64) {
        let window_id = self.selected_window_id;
        if self.window_record_buffer_id(window_id) == Some(buffer_id) {
            return;
        }
        self.unshow_window_buffer(window_id);
        self.show_buffer_in_window(window_id, buffer_id, false);
    }

    fn window_record_buffer_id(&self, window_id: u64) -> Option<u64> {
        self.find_record(window_id)
            .and_then(|window| window.slots.get(primitives::WINDOW_BUFFER_SLOT))
            .and_then(|value| value.integer_value())
            .map(|id| id.max(0) as u64)
    }

    /// window.c unshow_buffer: WINDOW_ID stops showing its buffer, which
    /// remembers the window's start, and takes the window's point unless
    /// the selected window shows it (that point is the buffer's own).
    pub(crate) fn unshow_window_buffer(&mut self, window_id: u64) {
        let Some(buffer_id) = self.window_record_buffer_id(window_id) else {
            return;
        };
        if !self.has_buffer_id(buffer_id) {
            return;
        }
        let slot = |interp: &Self, index: usize| {
            interp
                .find_record(window_id)
                .and_then(|window| window.slots.get(index))
                .and_then(|value| value.integer_value())
        };
        if let Some(start) = slot(self, primitives::WINDOW_START_SLOT) {
            self.buffer_last_window_start
                .insert(buffer_id, start.max(1) as usize);
        }
        if buffer_id != self.selected_window_buffer_id()
            && let Some(point) = slot(self, primitives::WINDOW_POINT_SLOT)
        {
            let point = point.max(1) as usize;
            if buffer_id == self.current_buffer_id {
                let mut buffer = self.buffer.borrow_mut();
                let point = point.clamp(buffer.point_min(), buffer.point_max());
                buffer.goto_char(point);
            } else if let Some(mut buffer) = self.get_buffer_by_id_mut(buffer_id) {
                let point = point.clamp(buffer.point_min(), buffer.point_max());
                buffer.goto_char(point);
            }
        }
    }

    /// window.c set_window_buffer's window state: WINDOW_ID shows
    /// BUFFER_ID from the buffer's last window start, with the buffer's
    /// point; KEEP (KEEP-MARGINS on the buffer the window already shows)
    /// leaves the start, point and hscroll alone.
    pub(crate) fn show_buffer_in_window(&mut self, window_id: u64, buffer_id: u64, keep: bool) {
        let (point_min, point_max, point) = if buffer_id == self.current_buffer_id {
            let buffer = self.buffer.borrow();
            (buffer.point_min(), buffer.point_max(), buffer.point())
        } else {
            self.get_buffer_by_id(buffer_id)
                .map(|buffer| (buffer.point_min(), buffer.point_max(), buffer.point()))
                .unwrap_or((1, 1, 1))
        };
        let start = self
            .buffer_last_window_start
            .get(&buffer_id)
            .copied()
            .unwrap_or(1)
            .clamp(point_min, point_max);
        if let Some(window) = self.find_record_mut(window_id) {
            if window.slots.len() <= primitives::WINDOW_START_AT_LINE_BEG_SLOT {
                window
                    .slots
                    .resize(primitives::WINDOW_START_AT_LINE_BEG_SLOT + 1, Value::Nil);
            }
            window.slots[primitives::WINDOW_BUFFER_SLOT] = Value::Integer(buffer_id as i64);
            window.slots[primitives::WINDOW_END_POS_SLOT] = Value::Integer(0);
            if !keep {
                window.slots[primitives::WINDOW_HSCROLL_SLOT] = Value::Integer(0);
                window.slots[primitives::WINDOW_MIN_HSCROLL_SLOT] = Value::Integer(0);
                window.slots[primitives::WINDOW_SUSPEND_AUTO_HSCROLL_SLOT] = Value::Nil;
                window.slots[primitives::WINDOW_POINT_SLOT] = Value::Integer(point as i64);
                window.slots[primitives::WINDOW_OLD_POINT_SLOT] = Value::Integer(point as i64);
                window.slots[primitives::WINDOW_START_SLOT] = Value::Integer(start as i64);
                window.slots[primitives::WINDOW_START_AT_LINE_BEG_SLOT] = Value::Nil;
                window.slots[primitives::WINDOW_FORCE_START_SLOT] = Value::Nil;
                window.slots[primitives::WINDOW_BASE_LINE_SLOT] = Value::Nil;
            }
        }
        self.wset_redisplay(window_id);
    }

    pub(crate) fn selected_frame_value(&self) -> Value {
        Value::Frame(self.selected_frame_id)
    }
    pub(crate) fn old_selected_frame_value(&self) -> Value {
        Value::Frame(self.old_selected_frame_id)
    }
    pub(crate) fn frame_is_live(&self, frame: crate::lisp::types::FrameRef) -> bool {
        frame.is_live()
    }
    pub fn frame_width(&self) -> i64 {
        self.selected_frame_id.borrow().width.max(1)
    }

    pub fn frame_height(&self) -> i64 {
        self.selected_frame_id.borrow().height.max(1)
    }

    /// Adopt the live terminal's display capabilities: color count and
    /// background class drive `defface' spec selection exactly as GNU's
    /// terminal-init path does (an xterm-family tty answers 8 colors and
    /// a light background).  Batch sessions keep GNU's dumb-terminal
    /// answers (0 colors, dark, mono); only the terminal frontend calls
    /// this.
    pub fn set_tty_display_colors(&mut self, color_cells: i64) {
        let id = self.selected_terminal_id();
        self.terminals
            .iter_mut()
            .find(|terminal| terminal.id == id)
            .expect("decoded terminal has state")
            .borrow_mut()
            .colors = color_cells.max(0);
    }

    pub(crate) fn tty_display_color_cells(&self) -> i64 {
        self.terminal_state(self.selected_terminal_id())
            .map_or(0, |terminal| terminal.colors)
    }

    /// term.c keeps the terminal's type string (tty->type, from $TERM);
    /// `tty-type' answers it for live tty frames and nil in batch.
    pub fn set_tty_terminal_type(&mut self, terminal_type: Option<String>) {
        let id = self.selected_terminal_id();
        let terminal = self
            .terminals
            .iter_mut()
            .find(|terminal| terminal.id == id)
            .expect("decoded terminal has state");
        let mut terminal = terminal.borrow_mut();
        terminal.kind = terminal_type;
        #[cfg(unix)]
        if terminal.device.is_none() && terminal.kind.is_some() {
            // The startup terminal is the existing standard-input device.
            // Store its actual device name so init_tty reuses it by name.
            let pointer = unsafe { libc::ttyname(libc::STDIN_FILENO) };
            if !pointer.is_null() {
                terminal.name = unsafe { std::ffi::CStr::from_ptr(pointer) }
                    .to_string_lossy()
                    .into_owned();
            }
        }
    }

    pub(crate) fn tty_terminal_type(&self) -> Option<std::cell::Ref<'_, str>> {
        std::cell::Ref::filter_map(
            self.terminal_state(self.selected_terminal_id())?,
            |terminal| terminal.kind.as_deref(),
        )
        .ok()
    }

    /// dispnew.c:init_display_interactive: the initial frame moves onto
    /// the terminal, change_frame_size giving it the terminal's size at
    /// once (the bars of make_initial_frame above its windows).  Only the
    /// terminal frontend calls this; batch frames keep their dumb 80x25.
    pub fn set_tty_frame_size(&mut self, width: i64, height: i64) {
        let frame = self.selected_frame_id;
        frame.borrow_mut().tty_sized = true;
        let mut env = crate::lisp::types::Env::new();
        let _ = primitives::change_frame_size(self, frame, width, height, false, false, &mut env);
    }

    /// dispnew.c:handle_window_change_signal: the terminal's size changed;
    /// each frame on it takes the new size at the next
    /// do_pending_window_change.
    pub(crate) fn tty_window_change(&mut self, width: i64, height: i64) {
        let terminal = self.selected_terminal_id();
        let frames: Vec<_> = self
            .frame_states
            .iter()
            .copied()
            .filter(|frame| {
                frame.is_live()
                    && frame.borrow().tty_sized
                    && frame.borrow().terminal.map(|terminal| terminal.id) == Some(terminal)
            })
            .collect();
        let mut env = crate::lisp::types::Env::new();
        for frame in frames {
            let _ =
                primitives::change_frame_size(self, frame, width, height, false, true, &mut env);
        }
    }

    pub(crate) fn frame_text_height(&self) -> i64 {
        self.selected_frame_id.borrow().text_height.max(1)
    }

    pub(crate) fn frame_parameter_override(&self, name: &str) -> Option<Value> {
        self.selected_frame_id
            .borrow()
            .parameter_overrides
            .iter()
            .find(|(parameter, _)| parameter == name)
            .map(|(_, value)| *value)
    }

    pub(crate) fn frame_name_value(&self) -> Value {
        self.selected_frame_id.name.get()
    }

    pub(crate) fn frame_and_buffer_state(&self) -> Value {
        self.frame_and_buffer_state
    }

    pub(crate) fn set_frame_and_buffer_state(&mut self, state: Value) {
        self.frame_and_buffer_state = state;
    }

    pub(crate) fn snapshot_window_configuration(&self) -> WindowConfigurationSnapshot {
        self.snapshot_frame_window_configuration(self.selected_frame_id)
    }

    fn frame_window_records(
        &self,
        frame_id: crate::lisp::types::FrameRef,
    ) -> impl Iterator<Item = RecordRef> + '_ {
        // window.c visits windows, not every Lisp pseudovector. Until
        // windows have their own allocation type, use the existing window
        // index and read each authoritative record. This creates no new
        // registry and retains the same record-id order, including deleted
        // windows, as the former full-record scan.
        self.record_ids_by_type_index
            .get("window")
            .into_iter()
            .flatten()
            .filter_map(|id| self.record_ref(*id))
            .filter(move |record| {
                record.kind == RecordKind::Window
                    && record.slots.get(primitives::WINDOW_FRAME_SLOT)
                        == Some(&Value::Frame(frame_id))
            })
    }

    fn snapshot_frame_window_configuration(
        &self,
        frame_id: crate::lisp::types::FrameRef,
    ) -> WindowConfigurationSnapshot {
        let frame = frame_id.borrow();
        WindowConfigurationSnapshot {
            frame_id,
            selected_frame_id: self.selected_frame_id,
            current_buffer_id: self.current_buffer_id(),
            selected_window_id: frame.selected_window_id(),
            selected_window_slots: self
                .find_record(frame.selected_window_id())
                .map(|record| record.slots.clone())
                .unwrap_or_default(),
            window_records: self
                .frame_window_records(frame_id)
                .map(|record| (record.id, record.slots.clone()))
                .collect(),
            root_window_id: frame.root_window_id(),
            frame_width: frame.width,
            frame_height: frame.height,
        }
    }

    pub(crate) fn window_configuration_value(
        &mut self,
        frame_id: crate::lisp::types::FrameRef,
    ) -> Value {
        let snapshot = self.snapshot_frame_window_configuration(frame_id);
        // The windows' slots are kept in vectors (window.c's saved_window
        // array holds each window's saved state in place): one allocation a
        // window, read back by `window_configuration_snapshot_from_value'
        // without a list walk.
        self.create_pseudovector(
            RecordKind::WindowConfiguration,
            "window-configuration",
            vec![
                Value::Integer(snapshot.current_buffer_id as i64),
                Value::Integer(snapshot.selected_window_id as i64),
                Value::vector(snapshot.selected_window_slots),
                Value::Integer(snapshot.frame_width),
                Value::Integer(snapshot.frame_height),
                self.record_value(snapshot.root_window_id),
                Value::vector(snapshot.window_records.into_iter().map(|(id, slots)| {
                    Value::cons(Value::Integer(id as i64), Value::vector(slots))
                })),
                Value::Frame(frame_id),
                Value::Frame(snapshot.selected_frame_id),
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
        if !self.frame_is_live(snapshot.frame_id) {
            return Ok(false);
        }
        self.restore_window_configuration(snapshot)?;
        Ok(true)
    }

    fn window_configuration_snapshot_from_value(
        &self,
        value: &Value,
    ) -> Option<WindowConfigurationSnapshot> {
        let Kind::Record(id) = value.kind() else {
            return None;
        };
        let record = self
            .find_record(id)
            .filter(|record| record.kind == RecordKind::WindowConfiguration)?;
        let slots = &record.slots;
        let vector_slots = |value: &Value| match value.kind() {
            Kind::Vector(vector) => Some(vector.slots().collect::<Vec<_>>()),
            _ => None,
        };
        let selected_window_id = slots
            .get(1)
            .and_then(|value| value.integer_value())
            .unwrap_or_default() as u64;
        let selected_window_slots = slots.get(2).and_then(vector_slots).unwrap_or_default();
        let window_records = slots
            .get(6)
            .and_then(|value| match value.kind() {
                Kind::Vector(records) => Some(
                    records
                        .slots()
                        .filter_map(|record| {
                            let (id, slots) = record.cons_values()?;
                            Some((id.integer_value()?.max(0) as u64, vector_slots(&slots)?))
                        })
                        .collect(),
                ),
                _ => None,
            })
            .unwrap_or_else(|| vec![(selected_window_id, selected_window_slots.clone())]);
        Some(WindowConfigurationSnapshot {
            frame_id: slots.get(7).and_then(|value| {
                if let Kind::Frame(id) = value.kind() {
                    Some(id)
                } else {
                    None
                }
            })?,
            selected_frame_id: slots.get(8).and_then(|value| {
                if let Kind::Frame(id) = value.kind() {
                    Some(id)
                } else {
                    None
                }
            })?,
            current_buffer_id: slots
                .first()
                .and_then(|value| value.integer_value())
                .unwrap_or_default() as u64,
            selected_window_id,
            selected_window_slots,
            window_records,
            root_window_id: slots
                .get(5)
                .and_then(|value| match value.kind() {
                    Kind::Record(id) => Some(id.id),
                    _ => None,
                })
                .unwrap_or(selected_window_id),
            frame_width: slots
                .get(3)
                .and_then(|value| value.integer_value())
                .unwrap_or(80),
            frame_height: slots
                .get(4)
                .and_then(|value| value.integer_value())
                .unwrap_or(24),
        })
    }

    /// Whether `window_configuration_snapshot_from_value' would find a
    /// configuration in VALUE: a window-configuration record whose frame
    /// slots (7 and 8) hold frames, the only parts it requires; the
    /// windows' slot vectors it copies are not built for the test.
    pub(crate) fn is_window_configuration_value(&self, value: &Value) -> bool {
        let Kind::Record(id) = value.kind() else {
            return false;
        };
        self.find_record(id)
            .filter(|record| record.kind == RecordKind::WindowConfiguration)
            .is_some_and(|record| {
                let frame = |index: usize| {
                    record
                        .slots
                        .get(index)
                        .is_some_and(|slot| matches!(slot.kind(), Kind::Frame(_)))
                };
                frame(7) && frame(8)
            })
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
        // window.c compare_window_configurations: the frame settings, then
        // each saved window by its layout alone (point, the start, hscroll
        // and redisplay's own state are ignored).
        let (left, right) = (saved_windows(&left), saved_windows(&right));
        Ok(left.0 == right.0 && left.1 == right.1)
    }

    pub(crate) fn restore_window_configuration(
        &mut self,
        snapshot: WindowConfigurationSnapshot,
    ) -> Result<(), LispError> {
        if !self.frame_is_live(snapshot.frame_id) {
            return Ok(());
        }
        // A frame has few windows: the saved ones are found by a search.
        let saved_windows = snapshot.window_records;
        let windows: Vec<_> = self.frame_window_records(snapshot.frame_id).collect();
        for window in windows {
            let Some(record) = self.find_record_mut(window) else {
                continue;
            };
            if let Some((_, slots)) = saved_windows.iter().find(|(id, _)| *id == record.id) {
                record.slots.clone_from(slots);
            } else {
                record
                    .slots
                    .resize(primitives::WINDOW_FRAME_SLOT + 1, Value::Nil);
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
        snapshot.frame_id.borrow_mut().root_window = self.record_ref(snapshot.root_window_id);
        self.note_selected_frame(snapshot.frame_id);
        self.set_selected_window_id(snapshot.selected_window_id);
        // GNU records frame dimensions for configuration equality, but
        // `set-window-configuration' does not rewind a frame resize:
        // adjust_frame_size (f, -1, -1, 4, ...) fits the restored tree to
        // the frame's current size.
        let _ = primitives::adjust_frame_size(
            self,
            snapshot.frame_id,
            -1,
            -1,
            4,
            false,
            &mut crate::lisp::types::Env::new(),
        );
        if self.frame_is_live(snapshot.selected_frame_id) {
            self.note_selected_frame(snapshot.selected_frame_id);
            let selected_window = self.selected_frame_id.borrow().selected_window_id();
            self.set_selected_window_id(selected_window);
        }
        if self.has_buffer_id(snapshot.current_buffer_id) {
            self.set_current_buffer_id(snapshot.current_buffer_id)?;
        }
        Ok(())
    }

    /// Remove a non-current buffer from the live buffer list.
    pub fn remove_buffer_id(&mut self, id: u64) -> Option<crate::lisp::types::BufferRef> {
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
        self.frames_discard_buffer(id);
        self.delete_buffer_overlays(id);
        let selected_window_showed_buffer = self.selected_window_buffer_id() == id;
        self.detach_markers_for_buffer(id);
        // A *scratch* made to replace the last buffer inherits its directory.
        let directory = self
            .buffer_local_value(id, "default-directory")
            .unwrap_or(Value::Nil);
        // The always-local slots the bindings hold, and the mark's state,
        // as Fkill_buffer leaves them for a killed buffer's readers.
        let fixed = self
            .buffer_locals
            .get(&id)
            .map(super::LocalCells::fixed_values)
            .unwrap_or_default();
        if let Some(mut buffer) = self.get_buffer_by_id_mut(id) {
            let mut slots = Box::new([None; super::FIXED_SLOTS + 1]);
            slots[..super::FIXED_SLOTS].copy_from_slice(&fixed);
            slots[super::FIXED_SLOTS] = Some(buffer.mark_active_value());
            buffer.killed_locals = Some(slots);
        }
        self.remove_buffer_locals(id);
        self.buffer_local_hooks.remove(&id);
        self.buffer_case_tables.remove(id);
        self.reset_buffer_syntax_table(id);
        self.labeled_restrictions
            .retain(|restriction| restriction.buffer_id != id);
        self.indirect_buffers
            .retain(|(buffer_id, base_id)| *buffer_id != id && *base_id != id);
        if let Some(mut buffer) = self.get_buffer_by_id_mut(id) {
            buffer.release_killed_storage();
        }
        if id == self.current_buffer_id {
            // GNU replaces a killed current buffer from the visible buffer
            // list (`other-buffer' policy), never from the interpreter's
            // storage stack.  A recently used internal minibuffer is often
            // last in that stack after an interactive prompt, but must not
            // become the ordinary selected window's buffer.
            let replacement_id = self
                .buffer_list
                .iter()
                .find(|(buffer_id, name)| *buffer_id != id && !name.starts_with(' '))
                .map(|(buffer_id, _)| *buffer_id);
            self.buffer_list.retain(|(buffer_id, _)| *buffer_id != id);
            if let Some((position, next_id)) = replacement_id.and_then(|next_id| {
                self.inactive_buffers
                    .iter()
                    .position(|(buffer_id, _)| *buffer_id == next_id)
                    .map(|position| (position, next_id))
            }) {
                let (_, next_buffer) = self.inactive_buffers.swap_remove(position);
                self.buffer = next_buffer;
                self.current_buffer_id = next_id;
            } else {
                let scratch_id = self.alloc_buffer_id();
                self.buffer = crate::lisp::types::BufferRef::new(
                    scratch_id,
                    crate::buffer::Buffer::new("*scratch*"),
                );
                self.current_buffer_id = scratch_id;
                self.buffer_list.push((scratch_id, "*scratch*".to_string()));
                self.reset_always_local_slots(scratch_id, directory);
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
                .and_then(|value| value.integer_value())
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

    pub fn alloc_record_id(&mut self) -> u64 {
        let id = self.next_record_id;
        self.next_record_id += 1;
        id
    }

    /// alloc.c:Fmake_finalizer after CHECK_TYPE: allocate the object and
    /// append it to the active list. The callback is the object's one slot.
    pub(crate) fn make_finalizer(&mut self, function: Value) -> Value {
        let object = crate::lisp::alloc::FinalizerRef::new(function);
        self.finalizers.append(object);
        Value::Finalizer(object)
    }

    /// alloc.c:queue_doomed_finalizers, before marking any doomed callback.
    pub(crate) fn queue_doomed_finalizers(&self, epoch: u32) {
        // SAFETY: only the currently yielded member moves. No allocation,
        // Lisp invocation or sweep occurs during this traversal.
        for object in unsafe { self.finalizers.iter() } {
            if !object.mark_bit().is_marked(epoch) && !object.function().is_nil() {
                self.doomed_finalizers.append(object);
            }
        }
    }

    /// alloc.c:mark_finalizer_list retains both the object and its callback.
    pub(crate) fn mark_doomed_finalizers(&self, marked: &mut crate::lisp::eval::LispReachability) {
        // SAFETY: tracing changes marks, never links, and invokes no Lisp.
        for object in unsafe { self.doomed_finalizers.iter() } {
            marked.mark(self, &Value::Finalizer(object));
        }
    }

    /// alloc.c:run_finalizers once the collection is complete: each
    /// doomed function runs once, under `inhibit-quit' bound to t, with a
    /// signal caught and logged as "finalizer failed: %S"
    /// (run_finalizer_function's internal_condition_case_1 with Qt).
    pub(crate) fn run_doomed_finalizers(&mut self, env: &mut Env) -> Result<(), LispError> {
        while let Some(object) = self.doomed_finalizers.pop_front() {
            let function = object.function();
            if function.is_nil() {
                continue;
            }
            // alloc.c clears the actual slot before calling Lisp: a
            // resurrected object or a nested GC cannot run it twice.
            object.set_function(Value::Nil);
            self.finalizers_run += 1;
            let restore = self.bind_special_dynamic("inhibit-quit", Value::T, env)?;
            let handlers = self.push_internal_condition_case(super::InternalHandlers::All);
            let result = self.call_function_value(function, None, &[], env);
            self.pop_handler_bindings(handlers);
            match result.map_err(LispError::into_kind) {
                Ok(_) => {}
                Err(error @ (LispErrorKind::Throw(_, _) | LispErrorKind::Terminate(_))) => {
                    self.restore_special_dynamic(restore, env)?;
                    return Err(LispError::from(error));
                }
                Err(error) => {
                    let condition =
                        crate::lisp::eval::error_condition_value(&LispError::from(error.clone()));
                    if let Ok(message) = crate::lisp::primitives::call(
                        self,
                        "format-message",
                        &[Value::string("finalizer failed: %S"), condition],
                        env,
                    ) && let Some(text) = crate::lisp::primitives::string_like(&message)
                    {
                        crate::lisp::primitives::log_message_text(self, &text.text, env);
                    }
                }
            }
            self.restore_special_dynamic(restore, env)?;
        }
        Ok(())
    }

    /// alloc.c:Fmake_marker allocates the object itself, with no id registry.
    pub fn make_marker(&mut self) -> Value {
        Value::Marker(crate::lisp::types::MarkerRef::new())
    }

    pub fn buffer_mark_marker_value(&mut self) -> Value {
        Value::Marker(
            self.buffer
                .mark_object()
                .expect("a live buffer owns its mark"),
        )
    }

    pub fn marker_position(&self, marker: crate::lisp::types::MarkerRef) -> Option<usize> {
        marker.position()
    }

    pub fn marker_buffer_id(&self, marker: crate::lisp::types::MarkerRef) -> Option<u64> {
        marker.buffer().map(|buffer| buffer.id)
    }

    pub fn marker_insertion_type(&self, marker: crate::lisp::types::MarkerRef) -> Option<bool> {
        Some(marker.insertion_type())
    }

    pub fn set_marker_insertion_type(
        &mut self,
        marker: crate::lisp::types::MarkerRef,
        insertion_type: bool,
    ) {
        marker.set_insertion_type(insertion_type);
    }

    pub fn set_marker(
        &mut self,
        marker: crate::lisp::types::MarkerRef,
        position: Option<usize>,
        buffer_id: Option<u64>,
    ) -> Result<(), LispError> {
        if let Some(position) = position
            && let Some(buffer) = buffer_id.and_then(|id| self.buffer_object(id))
        {
            let state = buffer.borrow();
            let position = position.clamp(1, state.size_total() + 1);
            marker.attach(buffer, position, state.marker_byte_position(position));
        } else {
            marker.detach();
        }
        Ok(())
    }

    pub fn copy_marker_value(
        &mut self,
        value: &Value,
        insertion_type: bool,
    ) -> Result<Value, LispError> {
        if !matches!(value.kind(), Kind::Nil | Kind::Marker(_) | Kind::Integer(_)) {
            return Err(LispError::WrongTypeArgument(
                "integer-or-marker-p".into(),
                *value,
            ));
        }
        let marker = crate::lisp::types::MarkerRef::new();
        match value.kind() {
            Kind::Marker(source) => {
                if let Some(buffer) = source.buffer() {
                    marker.attach(buffer, source.last_position(), source.bytepos());
                }
            }
            Kind::Integer(position) => {
                let buffer = self.buffer.borrow();
                let position = position.clamp(1, (buffer.size_total() + 1) as i64) as usize;
                marker.attach(self.buffer, position, buffer.marker_byte_position(position));
            }
            _ => {}
        }
        marker.set_insertion_type(insertion_type);
        Ok(Value::Marker(marker))
    }

    pub fn make_char_table(&mut self, subtype: Option<String>, initial: Value) -> Value {
        let purpose = subtype.as_deref().map_or(Value::Nil, Value::symbol);
        self.make_char_table_for_purpose(purpose, initial)
            .expect("internal character table purpose")
    }

    pub(crate) fn make_char_table_for_purpose(
        &self,
        purpose: Value,
        initial: Value,
    ) -> Result<Value, LispError> {
        let name = purpose.as_symbol()?;
        let extra = self
            .get_symbol_property(name, "char-table-extra-slots")
            .unwrap_or(Value::Nil);
        let extras = match extra.kind() {
            Kind::Nil => 0,
            Kind::Integer(n) if (0..=10).contains(&n) => n as usize,
            Kind::Integer(n) if n > 10 => {
                return Err(LispError::SignalValue(Value::list([
                    Value::symbol("args-out-of-range"),
                    extra,
                    Value::Nil,
                ])));
            }
            _ => return Err(LispError::WrongTypeArgument("wholenump".into(), extra)),
        };
        Ok(Value::CharTable(CharTableRef::new(
            purpose, initial, extras,
        )))
    }

    /// Standard tests read the table's actual arrays. No id-to-state lookup
    /// or entry snapshot is involved in ordinary lookup or mutation.
    pub(crate) fn equal_hash_entry(
        &self,
        table: crate::lisp::types::HashTableRef,
        key: &Value,
        env: &Env,
    ) -> Result<Option<Option<HashEntry>>, LispError> {
        let Some(test) = table.test().standard_test() else {
            return Ok(None);
        };
        let hash = crate::lisp::primitives::standard_hash_code(self, test, key);
        self.standard_hash_entry(table, test, hash, key, env)
            .map(Some)
    }

    fn standard_hash_entry(
        &self,
        table: crate::lisp::types::HashTableRef,
        test: RuntimeHashTest,
        hash: u32,
        key: &Value,
        env: &Env,
    ) -> Result<Option<HashEntry>, LispError> {
        let mut slot = table.first_in_bucket(hash);
        while slot >= 0 {
            let index = slot as usize;
            let (stored, value) = table.entry(index).expect("live hash chain");
            if crate::lisp::primitives::values_eq_in_env(self, key, &stored, env)
                || (table.stored_hash(index) == hash
                    && self.runtime_hash_keys_match(test, key, &stored, env)?)
            {
                return Ok(Some((index, stored, value)));
            }
            slot = table.next_in_bucket(index);
        }
        Ok(None)
    }

    fn runtime_hash_keys_match(
        &self,
        test: RuntimeHashTest,
        left: &Value,
        right: &Value,
        env: &Env,
    ) -> Result<bool, LispError> {
        Ok(match test {
            RuntimeHashTest::Eq => {
                crate::lisp::primitives::values_eq_in_env(self, left, right, env)
            }
            RuntimeHashTest::Eql => {
                crate::lisp::primitives::values_eql_in_env(self, left, right, env)
            }
            RuntimeHashTest::Equal => crate::lisp::primitives::values_equal(self, left, right)?,
        })
    }

    pub(crate) fn equal_hash_lookup(
        &self,
        table: crate::lisp::types::HashTableRef,
        key: &Value,
        env: &Env,
    ) -> Result<Option<Option<Value>>, LispError> {
        Ok(self
            .equal_hash_entry(table, key, env)?
            .map(|entry| entry.map(|(_, _, value)| value)))
    }

    pub(crate) fn equal_hash_lookup_key(
        &self,
        table: crate::lisp::types::HashTableRef,
        key: &Value,
        env: &Env,
    ) -> Result<Option<Value>, LispError> {
        Ok(self
            .equal_hash_entry(table, key, env)?
            .flatten()
            .map(|(_, key, _)| key))
    }

    pub(crate) fn equal_hash_put(
        &self,
        table: crate::lisp::types::HashTableRef,
        key: Value,
        value: Value,
        env: &Env,
    ) -> Result<bool, LispError> {
        let Some(test) = table.test().standard_test() else {
            return Ok(false);
        };
        let hash = crate::lisp::primitives::standard_hash_code(self, test, &key);
        if let Some((slot, _, _)) = self.standard_hash_entry(table, test, hash, &key, env)? {
            table.set_value(slot, value);
        } else {
            table.insert(hash, key, value);
        }
        Ok(true)
    }

    #[inline]
    /// The current canonical graph, including mutable descriptors. Unlike a
    /// Rust mutation stamp this observes raw native stores as well. The regex
    /// backend compiles tables into classes, so its derived caches must compare
    /// their actual inputs; ordinary table lookup never pays for this walk.
    pub(crate) fn char_table_chain_signature(
        &self,
        table: CharTableRef,
    ) -> CharTableChainSignature {
        CharTableChainSignature::capture(table)
    }

    pub(crate) fn cached_syntax_segments(
        &self,
        table_id: CharTableRef,
    ) -> Option<std::rc::Rc<Vec<(u32, u32, crate::lisp::primitives::syntax::SyntaxClass)>>> {
        self.syntax_segment_cache
            .borrow()
            .as_ref()
            .filter(|cache| cache.table_id == table_id && cache.chain.matches(Some(table_id)))
            .map(|cache| cache.segments.clone())
    }

    pub(crate) fn cache_syntax_segments(
        &self,
        table_id: CharTableRef,
        segments: std::rc::Rc<Vec<(u32, u32, crate::lisp::primitives::syntax::SyntaxClass)>>,
    ) {
        *self.syntax_segment_cache.borrow_mut() = Some(crate::lisp::eval::SyntaxSegmentCache {
            table_id,
            chain: self.char_table_chain_signature(table_id),
            segments,
        });
    }

    #[inline]
    pub fn char_table_set(
        &mut self,
        table: CharTableRef,
        key: u32,
        value: Value,
    ) -> Result<(), LispError> {
        table.set(key, value);
        Ok(())
    }

    pub fn char_table_set_range(
        &mut self,
        table: CharTableRef,
        start: u32,
        end: u32,
        value: Value,
    ) -> Result<(), LispError> {
        table.set_range(start, end, value);
        Ok(())
    }

    #[inline]
    pub fn char_table_get(&self, table: CharTableRef, key: u32) -> Option<Value> {
        Some(table.get(key))
    }

    pub(crate) fn char_table_effective_ranges(
        &self,
        table: CharTableRef,
    ) -> Option<Vec<CharTableEntry>> {
        Some(table.effective_ranges())
    }

    pub fn char_table_explicit_get(&self, mut table: CharTableRef, key: u32) -> Option<Value> {
        loop {
            let value = table.explicit_get(key);
            if !value.is_nil() {
                return Some(value);
            }
            table = table.parent()?;
        }
    }

    pub fn set_char_table_parent(
        &mut self,
        table: CharTableRef,
        parent: Option<CharTableRef>,
    ) -> Result<(), LispError> {
        let mut next = parent;
        while let Some(current) = next {
            if current == table {
                return Err(LispError::Signal(
                    "Attempt to make a chartable be its own parent".into(),
                ));
            }
            next = current.parent();
        }
        table.set_parent(parent);
        Ok(())
    }

    pub fn char_table_extra_slot(&self, table: CharTableRef, slot: usize) -> Option<Value> {
        table.extra(slot)
    }

    pub fn set_char_table_extra_slot(
        &mut self,
        table: CharTableRef,
        slot: usize,
        value: Value,
    ) -> Result<(), LispError> {
        if table.set_extra(slot, value) {
            Ok(())
        } else {
            Err(LispError::SignalValue(Value::list([
                Value::symbol("args-out-of-range"),
                Value::CharTable(table),
                Value::Integer(slot as i64),
            ])))
        }
    }

    pub fn clone_char_table(&mut self, table: CharTableRef) -> Result<Value, LispError> {
        Ok(Value::CharTable(table.copy()))
    }

    pub(crate) fn syntax_code_object(&self, code: usize) -> Value {
        let Kind::Vector(codes) = self.syntax_code_objects.kind() else {
            unreachable!()
        };
        codes.get(code).expect("syntax code")
    }

    pub fn copy_syntax_table(&mut self, table: CharTableRef) -> Result<Value, LispError> {
        let copy = table.copy();
        copy.set_default(Value::Nil);
        copy.set_parent(copy.parent().or(Some(self.standard_syntax_table_id)));
        Ok(Value::CharTable(copy))
    }

    #[cfg(test)]
    pub fn create_record(&mut self, type_name: &str, slots: Vec<Value>) -> Value {
        self.create_record_with_type(Value::symbol(type_name), slots)
    }

    #[cfg(test)]
    pub fn create_record_with_type(&mut self, type_tag: Value, slots: Vec<Value>) -> Value {
        Value::LispRecord(LispRecordRef::new(type_tag, &slots))
    }

    pub(crate) fn create_pseudovector(
        &mut self,
        kind: RecordKind,
        type_name: &str,
        mut slots: Vec<Value>,
    ) -> Value {
        if kind == RecordKind::Window {
            slots.resize(primitives::WINDOW_FRAME_SLOT + 1, Value::Nil);
            slots[primitives::WINDOW_FRAME_SLOT] = self.selected_frame_value();
        }
        let record = self.create_record_with_kind(Value::symbol(type_name), slots, kind);
        // window.c:make_window and copy_window: `++sequence_number'.
        if kind == RecordKind::Window
            && let Kind::Record(id) = record.kind()
        {
            self.window_sequence_number += 1;
            let number = self.window_sequence_number;
            self.window_sequence_numbers.insert(id.id, number);
        }
        record
    }

    /// The window's `sequence_number', as print.c prints it.
    pub(crate) fn window_sequence_number(&self, id: u64) -> Option<i64> {
        self.window_sequence_numbers.get(&id).copied()
    }

    /// alloc.c:Fmake_closure.  Both the ordinary primitive dispatcher and
    /// the native Lisp_Object ABI use this one C-owned operation so the
    /// prototype copy, constant-vector replacement, and errors cannot drift.
    pub(crate) fn make_closure(
        &mut self,
        prototype: &Value,
        closure_vars: &[Value],
    ) -> Result<Value, LispError> {
        let Kind::Closure(closure) = prototype.kind() else {
            return Err(LispError::WrongTypeArgument(
                "byte-code-function-p".into(),
                *prototype,
            ));
        };
        let constants = closure.get(2).ok_or_else(|| {
            LispError::Signal("make-closure prototype has no constants vector".into())
        })?;
        let Kind::Vector(constants) = constants.kind() else {
            return Err(LispError::WrongTypeArgument("vectorp".into(), constants));
        };
        if closure_vars.len() > constants.len() {
            return Err(LispError::Signal(
                "Closure vars do not fit in constvec".into(),
            ));
        }
        // alloc.c:Fmake_closure copies the actual constant vector and closure
        // slots. No host record, registry entry or detached slot array exists.
        let copied_constants = constants.shallow_copy();
        for (index, value) in closure_vars.iter().enumerate() {
            copied_constants.set(index, *value);
        }
        let copied = closure.shallow_copy();
        copied.initialize_slot(2, Value::Vector(copied_constants));
        Ok(Value::Closure(copied))
    }

    fn create_record_with_kind(
        &mut self,
        type_tag: Value,
        slots: Vec<Value>,
        kind: RecordKind,
    ) -> Value {
        let vector_slots = kind.gnu_vector_slots();
        if vector_slots != 0 {
            crate::lisp::native_comp::note_lisp_allocation(vector_slots.saturating_mul(8));
        }
        let id = self.alloc_record_id();
        let indexed_type = type_tag.symbol_str().map(str::to_owned);
        // alloc.c's allocate_record: the object in a vector block; the
        // registry names it by id for the side tables.
        let record = RecordRef::allocate(RecordState {
            id,
            owner: self.record_owner,
            type_tag,
            slots,
            kind,
        });
        self.records.insert(id, record);
        if let Some(type_name) = indexed_type {
            self.record_ids_by_type_index
                .entry(type_name)
                .or_default()
                .insert(id);
        }
        Value::Record(record)
    }

    /// The record KEY names: the object itself (a `RecordRef', the
    /// address) or, for the side tables that still know a record by its
    /// id, the registry's entry for that id (None once the sweep has
    /// freed it).
    pub fn find_record(&self, key: impl RecordKey) -> Option<&RecordState> {
        let record = key.record_ref(self)?;
        // SAFETY: an allocated record the caller reached (through a value
        // it holds or the registry, cleared after every sweep); the
        // borrow is tied to `self', which outlives the call, and the
        // collector runs only at a `maybe_gc' point, never inside it.
        Some(unsafe { &*record.as_ptr() })
    }

    pub fn find_record_mut(&mut self, key: impl RecordKey) -> Option<&mut RecordState> {
        let record = key.record_ref(self)?;
        // SAFETY: as `find_record'; the exclusive borrow of `self' keeps
        // any other path to the record's state out for its duration, as
        // the registry's `&mut' did.
        Some(unsafe { &mut *record.as_ptr() })
    }

    pub(crate) fn create_treesit_query(&mut self, language: Value, source: Value) -> Value {
        let query = self.create_pseudovector(
            RecordKind::TreeSitterCompiledQuery,
            "treesit-compiled-query",
            Vec::new(),
        );
        let Kind::Record(record_id) = query.kind() else {
            unreachable!("Tree-sitter queries use opaque record identities");
        };
        self.treesit_queries.push(TreeSitterQueryState {
            record_id: record_id.id,
            language,
            source,
            query: None,
        });
        Value::Record(record_id)
    }

    pub(crate) fn treesit_query_state(&self, value: &Value) -> Option<&TreeSitterQueryState> {
        let Kind::Record(record_id) = value.kind() else {
            return None;
        };
        self.treesit_queries
            .iter()
            .find(|query| query.record_id == record_id.id)
    }

    pub(crate) fn cache_treesit_query(
        &mut self,
        value: &Value,
        query: std::rc::Rc<tree_sitter::Query>,
    ) {
        let Kind::Record(record_id) = value.kind() else {
            unreachable!("only compiled Tree-sitter query records are cached");
        };
        self.treesit_queries
            .iter_mut()
            .find(|state| state.record_id == record_id.id)
            .expect("compiled Tree-sitter query state exists")
            .query = Some(query);
    }

    /// The records of KIND that FILTER admits, as objects: a caller that
    /// edits them takes each through `find_record_mut'.
    pub(crate) fn records_of_kind(
        &self,
        kind: RecordKind,
        filter: impl Fn(&RecordState) -> bool,
    ) -> Vec<RecordRef> {
        self.records
            .values()
            .copied()
            .filter(|record| record.kind == kind && filter(record))
            .collect()
    }

    pub(crate) fn record_ids_by_type(&self, type_name: &str) -> Vec<u64> {
        self.record_ids_by_type_index
            .get(type_name)
            .map(|ids| ids.iter().copied().collect())
            .unwrap_or_default()
    }

    /// How many records `record_ids_by_type' would list, without them.
    pub(crate) fn record_count_by_type(&self, type_name: &str) -> usize {
        self.record_ids_by_type_index
            .get(type_name)
            .map_or(0, |ids| ids.len())
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

    pub fn provide_feature(&mut self, feature: &str) {
        let mut features = self.provided_features.clone();
        if !features.iter().any(|name| name == feature) {
            features.push(feature.to_string());
        }
        self.set_global_binding(
            "features",
            Value::list(
                features
                    .into_iter()
                    .map(|value| Value::Symbol(value.into())),
            ),
        );
        if feature == "abbrev" {
            primitives::ensure_standard_abbrev_tables(self);
        }
    }

    fn record_provide_in_load_history(&mut self, feature: &str) {
        let Some(current_load_list) = self.lookup_var("current-load-list", &Env::new()) else {
            return;
        };
        if current_load_list.is_nil() {
            return;
        }
        let entry = Value::cons(
            Value::Symbol("provide".into()),
            Value::Symbol(feature.to_string().into()),
        );
        let entries = current_load_list.to_vec().unwrap_or_default();
        if entries.iter().any(|item| item == &entry) {
            return;
        }
        // GNU LOADHIST_ATTACH conses onto the front: the source file name
        // stays the LAST element (`macroexp-file-name' reads it there).
        self.set_global_binding("current-load-list", Value::cons(entry, current_load_list));
    }

    fn record_require_in_load_history(&mut self, feature: SymbolName) {
        // `require' only records dependencies while reading a file.  GNU
        // recognizes that state by the final string in current-load-list,
        // which is nil outside a load.
        let Some(current_load_list) = self
            .symbol_value_key(cached_symbol!("current-load-list"))
            .filter(|list| !list.is_nil())
        else {
            return;
        };
        let Ok(entries) = current_load_list.to_vec() else {
            return;
        };
        if !matches!(
            entries.last().map(|v| v.kind()),
            Some(Kind::StringObject(_))
        ) {
            return;
        }
        let entry = Value::cons(
            cached_symbol!("require").with(|symbol| Value::Symbol(*symbol)),
            Value::Symbol(feature),
        );
        if entries.iter().any(|item| item == &entry) {
            return;
        }
        self.store_current_load_list(Value::cons(entry, current_load_list));
    }

    /// lisp.h:LOADHIST_ATTACH's store into Vcurrent_load_list, a C
    /// variable no alias can name.
    fn store_current_load_list(&mut self, list: Value) {
        let symbol = cached_symbol!("current-load-list").with(|symbol| *symbol);
        self.set_global_binding_resolved(&symbol, list);
    }

    pub(crate) fn record_definition_in_load_history(&mut self, kind: &str, name: &str) {
        self.record_symbol_definition_in_load_history(kind, &SymbolName::intern_str(name));
    }

    pub(crate) fn record_symbol_definition_in_load_history(
        &mut self,
        kind: &str,
        symbol: &SymbolName,
    ) {
        let Some(current_load_list) = self.symbol_value_key(cached_symbol!("current-load-list"))
        else {
            return;
        };
        let entry = match kind {
            "defvar" => Value::Symbol(*symbol),
            "defun" => Value::cons(
                cached_symbol!("defun").with(|symbol| Value::Symbol(*symbol)),
                Value::Symbol(*symbol),
            ),
            _ => Value::cons(Value::symbol(kind), Value::Symbol(*symbol)),
        };
        // lisp.h:LOADHIST_ATTACH accepts an empty current-load-list too;
        // the entry remains a root outside a file load (e.g. defvaralias).
        // GNU conses definitions onto the front, a second
        // definition of the same name in one file included (a file defining
        // `f' twice lists `(defun . f)' twice), so the source-file string
        // remains last until build_load_history reverses the completed
        // entry.  A membership walk of the list here made each definition
        // cost the length of the file's list so far.
        self.store_current_load_list(Value::cons(entry, current_load_list));
    }

    /// Remember the definition hidden by a `defalias'-style operation.
    ///
    /// GNU stores alternating (FILE DEFINITION) pairs.  When the same file
    /// defines a symbol repeatedly, unloading it must restore the definition
    /// from before that file first touched the symbol, not an intermediate
    /// definition from the same load.
    pub(crate) fn record_function_redefinition(&mut self, name: &str, old_definition: Value) {
        if old_definition.is_nil() {
            return;
        }
        // data.c's add_to_function_history walks `current-load-list' to
        // its last element and takes it when it is a string; the walk
        // reads the cells in place (a copy of the list cost every
        // redefinition the length of the file's list so far).
        let mut file = Value::Nil;
        let mut tail = self
            .lookup_var("current-load-list", &Env::new())
            .unwrap_or(Value::Nil);
        let mut seen = 0usize;
        while let Kind::Cons(cell) = tail.kind() {
            let next = cell.cdr.get();
            if next.is_nil() {
                let last = cell.car.get();
                if matches!((last).kind(), Kind::StringObject(_)) {
                    file = last;
                }
            }
            seen += 1;
            if seen > 1 << 24 {
                break;
            }
            tail = next;
        }
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
    pub(crate) fn commit_entire_load_history(&mut self, filename: &Value, current: Value) {
        let mut entry = current.to_vec().unwrap_or_default();
        entry.reverse();
        if entry.is_empty() {
            return;
        }

        let mut history = self
            .lookup_var("load-history", &Env::new())
            .and_then(|value| value.to_vec().ok())
            .unwrap_or_default();
        history.retain(|existing| match existing.car() {
            Ok(existing_filename) => &existing_filename != filename,
            Err(_) => true,
        });
        history.insert(0, Value::list(entry));
        self.set_global_binding("load-history", Value::list(history));
    }

    pub(crate) fn provide_feature_with_after_load(
        &mut self,
        feature: &str,
        env: &mut Env,
    ) -> Result<Value, LispError> {
        self.provide_feature(feature);
        self.record_provide_in_load_history(feature);

        // GNU 30.2 fns.c:Fprovide runs the functions in the matching
        // `after-load-alist' entry after publishing FEATURE.  The alist and
        // its closures are owned by subr.el's real `eval-after-load'; the
        // Rust host only performs the C-owned lookup/invocation boundary.
        let after_load_alist = self
            .lookup_var("after-load-alist", env)
            .unwrap_or(Value::Nil);
        for entry in after_load_alist.to_vec()? {
            let Some((key, functions)) = entry.cons_values() else {
                continue;
            };
            if !matches!(key.kind(), Kind::Symbol(name) if name == feature) {
                continue;
            }
            for function in functions.to_vec()? {
                self.call_function_value(function, None, &[], env)?;
            }
            break;
        }
        Ok(Value::Symbol(feature.to_string().into()))
    }

    // Whether NAME has an interpreted (Lisp-defined) function binding,
    // as opposed to only a native dispatch arm.
    pub(crate) fn has_lisp_function(&self, name: &str) -> bool {
        self.globals.function_definition_by_name(name).is_some()
    }

    /// Whether FEATURE names a symbol in `features' (read in place; a
    /// circular or dotted list has none): by name, for a caller that
    /// holds no symbol, in whichever obarray it reads from.
    pub fn has_feature(&self, feature: &str) -> bool {
        let features = self
            .global_value("features")
            .unwrap_or_else(|| self.features_value());
        let named = |value: &Value| matches!(value.kind(), Kind::Symbol(name) if name == feature);
        if matches!(features.kind(), Kind::Vector(_)) {
            // A vector reads as `to_vec' gives it, marker first.
            return features
                .to_vec()
                .is_ok_and(|features| features.iter().any(named));
        }
        // Fmemq's walk, stopping at the first match.
        let mut tail = features;
        let mut seen = crate::lisp::types::CycleGuard::new();
        while let Some(cell) = tail.cons_ref() {
            if seen.step(crate::lisp::types::ConsCell::identity(&cell)) {
                return false;
            }
            if named(&cell.car.get()) {
                return true;
            }
            tail = cell.cdr.get();
        }
        false
    }

    /// Ffeaturep's and Frequire's `Fmemq (feature, Vfeatures)': FEATURE
    /// by its word, read in place (a circular or dotted list has none).
    pub(crate) fn has_feature_symbol(&self, feature: SymbolName) -> bool {
        let features = cached_symbol!("features")
            .with(|symbol| self.global_binding_value_symbol(symbol))
            .unwrap_or_else(|| self.features_value());
        let word = Value::Symbol(feature).word();
        if matches!(features.kind(), Kind::Vector(_)) {
            // A vector reads as `to_vec' gives it, marker first.
            return features
                .to_vec()
                .is_ok_and(|features| features.iter().any(|value| value.word() == word));
        }
        // Fmemq's walk, stopping at the first match.
        let mut tail = features;
        let mut seen = crate::lisp::types::CycleGuard::new();
        while let Some(cell) = tail.cons_ref() {
            if seen.step(crate::lisp::types::ConsCell::identity(&cell)) {
                return false;
            }
            if cell.car.get().word() == word {
                return true;
            }
            tail = cell.cdr.get();
        }
        false
    }

    pub(super) fn features_value(&self) -> Value {
        Value::list(
            self.provided_features
                .iter()
                .cloned()
                .map(|value| Value::Symbol(value.into()))
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

/// What compare_window_configurations reads of a configuration: the frame
/// settings, and each saved window in save_window_save's order (the
/// frame's window tree depth first, then the minibuffer window as the
/// root's next sibling) as whether it is the current window, its buffer
/// (nil for an internal window), pixel and character edges, display
/// table, margins and its parent's and previous sibling's indices among
/// the saved windows.
type SavedWindow = (bool, [Value; 12], Option<usize>, Option<usize>);

fn saved_windows(
    snapshot: &WindowConfigurationSnapshot,
) -> (
    (i64, i64, crate::lisp::types::FrameRef, u64),
    Vec<SavedWindow>,
) {
    let slots_of = |id: u64| {
        snapshot
            .window_records
            .iter()
            .find(|(window, _)| *window == id)
            .map(|(_, slots)| slots.as_slice())
    };
    let window_id = |value: Option<&Value>| match value.map(|value| value.kind()) {
        Some(Kind::Record(record)) => Some(record.id),
        _ => None,
    };
    let mut order = Vec::new();
    // save_window_save: each window, then its children, then its next
    // sibling.
    let mut stack = vec![snapshot.root_window_id];
    while let Some(id) = stack.pop() {
        if order.contains(&id) {
            continue;
        }
        let Some(slots) = slots_of(id) else {
            continue;
        };
        order.push(id);
        if id != snapshot.root_window_id
            && let Some(next) = window_id(slots.get(primitives::WINDOW_NEXT_SIBLING_SLOT))
        {
            stack.push(next);
        }
        if let Some(child) = window_id(slots.get(primitives::WINDOW_FIRST_CHILD_SLOT)) {
            stack.push(child);
        }
    }
    let minibuffer = snapshot.window_records.iter().find(|(_, slots)| {
        matches!(
            slots.get(primitives::WINDOW_KIND_SLOT).map(|value| value.kind()),
            Some(Kind::Symbol(ref kind)) if kind == primitives::MINIBUFFER_WINDOW_KIND
        )
    });
    if let Some((id, _)) = minibuffer {
        order.push(*id);
    }
    let index_of = |value: Option<&Value>| {
        window_id(value).and_then(|id| order.iter().position(|window| *window == id))
    };
    let windows = order
        .iter()
        .map(|&id| {
            let slots = slots_of(id).unwrap_or_default();
            let slot = |index| slots.get(index).copied().unwrap_or(Value::Nil);
            // A record from before the character geometry slots reads its
            // pixel geometry (primitives::window_char_geometry).
            let char_slot = |index, pixel| {
                slots
                    .get(index)
                    .copied()
                    .filter(|value: &Value| !value.is_nil())
                    .unwrap_or_else(|| slot(pixel))
            };
            let prev = if Some(id) == minibuffer.map(|(id, _)| *id) {
                Some(0)
            } else {
                index_of(slots.get(primitives::WINDOW_PREV_SIBLING_SLOT))
            };
            (
                id == snapshot.selected_window_id,
                [
                    slot(primitives::WINDOW_BUFFER_SLOT),
                    slot(primitives::WINDOW_PIXEL_LEFT_SLOT),
                    slot(primitives::WINDOW_PIXEL_TOP_SLOT),
                    slot(primitives::WINDOW_PIXEL_HEIGHT_SLOT),
                    slot(primitives::WINDOW_PIXEL_WIDTH_SLOT),
                    char_slot(
                        primitives::WINDOW_LEFT_COL_SLOT,
                        primitives::WINDOW_PIXEL_LEFT_SLOT,
                    ),
                    char_slot(
                        primitives::WINDOW_TOP_LINE_SLOT,
                        primitives::WINDOW_PIXEL_TOP_SLOT,
                    ),
                    char_slot(
                        primitives::WINDOW_TOTAL_COLS_SLOT,
                        primitives::WINDOW_PIXEL_WIDTH_SLOT,
                    ),
                    char_slot(
                        primitives::WINDOW_TOTAL_LINES_SLOT,
                        primitives::WINDOW_PIXEL_HEIGHT_SLOT,
                    ),
                    slot(primitives::WINDOW_DISPLAY_TABLE_SLOT),
                    slot(primitives::WINDOW_LEFT_MARGIN_COLS_SLOT),
                    slot(primitives::WINDOW_RIGHT_MARGIN_COLS_SLOT),
                ],
                index_of(slots.get(primitives::WINDOW_PARENT_SLOT)),
                prev,
            )
        })
        .collect();
    (
        (
            snapshot.frame_width,
            snapshot.frame_height,
            snapshot.selected_frame_id,
            snapshot.current_buffer_id,
        ),
        windows,
    )
}

impl Interpreter {
    /// keyboard.c:menu_item_eval_property: SEXPR evaluated as eval_dyn
    /// does, with `inhibit-redisplay' bound to t and the error handler
    /// established first. Ordinary errors mean nil; quit and other
    /// nonlocal exits still propagate.
    pub(crate) fn menu_item_eval_property(
        &mut self,
        sexpr: Value,
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let restore = self.bind_special_dynamic("inhibit-redisplay", Value::T, env)?;
        let handlers = self.push_internal_condition_case(super::InternalHandlers::Error);
        let depth = env.len();
        let result = crate::lisp::primitives::call(self, "eval", &[sexpr, Value::Nil], env);
        self.pop_handler_bindings(handlers);
        env.truncate(depth);
        let result = match result {
            Err(error)
                if !matches!(
                    error.kind(),
                    LispErrorKind::Throw(_, _) | LispErrorKind::Terminate(_)
                ) && error.condition_type() != "quit"
                    && self
                        .error_condition_names(&error.condition_type())
                        .iter()
                        .any(|name| name.as_str() == "error") =>
            {
                self.clear_batch_error_backtrace();
                Ok(Value::Nil)
            }
            result => result,
        };
        self.with_lisp_stack_roots(&result, |interp| {
            interp.restore_special_dynamic(restore, env)
        })?;
        result
    }

    /// eval.c:safe_funcall: establish the catch before invoking Lisp, so
    /// outer handler-bind handlers and the debugger do not observe muted
    /// errors. Log through add_to_log's sink, without replacing the echo.
    pub(crate) fn safe_funcall(
        &mut self,
        function: Value,
        args: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let restore = self.bind_special_dynamic("inhibit-redisplay", Value::T, env)?;
        let handlers = self.push_internal_condition_case(super::InternalHandlers::All);
        let depth = env.len();
        let result = self.call_function_value(function, None, args, env);
        self.pop_handler_bindings(handlers);
        env.truncate(depth);
        let result = match result.map_err(LispError::into_kind) {
            Err(error @ (LispErrorKind::Throw(_, _) | LispErrorKind::Terminate(_))) => {
                Err(LispError::from(error))
            }
            Err(error) => {
                self.clear_batch_error_backtrace();
                let message = primitives::call(
                    self,
                    "format-message",
                    &[
                        Value::string("Error muted by safe_call: %S signaled %S"),
                        Value::list(std::iter::once(function).chain(args.iter().cloned())),
                        super::error_condition_value(&LispError::from(error.clone())),
                    ],
                    env,
                );
                message.map(|message| {
                    if let Some(message) = primitives::string_like(&message) {
                        primitives::log_message_text(self, &message.text, env);
                    }
                    Value::Nil
                })
            }
            Ok(value) => Ok(value),
        };
        self.restore_special_dynamic(restore, env)?;
        result
    }
}

/// buffer.c:nsberror: "No buffer named NAME" for a string SPEC.
pub(crate) fn nsberror(spec: &Value) -> LispError {
    match primitives::string_text(spec) {
        Ok(name) => LispError::Signal(format!("No buffer named {name}")),
        Err(_) => LispError::Signal("Invalid buffer argument".into()),
    }
}

#[cfg(test)]
mod runtime_index_tests {
    use super::{Interpreter, Value};
    use crate::lisp::types::Kind;

    #[test]
    fn canonical_char_tables_resolve_their_own_slots() {
        let mut interp = Interpreter::new();
        let Kind::CharTable(first_id) = interp
            .make_char_table(Some("first".into()), Value::Integer(11))
            .kind()
        else {
            unreachable!("make_char_table must return a char table")
        };
        let Kind::CharTable(second_id) = interp
            .make_char_table(Some("second".into()), Value::Integer(22))
            .kind()
        else {
            unreachable!("make_char_table must return a char table")
        };

        assert_eq!(
            interp.char_table_get(first_id, 'x' as u32),
            Some(Value::Integer(11))
        );
        assert_eq!(
            interp.char_table_get(second_id, 'x' as u32),
            Some(Value::Integer(22))
        );
        assert_ne!(first_id, second_id);
        assert_eq!(Value::CharTable(first_id).word(), first_id.identity() | 5);
    }

    #[test]
    fn char_table_radix_preserves_overrides_inheritance_and_mutation() {
        let mut interp = Interpreter::new();
        let Kind::CharTable(parent_id) = interp
            .make_char_table(Some("parent".into()), Value::Integer(10))
            .kind()
        else {
            unreachable!("make_char_table must return a char table")
        };
        interp
            .char_table_set_range(parent_id, 'a' as u32, 'z' as u32, Value::Integer(20))
            .expect("parent range must be writable");

        let Kind::CharTable(child_id) = interp
            .make_char_table(Some("child".into()), Value::Integer(30))
            .kind()
        else {
            unreachable!("make_char_table must return a char table")
        };
        interp
            .set_char_table_parent(child_id, Some(parent_id))
            .expect("child must accept a live parent");
        interp
            .char_table_set_range(child_id, 'm' as u32, 'z' as u32, Value::Integer(40))
            .expect("child range must be writable");
        interp
            .char_table_set(child_id, 'x' as u32, Value::Integer(50))
            .expect("child character must be writable");
        interp
            .char_table_set(child_id, 'x' as u32, Value::Nil)
            .expect("explicit nil must be writable");

        assert_eq!(
            interp.char_table_get(child_id, 'a' as u32),
            Some(Value::Integer(30))
        );
        assert_eq!(
            interp.char_table_get(child_id, 'm' as u32),
            Some(Value::Integer(40))
        );
        assert_eq!(
            interp.char_table_get(child_id, 'x' as u32),
            Some(Value::Integer(30))
        );

        // Initialized content survives writes to the parent.
        interp
            .char_table_set(parent_id, 'a' as u32, Value::Integer(21))
            .expect("parent character must remain writable");
        assert_eq!(
            interp.char_table_get(child_id, 'a' as u32),
            Some(Value::Integer(30))
        );

        let Kind::CharTable(clone_id) = interp
            .clone_char_table(child_id)
            .expect("live child table must be cloneable")
            .kind()
        else {
            unreachable!("clone_char_table must return a char table")
        };
        interp
            .char_table_set(child_id, 'x' as u32, Value::Integer(51))
            .expect("original table must remain writable after cloning");
        assert_eq!(
            interp.char_table_get(child_id, 'x' as u32),
            Some(Value::Integer(51))
        );
        assert_eq!(
            interp.char_table_get(clone_id, 'x' as u32),
            Some(Value::Integer(30))
        );

        // Non-ASCII ranges use the same authoritative radix leaves.
        interp
            .char_table_set_range(child_id, 0x100, 0x200, Value::Integer(60))
            .expect("non-ASCII range must be writable");
        interp
            .char_table_set_range(child_id, 0x180, 0x180, Value::Integer(61))
            .expect("non-ASCII override must be writable");
        assert_eq!(
            interp.char_table_get(child_id, 0x180),
            Some(Value::Integer(61))
        );

        // A full-range store replaces the contents without changing default.
        child_id.set_all(Value::Nil);
        child_id.set('q' as u32, Value::Integer(70));
        assert_eq!(
            interp.char_table_get(child_id, 'q' as u32),
            Some(Value::Integer(70))
        );
        assert_eq!(
            interp.char_table_get(child_id, 'x' as u32),
            Some(Value::Integer(30))
        );
        child_id.set_all(Value::Nil);
        assert_eq!(
            interp.char_table_get(child_id, 'q' as u32),
            Some(Value::Integer(30))
        );
        child_id.set_default(Value::Nil);
        assert_eq!(child_id.get('a' as u32), Value::Integer(21));
        assert_eq!(child_id.get('q' as u32), Value::Integer(20));
    }

    #[test]
    fn record_type_index_tracks_creation_and_retagging() {
        let mut interp = Interpreter::new();
        // The old index included every generic record. It now belongs only
        // to host pseudovectors; real type-slot changes are direct stores.
        let before_records = interp.records.len();
        let Kind::LispRecord(first) = interp.create_record("before", Vec::new()).kind() else {
            unreachable!("inline record")
        };
        let Kind::LispRecord(second) = interp.create_record("before", Vec::new()).kind() else {
            unreachable!("inline record")
        };
        assert!(!first.ptr_eq(&second));
        assert_eq!(first.type_tag(), Value::symbol("before"));
        assert_eq!(second.type_tag(), Value::symbol("before"));
        assert!(interp.record_ids_by_type("before").is_empty());
        first.set(0, Value::symbol("after"));
        assert_eq!(first.type_tag(), Value::symbol("after"));
        assert_eq!(second.type_tag(), Value::symbol("before"));
        second.set(0, Value::symbol("after"));
        assert_eq!(second.type_tag(), first.type_tag());
        assert!(interp.record_ids_by_type("after").is_empty());
        let descriptor = interp.create_record("descriptor", vec![Value::symbol("public-type")]);
        first.set(0, descriptor);
        assert_eq!(first.type_tag(), descriptor);
        assert_eq!(
            interp.records.len(),
            before_records,
            "generic records allocate no host ids"
        );
        let Kind::Record(host) = interp
            .create_pseudovector(super::RecordKind::Window, "index-host-window", Vec::new())
            .kind()
        else {
            unreachable!("host window")
        };
        assert_eq!(
            interp.record_ids_by_type("index-host-window"),
            vec![host.id]
        );
    }

    #[test]
    fn buffer_local_maps_preserve_per_buffer_order_and_lifecycle() {
        let mut interp = Interpreter::new();
        interp.set_buffer_local_value(41, "first", Value::Integer(1));
        interp.set_buffer_local_value(42, "first", Value::Integer(20));
        interp.set_buffer_local_value(41, "second", Value::Integer(2));
        interp.set_buffer_local_value(41, "first", Value::Integer(10));
        interp.set_buffer_local_hook(41, "first", vec![Value::symbol("local-hook")]);
        interp.set_buffer_local_hook(42, "first", vec![Value::symbol("other-hook")]);

        assert_eq!(
            interp.buffer_local_variables(41),
            vec![
                ("first".into(), Value::Integer(10)),
                ("second".into(), Value::Integer(2)),
            ]
        );
        assert_eq!(
            interp.buffer_local_hook(41, "first"),
            Some(vec![Value::symbol("local-hook")])
        );

        interp.remove_buffer_local_value(41, "first");
        assert_eq!(
            interp.buffer_local_variables(41),
            vec![("second".into(), Value::Integer(2))]
        );
        assert!(interp.buffer_local_hook(41, "first").is_none());
        assert_eq!(
            interp.buffer_local_value(42, "first"),
            Some(Value::Integer(20))
        );
        assert_eq!(
            interp.buffer_local_hook(42, "first"),
            Some(vec![Value::symbol("other-hook")])
        );

        interp.clear_buffer_local_state(42);
        assert!(interp.buffer_local_variables(42).is_empty());
        assert!(interp.buffer_local_hook(42, "first").is_none());
    }
}
