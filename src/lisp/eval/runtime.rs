use super::*;

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

    pub(crate) fn resolve_load_target(
        &mut self,
        target: &str,
    ) -> Result<Option<PathBuf>, LispError> {
        crate::lisp::primitives::resolve_load_target_in_env(self, target, &Env::new())
    }

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
        let result = self.load_target_with_env(target, env);
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
        let mut result = primitives::native_elisp_load(
            self,
            &Value::string(filename),
            false,
            &mut load_environment,
        );
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
    fn effective_quote_pair(&self, env: &Env) -> (char, char) {
        match crate::lisp::primitives::values::effective_text_quoting_style(self, env) {
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
        feature: &str,
        target: Option<&str>,
        env: &mut Env,
    ) -> Result<Value, LispError> {
        // GNU records a file's dependency even when FEATURE was loaded
        // already.  `file-dependents' and `unload-feature' derive their
        // dependency graph from these entries.
        self.record_require_in_load_history(feature);
        if self.has_feature(feature) {
            return Ok(Value::Symbol(feature.to_string().into()));
        }
        let load_target = target.unwrap_or(feature);
        let (open, close) = self.effective_quote_pair(env);
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
        if !self.has_feature(feature) {
            // GNU fns.c:Frequire signals unconditionally when the loaded
            // file did not provide FEATURE, naming the file `load-history'
            // records last (the resolved path, not the feature), or
            // "Required feature ... was not provided" when there is none.
            let loaded_file = self
                .lookup_var("load-history", env)
                .and_then(|history| history.car().ok())
                .and_then(|entry| entry.car().ok())
                .and_then(|file| crate::lisp::primitives::string_like(&file).map(|s| s.text));
            return Err(LispError::Signal(match loaded_file {
                Some(file) => {
                    format!("Loading file {file} failed to provide feature {open}{feature}{close}")
                }
                None => format!("Required feature {open}{feature}{close} was not provided"),
            }));
        }
        Ok(Value::Symbol(feature.to_string().into()))
    }

    /// Resolve a Lisp string-or-buffer value to a live buffer ID.
    pub fn resolve_buffer_id(&self, value: &Value) -> Result<u64, LispError> {
        match value {
            Value::Buffer(buffer) if self.has_buffer_id(buffer.id) => Ok(buffer.id),
            Value::Buffer(buffer) => {
                self.find_buffer(&buffer.name)
                    .map(|(id, _)| id)
                    .ok_or_else(|| {
                        if std::env::var_os("EMAXX_DEBUG_SEMANTIC").is_some() {
                            eprintln!(
                                "[buf] resolve failed for dead buffer {} (current {})",
                                buffer.name, self.buffer.name
                            );
                        }
                        LispError::Signal(format!("No buffer named {}", buffer.name))
                    })
            }
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
        let current_id = self.current_buffer_id;
        let current_point = self.buffer.point();
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
        // A localized GNU forwarding symbol loads the newly current
        // buffer's cell into its C variable during a buffer switch.
        self.refresh_forwarded_eval_cells();
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
        if selected_window_already_displays_target {
            return Ok(());
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

    pub(crate) fn root_window_value(&self) -> Value {
        Value::Record(self.root_window_id)
    }

    pub(crate) fn set_root_window_id(&mut self, id: u64) {
        self.root_window_id = id;
    }

    pub(crate) fn minibuffer_window_id(&self) -> u64 {
        self.minibuffer_window_id
    }

    pub(crate) fn minibuffer_window_value(&self) -> Value {
        Value::Record(self.minibuffer_window_id)
    }

    pub(crate) fn set_minibuffer_window_id(&mut self, id: u64) {
        self.minibuffer_window_id = id;
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

    /// Adopt the live terminal's display capabilities: color count and
    /// background class drive `defface' spec selection exactly as GNU's
    /// terminal-init path does (an xterm-family tty answers 8 colors and
    /// a light background).  Batch sessions keep GNU's dumb-terminal
    /// answers (0 colors, dark, mono); only the terminal frontend calls
    /// this.
    pub fn set_tty_display_colors(&mut self, color_cells: i64) {
        self.tty_display_color_cells = color_cells.max(0);
    }

    pub(crate) fn tty_display_color_cells(&self) -> i64 {
        self.tty_display_color_cells
    }

    /// term.c keeps the terminal's type string (tty->type, from $TERM);
    /// `tty-type' answers it for live tty frames and nil in batch.
    pub fn set_tty_terminal_type(&mut self, terminal_type: Option<String>) {
        self.tty_terminal_type = terminal_type;
    }

    pub(crate) fn tty_terminal_type(&self) -> Option<&str> {
        self.tty_terminal_type.as_deref()
    }

    /// Adopt the terminal's real geometry: a tty frame's height counts
    /// every screen row (GNU's FRAME_TOTAL_LINES), so the root window
    /// keeps rows − 1 lines above the one-line minibuffer.  Batch frames
    /// keep their dumb 80×25 shape; only the terminal frontend calls this.
    pub fn set_tty_frame_size(&mut self, width: i64, height: i64) {
        let width = width.max(1);
        let height = height.max(2);
        // GNU's tty frame reserves FRAME_MENU_BAR_LINES above the window
        // tree (frame.c adjust_frame_size); the `menu-bar-lines' frame
        // parameter is menu-bar-mode's channel for that count.
        // A tty menu bar is at most one screen line (GNU's tty frames
        // force menu_bar_lines to 0 or 1 regardless of the parameter).
        let menu_bar_lines = self
            .frame_parameter_override("menu-bar-lines")
            .and_then(|value| value.as_integer().ok())
            .unwrap_or(1)
            .clamp(0, 1)
            .min(height - 2);
        self.tty_frame_sized = true;
        if let Some(frame) = self.selected_frame_state_mut() {
            frame.width = width;
            frame.height = height;
            frame.text_height = height - menu_bar_lines;
            frame.parameter_width = width;
            frame.parameter_height = height;
        }
        self.resize_frame_window_records();
    }

    /// Recompute the tty window tree against the current frame
    /// parameters — the resize `menu-bar-mode' triggers when it stores a
    /// new `menu-bar-lines' count.  A no-op until a live tty has
    /// published its size: batch frames ignore the parameter, as GNU's
    /// batch frames do.
    pub(crate) fn refresh_tty_frame_layout(&mut self) {
        if !self.tty_frame_sized {
            return;
        }
        let width = self.frame_width();
        let height = self.frame_height();
        self.set_tty_frame_size(width, height);
    }

    pub(crate) fn frame_text_height(&self) -> i64 {
        self.selected_frame_state()
            .map(|frame| frame.text_height)
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
        if self.find_record(self.root_window_id).is_some() {
            self.resize_window_record_tree(
                self.root_window_id,
                (width, root_height, 0, top_margin),
            );
        }
        if self.find_record(self.minibuffer_window_id).is_some() {
            let minibuffer_id = self.minibuffer_window_id;
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
                .filter(|record| record.kind == RecordKind::Window)
                .map(|record| (record.id, record.slots.clone()))
                .collect(),
            root_window_id: self.root_window_id,
            frame_width: self.frame_width(),
            frame_height: self.frame_height(),
        }
    }

    pub(crate) fn window_configuration_value(&mut self) -> Value {
        let snapshot = self.snapshot_window_configuration();
        self.create_pseudovector(
            RecordKind::WindowConfiguration,
            "window-configuration",
            vec![
                Value::Integer(snapshot.current_buffer_id as i64),
                Value::Integer(snapshot.selected_window_id as i64),
                Value::list(snapshot.selected_window_slots),
                Value::Integer(snapshot.frame_width),
                Value::Integer(snapshot.frame_height),
                Value::Record(snapshot.root_window_id),
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
            .filter(|record| record.kind == RecordKind::WindowConfiguration)?;
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
            root_window_id: slots
                .get(5)
                .and_then(|value| match value {
                    Value::Record(id) => Some(*id),
                    _ => None,
                })
                .unwrap_or(selected_window_id),
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
            .filter(|record| record.kind == RecordKind::Window)
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
        self.root_window_id = snapshot.root_window_id;
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

    pub(crate) fn class_value(&self, name: &str) -> Option<Value> {
        // GNU cl-macs.el defines `cl--find-class' as this public property.
        self.get_symbol_property(name, "cl--class")
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
        self.killed_buffer_file_names.insert(
            id,
            self.get_buffer_by_id(id)
                .and_then(|buffer| buffer.file.clone()),
        );
        let selected_window_showed_buffer = self.selected_window_buffer_id() == id;
        self.detach_markers_for_buffer(id);
        if let Some(marker_id) = self.buffer_mark_marker_ids.remove(&id)
            && let Some(marker) = self.find_marker_mut(marker_id)
        {
            marker.mark_buffer_id = None;
        }
        self.buffer_locals.remove(&id);
        self.buffer_local_hooks.remove(&id);
        self.labeled_restrictions
            .retain(|restriction| restriction.buffer_id != id);
        self.indirect_buffers
            .retain(|(buffer_id, base_id)| *buffer_id != id && *base_id != id);
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

    pub(crate) fn killed_buffer_file_name(&self, id: u64) -> Option<&Option<String>> {
        self.killed_buffer_file_names.get(&id)
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
            mark_buffer_id: None,
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
            marker.mark_buffer_id = Some(buffer_id);
        }
        self.set_marker(marker_id, mark, mark.map(|_| buffer_id))
            .expect("the persistent buffer mark is a live marker");
        Value::Marker(marker_id)
    }

    pub(super) fn marker_index(id: u64) -> Option<usize> {
        usize::try_from(id.checked_sub(1)?).ok()
    }

    pub fn find_marker(&self, id: u64) -> Option<&MarkerState> {
        let index = Self::marker_index(id)?;
        self.markers.get(index).filter(|marker| marker.id == id)
    }

    pub fn find_marker_mut(&mut self, id: u64) -> Option<&mut MarkerState> {
        let index = Self::marker_index(id)?;
        self.markers.get_mut(index).filter(|marker| marker.id == id)
    }

    fn update_marker_buffer_index(
        &mut self,
        marker_id: u64,
        previous_buffer_id: Option<u64>,
        buffer_id: Option<u64>,
    ) {
        if previous_buffer_id == buffer_id {
            return;
        }
        if let Some(previous_buffer_id) = previous_buffer_id {
            let remove_empty_entry = self
                .markers_by_buffer
                .get_mut(&previous_buffer_id)
                .is_some_and(|marker_ids| {
                    marker_ids.remove(&marker_id);
                    marker_ids.is_empty()
                });
            if remove_empty_entry {
                self.markers_by_buffer.remove(&previous_buffer_id);
            }
        }
        if let Some(buffer_id) = buffer_id {
            self.markers_by_buffer
                .entry(buffer_id)
                .or_default()
                .insert(marker_id);
        }
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
            .find_marker(id)
            .and_then(|marker| marker.mark_buffer_id);
        let previous_buffer_id;
        {
            let marker = self
                .find_marker_mut(id)
                .ok_or_else(|| LispError::TypeError("marker".into(), format!("marker<{}>", id)))?;
            previous_buffer_id = marker.buffer_id;
            marker.buffer_id = buffer_id;
            marker.position = position;
            if let Some(pos) = position {
                marker.last_position = Some(pos);
            }
        }
        self.update_marker_buffer_index(id, previous_buffer_id, buffer_id);
        if let Some(mark_buffer_id) = mark_buffer_id
            && let Some(buffer) = self.get_buffer_by_id_mut(mark_buffer_id)
        {
            if buffer_id == Some(mark_buffer_id) {
                if let Some(position) = position {
                    buffer.set_mark_position(position);
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
                return Err(LispError::WrongTypeArgument(
                    "integer-or-marker-p".into(),
                    value.clone(),
                ));
            }
        }
        self.set_marker_insertion_type(marker_id, insertion_type);
        Ok(marker_value)
    }

    pub fn make_char_table(&mut self, subtype: Option<String>, default: Value) -> Value {
        let id = self.next_char_table_id;
        self.next_char_table_id += 1;
        self.char_tables
            .push(CharTableState::new(id, subtype, default));
        Value::CharTable(id)
    }

    pub fn replace_hash_table_runtime_entries(
        &mut self,
        id: u64,
        test: &str,
        entries: Vec<(Value, Value)>,
    ) {
        let requested = self
            .find_record(id)
            .and_then(|record| record.slots.get(2))
            .and_then(|value| value.as_integer().ok())
            .and_then(|value| usize::try_from(value).ok())
            .unwrap_or(0);
        let previous_capacity = self.gnu_hash_table_capacity(id).unwrap_or(requested);
        let capacity = super::gnu_hash_grown_capacity(previous_capacity, entries.len());
        let test = match test {
            "eq" => RuntimeHashTest::Eq,
            "eql" => RuntimeHashTest::Eql,
            "equal" => RuntimeHashTest::Equal,
            _ => {
                self.equal_hash_tables.remove(&id);
                if entries.is_empty() {
                    self.custom_hash_tables
                        .insert(id, CustomHashTableState::empty(capacity));
                } else {
                    // Restoring a serialized custom table has no saved hash
                    // codes.  Leave it on the correct linear fallback until
                    // it is cleared; ordinary construction starts empty and
                    // stays on the indexed path.
                    self.custom_hash_tables.remove(&id);
                }
                return;
            }
        };
        self.custom_hash_tables.remove(&id);
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
                capacity,
                slot_indices: (0..entries.len()).collect(),
                next_slot: entries.len(),
                entries,
                free_slots: Vec::new(),
                key_index,
            },
        );
    }

    pub(crate) fn gnu_hash_table_capacity(&self, id: u64) -> Option<usize> {
        let record = self
            .find_record(id)
            .filter(|record| record.kind == RecordKind::HashTable)?;
        if let Some(state) = self.equal_hash_tables.get(&id) {
            return Some(state.capacity);
        }
        if let Some(state) = self.custom_hash_tables.get(&id) {
            return Some(state.capacity);
        }
        let requested = record
            .slots
            .get(2)
            .and_then(|value| value.as_integer().ok())
            .and_then(|value| usize::try_from(value).ok())
            .unwrap_or(0);
        let high_water = record
            .slots
            .get(1)
            .map(crate::lisp::json::hash_table_entry_list_len)
            .unwrap_or(0);
        Some(super::gnu_hash_grown_capacity(requested, high_water))
    }

    fn note_gnu_hash_table_growth(&self, before: usize, after: usize) {
        let old_bytes = super::gnu_hash_table_storage_bytes(before);
        let new_bytes = super::gnu_hash_table_storage_bytes(after);
        crate::lisp::native_comp::note_lisp_allocation(new_bytes.saturating_sub(old_bytes));
    }

    pub fn hash_table_runtime_entries(&self, id: u64) -> Option<&Vec<(Value, Value)>> {
        self.equal_hash_tables
            .get(&id)
            .map(|state| &state.entries)
            .or_else(|| self.custom_hash_tables.get(&id).map(|state| &state.entries))
    }

    /// Return the first live key/value slot at or after `minimum_slot`.
    /// fns.c:DOHASH_SAFE advances through the hash table's numeric storage
    /// slots and reloads each entry after the preceding callback, rather than
    /// snapshotting the whole table.  The compact Rust vectors are maintained
    /// in that same slot order, so a partition point recovers the next GNU
    /// slot without scanning unused capacity.
    pub(crate) fn hash_table_entry_at_or_after(
        &self,
        id: u64,
        minimum_slot: usize,
    ) -> Option<Option<(usize, Value, Value)>> {
        let (entries, slot_indices) = if let Some(state) = self.equal_hash_tables.get(&id) {
            (&state.entries, &state.slot_indices)
        } else {
            let state = self.custom_hash_tables.get(&id)?;
            (&state.entries, &state.slot_indices)
        };
        let index = slot_indices.partition_point(|slot| *slot < minimum_slot);
        Some(
            entries
                .get(index)
                .zip(slot_indices.get(index))
                .map(|((key, value), slot)| (*slot, key.clone(), value.clone())),
        )
    }

    pub(crate) fn has_custom_hash_table_index(&self, id: u64) -> bool {
        self.custom_hash_tables.contains_key(&id)
    }

    pub(crate) fn custom_hash_candidates(
        &self,
        id: u64,
        hash: i64,
    ) -> Option<Vec<(usize, Value, Value)>> {
        let state = self.custom_hash_tables.get(&id)?;
        Some(
            state
                .key_index
                .get(&hash)
                .into_iter()
                .flatten()
                .filter_map(|&index| {
                    state
                        .entries
                        .get(index)
                        .map(|(key, value)| (index, key.clone(), value.clone()))
                })
                .collect(),
        )
    }

    pub(crate) fn custom_hash_put_at(
        &mut self,
        id: u64,
        hash: i64,
        existing_index: Option<usize>,
        key: Value,
        value: Value,
    ) -> bool {
        let capacity_before = self.gnu_hash_table_capacity(id).unwrap_or(0);
        let Some(state) = self.custom_hash_tables.get_mut(&id) else {
            return false;
        };
        if let Some(index) = existing_index {
            let Some((_, existing_value)) = state.entries.get_mut(index) else {
                return false;
            };
            *existing_value = value;
            return true;
        }

        let slot = state.free_slots.pop().unwrap_or_else(|| {
            let slot = state.next_slot;
            state.next_slot += 1;
            slot
        });
        let index = state
            .slot_indices
            .binary_search(&slot)
            .unwrap_or_else(|index| index);
        let inserted_in_middle = index != state.entries.len();
        state.slot_indices.insert(index, slot);
        state.entries.insert(index, (key, value));
        state.hashes.insert(index, hash);
        if inserted_in_middle {
            state.rebuild_index();
        } else {
            state.key_index.entry(hash).or_default().push(index);
        }
        if let Some(state) = self.custom_hash_tables.get_mut(&id) {
            state.capacity = super::gnu_hash_grown_capacity(capacity_before, state.next_slot);
        }
        let capacity_after = self.gnu_hash_table_capacity(id).unwrap_or(capacity_before);
        self.note_gnu_hash_table_growth(capacity_before, capacity_after);
        true
    }

    pub(crate) fn custom_hash_remove_at(&mut self, id: u64, index: usize) -> bool {
        let Some(state) = self.custom_hash_tables.get_mut(&id) else {
            return false;
        };
        if index >= state.entries.len() {
            return false;
        }
        state.entries.remove(index);
        state.hashes.remove(index);
        let freed_slot = state.slot_indices.remove(index);
        state.free_slots.push(freed_slot);
        state.rebuild_index();
        true
    }

    pub(crate) fn clear_custom_hash_table(&mut self, id: u64) -> bool {
        let Some(state) = self.custom_hash_tables.get_mut(&id) else {
            return false;
        };
        state.entries.clear();
        state.hashes.clear();
        state.slot_indices.clear();
        state.free_slots.clear();
        state.next_slot = 0;
        state.key_index.clear();
        true
    }

    /// fns.c:sweep_weak_table removes entries inside the collector.  It does
    /// not call `remhash', consult the public mutability guard, or invoke a
    /// user hash function.  Preserve allocated capacity and slot/free-list
    /// state while rebuilding only the derived lookup index.
    pub(crate) fn sweep_weak_hash_table(
        &mut self,
        id: u64,
        entries: Vec<(Value, Value)>,
        keep: &[bool],
    ) {
        if let Some(mut state) = self.equal_hash_tables.remove(&id) {
            for index in (0..state.entries.len()).rev() {
                if !keep.get(index).copied().unwrap_or(false) {
                    state.entries.remove(index);
                    let freed_slot = state.slot_indices.remove(index);
                    state.free_slots.push(freed_slot);
                }
            }
            state.key_index.clear();
            for (index, (key, _)) in state.entries.iter().enumerate() {
                let hash = crate::lisp::primitives::runtime_hash_bucket_key(self, state.test, key);
                state.key_index.entry(hash).or_default().push(index);
            }
            self.equal_hash_tables.insert(id, state);
            return;
        }
        if let Some(mut state) = self.custom_hash_tables.remove(&id) {
            for index in (0..state.entries.len()).rev() {
                if !keep.get(index).copied().unwrap_or(false) {
                    state.entries.remove(index);
                    state.hashes.remove(index);
                    let freed_slot = state.slot_indices.remove(index);
                    state.free_slots.push(freed_slot);
                }
            }
            state.rebuild_index();
            self.custom_hash_tables.insert(id, state);
            return;
        }

        let retained = entries
            .into_iter()
            .zip(keep.iter().copied())
            .filter_map(|(entry, keep)| keep.then_some(entry))
            .collect::<Vec<_>>();
        if let Some(record) = self.find_record_mut(id)
            && record.kind == RecordKind::HashTable
        {
            if record.slots.len() < 2 {
                record.slots.resize(2, Value::Nil);
            }
            record.slots[1] = crate::lisp::primitives::hash_table_entries_to_value(retained);
        }
    }

    /// Enter GNU fns.c's immutable critical section for a user-defined hash
    /// or comparison call.  A nested callback on the same table observes the
    /// existing section and must not restore mutability when it returns.
    pub(crate) fn enter_hash_table_test(&mut self, id: u64) -> bool {
        self.hash_tables_under_test.insert(id)
    }

    pub(crate) fn leave_hash_table_test(&mut self, id: u64, entered: bool) {
        if entered {
            self.hash_tables_under_test.remove(&id);
        }
    }

    pub(crate) fn hash_table_is_mutable(&self, id: u64) -> bool {
        !self.hash_tables_under_test.contains(&id) && !self.immutable_hash_tables.contains(&id)
    }

    pub(crate) fn mark_hash_table_immutable(&mut self, id: u64) {
        self.immutable_hash_tables.insert(id);
    }

    fn value_contains_positioned_symbol(
        &self,
        value: &Value,
        visited: &mut std::collections::HashSet<usize>,
    ) -> bool {
        if crate::lisp::primitives::symbol_with_pos_parts(self, value).is_some() {
            return true;
        }
        let Value::Cons(cell) = value else {
            return false;
        };
        let identity = crate::lisp::types::ConsCell::identity(cell);
        if !visited.insert(identity) {
            return false;
        }
        self.value_contains_positioned_symbol(&cell.car.borrow(), visited)
            || self.value_contains_positioned_symbol(&cell.cdr.borrow(), visited)
    }

    pub fn reindex_hash_table_runtime_entries_in_env(&mut self, id: u64, env: &Env) {
        let Some(state) = self.equal_hash_tables.get(&id) else {
            return;
        };
        let test = state.test;
        let positions_enabled = test == RuntimeHashTest::Equal
            && crate::lisp::primitives::symbols_with_pos_enabled(self, env);
        let hashes = state
            .entries
            .iter()
            .map(|(key, _)| {
                if positions_enabled
                    && self.value_contains_positioned_symbol(
                        key,
                        &mut std::collections::HashSet::new(),
                    )
                {
                    // GNU hashes the bare-symbol projection while the dynamic
                    // mode is enabled.  This reserved bucket is internal and
                    // deliberately unreachable by the ordinary, disabled
                    // structural hash; enabled operations use the exact
                    // env-aware scan below.  Thus toggling the mode preserves
                    // GNU's stale-bucket miss instead of finding the wrapper
                    // under a hash that was never used to insert it.
                    Some(i64::MIN)
                } else {
                    crate::lisp::primitives::runtime_hash_bucket_key(self, test, key)
                }
            })
            .collect::<Vec<_>>();
        let mut key_index: HashMap<
            Option<i64>,
            Vec<usize>,
            crate::lisp::primitives::FnvBuildHasher,
        > = HashMap::default();
        for (index, hash) in hashes.into_iter().enumerate() {
            key_index.entry(hash).or_default().push(index);
        }
        self.equal_hash_tables
            .get_mut(&id)
            .expect("hash table disappeared while reindexing")
            .key_index = key_index;
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
            RuntimeHashTest::Eql => {
                crate::lisp::primitives::values_eql_in_env(self, stored, probe, env)
            }
            RuntimeHashTest::Equal => {
                crate::lisp::primitives::values_equal_in_env(self, stored, probe, env)
            }
        }
    }

    pub fn equal_hash_lookup(&self, id: u64, key: &Value, env: &Env) -> Option<Option<Value>> {
        let state = self.equal_hash_tables.get(&id)?;
        // `equal' dynamically treats a symbol-with-position as its bare
        // symbol while this byte-compiler switch is enabled.  Scan the
        // authoritative entries rather than returning a fallback sentinel:
        // internal C-equivalent callers such as purecopy use this API
        // directly and must retain complete hash-table behavior too.
        if state.test == RuntimeHashTest::Equal
            && crate::lisp::primitives::symbols_with_pos_enabled(self, env)
        {
            return Some(
                state
                    .entries
                    .iter()
                    .find(|(existing, _)| {
                        self.runtime_hash_keys_match(state.test, existing, key, env)
                    })
                    .map(|(_, value)| value.clone()),
            );
        }
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
        let capacity_before = self.gnu_hash_table_capacity(id).unwrap_or(0);
        let Some(state) = self.equal_hash_tables.get(&id) else {
            return false;
        };
        let test = state.test;
        let positioned_equal = test == RuntimeHashTest::Equal
            && crate::lisp::primitives::symbols_with_pos_enabled(self, env);
        let hash = if positioned_equal
            && self.value_contains_positioned_symbol(&key, &mut std::collections::HashSet::new())
        {
            Some(i64::MIN)
        } else {
            crate::lisp::primitives::runtime_hash_bucket_key(self, test, &key)
        };
        let existing_index = if positioned_equal {
            state
                .entries
                .iter()
                .position(|(existing, _)| self.runtime_hash_keys_match(test, existing, &key, env))
        } else {
            state
                .key_index
                .get(&hash)
                .into_iter()
                .flatten()
                .copied()
                .find(|index| {
                    state.entries.get(*index).is_some_and(|(existing, _)| {
                        self.runtime_hash_keys_match(test, existing, &key, env)
                    })
                })
        };

        let inserted_in_middle = {
            let state = self
                .equal_hash_tables
                .get_mut(&id)
                .expect("equal hash table disappeared during lookup");
            if let Some(index) = existing_index {
                state.entries[index].1 = value;
                false
            } else {
                let slot = state.free_slots.pop().unwrap_or_else(|| {
                    let slot = state.next_slot;
                    state.next_slot += 1;
                    slot
                });
                let index = state
                    .slot_indices
                    .binary_search(&slot)
                    .unwrap_or_else(|i| i);
                let inserted_in_middle = index != state.entries.len();
                state.slot_indices.insert(index, slot);
                state.entries.insert(index, (key, value));
                if !inserted_in_middle {
                    state.key_index.entry(hash).or_default().push(index);
                }
                inserted_in_middle
            }
        };

        // Inserting into a reused slot can shift compact-vector indexes, so
        // rebuild the acceleration index from the authoritative slot order.
        // The normal append path remains O(1).  GNU likewise only rebuilds
        // bucket links when storage moves.
        if inserted_in_middle {
            let mut key_index: HashMap<
                Option<i64>,
                Vec<usize>,
                crate::lisp::primitives::FnvBuildHasher,
            > = HashMap::default();
            let state = self
                .equal_hash_tables
                .get(&id)
                .expect("equal hash table disappeared after insertion");
            for (index, (entry_key, _)) in state.entries.iter().enumerate() {
                let hash = if positioned_equal
                    && self.value_contains_positioned_symbol(
                        entry_key,
                        &mut std::collections::HashSet::new(),
                    ) {
                    Some(i64::MIN)
                } else {
                    crate::lisp::primitives::runtime_hash_bucket_key(self, test, entry_key)
                };
                key_index.entry(hash).or_default().push(index);
            }
            self.equal_hash_tables
                .get_mut(&id)
                .expect("equal hash table disappeared after index rebuild")
                .key_index = key_index;
        }
        if let Some(state) = self.equal_hash_tables.get_mut(&id) {
            state.capacity = super::gnu_hash_grown_capacity(capacity_before, state.next_slot);
        }
        let capacity_after = self.gnu_hash_table_capacity(id).unwrap_or(capacity_before);
        self.note_gnu_hash_table_growth(capacity_before, capacity_after);
        true
    }

    pub fn equal_hash_remove(&mut self, id: u64, key: &Value, env: &Env) -> Option<bool> {
        let state = self.equal_hash_tables.get(&id)?;
        let test = state.test;
        let positioned_equal = test == RuntimeHashTest::Equal
            && crate::lisp::primitives::symbols_with_pos_enabled(self, env);
        let hash = crate::lisp::primitives::runtime_hash_bucket_key(self, test, key);
        let existing_index = if positioned_equal {
            state
                .entries
                .iter()
                .position(|(existing, _)| self.runtime_hash_keys_match(test, existing, key, env))
        } else {
            state
                .key_index
                .get(&hash)
                .into_iter()
                .flatten()
                .copied()
                .find(|index| {
                    state.entries.get(*index).is_some_and(|(existing, _)| {
                        self.runtime_hash_keys_match(test, existing, key, env)
                    })
                })
        };
        let Some(existing_index) = existing_index else {
            return Some(false);
        };
        let state = self
            .equal_hash_tables
            .get_mut(&id)
            .expect("equal hash table disappeared during removal");
        state.entries.remove(existing_index);
        let freed_slot = state.slot_indices.remove(existing_index);
        state.free_slots.push(freed_slot);

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
            let hash = if positioned_equal
                && self.value_contains_positioned_symbol(
                    entry_key,
                    &mut std::collections::HashSet::new(),
                ) {
                Some(i64::MIN)
            } else {
                crate::lisp::primitives::runtime_hash_bucket_key(self, test, entry_key)
            };
            key_index.entry(hash).or_default().push(index);
        }
        self.equal_hash_tables
            .get_mut(&id)
            .expect("equal hash table disappeared after rebuilding")
            .key_index = key_index;
        Some(true)
    }

    pub fn find_char_table(&self, id: u64) -> Option<&CharTableState> {
        let index = usize::try_from(id.checked_sub(1)?).ok()?;
        self.char_tables.get(index).filter(|table| table.id == id)
    }

    pub fn find_char_table_mut(&mut self, id: u64) -> Option<&mut CharTableState> {
        let index = usize::try_from(id.checked_sub(1)?).ok()?;
        if self
            .char_tables
            .get(index)
            .is_none_or(|table| table.id != id)
        {
            return None;
        }
        // This is the sole native door to mutable table contents.  Bump one
        // shared generation here so derived views over parent chains cannot
        // survive a write through an otherwise unrelated public operation.
        self.char_table_mutation_generation = self.char_table_mutation_generation.wrapping_add(1);
        self.char_tables
            .get_mut(index)
            .filter(|table| table.id == id)
    }

    pub(crate) fn cached_regexp_syntax_classes(&self, table_id: u64) -> Option<[String; 16]> {
        self.regexp_syntax_class_cache
            .borrow()
            .as_ref()
            .filter(|cache| {
                cache.table_id == table_id
                    && cache.char_table_generation == self.char_table_mutation_generation
            })
            .map(|cache| cache.rendered.clone())
    }

    pub(crate) fn cached_syntax_segments(
        &self,
        table_id: u64,
    ) -> Option<std::rc::Rc<Vec<(u32, u32, crate::lisp::primitives::syntax::SyntaxClass)>>> {
        self.syntax_segment_cache
            .borrow()
            .as_ref()
            .filter(|cache| {
                cache.table_id == table_id
                    && cache.char_table_generation == self.char_table_mutation_generation
            })
            .map(|cache| cache.segments.clone())
    }

    pub(crate) fn cache_syntax_segments(
        &self,
        table_id: u64,
        segments: std::rc::Rc<Vec<(u32, u32, crate::lisp::primitives::syntax::SyntaxClass)>>,
    ) {
        *self.syntax_segment_cache.borrow_mut() = Some(crate::lisp::eval::SyntaxSegmentCache {
            table_id,
            char_table_generation: self.char_table_mutation_generation,
            segments,
        });
    }

    pub(crate) fn cache_regexp_syntax_classes(&self, table_id: u64, rendered: [String; 16]) {
        *self.regexp_syntax_class_cache.borrow_mut() = Some(RegexpSyntaxClassCache {
            table_id,
            char_table_generation: self.char_table_mutation_generation,
            rendered,
        });
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
        table.push_entry(CharTableEntry {
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

    /// The shared char-table write generation (see find_char_table_mut):
    /// caches derived from any char table key on this to observe every
    /// mutation, exactly as GNU's compile_pattern re-checks its cached
    /// entry's syntax table with EQ before reuse.
    pub(crate) fn char_table_generation(&self) -> u64 {
        self.char_table_mutation_generation
    }

    pub fn char_table_get(&self, id: u64, key: u32) -> Option<Value> {
        let table = self.find_char_table(id)?;
        if let Some(entry) = table.explicit_entry(key) {
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

    /// Resolve the first explicit entry in a character-table parent chain
    /// without cloning its Lisp value.  When no entry applies, return the
    /// terminal table whose default owns the result.  Subsystems that need a
    /// native view (notably syntax scanning) can thereby avoid constructing a
    /// public Lisp descriptor merely to decode it again.
    pub(crate) fn char_table_explicit_or_terminal(
        &self,
        mut id: u64,
        key: u32,
    ) -> Option<(Option<&Value>, &CharTableState)> {
        for _ in 0..=self.char_tables.len() {
            let table = self.find_char_table(id)?;
            if let Some(entry) = table.explicit_entry(key) {
                return Some((Some(&entry.value), table));
            }
            let Some(parent_id) = table.parent else {
                return Some((None, table));
            };
            if self.find_char_table(parent_id).is_none() {
                return Some((None, table));
            }
            id = parent_id;
        }
        None
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
        Some(self.find_char_table(id)?.effective_ranges())
    }

    /// The raw write log of char-table ID, in order; `equal' compares two
    /// tables entry by entry, so a copy that must stay `equal' to its
    /// original rewrites values through `char_table_replace_entries'
    /// rather than appending.
    pub(crate) fn char_table_entries(&self, id: u64) -> Option<Vec<CharTableEntry>> {
        Some(self.find_char_table(id)?.entries.clone())
    }

    pub(crate) fn char_table_replace_entries(
        &mut self,
        id: u64,
        entries: Vec<CharTableEntry>,
    ) -> Result<(), LispError> {
        let table = self.find_char_table_mut(id).ok_or_else(|| {
            LispError::TypeError("char-table".into(), format!("char-table<{id}>"))
        })?;
        table.replace_entries(entries);
        Ok(())
    }

    pub fn char_table_subtype(&self, id: u64) -> Option<Option<String>> {
        self.find_char_table(id).map(|table| table.subtype.clone())
    }

    pub fn char_table_parent(&self, id: u64) -> Option<Option<u64>> {
        self.find_char_table(id).map(|table| table.parent)
    }

    pub fn char_table_explicit_get(&self, id: u64, key: u32) -> Option<Value> {
        let table = self.find_char_table(id)?;
        if let Some(entry) = table.explicit_entry(key) {
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

    /// Copy a syntax table with GNU's syntax-specific default and parent
    /// rules.  A raw character-table clone is not sufficient: only the
    /// standard syntax table owns a root default, while every copy inherits
    /// from the standard table when its source had no parent.
    pub fn copy_syntax_table(&mut self, id: u64) -> Result<Value, LispError> {
        let copy = self.clone_char_table(id)?;
        let Value::CharTable(copy_id) = copy else {
            unreachable!("clone_char_table returns a character table")
        };
        let standard_id = self.standard_syntax_table_id;
        let table = self.find_char_table_mut(copy_id).ok_or_else(|| {
            LispError::TypeError("char-table".into(), format!("char-table<{copy_id}>"))
        })?;
        table.default = Value::Nil;
        table.parent.get_or_insert(standard_id);
        Ok(Value::CharTable(copy_id))
    }

    pub fn create_record(&mut self, type_name: &str, slots: Vec<Value>) -> Value {
        self.create_record_with_type(Value::symbol(type_name), slots)
    }

    pub fn create_record_with_type(&mut self, type_tag: Value, slots: Vec<Value>) -> Value {
        self.create_record_with_kind(type_tag, slots, RecordKind::Record)
    }

    pub(crate) fn create_pseudovector(
        &mut self,
        kind: RecordKind,
        type_name: &str,
        slots: Vec<Value>,
    ) -> Value {
        debug_assert_ne!(kind, RecordKind::Record);
        self.create_record_with_kind(Value::symbol(type_name), slots, kind)
    }

    /// alloc.c:Fmake_closure.  Both the ordinary primitive dispatcher and
    /// the native Lisp_Object ABI use this one C-owned operation so the
    /// prototype copy, constant-vector replacement, and errors cannot drift.
    pub(crate) fn make_closure(
        &mut self,
        prototype: &Value,
        closure_vars: &[Value],
    ) -> Result<Value, LispError> {
        let Value::Record(id) = prototype else {
            return Err(LispError::WrongTypeArgument(
                "byte-code-function-p".into(),
                prototype.clone(),
            ));
        };
        let Some(record) = self.find_record(*id) else {
            return Err(LispError::WrongTypeArgument(
                "byte-code-function-p".into(),
                prototype.clone(),
            ));
        };
        if record.kind != RecordKind::Closure {
            return Err(LispError::WrongTypeArgument(
                "byte-code-function-p".into(),
                prototype.clone(),
            ));
        }
        let mut slots = record.slots.clone();
        let mut constants = slots
            .get(2)
            .and_then(|slot| crate::lisp::primitives::vector_items(slot).ok())
            .ok_or_else(|| {
                LispError::Signal("make-closure prototype has no constants vector".into())
            })?;
        if closure_vars.len() > constants.len() {
            return Err(LispError::Signal(
                "Closure vars do not fit in constvec".into(),
            ));
        }
        constants[..closure_vars.len()].clone_from_slice(closure_vars);
        slots[2] = Value::vector(constants);
        Ok(self.create_pseudovector(RecordKind::Closure, "byte-code-function", slots))
    }

    fn create_record_with_kind(
        &mut self,
        type_tag: Value,
        slots: Vec<Value>,
        kind: RecordKind,
    ) -> Value {
        let vector_slots = kind.gnu_vector_slots(slots.len());
        if vector_slots != 0 {
            crate::lisp::native_comp::note_lisp_allocation(vector_slots.saturating_mul(8));
        }
        let id = self.alloc_record_id();
        let indexed_type = type_tag.as_symbol().ok().map(str::to_owned);
        self.records.push(RecordState {
            id,
            type_tag,
            slots,
            kind,
        });
        if let Some(type_name) = indexed_type {
            self.record_ids_by_type_index
                .entry(type_name)
                .or_default()
                .insert(id);
        }
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
        // or materialized keymap index for this record can no longer be
        // trusted (see bytecode::vm and primitives::keymap_direct_bindings).
        if let Some(slot) = (id as usize)
            .checked_sub(1)
            .and_then(|index| self.bytecode_program_cache.get_mut(index))
        {
            *slot = None;
        }
        if let Some(slot) = (id as usize)
            .checked_sub(1)
            .and_then(|index| self.keymap_bindings_cache.get_mut().get_mut(index))
        {
            *slot = None;
        }
        let index = (id as usize).checked_sub(1)?;
        match self.records.get(index) {
            Some(record) if record.id == id => self.records.get_mut(index),
            _ => self.records.iter_mut().find(|record| record.id == id),
        }
    }

    pub(crate) fn register_keymap_public_cons_owners(&mut self, keymap_id: u64, view: &Value) {
        if let Some(old_ids) = self.keymap_public_cons_ids.remove(&keymap_id) {
            for cell_id in old_ids {
                let mut remove = false;
                if let Some(owners) = self.keymap_public_cons_owners.get_mut(&cell_id) {
                    owners.retain(|owner| *owner != keymap_id);
                    remove = owners.is_empty();
                }
                if remove {
                    self.keymap_public_cons_owners.remove(&cell_id);
                }
            }
        }

        let mut seen = std::collections::HashSet::new();
        let mut owned_ids = Vec::new();
        let mut tail = view.clone();
        while let Value::Cons(cell) = tail {
            let cell_id = crate::lisp::types::ConsCell::identity(&cell);
            if !seen.insert(cell_id) {
                break;
            }
            owned_ids.push(cell_id);
            self.keymap_public_cons_owners
                .entry(cell_id)
                .or_default()
                .push(keymap_id);

            // A binding pair is itself mutable keymap structure.  Do not
            // claim arbitrary binding definitions or included keymap roots;
            // those either are not structure or have their own owner.
            let entry = cell.car.borrow().clone();
            if let Value::Cons(entry_cell) = &entry
                && !matches!(entry.car(), Ok(Value::Symbol(ref name)) if name == "keymap")
            {
                let entry_id = crate::lisp::types::ConsCell::identity(entry_cell);
                owned_ids.push(entry_id);
                self.keymap_public_cons_owners
                    .entry(entry_id)
                    .or_default()
                    .push(keymap_id);
            }
            tail = cell.cdr.borrow().clone();
        }
        self.keymap_public_cons_ids.insert(keymap_id, owned_ids);
    }

    pub(crate) fn keymap_public_cons_owner_ids(&self, value: &Value) -> Vec<u64> {
        value
            .cons_id()
            .and_then(|id| self.keymap_public_cons_owners.get(&id))
            .cloned()
            .unwrap_or_default()
    }

    pub(crate) fn keymap_public_root_owner_id(&self, value: &Value) -> Option<u64> {
        let root = value.cons_id()?;
        self.keymap_public_cons_owners
            .get(&root)?
            .iter()
            .copied()
            .find(|owner| {
                self.keymap_public_cons_ids
                    .get(owner)
                    .and_then(|ids| ids.first())
                    .is_some_and(|id| *id == root)
            })
    }

    pub(crate) fn create_treesit_query(&mut self, language: Value, source: Value) -> Value {
        let query = self.create_pseudovector(
            RecordKind::TreeSitterCompiledQuery,
            "treesit-compiled-query",
            Vec::new(),
        );
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

    pub(crate) fn record_ids_by_type(&self, type_name: &str) -> Vec<u64> {
        self.record_ids_by_type_index
            .get(type_name)
            .map(|ids| ids.iter().copied().collect())
            .unwrap_or_default()
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
        let equal_hash_state = self.equal_hash_tables.get(&id).cloned();
        let custom_hash_state = self.custom_hash_tables.get(&id).cloned();
        let record = self
            .find_record(id)
            .cloned()
            .ok_or_else(|| LispError::TypeError("record".into(), format!("record<{id}>")))?;
        let mut slots = record.slots;
        if let Some(entries) = &hash_entries
            && slots.len() >= 2
        {
            // The sidecar below is GNU's key_and_value storage.  A second
            // Lisp-list representation would create non-GNU cons cells.
            let _ = entries;
            slots[1] = Value::Nil;
        }
        let test = slots
            .first()
            .and_then(|value| value.as_symbol().ok())
            .unwrap_or("eql")
            .to_string();
        let copy = self.create_record_with_kind(record.type_tag, slots, record.kind);
        if let Value::Record(copy_id) = &copy {
            if let Some(state) = custom_hash_state {
                crate::lisp::native_comp::note_lisp_allocation(
                    super::gnu_hash_table_storage_bytes(state.capacity),
                );
                self.equal_hash_tables.remove(copy_id);
                self.custom_hash_tables.insert(*copy_id, state);
            } else if let Some(state) = equal_hash_state {
                crate::lisp::native_comp::note_lisp_allocation(
                    super::gnu_hash_table_storage_bytes(state.capacity),
                );
                self.custom_hash_tables.remove(copy_id);
                self.equal_hash_tables.insert(*copy_id, state);
            } else if let Some(entries) = hash_entries {
                self.replace_hash_table_runtime_entries(*copy_id, &test, entries);
            }
        }
        Ok(copy)
    }

    // `aset' on a record's type slot stores the Lisp object verbatim.  GNU
    // permits both symbols and arbitrary type descriptors here.
    pub(crate) fn retag_record(&mut self, id: u64, type_tag: Value) -> Result<(), LispError> {
        let Some((record_kind, previous_type_name)) = self
            .find_record(id)
            .map(|record| (record.kind, record.symbol_type_name().map(str::to_owned)))
        else {
            return Err(LispError::TypeError(
                "record".into(),
                format!("record<{id}>"),
            ));
        };
        if record_kind != RecordKind::Record {
            return Err(LispError::TypeError(
                "record".into(),
                format!("record<{id}>"),
            ));
        }
        if let Some(previous_type_name) = previous_type_name {
            let remove_previous_type = self
                .record_ids_by_type_index
                .get_mut(&previous_type_name)
                .is_some_and(|ids| {
                    ids.remove(&id);
                    ids.is_empty()
                });
            if remove_previous_type {
                self.record_ids_by_type_index.remove(&previous_type_name);
            }
        }
        if let Ok(type_name) = type_tag.as_symbol() {
            self.record_ids_by_type_index
                .entry(type_name.to_string())
                .or_default()
                .insert(id);
        }
        self.find_record_mut(id)
            .expect("record identity was validated before retagging")
            .type_tag = type_tag;
        Ok(())
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

    fn record_require_in_load_history(&mut self, feature: &str) {
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
            Value::Symbol(feature.to_string().into()),
        );
        if entries.iter().any(|item| item == &entry) {
            return;
        }
        self.set_global_binding("current-load-list", Value::cons(entry, current_load_list));
    }

    pub(crate) fn record_definition_in_load_history(&mut self, kind: &str, name: &str) {
        let Some(current_load_list) = self.lookup_var("current-load-list", &Env::new()) else {
            return;
        };
        if current_load_list.is_nil() {
            return;
        }
        let entry = if kind == "defvar" {
            Value::Symbol(name.to_string().into())
        } else {
            Value::cons(
                Value::Symbol(kind.to_string().into()),
                Value::Symbol(name.to_string().into()),
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
        if old_definition.is_nil() {
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

    pub fn unprovide_feature(&mut self, feature: &str) {
        let features = self
            .provided_features
            .iter()
            .filter(|name| name.as_str() != feature)
            .cloned()
            .map(|value| Value::Symbol(value.into()));
        self.set_global_binding("features", Value::list(features));
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
            if !matches!(key, Value::Symbol(name) if name == feature) {
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

#[cfg(test)]
mod runtime_index_tests {
    use super::{CharTableEntry, Interpreter, Value};

    #[test]
    fn dense_char_table_ids_resolve_their_own_slots() {
        let mut interp = Interpreter::new();
        let Value::CharTable(first_id) =
            interp.make_char_table(Some("first".into()), Value::Integer(11))
        else {
            unreachable!("make_char_table must return a char table")
        };
        let Value::CharTable(second_id) =
            interp.make_char_table(Some("second".into()), Value::Integer(22))
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
        assert!(interp.find_char_table(0).is_none());
        assert!(interp.find_char_table(second_id + 1).is_none());
    }

    #[test]
    fn ascii_char_table_index_preserves_overrides_inheritance_and_mutation() {
        let mut interp = Interpreter::new();
        let Value::CharTable(parent_id) =
            interp.make_char_table(Some("parent".into()), Value::Integer(10))
        else {
            unreachable!("make_char_table must return a char table")
        };
        interp
            .char_table_set_range(parent_id, 'a' as u32, 'z' as u32, Value::Integer(20))
            .expect("parent range must be writable");

        let Value::CharTable(child_id) =
            interp.make_char_table(Some("child".into()), Value::Integer(30))
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
            Some(Value::Integer(20))
        );
        assert_eq!(
            interp.char_table_get(child_id, 'm' as u32),
            Some(Value::Integer(40))
        );
        assert_eq!(
            interp.char_table_get(child_id, 'x' as u32),
            Some(Value::Nil)
        );

        // Inherited results are deliberately not cached in the child.
        interp
            .char_table_set(parent_id, 'a' as u32, Value::Integer(21))
            .expect("parent character must remain writable");
        assert_eq!(
            interp.char_table_get(child_id, 'a' as u32),
            Some(Value::Integer(21))
        );

        let Value::CharTable(clone_id) = interp
            .clone_char_table(child_id)
            .expect("live child table must be cloneable")
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
            Some(Value::Nil)
        );

        // Non-ASCII characters retain the authoritative reverse range scan.
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

        // Whole-vector replacement and clearing rebuild the derived index.
        interp
            .find_char_table_mut(child_id)
            .expect("child table must remain live")
            .replace_entries(vec![CharTableEntry {
                start: 'q' as u32,
                end: 'q' as u32,
                value: Value::Integer(70),
            }]);
        assert_eq!(
            interp.char_table_get(child_id, 'q' as u32),
            Some(Value::Integer(70))
        );
        assert_eq!(
            interp.char_table_get(child_id, 'x' as u32),
            Some(Value::Integer(20))
        );
        interp
            .find_char_table_mut(child_id)
            .expect("child table must remain live")
            .clear_entries();
        assert_eq!(
            interp.char_table_get(child_id, 'q' as u32),
            Some(Value::Integer(20))
        );
    }

    #[test]
    fn record_type_index_tracks_creation_and_retagging() {
        let mut interp = Interpreter::new();
        let Value::Record(first_id) = interp.create_record("before", Vec::new()) else {
            unreachable!("create_record must return a record")
        };
        let Value::Record(second_id) = interp.create_record("before", Vec::new()) else {
            unreachable!("create_record must return a record")
        };

        assert_eq!(
            interp.record_ids_by_type("before"),
            vec![first_id, second_id]
        );
        interp
            .retag_record(first_id, Value::symbol("after"))
            .expect("record must remain live");
        assert_eq!(interp.record_ids_by_type("before"), vec![second_id]);
        assert_eq!(interp.record_ids_by_type("after"), vec![first_id]);

        interp
            .retag_record(second_id, Value::symbol("after"))
            .expect("record must remain live");
        assert!(interp.record_ids_by_type("before").is_empty());
        assert_eq!(
            interp.record_ids_by_type("after"),
            vec![first_id, second_id]
        );

        let descriptor = interp.create_record("descriptor", vec![Value::symbol("public-type")]);
        interp
            .retag_record(first_id, descriptor.clone())
            .expect("record must accept an arbitrary Lisp type descriptor");
        assert_eq!(interp.record_ids_by_type("after"), vec![second_id]);
        assert_eq!(
            interp.find_record(first_id).map(|record| &record.type_tag),
            Some(&descriptor)
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
