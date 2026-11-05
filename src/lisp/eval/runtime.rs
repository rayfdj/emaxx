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

    /// buffer.c:BUFFER_LIVE_P of the buffer ID names: a buffer of
    /// Vbuffer_alist, or print.c's hidden prin1 buffer, the one live
    /// buffer on no list, while it is held.
    pub fn has_buffer_id(&self, id: u64) -> bool {
        self.buffer_list
            .iter()
            .any(|(buffer_id, _)| *buffer_id == id)
            || (self.prin1_to_string_buffer == Some(id)
                && (id == self.current_buffer_id
                    || self
                        .inactive_buffers
                        .iter()
                        .any(|(buffer_id, _)| *buffer_id == id)))
    }

    /// The live buffer named NAME, text the host names (a buffer it made,
    /// such as *Messages*), with its name object.
    pub fn find_buffer(&self, name: &str) -> Option<(u64, Value)> {
        self.buffer_list
            .iter()
            .find(|(_, n)| primitives::string_is_text(n, name))
            .copied()
    }

    /// The id of the live buffer named NAME, host text.
    pub(crate) fn find_buffer_id(&self, name: &str) -> Option<u64> {
        self.find_buffer(name).map(|(id, _)| id)
    }

    /// buffer.c:Fget_buffer's search of Vbuffer_alist for the string
    /// NAME: assoc_ignore_text_properties, the first buffer whose name is
    /// `string-equal' to it.
    pub(crate) fn find_buffer_named(&self, name: &Value) -> Option<u64> {
        // buffer.c:Fget_buffer: the first buffer of the list whose name is
        // Fstring_equal to NAME (assoc_ignore_text_properties), NAME's
        // characters and bytes read once.
        let Kind::StringObject(key) = name.kind() else {
            return None;
        };
        // SAFETY: the walk runs no Lisp, collects nothing and stores into
        // no string.
        let (bytes, chars) = unsafe { key.bytes_and_chars_unguarded() };
        self.buffer_list
            .iter()
            .find(|(_, other)| {
                other.word() == name.word()
                    || match other.kind() {
                        Kind::StringObject(other) => {
                            // SAFETY: as above.
                            let (other_bytes, other_chars) =
                                unsafe { other.bytes_and_chars_unguarded() };
                            other_chars == chars && other_bytes == bytes
                        }
                        _ => false,
                    }
            })
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

    /// lread.c:save_match_data_load: Fload with the match data saved
    /// around it, so an autoload triggered between a `string-match' and
    /// its `match-end' (dired-aux's `cl-member' inside
    /// `dired--need-confirm-positions') leaves the caller's match data
    /// intact.
    fn save_match_data_load(
        &mut self,
        arguments: [Value; 5],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let saved = crate::lisp::primitives::call(self, "match-data", &[], env)?;
        // The saved data holds markers (a buffer match), and lread.c keeps
        // it on the specpdl (record_unwind_save_match_data) where the
        // collector reaches it; held here through the load, it is a
        // registered root, or a collection inside the load unchains its
        // markers and the restore signals.
        let result = self.with_lisp_stack_roots(&saved, |interp| {
            primitives::load_file(interp, &arguments, env).map(|(value, _)| value)
        });
        let restored = self.with_lisp_stack_roots(&result, |interp| {
            crate::lisp::primitives::call(interp, "set-match-data", &[saved, Value::T], env)
        });
        restored?;
        result
    }

    /// eval.c:load_with_autoload_queue: a load an autoload or `require'
    /// asked for, made with `Vautoload_queue' t so that `defalias' and
    /// `provide' record what they replace, and undone by un_autoload when
    /// the load signals: an autoload is a request to call a function, not
    /// to load a file.  ARGUMENTS are Fload's.
    pub(crate) fn load_with_autoload_queue(
        &mut self,
        arguments: [Value; 5],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let oldqueue = self.autoload_queue;
        self.autoload_queue = Value::T;
        let result = self.with_lisp_stack_roots(&oldqueue, |interp| {
            interp.save_match_data_load(arguments, env)
        });
        // Once loading finishes, don't undo it.
        if result.is_ok() {
            self.autoload_queue = Value::T;
        }
        // un_autoload leaves OLDQUEUE in the marked queue first.
        let undone =
            self.with_lisp_stack_roots(&result, |interp| interp.un_autoload(oldqueue, env));
        undone?;
        result
    }

    /// eval.c:un_autoload: restore `features' and the definitions the
    /// current `Vautoload_queue' records, newest first, and leave OLDQUEUE
    /// in its place.  A replaced definition comes back as the one
    /// `function-history' saved for it.
    fn un_autoload(&mut self, oldqueue: Value, env: &mut Env) -> Result<(), LispError> {
        let held = self.autoload_queue;
        self.autoload_queue = oldqueue;
        self.with_lisp_stack_roots(&held, |interp| {
            let mut queue = held;
            while let Kind::Cons(cell) = queue.kind() {
                let first = cell.car.get();
                if let Kind::Cons(entry) = first.kind()
                    && entry.car.get() == Value::Integer(0)
                {
                    interp.set_global_binding_key(cached_symbol!("features"), entry.cdr.get());
                } else {
                    let history = crate::lisp::primitives::call(
                        interp,
                        "get",
                        &[first, Value::symbol("function-history")],
                        env,
                    )?;
                    let rest = crate::lisp::primitives::call(interp, "cdr", &[history], env)?;
                    let definition = crate::lisp::primitives::call(interp, "car", &[rest], env)?;
                    crate::lisp::primitives::call(interp, "fset", &[first, definition], env)?;
                }
                queue = cell.cdr.get();
            }
            Ok(())
        })
    }

    /// The `--temacs' MODE this process will dump with, while lisp.h's
    /// will_dump_p holds: eval.c and fns.c clear gflags.will_dump_ before
    /// the error an autoload or `require' signals there, so reporting it
    /// cannot land here again.
    fn will_dump_mode(&self, env: &Env) -> Option<String> {
        if self.will_dump_reported {
            return None;
        }
        self.lookup_var_key(cached_symbol!("dump-mode"), env)
            .and_then(|mode| crate::lisp::primitives::string_like(&mode).map(|mode| mode.text))
            .filter(|mode| mode == "pdump" || mode == "pbootstrap")
    }

    /// lisp.h's will_dump_p.
    pub(crate) fn will_dump(&self, env: &Env) -> bool {
        self.will_dump_mode(env).is_some()
    }

    /// will_dump_p && !will_bootstrap_p: the `--temacs=pdump' process,
    /// which loadup.el must not make autoload or require (the preloaded
    /// files are listed explicitly).
    fn will_dump_without_bootstrap(&self, env: &Env) -> bool {
        self.will_dump_mode(env).is_some_and(|mode| mode == "pdump")
    }

    /// eval.c:Fautoload_do_load: load FUNDEF's file when it is an
    /// autoload (for MACRO-ONLY `macro', only one that defines a macro)
    /// and return FUNNAME's new definition; nil with FUNNAME nil, or for a
    /// best-effort MACRO-ONLY load of a function's file, whose absence is
    /// then no error.
    pub(crate) fn autoload_do_load(
        &mut self,
        fundef: Value,
        funname: Value,
        macro_only: Value,
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if !matches!(
            fundef.cons_values().map(|(head, _)| head.kind()),
            Some(Kind::Symbol(head)) if head.is_named("autoload")
        ) {
            return Ok(fundef);
        }
        let kind = crate::lisp::primitives::call(self, "nth", &[Value::Integer(4), fundef], env)?;
        let loads_macro = matches!(kind.kind(), Kind::T)
            || matches!(kind.kind(), Kind::Symbol(symbol) if symbol.is_named("t") || symbol.is_named("macro"));
        if matches!(macro_only.kind(), Kind::Symbol(symbol) if symbol.is_named("macro"))
            && !loads_macro
        {
            return Ok(fundef);
        }
        if self.will_dump_without_bootstrap(env) {
            self.will_dump_reported = true;
            let name = crate::lisp::primitives::call(self, "symbol-name", &[funname], env)?;
            return Err(primitives::values::doprnt_error_sdata(
                None,
                "Attempt to autoload {} while preparing to dump",
                &[name],
            ));
        }
        let symbol = crate::lisp::primitives::checked_symbol_name(self, &funname, env)?;
        let ignore_errors = if loads_macro { Value::Nil } else { macro_only };
        let file = crate::lisp::primitives::call(self, "cdr", &[fundef], env)?;
        let file = crate::lisp::primitives::call(self, "car", &[file], env)?;
        self.with_lisp_stack_roots(&[fundef, funname], |interp| {
            interp.load_with_autoload_queue(
                [file, ignore_errors, Value::T, Value::Nil, Value::T],
                env,
            )
        })?;
        if funname.is_nil() || ignore_errors.is_truthy() {
            return Ok(Value::Nil);
        }
        let function = self.with_lisp_stack_roots(&fundef, |interp| {
            crate::lisp::primitives::call(
                interp,
                "indirect-function",
                std::slice::from_ref(&funname),
                env,
            )
        })?;
        if crate::lisp::primitives::values_equal(self, &function, &fundef)? {
            // The file `load-history' names last, as Frequire's message.
            let loaded = self
                .lookup_var_key(cached_symbol!("load-history"), env)
                .unwrap_or(Value::Nil);
            let loaded = crate::lisp::primitives::call(self, "car", &[loaded], env)?;
            let loaded = crate::lisp::primitives::call(self, "car", &[loaded], env)?;
            let file = crate::lisp::primitives::string_like(&loaded)
                .map(|string| string.text)
                .unwrap_or_default();
            return Err(LispError::Signal(format!(
                "Autoloading file {file} failed to define function {symbol}"
            )));
        }
        Ok(function)
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
                .lookup_var_key(cached_symbol!("current-load-list"), &load_environment)
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
        crate::lisp::primitives::values::doprnt_quote_pair(self)
    }

    /// fns.c:Frequire's `require_nesting_list' around LOAD: FEATURE (EQ)
    /// may re-enter four active loads; the fifth signals, naming NAME.
    fn with_require_nesting<T>(
        &mut self,
        feature: SymbolName,
        name: &str,
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
            .filter(|nested| **nested == feature)
            .count();
        if nesting > 3 {
            // fns.c:3762 signals through `error' too, so this message is
            // requoted by the same rule as the one below.
            let (open, close) = quotes;
            return Err(LispError::Signal(format!(
                "Recursive {open}require{close} for feature {open}{name}{close}"
            )));
        }

        self.require_nesting.push(feature);
        let result = load(self);
        let popped = self.require_nesting.pop();
        debug_assert!(popped == Some(feature));
        result
    }

    /// fns.c:Frequire for FEATURE, FILENAME and NOERROR.
    pub(crate) fn require_feature_with_target(
        &mut self,
        symbol: SymbolName,
        filename: Value,
        noerror: Value,
        env: &mut Env,
    ) -> Result<Value, LispError> {
        // GNU records a file's dependency even when FEATURE was loaded
        // already.  `file-dependents' and `unload-feature' derive their
        // dependency graph from these entries.
        self.record_require_in_load_history(symbol);
        if self.has_feature_symbol(symbol) {
            return Ok(Value::Symbol(symbol));
        }
        // SDATA (SYMBOL_NAME (feature)): the name an uninterned feature
        // shares with the interned one, which names the file too.
        let feature = symbol.name_text();
        let (open, close) = self.effective_quote_pair();
        if self.will_dump_without_bootstrap(env) {
            self.will_dump_reported = true;
            return Err(LispError::Signal(format!(
                "(require {feature}) while preparing to dump"
            )));
        }
        // Fload with NOERROR, which only excuses FILE's absence, and a
        // mandatory suffix when the feature's name is the file's.
        let (file, must_suffix) = if filename.is_nil() {
            (symbol.lisp_name(), Value::T)
        } else {
            (filename, Value::Nil)
        };
        let loaded = self.with_require_nesting(symbol, &feature, (open, close), |interp| {
            interp.load_with_autoload_queue([file, noerror, Value::T, Value::Nil, must_suffix], env)
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
                .lookup_var_key(cached_symbol!("load-history"), env)
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
        match buffer_or_name.kind() {
            Kind::Buffer(buffer) => Ok(Some(buffer)),
            Kind::StringObject(_) => Ok(self
                .find_buffer_named(buffer_or_name)
                .and_then(|id| self.buffer_object(id))),
            _ => Err(LispError::WrongTypeArgument(
                "stringp".into(),
                *buffer_or_name,
            )),
        }
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
        .or_else(|error| self.find_buffer_named(value).ok_or(error))
    }

    /// Create and register a new empty buffer named NAME, host text.
    pub fn create_buffer(&mut self, name: &str) -> (u64, Value) {
        let id = self.create_buffer_named(Value::string(name));
        (
            id,
            self.buffer_list
                .last()
                .map_or(Value::Nil, |(_, name)| *name),
        )
    }

    /// buffer.c:Fget_buffer_create's new buffer for NAME, a string no live
    /// buffer has: BVAR (b, name) and last_name are a copy of NAME without
    /// its text properties (`Fcopy_sequence' then set_string_intervals),
    /// the object Vbuffer_alist's new last entry holds.
    pub(crate) fn create_buffer_named(&mut self, name: Value) -> u64 {
        let name = match name.kind() {
            Kind::StringObject(state) => Value::StringObject(
                state
                    .borrow()
                    .copy_without_properties(crate::lisp::alloc::StringAllocation::Ordinary),
            ),
            _ => name,
        };
        // GNU reset_buffer initializes a new buffer's directory from the
        // current buffer's slot (a `let' of it there included).
        let inherited_directory = self
            .symbol_value_key(cached_symbol!("default-directory"))
            .unwrap_or(Value::Nil);
        let id = self.alloc_buffer_id();
        let mut buffer = crate::buffer::Buffer::named(name);
        // buffer.c:Fget_buffer_create: `bset_last_name (b, name)'.
        buffer.set_last_name(name);
        buffer.text_conversion_style = self.native_text_conversion_style_default();
        // buffer.c:Fget_buffer_create: `bset_undo_list (b, SREF (name, 0)
        // != ' ' ? Qnil : Qt)', no undo kept in a buffer whose name starts
        // with a space (`with-temp-buffer's, the printer's, `load's).
        if primitives::string_starts_with_byte(&name, b' ') {
            buffer.set_undo_list(Value::T);
        }
        self.inactive_buffers
            .push((id, crate::lisp::types::BufferRef::new(id, buffer)));
        self.buffer_list.push((id, name));
        // Every always-local slot is the new buffer's own from the start,
        // so neither a `let' in the creating buffer nor a `setq-default'
        // (buffer_defaults) reaches it.
        self.reset_always_local_slots(id, inherited_directory);
        primitives::casetab::reset_buffer_case_tables(self, id);
        id
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
            // Fset_buffer's error for a buffer no longer live (print.c's
            // print_prepare and the others select theirs through it).
            .ok_or_else(|| LispError::Signal("Selecting deleted buffer".into()))?;
        // buffer.c:set_buffer_internal_2 invalidates the indentation cache.
        self.invalidate_current_column();
        let current_id = self.current_buffer_id;
        let (_, next_buffer) = self.inactive_buffers.swap_remove(pos);
        let current_buffer = std::mem::replace(&mut self.buffer, next_buffer);
        self.inactive_buffers.push((current_id, current_buffer));
        self.current_buffer_id = id;
        if !self.indirect_buffers.is_empty() {
            self.share_undo_list_with_base(current_id, id);
            // record_buffer_markers of the old buffer, fetch_buffer_markers
            // of the new.
            current_buffer.borrow_mut().leave_position_markers();
            self.buffer.borrow_mut().enter_position_markers();
        }
        // The C variable record_point reads, which a buffer made since its
        // last store has yet to hold.
        self.buffer
            .borrow_mut()
            .set_undo_inhibit_record_point(self.undo_inhibit_record_point);
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
        let selected_window_already_displays_target = primitives::window_slot_buffer_id(
            self.find_record(self.selected_window_id)
                .and_then(|window| window.slots.get(primitives::WINDOW_BUFFER_SLOT)),
        ) == Some(id);
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
        self.unshow_window_buffer(window_id)?;
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
        primitives::window_slot_buffer_id(
            self.find_record(self.selected_window_id)
                .and_then(|record| record.slots.get(primitives::WINDOW_BUFFER_SLOT)),
        )
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
        primitives::window_marker_position(
            self,
            self.selected_window_id,
            primitives::WINDOW_START_SLOT,
        )
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
        let buffer_id = self.selected_window_buffer_id();
        primitives::set_window_marker_restricted(
            self,
            self.selected_window_id,
            primitives::WINDOW_START_SLOT,
            buffer_id,
            start,
        );
    }

    /// Show BUFFER_ID in the selected window (set-window-buffer without
    /// its Lisp bookkeeping); showing the buffer it already shows changes
    /// nothing.
    pub fn set_selected_window_buffer_id(&mut self, buffer_id: u64) -> Result<(), LispError> {
        let window_id = self.selected_window_id;
        if self.window_record_buffer_id(window_id) == Some(buffer_id) {
            return Ok(());
        }
        self.unshow_window_buffer(window_id)?;
        self.show_buffer_in_window(window_id, buffer_id, false);
        Ok(())
    }

    fn window_record_buffer_id(&self, window_id: u64) -> Option<u64> {
        primitives::window_slot_buffer_id(
            self.find_record(window_id)
                .and_then(|window| window.slots.get(primitives::WINDOW_BUFFER_SLOT)),
        )
    }

    /// window.c:unshow_buffer: WINDOW_ID stops showing its buffer.  The
    /// buffer's last_window_start becomes the window's start; the buffer
    /// takes the window's point unless the selected window shows it (that
    /// point is the buffer's own) or the window it was last selected in
    /// is another still showing it; and it forgets WINDOW_ID as that
    /// window.  A start or point marker that points nowhere is
    /// marker_position's error, before any of this changes.
    pub(crate) fn unshow_window_buffer(&mut self, window_id: u64) -> Result<(), LispError> {
        let Some(buffer_id) = self.window_record_buffer_id(window_id) else {
            return Ok(());
        };
        if !self.has_buffer_id(buffer_id) {
            return Ok(());
        }
        let marker_position = |interp: &Self, slot: usize| -> Result<Option<usize>, LispError> {
            match primitives::window_marker(interp, window_id, slot) {
                Some(marker) => marker
                    .position()
                    .map(Some)
                    .ok_or_else(|| LispError::Signal("Marker does not point anywhere".into())),
                None => Ok(None),
            }
        };
        let start = marker_position(self, primitives::WINDOW_START_SLOT)?;
        let last_selected = self
            .get_buffer_by_id(buffer_id)
            .map(|buffer| buffer.last_selected_window())
            .unwrap_or(Value::Nil);
        let last_selected_id = primitives::window_record_id_from_value(self, &last_selected);
        let point = if buffer_id != self.selected_window_buffer_id()
            && !last_selected_id.is_some_and(|id| {
                id != window_id && self.window_record_buffer_id(id) == Some(buffer_id)
            }) {
            marker_position(self, primitives::WINDOW_POINT_SLOT)?
        } else {
            None
        };
        if let Some(mut buffer) = self.get_buffer_by_id_mut(buffer_id) {
            if let Some(start) = start {
                buffer.set_last_window_start(start);
            }
            if let Some(point) = point {
                let point = point.clamp(buffer.point_min(), buffer.point_max());
                buffer.goto_char(point);
            }
            if last_selected_id == Some(window_id) {
                buffer.set_last_selected_window(Value::Nil);
            }
        }
        Ok(())
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
            .get_buffer_by_id(buffer_id)
            .map(|buffer| buffer.last_window_start())
            .unwrap_or(1)
            .clamp(point_min, point_max);
        let buffer = self.buffer_value(buffer_id).unwrap_or(Value::Nil);
        if let Some(window) = self.find_record_mut(window_id) {
            if window.slots.len() <= primitives::WINDOW_START_AT_LINE_BEG_SLOT {
                window
                    .slots
                    .resize(primitives::WINDOW_START_AT_LINE_BEG_SLOT + 1, Value::Nil);
            }
            // wset_buffer: w->contents is the buffer object.
            window.slots[primitives::WINDOW_BUFFER_SLOT] = buffer;
            window.slots[primitives::WINDOW_END_POS_SLOT] = Value::Integer(0);
            if !keep {
                window.slots[primitives::WINDOW_HSCROLL_SLOT] = Value::Integer(0);
                window.slots[primitives::WINDOW_MIN_HSCROLL_SLOT] = Value::Integer(0);
                window.slots[primitives::WINDOW_SUSPEND_AUTO_HSCROLL_SLOT] = Value::Nil;
                window.slots[primitives::WINDOW_START_AT_LINE_BEG_SLOT] = Value::Nil;
                window.slots[primitives::WINDOW_FORCE_START_SLOT] = Value::Nil;
                window.slots[primitives::WINDOW_BASE_LINE_SLOT] = Value::Nil;
            }
        }
        if !keep {
            for (slot, position) in [
                (primitives::WINDOW_POINT_SLOT, point),
                (primitives::WINDOW_OLD_POINT_SLOT, point),
                (primitives::WINDOW_START_SLOT, start),
            ] {
                primitives::set_window_marker(self, window_id, slot, buffer_id, position);
            }
        }
        // The point markers take the buffer's `window-point-insertion-type'.
        primitives::set_window_point_insertion_type(self, window_id, buffer_id);
        // set_window_buffer: the selected window is its buffer's last.
        if window_id == self.selected_window_id {
            let window = self.record_value(window_id);
            if let Some(mut shown) = self.get_buffer_by_id_mut(buffer_id) {
                shown.set_last_selected_window(window);
            }
        }
        self.stamp_buffer_display(buffer_id);
        self.wset_redisplay(window_id);
    }

    /// buffer.c:reset_buffer: the visited file, its modification time,
    /// the auto-save state and the display stamps forgotten, `default-
    /// directory' the current buffer's, as get_minibuffer gives a
    /// minibuffer it reuses (whose overlays it deletes first).
    pub(crate) fn reset_buffer(&mut self, buffer_id: u64) {
        let directory = self
            .buffer_local_value_key(self.current_buffer_id, cached_symbol!("default-directory"))
            .unwrap_or(Value::Nil);
        if let Some(mut buffer) = self.get_buffer_by_id_mut(buffer_id) {
            buffer.file = None;
            buffer.file_truename = None;
            buffer.set_visited_file_modtime(crate::buffer::VisitedModtime::UNKNOWN);
            buffer.set_autosave_modiff(0);
            buffer.auto_save_failure_time = 0;
            buffer.set_mark_active_value(Value::Nil);
        }
        if let Some(mut buffer) = self.get_buffer_by_id_mut(buffer_id) {
            buffer.set_last_window_start(1);
            buffer.set_last_selected_window(Value::Nil);
        }
        for (name, value) in [
            ("default-directory", directory),
            ("buffer-saved-size", Value::Integer(0)),
            ("buffer-backed-up", Value::Nil),
            ("local-minor-modes", Value::Nil),
            ("buffer-auto-save-file-name", Value::Nil),
            ("buffer-read-only", Value::Nil),
            ("point-before-scroll", Value::Nil),
            ("buffer-file-format", Value::Nil),
            ("buffer-auto-save-file-format", Value::T),
            ("buffer-display-count", Value::Integer(0)),
            ("buffer-display-time", Value::Nil),
        ] {
            self.set_buffer_local_value(buffer_id, name, value);
        }
    }

    /// window.c:set_window_buffer's time stamps of buffer display: an
    /// integer `buffer-display-count' counts up, and `buffer-display-time'
    /// becomes the current time.
    pub(crate) fn stamp_buffer_display(&mut self, buffer_id: u64) {
        let count = self
            .buffer_local_value_key(buffer_id, cached_symbol!("buffer-display-count"))
            .unwrap_or(Value::Nil);
        let count = match count.kind() {
            Kind::Integer(count) => Some(primitives::normalize_bigint_value(
                num_bigint::BigInt::from(count) + 1,
            )),
            Kind::BigInteger(count) => Some(primitives::normalize_bigint_value(
                num_bigint::BigInt::from(count) + 1,
            )),
            _ => None,
        };
        if let Some(count) = count {
            self.set_buffer_local_value_key(
                buffer_id,
                cached_symbol!("buffer-display-count"),
                count,
            );
        }
        if let Ok(now) = primitives::lisp_current_time(self) {
            self.set_buffer_local_value_key(buffer_id, cached_symbol!("buffer-display-time"), now);
        }
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

    /// The selected frame's param_alist entry for the parameter named
    /// NAME, when it has one.
    /// Fassq (KEY, the selected frame's param_alist)'s value.
    pub(crate) fn frame_parameter_override(
        &self,
        wanted: crate::lisp::types::symbols::SymbolKey,
    ) -> Option<Value> {
        let mut tail = self.selected_frame_id.borrow().param_alist;
        while let Some((entry, rest)) = tail.cons_values() {
            if let Some((key, value)) = entry.cons_values()
                && matches!(key.kind(), crate::lisp::types::Kind::Symbol(symbol)
                    if wanted.with(|wanted| *wanted == symbol))
            {
                return Some(value);
            }
            tail = rest;
        }
        None
    }

    pub(crate) fn frame_and_buffer_state(&self) -> Value {
        self.frame_and_buffer_state
    }

    pub(crate) fn set_frame_and_buffer_state(&mut self, state: Value) {
        self.frame_and_buffer_state = state;
    }

    pub(crate) fn snapshot_window_configuration(&mut self) -> WindowConfigurationSnapshot {
        self.snapshot_frame_window_configuration(self.selected_frame_id)
    }

    /// window.c:Fcurrent_window_configuration's data: the frame's
    /// settings and save_window_save of its window tree.
    fn snapshot_frame_window_configuration(
        &mut self,
        frame_id: crate::lisp::types::FrameRef,
    ) -> WindowConfigurationSnapshot {
        let persistent = self.window_persistent_parameters();
        let (selected_window_id, root_window_id, frame_width, frame_height) = {
            let frame = frame_id.borrow();
            (
                frame.selected_window_id(),
                frame.root_window_id(),
                frame.width,
                frame.height,
            )
        };
        let mut saved_windows = crate::lisp::alloc::RootedVec::new();
        let mut order = Vec::new();
        self.save_window_save(
            Some(root_window_id).filter(|id| *id != 0),
            &persistent,
            &mut saved_windows,
            &mut order,
        );
        WindowConfigurationSnapshot {
            frame_id,
            selected_frame_id: self.selected_frame_id,
            current_buffer_id: self.current_buffer_id(),
            selected_window_id,
            root_window_id,
            frame_width,
            frame_height,
            saved_windows,
        }
    }

    /// Vwindow_persistent_parameters as save_window_save reads it: reset
    /// to nil when its tail is circular.
    fn window_persistent_parameters(&mut self) -> Value {
        let key = cached_symbol!("window-persistent-parameters").symbol();
        let list = self.global_binding_value_symbol(&key).unwrap_or(Value::Nil);
        let (mut hare, mut tortoise) = (list, list);
        while let Some(cell) = hare.cons_ref() {
            hare = cell.cdr.get();
            let Some(cell) = hare.cons_ref() else {
                break;
            };
            hare = cell.cdr.get();
            tortoise = tortoise
                .cons_ref()
                .map_or(Value::Nil, |cell| cell.cdr.get());
            if hare.word() == tortoise.word() {
                self.set_c_variable("window-persistent-parameters", Value::Nil);
                return Value::Nil;
            }
        }
        list
    }

    /// window.c:save_window_save from WINDOW: each window, then an
    /// internal window's subwindows, then its `next' (the root's is the
    /// minibuffer window), one saved-window vector each in SAVED; ORDER
    /// holds the windows saved so far, GNU's temslot indices.
    fn save_window_save(
        &self,
        mut window: Option<u64>,
        persistent: &Value,
        saved: &mut crate::lisp::alloc::RootedVec<Value>,
        order: &mut Vec<u64>,
    ) {
        while let Some(id) = window {
            let Some(record) = self.find_record(id) else {
                return;
            };
            let slot = |index: usize| record.slots.get(index).copied().unwrap_or(Value::Nil);
            let link = |index: usize| match slot(index).kind() {
                Kind::Record(linked) => Some(linked.id),
                _ => None,
            };
            let temslot = |order: &[u64], linked: Option<u64>| {
                linked
                    .and_then(|linked| order.iter().position(|saved| *saved == linked))
                    .map_or(Value::Nil, |index| Value::Integer(index as i64))
            };
            let kind =
                primitives::WindowKind::of_slot(record.slots.get(primitives::WINDOW_KIND_SLOT));
            let mut p = vec![Value::Nil; SAVED_WINDOW_SIZE];
            p[SW_WINDOW] = self.record_value(id);
            // p->buffer: a leaf's contents.
            let buffer = if kind.is_internal() {
                None
            } else {
                match slot(primitives::WINDOW_BUFFER_SLOT).kind() {
                    Kind::Buffer(buffer) => Some(buffer),
                    Kind::Integer(buffer) => self.buffer_object(buffer.max(0) as u64),
                    _ => None,
                }
            };
            p[SW_BUFFER] = buffer.map_or(Value::Nil, Value::Buffer);
            for (field, index) in [
                (SW_PIXEL_LEFT, primitives::WINDOW_PIXEL_LEFT_SLOT),
                (SW_PIXEL_TOP, primitives::WINDOW_PIXEL_TOP_SLOT),
                (SW_PIXEL_HEIGHT, primitives::WINDOW_PIXEL_HEIGHT_SLOT),
                (SW_PIXEL_WIDTH, primitives::WINDOW_PIXEL_WIDTH_SLOT),
                (SW_LEFT_COL, primitives::WINDOW_LEFT_COL_SLOT),
                (SW_TOP_LINE, primitives::WINDOW_TOP_LINE_SLOT),
                (SW_TOTAL_COLS, primitives::WINDOW_TOTAL_COLS_SLOT),
                (SW_TOTAL_LINES, primitives::WINDOW_TOTAL_LINES_SLOT),
                (SW_NORMAL_COLS, primitives::WINDOW_NORMAL_WIDTH_SLOT),
                (SW_NORMAL_LINES, primitives::WINDOW_NORMAL_HEIGHT_SLOT),
                (SW_HSCROLL, primitives::WINDOW_HSCROLL_SLOT),
                (SW_MIN_HSCROLL, primitives::WINDOW_MIN_HSCROLL_SLOT),
                (
                    SW_SUSPEND_AUTO_HSCROLL,
                    primitives::WINDOW_SUSPEND_AUTO_HSCROLL_SLOT,
                ),
                (SW_DISPLAY_TABLE, primitives::WINDOW_DISPLAY_TABLE_SLOT),
                (
                    SW_LEFT_MARGIN_COLS,
                    primitives::WINDOW_LEFT_MARGIN_COLS_SLOT,
                ),
                (
                    SW_RIGHT_MARGIN_COLS,
                    primitives::WINDOW_RIGHT_MARGIN_COLS_SLOT,
                ),
                (SW_DEDICATED, primitives::WINDOW_DEDICATED_SLOT),
                (
                    SW_COMBINATION_LIMIT,
                    primitives::WINDOW_COMBINATION_LIMIT_SLOT,
                ),
            ] {
                p[field] = slot(index);
            }
            // A window with its frame's fringes and scroll bars, scrolled
            // by whole columns only.
            p[SW_HSCROLL_WHOLE] = Value::Integer(0);
            p[SW_VSCROLL] = Value::Integer(0);
            for field in [
                SW_LEFT_FRINGE_WIDTH,
                SW_RIGHT_FRINGE_WIDTH,
                SW_SCROLL_BAR_WIDTH,
                SW_SCROLL_BAR_HEIGHT,
            ] {
                p[field] = Value::Integer(-1);
            }
            p[SW_VERTICAL_SCROLL_BAR_TYPE] = Value::T;
            p[SW_HORIZONTAL_SCROLL_BAR_TYPE] = Value::T;
            // Only the persistent parameters, each with its value or nil.
            let parameters = slot(primitives::WINDOW_PARAMETERS_SLOT);
            let mut saved_parameters = Value::Nil;
            let mut tail = *persistent;
            while let Some(cell) = tail.cons_ref() {
                if let Some(pers) = cell.car.get().cons_ref()
                    && !pers.cdr.get().is_nil()
                {
                    let name = pers.car.get();
                    let value = assq_plain(name, parameters).map_or(Value::Nil, |entry| entry.1);
                    saved_parameters = Value::cons(Value::cons(name, value), saved_parameters);
                }
                tail = cell.cdr.get();
            }
            p[SW_WINDOW_PARAMETERS] = saved_parameters;
            if let Some(buffer) = buffer {
                // The window's point, its old point and its start as
                // markers in its buffer, the selected window's point
                // taken from the buffer (its own is stale), the points of
                // `window-point-insertion-type' in that buffer.
                let insertion_type = primitives::window_point_insertion_type(self, buffer.id);
                let selected = id == self.selected_window_id();
                for (field, index) in [
                    (SW_POINTM, primitives::WINDOW_POINT_SLOT),
                    (SW_OLD_POINTM, primitives::WINDOW_OLD_POINT_SLOT),
                    (SW_START, primitives::WINDOW_START_SLOT),
                ] {
                    let copy = crate::lisp::types::MarkerRef::new();
                    if index == primitives::WINDOW_POINT_SLOT && selected {
                        let state = buffer.borrow();
                        let point = state.point();
                        copy.attach(buffer, point, state.marker_byte_position(point));
                    } else if let Kind::Marker(marker) = slot(index).kind()
                        && let Some(owner) = marker.buffer()
                    {
                        copy.attach(owner, marker.last_position(), marker.bytepos());
                    }
                    copy.set_insertion_type(insertion_type && field != SW_START);
                    p[field] = Value::Marker(copy);
                }
                p[SW_START_AT_LINE_BEG] =
                    Value::from_bool(slot(primitives::WINDOW_START_AT_LINE_BEG_SLOT).is_truthy());
            }
            p[SW_PARENT] = temslot(order, link(primitives::WINDOW_PARENT_SLOT));
            p[SW_PREV] = temslot(order, link(primitives::WINDOW_PREV_SIBLING_SLOT));
            order.push(id);
            let child = (kind.is_internal())
                .then(|| link(primitives::WINDOW_FIRST_CHILD_SLOT))
                .flatten();
            let next = link(primitives::WINDOW_NEXT_SIBLING_SLOT);
            saved.push(Value::vector(p));
            if child.is_some() {
                self.save_window_save(child, persistent, saved, order);
            }
            window = next;
        }
    }

    pub(crate) fn window_configuration_value(
        &mut self,
        frame_id: crate::lisp::types::FrameRef,
    ) -> Value {
        let snapshot = self.snapshot_frame_window_configuration(frame_id);
        // The saved windows in one vector of vectors, window.c's
        // saved_windows, read back by
        // `window_configuration_snapshot_from_value' in place.
        let saved = Value::vector(snapshot.saved_windows.iter().copied());
        self.create_pseudovector(
            RecordKind::WindowConfiguration,
            "window-configuration",
            vec![
                Value::Integer(snapshot.current_buffer_id as i64),
                Value::Integer(snapshot.selected_window_id as i64),
                Value::Nil,
                Value::Integer(snapshot.frame_width),
                Value::Integer(snapshot.frame_height),
                self.record_value(snapshot.root_window_id),
                saved,
                Value::Frame(frame_id),
                Value::Frame(snapshot.selected_frame_id),
            ],
        )
    }

    pub(crate) fn apply_window_configuration_value(
        &mut self,
        value: &Value,
        dont_set_miniwindow: bool,
    ) -> Result<bool, LispError> {
        // CHECK_WINDOW_CONFIGURATION (configuration).
        let Some(snapshot) = self.window_configuration_snapshot_from_value(value) else {
            return Err(LispError::WrongTypeArgument(
                "window-configuration-p".into(),
                *value,
            ));
        };
        if !self.frame_is_live(snapshot.frame_id) {
            return Ok(false);
        }
        self.restore_window_configuration_with(snapshot, dont_set_miniwindow)?;
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
        let frame = |index: usize| match slots.get(index)?.kind() {
            Kind::Frame(frame) => Some(frame),
            _ => None,
        };
        let selected_window_id = slots
            .get(1)
            .and_then(|value| value.integer_value())
            .unwrap_or_default() as u64;
        let saved_windows = match slots.get(6).map(|value| value.kind()) {
            Some(Kind::Vector(saved)) => saved.slots().collect(),
            _ => crate::lisp::alloc::RootedVec::new(),
        };
        Some(WindowConfigurationSnapshot {
            frame_id: frame(7)?,
            selected_frame_id: frame(8)?,
            current_buffer_id: slots
                .first()
                .and_then(|value| value.integer_value())
                .unwrap_or_default() as u64,
            selected_window_id,
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
            saved_windows,
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
                    LispError::WrongTypeArgument("window-configuration-p".into(), *value)
                })
        };
        let left = snapshot(left)?;
        let right = snapshot(right)?;
        // window.c compare_window_configurations: the frame settings, then
        // each saved window by its layout alone (point, the start, hscroll
        // and redisplay's own state are ignored).
        let (left, right) = (saved_windows(&left), saved_windows(&right));
        // Every field by EQ, as compare_window_configurations reads it.
        let same_window = |a: &SavedWindow, b: &SavedWindow| {
            a.0 == b.0
                && a.2 == b.2
                && a.3 == b.3
                && a.1.iter().zip(&b.1).all(|(a, b)| a.word() == b.word())
        };
        Ok(left.0 == right.0
            && left.1.len() == right.1.len()
            && left.1.iter().zip(&right.1).all(|(a, b)| same_window(a, b)))
    }

    pub(crate) fn restore_window_configuration(
        &mut self,
        snapshot: WindowConfigurationSnapshot,
    ) -> Result<(), LispError> {
        self.restore_window_configuration_with(snapshot, false)
    }

    /// window.c:Fset_window_configuration.  DONT_SET_MINIWINDOW keeps the
    /// minibuffer window's buffer as it is.
    pub(crate) fn restore_window_configuration_with(
        &mut self,
        snapshot: WindowConfigurationSnapshot,
        dont_set_miniwindow: bool,
    ) -> Result<(), LispError> {
        if !self.frame_is_live(snapshot.frame_id) {
            return Ok(());
        }
        let saved_windows = snapshot.saved_windows;
        // Each saved window's window, and the buffer save_window_save kept.
        let saved_ids: Vec<Option<u64>> = saved_windows
            .iter()
            .map(|saved| match saved_field(saved, SW_WINDOW).kind() {
                Kind::Record(window) => Some(window.id),
                _ => None,
            })
            .collect();
        let saved_buffer = |saved: &Value| match saved_field(saved, SW_BUFFER).kind() {
            Kind::Buffer(buffer) => Some(buffer.id),
            Kind::Integer(id) => Some(id.max(0) as u64),
            _ => None,
        };
        let new_current_buffer =
            Some(snapshot.current_buffer_id).filter(|id| self.has_buffer_id(*id));
        let current_window = snapshot.selected_window_id;
        // The point of the buffer current when the configuration was saved
        // that the restore preserves: when the window to be selected shows
        // it and another window is selected, the point that window keeps.
        let old_point = new_current_buffer.map(|buffer| {
            let selected = self.selected_window_id();
            let from_current_window = self.window_record_buffer_id(current_window) == Some(buffer)
                && selected != current_window
                && (buffer != self.current_buffer_id()
                    || self.window_record_buffer_id(selected) == Some(buffer));
            let window_point = from_current_window
                .then(|| {
                    primitives::window_marker_position(
                        self,
                        current_window,
                        primitives::WINDOW_POINT_SLOT,
                    )
                })
                .flatten();
            window_point.unwrap_or_else(|| self.buffer_point_by_id(buffer))
        });
        // A window that gets another buffer records its old one.
        for (saved, window) in saved_windows.iter().zip(&saved_ids) {
            let Some(window) = window else {
                continue;
            };
            if let (Some(current), Some(saved)) =
                (self.window_record_buffer_id(*window), saved_buffer(saved))
                && current != saved
                && self.has_buffer_id(saved)
                && !self.buffer_is_minibuffer(saved)
            {
                let window = self.record_value(*window);
                primitives::call_named_function(
                    self,
                    "record-window-buffer",
                    &[window],
                    &mut crate::lisp::types::Env::new(),
                )?;
            }
        }
        // "Swap out" point from the selected window's buffer into the
        // window itself, before the windows' contents are restored.
        let selected = self.selected_window_id();
        if let Some(buffer) = self.window_record_buffer_id(selected)
            && self.has_buffer_id(buffer)
        {
            let point = self.buffer_point_by_id(buffer);
            primitives::set_window_marker(
                self,
                selected,
                primitives::WINDOW_POINT_SLOT,
                buffer,
                point,
            );
        }
        // delete_all_child_windows (FRAME_ROOT_WINDOW (f)): every window
        // of the frame's tree, the minibuffer window the root's `next'
        // (frame.c make_frame links them), is "deleted", a leaf's buffer
        // unshown and its markers unchained, the buffer squirreled away in
        // case the saved one is dead.
        let mut squirreled = Vec::new();
        let root = snapshot.frame_id.borrow().root_window_id();
        self.unshow_window_tree(Some(root).filter(|id| *id != 0), &mut squirreled)?;
        // Each saved window revived: its links rebuilt from the saved
        // indices, the fields save_window_save saved written back (the
        // others, its previous and next buffers, cursor type, use time and
        // redisplay's state, are left alone), its squirreled buffer put
        // back, and its saved persistent parameters restored.
        // What each saved window is, as GNU's contents and flags make it:
        // the frame's minibuffer window, a leaf with its saved buffer, or
        // the internal window whose first child's total_cols differ from
        // its own when it combines horizontally (wset_combination).
        let minibuffer_window = snapshot.frame_id.borrow().minibuffer_window_id();
        let total_cols = |saved: &Value| {
            Some(saved_field(saved, SW_TOTAL_COLS))
                .filter(|value| !value.is_nil())
                .unwrap_or_else(|| saved_field(saved, SW_PIXEL_WIDTH))
        };
        let kinds: Vec<primitives::WindowKind> = saved_windows
            .iter()
            .enumerate()
            .map(|(k, saved)| {
                if saved_ids[k] == Some(minibuffer_window) {
                    return primitives::WindowKind::Minibuffer;
                }
                if !saved_field(saved, SW_BUFFER).is_nil() {
                    return primitives::WindowKind::Leaf;
                }
                match saved_windows.iter().find(|child| {
                    saved_index(child, SW_PARENT) == Some(k)
                        && saved_index(child, SW_PREV).is_none()
                }) {
                    Some(child) if total_cols(child).word() != total_cols(saved).word() => {
                        primitives::WindowKind::InternalHorizontal
                    }
                    Some(_) => primitives::WindowKind::InternalVertical,
                    None => primitives::WindowKind::Leaf,
                }
            })
            .collect();
        for (k, (saved, window)) in saved_windows.iter().zip(&saved_ids).enumerate() {
            let Some(window) = *window else {
                continue;
            };
            let link = |field| {
                saved_index(saved, field).and_then(|index| saved_ids.get(index).copied().flatten())
            };
            self.set_window_slot(window, primitives::WINDOW_NEXT_SIBLING_SLOT, Value::Nil);
            let parent = link(SW_PARENT);
            let parent_value = parent.map_or(Value::Nil, |parent| self.record_value(parent));
            self.set_window_slot(window, primitives::WINDOW_PARENT_SLOT, parent_value);
            let this = self.record_value(window);
            if let Some(prev) = link(SW_PREV) {
                let prev_value = self.record_value(prev);
                self.set_window_slot(window, primitives::WINDOW_PREV_SIBLING_SLOT, prev_value);
                self.set_window_slot(prev, primitives::WINDOW_NEXT_SIBLING_SLOT, this);
            } else {
                self.set_window_slot(window, primitives::WINDOW_PREV_SIBLING_SLOT, Value::Nil);
                // wset_combination: the parent's contents its first child.
                if let Some(parent) = parent {
                    self.set_window_slot(parent, primitives::WINDOW_FIRST_CHILD_SLOT, this);
                }
            }
            if let Some(buffer) = squirreled
                .iter()
                .find(|(id, _)| *id == window)
                .map(|(_, buffer)| *buffer)
            {
                let object = self.buffer_value(buffer).unwrap_or(Value::Nil);
                self.set_window_slot(window, primitives::WINDOW_BUFFER_SLOT, object);
            }
            for (field, index) in [
                (SW_PIXEL_LEFT, primitives::WINDOW_PIXEL_LEFT_SLOT),
                (SW_PIXEL_TOP, primitives::WINDOW_PIXEL_TOP_SLOT),
                (SW_PIXEL_WIDTH, primitives::WINDOW_PIXEL_WIDTH_SLOT),
                (SW_PIXEL_HEIGHT, primitives::WINDOW_PIXEL_HEIGHT_SLOT),
                (SW_LEFT_COL, primitives::WINDOW_LEFT_COL_SLOT),
                (SW_TOP_LINE, primitives::WINDOW_TOP_LINE_SLOT),
                (SW_TOTAL_COLS, primitives::WINDOW_TOTAL_COLS_SLOT),
                (SW_TOTAL_LINES, primitives::WINDOW_TOTAL_LINES_SLOT),
                (SW_NORMAL_COLS, primitives::WINDOW_NORMAL_WIDTH_SLOT),
                (SW_NORMAL_LINES, primitives::WINDOW_NORMAL_HEIGHT_SLOT),
                (SW_HSCROLL, primitives::WINDOW_HSCROLL_SLOT),
                (
                    SW_SUSPEND_AUTO_HSCROLL,
                    primitives::WINDOW_SUSPEND_AUTO_HSCROLL_SLOT,
                ),
                (SW_MIN_HSCROLL, primitives::WINDOW_MIN_HSCROLL_SLOT),
                (SW_DISPLAY_TABLE, primitives::WINDOW_DISPLAY_TABLE_SLOT),
                (
                    SW_LEFT_MARGIN_COLS,
                    primitives::WINDOW_LEFT_MARGIN_COLS_SLOT,
                ),
                (
                    SW_RIGHT_MARGIN_COLS,
                    primitives::WINDOW_RIGHT_MARGIN_COLS_SLOT,
                ),
                (SW_DEDICATED, primitives::WINDOW_DEDICATED_SLOT),
                (
                    SW_COMBINATION_LIMIT,
                    primitives::WINDOW_COMBINATION_LIMIT_SLOT,
                ),
            ] {
                self.set_window_slot(window, index, saved_field(saved, field));
            }
            self.set_window_slot(window, primitives::WINDOW_KIND_SLOT, kinds[k].value());
            // Only the saved parameters: a nil one resets a non-nil
            // association and makes none, any other is set.
            let mut tail = saved_field(saved, SW_WINDOW_PARAMETERS);
            while let Some(cell) = tail.cons_ref() {
                if let Some(pers) = cell.car.get().cons_ref() {
                    let (name, value) = (pers.car.get(), pers.cdr.get());
                    let parameters = self
                        .find_record(window)
                        .and_then(|record| record.slots.get(primitives::WINDOW_PARAMETERS_SLOT))
                        .copied()
                        .unwrap_or(Value::Nil);
                    let entry = {
                        let mut tail = parameters;
                        let mut walk = crate::lisp::types::TailWalk::new(tail);
                        let mut found = None;
                        while let Some(cell) = tail.cons_ref() {
                            if let Some(entry) = cell.car.get().cons_ref()
                                && entry.car.get().word() == name.word()
                            {
                                found = Some(entry);
                                break;
                            }
                            tail = cell.cdr.get();
                            if walk.cycled_no_quit(tail) {
                                break;
                            }
                        }
                        found
                    };
                    match entry {
                        Some(entry) if value.is_nil() => {
                            if !entry.cdr.get().is_nil() {
                                entry.cdr.set(Value::Nil);
                            }
                        }
                        None if value.is_nil() => {}
                        // Fset_window_parameter.
                        Some(entry) => entry.cdr.set(value),
                        None => self.set_window_slot(
                            window,
                            primitives::WINDOW_PARAMETERS_SLOT,
                            Value::cons(Value::cons(name, value), parameters),
                        ),
                    }
                }
                tail = cell.cdr.get();
            }
        }
        let restore_killed = self
            .global_binding_value("window-restore-killed-buffer-windows")
            .unwrap_or(Value::Nil);
        let restore_killed_function =
            primitives::function_value_p(self, &restore_killed, &crate::lisp::types::Env::new());
        // The lists for `window-restore-killed-buffer-windows', held across
        // the calls into Lisp below.
        let mut kept_windows = crate::lisp::alloc::RootedVec::new();
        let mut dead_windows = Vec::new();
        for (slots, window) in saved_windows.iter().zip(&saved_ids) {
            let Some(window) = window else {
                continue;
            };
            let mini = *window == minibuffer_window;
            let saved = saved_buffer(slots);
            let saved_start = Some(saved_field(slots, SW_START));
            let saved_point = Some(saved_field(slots, SW_POINTM));
            let kept = |interp: &Self, live: bool| {
                Value::list([
                    interp.record_value(*window),
                    match saved_field(slots, SW_BUFFER).kind() {
                        Kind::Buffer(_) => saved_field(slots, SW_BUFFER),
                        _ => saved
                            .and_then(|id| interp.buffer_identity_value(id))
                            .unwrap_or(Value::Nil),
                    },
                    marker_last_position(saved_start),
                    marker_last_position(saved_point),
                    saved_field(slots, SW_DEDICATED),
                    if live { Value::T } else { Value::Nil },
                ])
            };
            if (!dont_set_miniwindow || !mini)
                && let Some(buffer) = saved.filter(|id| self.has_buffer_id(*id))
            {
                // The saved buffer is installed, the window's markers
                // set_marker_restricted to the saved ones (a saved marker
                // that points nowhere unchains the window's).
                let object = self.buffer_value(buffer).unwrap_or(Value::Nil);
                self.set_window_slot(*window, primitives::WINDOW_BUFFER_SLOT, object);
                self.set_window_slot(
                    *window,
                    primitives::WINDOW_START_AT_LINE_BEG_SLOT,
                    Value::from_bool(saved_field(slots, SW_START_AT_LINE_BEG).is_truthy()),
                );
                for (slot, field) in [
                    (primitives::WINDOW_START_SLOT, SW_START),
                    (primitives::WINDOW_POINT_SLOT, SW_POINTM),
                    (primitives::WINDOW_OLD_POINT_SLOT, SW_OLD_POINTM),
                ] {
                    match primitives::slot_marker_position(Some(&saved_field(slots, field))) {
                        Some(position) => primitives::set_window_marker_restricted(
                            self, *window, slot, buffer, position,
                        ),
                        None => {
                            if let Some(marker) = primitives::window_marker(self, *window, slot) {
                                marker.detach();
                            }
                        }
                    }
                }
                // The point of the buffer current when the configuration
                // was saved is not restored.
                if Some(buffer) != new_current_buffer
                    && buffer == self.current_buffer_id()
                    && let Some(point) = primitives::window_marker_position(
                        self,
                        *window,
                        primitives::WINDOW_POINT_SLOT,
                    )
                {
                    let mut state = self.buffer.borrow_mut();
                    let point = point.clamp(state.point_min(), state.point_max());
                    state.goto_char(point);
                }
            } else if let Some(buffer_id) = squirreled
                .iter()
                .find(|(id, _)| id == window)
                .map(|(_, buffer)| *buffer)
                .filter(|id| self.has_buffer_id(*id))
            {
                // The window keeps its old buffer; its markers, which
                // point nowhere, go to the start of the accessible portion
                // and to the buffer's point.
                let point = self.buffer_point_by_id(buffer_id);
                let object = self.buffer_value(buffer_id).unwrap_or(Value::Nil);
                self.set_window_slot(*window, primitives::WINDOW_BUFFER_SLOT, object);
                for (slot, position) in [
                    (primitives::WINDOW_START_SLOT, 0),
                    (primitives::WINDOW_POINT_SLOT, point),
                    (primitives::WINDOW_OLD_POINT_SLOT, point),
                ] {
                    if primitives::window_marker(self, *window, slot)
                        .is_none_or(|marker| marker.buffer().is_none())
                    {
                        primitives::set_window_marker_restricted(
                            self, *window, slot, buffer_id, position,
                        );
                    }
                }
                self.set_window_slot(*window, primitives::WINDOW_START_AT_LINE_BEG_SLOT, Value::T);
                if restore_killed_function && !mini {
                    let entry = kept(self, true);
                    kept_windows.push(entry);
                }
            } else if primitives::window_marker(self, *window, primitives::WINDOW_START_SLOT)
                .is_some()
            {
                // A leaf window with no live buffer gets one, its markers
                // at the start of the accessible portion.
                let buffer = self.other_buffer_safely(self.current_buffer_id())?;
                let object = self.buffer_value(buffer).unwrap_or(Value::Nil);
                self.set_window_slot(*window, primitives::WINDOW_BUFFER_SLOT, object);
                for slot in primitives::WINDOW_MARKER_SLOTS {
                    primitives::set_window_marker_restricted(self, *window, slot, buffer, 0);
                }
                self.set_window_slot(*window, primitives::WINDOW_START_AT_LINE_BEG_SLOT, Value::T);
                if !mini {
                    let dedicated = saved_field(slots, SW_DEDICATED);
                    if restore_killed_function {
                        let entry = kept(self, false);
                        kept_windows.push(entry);
                    } else if restore_killed.is_key(cached_symbol!("delete"))
                        || (!dedicated.is_nil()
                            && (restore_killed.is_nil()
                                || restore_killed.is_key(cached_symbol!("dedicated"))))
                    {
                        dead_windows.push(*window);
                    }
                    self.set_window_slot(*window, primitives::WINDOW_DEDICATED_SLOT, Value::Nil);
                }
            }
        }
        snapshot.frame_id.borrow_mut().root_window = self.record_ref(snapshot.root_window_id);
        // Arrange not to restore point in the buffer that was current when
        // the configuration was saved.
        if let Some(point) = old_point
            && let Some(buffer) = new_current_buffer
            && self.window_record_buffer_id(current_window) == Some(buffer)
        {
            primitives::set_window_marker_restricted(
                self,
                current_window,
                primitives::WINDOW_POINT_SLOT,
                buffer,
                point,
            );
        }
        // select_window (current_window, Qt, true): its buffer current,
        // at the point of its point marker (no point swapped out of the
        // window selected before, whose point went into it above).
        self.note_selected_frame(snapshot.frame_id);
        self.set_selected_window_id(snapshot.selected_window_id);
        if let Some(buffer) = self
            .window_record_buffer_id(current_window)
            .filter(|id| self.has_buffer_id(*id))
        {
            self.set_current_buffer_id(buffer)?;
            if let Some(point) = primitives::window_marker_position(
                self,
                current_window,
                primitives::WINDOW_POINT_SLOT,
            ) {
                let mut state = self.buffer.borrow_mut();
                let point = point.clamp(state.point_min(), state.point_max());
                state.goto_char(point);
            }
            // select_window's bset_last_selected_window, and
            // Fset_window_configuration's own after it.
            let window = self.record_value(current_window);
            if let Some(mut state) = self.get_buffer_by_id_mut(buffer) {
                state.set_last_selected_window(window);
            }
        }
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
        // The windows whose buffers were killed, deleted when they can be.
        let root = snapshot.frame_id.borrow().root_window_id();
        for window in dead_windows {
            if window == root || !self.window_is_live_record(window) {
                continue;
            }
            let window = self.record_value(window);
            let mut env = crate::lisp::types::Env::new();
            if primitives::call_named_function(self, "window-deletable-p", &[window], &mut env)?
                .is_truthy()
            {
                primitives::call_named_function(self, "delete-window", &[window], &mut env)?;
            }
        }
        // Fset_window_configuration's select_window (current_window, Qnil):
        // the configuration's window gets a new use time and its buffer is
        // recorded, buffer-list-update-hook run with it current.
        let current_window = self.record_value(snapshot.selected_window_id);
        if let Some(buffer_id) = primitives::window_buffer_id(self, &current_window)
            .filter(|buffer_id| self.has_buffer_id(*buffer_id))
        {
            self.record_window_selection(snapshot.selected_window_id);
            self.record_buffer_front(buffer_id);
            if !self.buffer_hooks_inhibited(buffer_id) {
                self.set_current_buffer_id(buffer_id)?;
                primitives::run_hook(
                    self,
                    &cached_symbol!("buffer-list-update-hook").symbol(),
                    &mut crate::lisp::types::Env::new(),
                )?;
            }
        }
        if self.frame_is_live(snapshot.selected_frame_id) {
            self.note_selected_frame(snapshot.selected_frame_id);
            let selected_window = self.selected_frame_id.borrow().selected_window_id();
            self.set_selected_window_id(selected_window);
        }
        if let Some(buffer) = new_current_buffer {
            self.set_current_buffer_id(buffer)?;
            // A new current buffer the selected window does not show goes
            // to its old point (Bug#12208).
            if self.selected_window_buffer_id() != buffer
                && let Some(point) = old_point
            {
                let mut state = self.buffer.borrow_mut();
                let point = point.clamp(state.point_min(), state.point_max());
                state.goto_char(point);
            }
        }
        if restore_killed_function {
            let frame = Value::Frame(snapshot.frame_id);
            let kept = Value::list(kept_windows);
            self.safe_funcall(
                restore_killed,
                &[frame, kept, Value::symbol("configuration")],
                &mut crate::lisp::types::Env::new(),
            )?;
        }
        Ok(())
    }

    /// The start and point markers of each window, moved at their
    /// positions to the buffer object that now has the window's buffer id:
    /// the windows made before an image's buffers were installed, which
    /// make_frame would have set in those buffers.
    pub(crate) fn rebind_window_markers(&mut self) {
        for window in self.record_ids_by_type("window") {
            let Some(buffer_id) = self.window_record_buffer_id(window) else {
                continue;
            };
            let Some(buffer) = self.buffer_object(buffer_id) else {
                continue;
            };
            for slot in primitives::WINDOW_MARKER_SLOTS {
                if let Some(marker) = primitives::window_marker(self, window, slot)
                    && marker.buffer().is_some_and(|owner| !owner.ptr_eq(&buffer))
                {
                    let _ = self.set_marker(marker, Some(marker.last_position()), Some(buffer_id));
                }
            }
        }
    }

    /// window.c:delete_all_child_windows over the tree from WINDOW, the
    /// siblings after it first and children before their parents: each
    /// leaf's buffer unshown and its markers unchained, its buffer kept in
    /// SQUIRRELED (GNU's combination_limit) for Fset_window_configuration.
    fn unshow_window_tree(
        &mut self,
        window: Option<u64>,
        squirreled: &mut Vec<(u64, u64)>,
    ) -> Result<(), LispError> {
        let Some(window) = window else {
            return Ok(());
        };
        let link = |interp: &Self, slot: usize| match interp
            .find_record(window)
            .and_then(|record| record.slots.get(slot))
            .map(|value| value.kind())
        {
            Some(Kind::Record(record)) => Some(record.id),
            _ => None,
        };
        let next = link(self, primitives::WINDOW_NEXT_SIBLING_SLOT);
        self.unshow_window_tree(next, squirreled)?;
        let internal = primitives::WindowKind::of_slot(
            self.find_record(window)
                .and_then(|record| record.slots.get(primitives::WINDOW_KIND_SLOT)),
        )
        .is_internal();
        if let Some(child) = link(self, primitives::WINDOW_FIRST_CHILD_SLOT).filter(|_| internal) {
            self.unshow_window_tree(Some(child), squirreled)?;
            // wset_combination (w, false, Qnil): no contents.
            self.set_window_slot(window, primitives::WINDOW_FIRST_CHILD_SLOT, Value::Nil);
        } else if let Some(buffer) = self.window_record_buffer_id(window) {
            self.unshow_window_buffer(window)?;
            primitives::unchain_window_markers(self, window);
            squirreled.push((window, buffer));
            // wset_buffer (w, Qnil).
            self.set_window_slot(window, primitives::WINDOW_BUFFER_SLOT, Value::Nil);
        }
        // A window with no contents is no valid window.
        self.set_window_slot(
            window,
            primitives::WINDOW_KIND_SLOT,
            primitives::WindowKind::Deleted.value(),
        );
        Ok(())
    }

    /// BUF_PT of buffer ID.
    pub(crate) fn buffer_point_by_id(&self, id: u64) -> usize {
        if id == self.current_buffer_id {
            self.buffer.borrow().point()
        } else {
            self.get_buffer_by_id(id).map_or(1, |buffer| buffer.point())
        }
    }

    fn set_window_slot(&mut self, window: u64, slot: usize, value: Value) {
        if let Some(record) = self.find_record_mut(window) {
            if record.slots.len() <= slot {
                record.slots.resize(slot + 1, Value::Nil);
            }
            record.slots[slot] = value;
        }
    }

    /// Fminibufferp with LIVE nil: Fmemq of Vminibuffer_list.
    fn buffer_is_minibuffer(&self, id: u64) -> bool {
        self.buffer_value(id)
            .is_some_and(|buffer| primitives::minibuffer_list_has(self, &buffer))
    }

    /// WINDOW_LIVE_P: a window that shows a buffer.
    fn window_is_live_record(&self, id: u64) -> bool {
        self.find_record(id).is_some_and(|record| {
            record.kind == RecordKind::Window
                && primitives::window_slot_buffer_id(
                    record.slots.get(primitives::WINDOW_BUFFER_SLOT),
                )
                .is_some()
                && primitives::WindowKind::of_slot(record.slots.get(primitives::WINDOW_KIND_SLOT))
                    != primitives::WindowKind::Deleted
        })
    }

    /// buffer.c:other_buffer_safely: the first live buffer that is not
    /// BUFFER and not hidden, else `get-scratch-buffer-create''s, else a
    /// *scratch* made in its major mode.
    fn other_buffer_safely(&mut self, buffer: u64) -> Result<u64, LispError> {
        if let Some((id, _)) = self
            .buffer_list
            .iter()
            .find(|(id, name)| *id != buffer && !primitives::string_starts_with_byte(name, b' '))
        {
            return Ok(*id);
        }
        let mut env = crate::lisp::types::Env::new();
        if let Ok(create) =
            self.lookup_function_key(cached_symbol!("get-scratch-buffer-create"), &env)
        {
            let scratch = self.safe_funcall(create, &[], &mut env)?;
            if let Kind::Buffer(scratch) = scratch.kind()
                && let Some(id) = self.live_buffer_id(scratch)
            {
                return Ok(id);
            }
        }
        let scratch = primitives::call(
            self,
            "get-buffer-create",
            &[Value::string("*scratch*")],
            &mut env,
        )?;
        primitives::call(self, "set-buffer-major-mode", &[scratch], &mut env)?;
        match scratch.kind() {
            Kind::Buffer(scratch) => self
                .live_buffer_id(scratch)
                .ok_or_else(|| LispError::Signal("Selecting deleted buffer".into())),
            _ => Err(LispError::Signal("Selecting deleted buffer".into())),
        }
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
        // bset_name (b, Qnil): the object answers BUFFER_LIVE_P no more.
        if let Some(object) = self.buffer_object(id) {
            object.live.set(false);
        }
        self.frames_discard_buffer(id);
        self.delete_buffer_overlays(id);
        let selected_window_showed_buffer = self.selected_window_buffer_id() == id;
        self.detach_markers_for_buffer(id);
        // An indirect buffer stops sharing its base's text (and chain).
        if let Some(object) = self.buffer_object(id) {
            object.leave_marker_chain();
        }
        // A *scratch* made to replace the last buffer inherits its directory.
        let directory = self
            .buffer_local_value_key(id, cached_symbol!("default-directory"))
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
        self.buffer_case_tables.remove(id);
        self.reset_buffer_syntax_table(id);
        self.reset_buffer_category_table(id);
        self.labeled_restrictions
            .retain(|restriction| restriction.buffer_id != id);
        self.indirect_buffers
            .retain(|(buffer_id, base_id)| *buffer_id != id && *base_id != id);
        if let Some(mut buffer) = self.get_buffer_by_id_mut(id) {
            // reset_buffer_local_variables (b, 1): bset_keymap (b, Qnil).
            buffer.set_keymap(Value::Nil);
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
                .find(|(buffer_id, name)| {
                    *buffer_id != id && !primitives::string_starts_with_byte(name, b' ')
                })
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
                self.buffer.borrow_mut().enter_position_markers();
            } else {
                let scratch_id = self.alloc_buffer_id();
                self.buffer = crate::lisp::types::BufferRef::new(
                    scratch_id,
                    crate::buffer::Buffer::new("*scratch*"),
                );
                self.current_buffer_id = scratch_id;
                let scratch_name = self.buffer.borrow().name();
                self.buffer_list.push((scratch_id, scratch_name));
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
        let replacement = self.buffer_value(replacement_id).unwrap_or(Value::Nil);
        let replacement_start = self
            .buffer_bounds_by_id(replacement_id)
            .map(|(point_min, _)| point_min)
            .unwrap_or(1);
        let window_ids = self.record_ids_by_type("window");
        for window_id in window_ids {
            let showed_killed_buffer = primitives::window_slot_buffer_id(
                self.find_record(window_id)
                    .and_then(|window| window.slots.get(primitives::WINDOW_BUFFER_SLOT)),
            ) == Some(id);
            if !showed_killed_buffer {
                continue;
            }
            if let Some(window) = self.find_record_mut(window_id) {
                window.slots[primitives::WINDOW_BUFFER_SLOT] = replacement;
                window.slots[primitives::WINDOW_START_SLOT] =
                    Value::Integer(replacement_start as i64);
            }
        }
        if selected_window_showed_buffer
            && self.set_selected_window_buffer_id(replacement_id).is_err()
        {
            // unshow_buffer's error for a start or point that points
            // nowhere: the window still stops showing the dying buffer.
            self.show_buffer_in_window(self.selected_window_id, replacement_id, false);
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
        // chartab.c:Fmake_char_table's CHECK_SYMBOL, then Fget (PURPOSE,
        // Qchar_table_extra_slots) of the symbol itself.
        if !purpose.is_symbol() {
            return Err(LispError::WrongTypeArgument("symbolp".into(), purpose));
        }
        let extra = self
            .get_property_of_symbol_value(purpose, "char-table-extra-slots")
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

    /// fns.c:Fprovide's update of `features': FEATURE is consed onto the
    /// front of the list unless `memq' finds it there already.
    fn provide_feature(&mut self, feature: SymbolName, env: &mut Env) -> Result<(), LispError> {
        let symbol = Value::Symbol(feature);
        let features = self.global_value("features").unwrap_or(Value::Nil);
        // Fprovide records the list it replaces for un_autoload.
        if !self.autoload_queue.is_nil() {
            self.autoload_queue = Value::cons(
                Value::cons(Value::Integer(0), features),
                self.autoload_queue,
            );
        }
        if crate::lisp::primitives::call(self, "memq", &[symbol, features], env)?.is_nil() {
            self.set_global_binding_key(cached_symbol!("features"), Value::cons(symbol, features));
        }
        Ok(())
    }

    /// lisp.h:LOADHIST_ATTACH of (provide . FEATURE): consed onto
    /// `current-load-list' each time, inside a load or not.
    fn record_provide_in_load_history(&mut self, feature: SymbolName) {
        let current_load_list = self
            .symbol_value_key(cached_symbol!("current-load-list"))
            .unwrap_or(Value::Nil);
        let entry = Value::cons(Value::symbol("provide"), Value::Symbol(feature));
        // GNU LOADHIST_ATTACH conses onto the front: the source file name
        // stays the LAST element (`macroexp-file-name' reads it there).
        self.set_global_binding_key(
            cached_symbol!("current-load-list"),
            Value::cons(entry, current_load_list),
        );
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
    pub(crate) fn record_function_redefinition(
        &mut self,
        symbol: &SymbolName,
        old_definition: Value,
    ) {
        if old_definition.is_nil() {
            return;
        }
        // data.c:defalias queues the symbol for un_autoload first.
        if !self.autoload_queue.is_nil() {
            self.autoload_queue = Value::cons(Value::Symbol(*symbol), self.autoload_queue);
        }
        // data.c's add_to_function_history walks `current-load-list' to
        // its last element and takes it when it is a string; the walk
        // reads the cells in place (a copy of the list cost every
        // redefinition the length of the file's list so far).
        let mut file = Value::Nil;
        let mut tail = self
            .lookup_var_key(cached_symbol!("current-load-list"), &Env::new())
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
        let past = cached_symbol!("function-history")
            .with(|property| self.get_symbol_property_of(symbol, property))
            .unwrap_or(Value::Nil);
        let Ok(mut entries) = past.to_vec() else {
            self.put_symbol_property_on(
                symbol,
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
        self.put_symbol_property_on(symbol, "function-history", Value::list(entries));
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
            .lookup_var_key(cached_symbol!("load-history"), &Env::new())
            .and_then(|value| value.to_vec().ok())
            .unwrap_or_default();
        history.retain(|existing| match existing.car() {
            Ok(existing_filename) => &existing_filename != filename,
            Err(_) => true,
        });
        history.insert(0, Value::list(entry));
        self.set_global_binding_key(cached_symbol!("load-history"), Value::list(history));
    }

    /// fns.c:Fprovide of FEATURE with SUBFEATURES (a list).
    pub(crate) fn provide_feature_with_after_load(
        &mut self,
        feature: SymbolName,
        subfeatures: Value,
        env: &mut Env,
    ) -> Result<(), LispError> {
        self.provide_feature(feature, env)?;
        if subfeatures.is_truthy() {
            self.put_symbol_property_on(&feature, "subfeatures", subfeatures);
        }
        self.record_provide_in_load_history(feature);

        // GNU 30.2 fns.c:Fprovide runs the functions in the matching
        // `after-load-alist' entry after publishing FEATURE (Fassq, then
        // Fmapc of funcall over its cdr).  The alist and its closures are
        // owned by subr.el's real `eval-after-load'; the Rust host only
        // performs the C-owned lookup/invocation boundary.
        let after_load_alist = self
            .lookup_var_key(cached_symbol!("after-load-alist"), env)
            .unwrap_or(Value::Nil);
        let entry = crate::lisp::primitives::call(
            self,
            "assq",
            &[Value::Symbol(feature), after_load_alist],
            env,
        )?;
        if let Some((_, functions)) = entry.cons_values() {
            crate::lisp::primitives::call(
                self,
                "mapc",
                &[Value::symbol("funcall"), functions],
                env,
            )?;
        }
        Ok(())
    }

    /// Ffboundp of the interned symbol NAME: its function cell is not nil.
    /// The preloaded Lisp a C path calls (`syntax-ppss', the OClosure
    /// generics) is absent from a runtime that did not load it.
    pub(crate) fn is_fbound(&self, name: &str) -> bool {
        self.globals.function_by_name(name).is_some()
    }

    /// Whether FEATURE names a symbol in `features' (read in place; a
    /// circular or dotted list has none): by name, for a caller that
    /// holds no symbol, in whichever obarray it reads from.
    pub fn has_feature(&self, feature: &str) -> bool {
        let features = self
            .global_value("features")
            .unwrap_or_else(|| self.features_value());
        let named =
            |value: &Value| matches!(value.kind(), Kind::Symbol(name) if name.is_named(feature));
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

    /// The features the C code provides before any Lisp runs.
    pub(super) fn features_value(&self) -> Value {
        Value::list(
            super::STARTUP_FEATURES
                .iter()
                .map(|feature| Value::Symbol(feature.name.into())),
        )
    }

    pub(crate) fn combined_after_change(&self) -> Option<&CombinedAfterChangeState> {
        self.combined_after_change.as_ref()
    }

    /// insdel.c's `combine_after_change_list = Fcons (elt, ...)' and
    /// `combine_after_change_buffer = Fcurrent_buffer ()': the change
    /// joins those pending, whichever buffer they were recorded in (one
    /// Fcombine_after_change_execute re-deferred), and BUFFER_ID is the
    /// buffer they are signalled in.
    pub(crate) fn record_combined_after_change(&mut self, buffer_id: u64, change: (i64, i64, i64)) {
        match self.combined_after_change.as_mut() {
            Some(pending) => {
                pending.changes.push(change);
                pending.buffer_id = buffer_id;
            }
            None => {
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
/// settings, and each saved window in save_window_save's order as whether
/// it is the current window, its buffer (nil for an internal window),
/// pixel and character edges, display table, margins and its parent's
/// and previous sibling's indices among the saved windows.
type SavedWindow = (bool, [Value; 12], Option<usize>, Option<usize>);

fn saved_windows(
    snapshot: &WindowConfigurationSnapshot,
) -> (
    (i64, i64, crate::lisp::types::FrameRef, u64),
    Vec<SavedWindow>,
) {
    let windows = snapshot
        .saved_windows
        .iter()
        .map(|saved| {
            let field = |index| saved_field(saved, index);
            // A window from before the character geometry slots reads its
            // pixel geometry (primitives::window_char_geometry).
            let char_field = |index, pixel| {
                Some(field(index))
                    .filter(|value: &Value| !value.is_nil())
                    .unwrap_or_else(|| field(pixel))
            };
            let current = matches!(field(SW_WINDOW).kind(), Kind::Record(window)
                if window.id == snapshot.selected_window_id);
            (
                current,
                [
                    field(SW_BUFFER),
                    field(SW_PIXEL_LEFT),
                    field(SW_PIXEL_TOP),
                    field(SW_PIXEL_HEIGHT),
                    field(SW_PIXEL_WIDTH),
                    char_field(SW_LEFT_COL, SW_PIXEL_LEFT),
                    char_field(SW_TOP_LINE, SW_PIXEL_TOP),
                    char_field(SW_TOTAL_COLS, SW_PIXEL_WIDTH),
                    char_field(SW_TOTAL_LINES, SW_PIXEL_HEIGHT),
                    field(SW_DISPLAY_TABLE),
                    field(SW_LEFT_MARGIN_COLS),
                    field(SW_RIGHT_MARGIN_COLS),
                ],
                saved_index(saved, SW_PARENT),
                saved_index(saved, SW_PREV),
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
    // error ("No buffer named %s", SDATA (spec)).
    match spec.kind() {
        Kind::StringObject(_) => {
            primitives::values::doprnt_error_sdata(None, "No buffer named {}", &[*spec])
        }
        _ => LispError::Signal("Invalid buffer argument".into()),
    }
}

/// Fmarker_last_position of a saved window marker; an integer, from a
/// configuration saved without markers, reads as itself.
/// window.c's struct saved_window, kept as a vector of its 38 fields in
/// its order (make_nil_vector (VECSIZE (struct saved_window))): what
/// save_window_save copies from a window and Fset_window_configuration
/// writes back, the parent's and previous sibling's indices among the
/// saved windows (temslot) included.  Emaxx keeps no window state for
/// hscroll_whole, vscroll, the fringes and the scroll bars; those fields
/// hold the values GNU saves for a window that has its frame's defaults.
const SW_WINDOW: usize = 0;
const SW_BUFFER: usize = 1;
const SW_START: usize = 2;
const SW_POINTM: usize = 3;
const SW_OLD_POINTM: usize = 4;
const SW_PIXEL_LEFT: usize = 5;
const SW_PIXEL_TOP: usize = 6;
const SW_PIXEL_HEIGHT: usize = 7;
const SW_PIXEL_WIDTH: usize = 8;
const SW_LEFT_COL: usize = 9;
const SW_TOP_LINE: usize = 10;
const SW_TOTAL_COLS: usize = 11;
const SW_TOTAL_LINES: usize = 12;
const SW_NORMAL_COLS: usize = 13;
const SW_NORMAL_LINES: usize = 14;
const SW_HSCROLL: usize = 15;
const SW_MIN_HSCROLL: usize = 16;
const SW_HSCROLL_WHOLE: usize = 17;
const SW_SUSPEND_AUTO_HSCROLL: usize = 18;
const SW_VSCROLL: usize = 19;
const SW_PARENT: usize = 20;
const SW_PREV: usize = 21;
const SW_START_AT_LINE_BEG: usize = 22;
const SW_DISPLAY_TABLE: usize = 23;
const SW_LEFT_MARGIN_COLS: usize = 24;
const SW_RIGHT_MARGIN_COLS: usize = 25;
const SW_LEFT_FRINGE_WIDTH: usize = 26;
const SW_RIGHT_FRINGE_WIDTH: usize = 27;
const SW_SCROLL_BAR_WIDTH: usize = 30;
const SW_VERTICAL_SCROLL_BAR_TYPE: usize = 31;
const SW_SCROLL_BAR_HEIGHT: usize = 32;
const SW_HORIZONTAL_SCROLL_BAR_TYPE: usize = 33;
const SW_DEDICATED: usize = 35;
const SW_COMBINATION_LIMIT: usize = 36;
const SW_WINDOW_PARAMETERS: usize = 37;
const SAVED_WINDOW_SIZE: usize = 38;

/// A saved window's FIELD.
fn saved_field(saved: &Value, field: usize) -> Value {
    match saved.kind() {
        Kind::Vector(vector) => vector.get(field).unwrap_or(Value::Nil),
        _ => Value::Nil,
    }
}

/// The index a saved window's parent or previous-sibling FIELD holds.
fn saved_index(saved: &Value, field: usize) -> Option<usize> {
    saved_field(saved, field)
        .integer_value()
        .and_then(|index| usize::try_from(index).ok())
}

/// fns.c:Fassq of KEY in ALIST, by the words, as the walk over an alist
/// of symbols reads it: the entry's key and value.
fn assq_plain(key: Value, alist: Value) -> Option<(Value, Value)> {
    let mut tail = alist;
    let mut walk = crate::lisp::types::TailWalk::new(tail);
    while let Some(cell) = tail.cons_ref() {
        if let Some(entry) = cell.car.get().cons_ref()
            && entry.car.get().word() == key.word()
        {
            return Some((entry.car.get(), entry.cdr.get()));
        }
        tail = cell.cdr.get();
        if walk.cycled_no_quit(tail) {
            return None;
        }
    }
    None
}

fn marker_last_position(value: Option<Value>) -> Value {
    match value.map(|value| value.kind()) {
        Some(Kind::Marker(marker)) => Value::Integer(marker.last_position() as i64),
        Some(Kind::Integer(position)) => Value::Integer(position),
        _ => Value::Nil,
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
        interp.set_buffer_local_value_key(41, cached_symbol!("second"), Value::Integer(2));
        interp.set_buffer_local_value(41, "first", Value::Integer(10));

        assert_eq!(
            interp.buffer_local_variables(41),
            vec![
                ("first".into(), Value::Integer(10)),
                ("second".into(), Value::Integer(2)),
            ]
        );

        interp.remove_buffer_local_value(41, "first");
        assert_eq!(
            interp.buffer_local_variables(41),
            vec![("second".into(), Value::Integer(2))]
        );
        assert_eq!(
            interp.buffer_local_value(42, "first"),
            Some(Value::Integer(20))
        );

        interp.clear_buffer_local_state(42);
        assert!(interp.buffer_local_variables(42).is_empty());
    }
}
